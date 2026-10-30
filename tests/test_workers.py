"""Tests for the persistence workers (PostgreSQL required; full flow also needs Kafka)."""

from __future__ import annotations

import dataclasses
import logging
import threading
import uuid
from datetime import datetime, timezone

import pytest

from config import SimulatorSettings
from database.db import Database
from simulator.engine import BuildingSimulator
from streaming.consumer import ConsumedMessage, TransientProcessingError
from workers.energy_processor import EnergyProcessorHandler
from workers.record_persistence import RecordPersistenceHandler
from workers.sensor_persistence import SensorPersistenceHandler

pytestmark = pytest.mark.postgres

T0 = datetime(2026, 1, 14, 9, 0, tzinfo=timezone.utc)
LOGGER = logging.getLogger("tests.workers")


def sim_events(ticks=2, seed=3):
    sim = BuildingSimulator(SimulatorSettings(interval_seconds=2, speed_factor=30,
                                              random_seed=seed, start_time=T0))
    return [e for _ in range(ticks) for e in sim.tick()]


def message(value, topic="building_sensor_data", offset=0):
    return ConsumedMessage(topic=topic, key=value.get("zone_id"), value=value, partition=0,
                           offset=offset, timestamp_ms=None)


def count(db, table):
    with db.transaction() as conn:
        return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def test_sensor_handler_stores_dedupes_and_rejects(db):
    handler = SensorPersistenceHandler(db, LOGGER)
    events = sim_events()
    for i, event in enumerate(events):
        handler(message(event, offset=i))
    handler(message(events[0], offset=0))                          # redelivery
    handler(message({**events[1], "temperature_c": 500}, offset=99))  # invalid
    assert handler.stats["stored"] == len(events)
    assert handler.stats["duplicates"] == 1 and handler.stats["rejected"] == 1
    assert count(db, "sensor_readings") == len(events)
    with db.transaction() as conn:
        row = conn.execute("SELECT worker, kafka_offset, errors FROM invalid_events").fetchone()
    assert row[0] == "sensor_persistence" and row[1] == 99 and "temperature_c" in row[2][0]


def test_energy_handler_adds_features_and_carbon(db):
    handler = EnergyProcessorHandler.create(db, LOGGER)
    handler(message({"event_id": str(uuid.uuid4()), "region": "GB", "source": "test",
                     "observed_at": T0.isoformat(), "intensity_g_per_kwh": 200.0},
                    topic="carbon_intensity"))
    events = sim_events(ticks=3)
    for i, event in enumerate(events):
        handler(message(event, offset=i))
    assert handler.stats["stored"] == len(events) and handler.stats["carbon_updates"] == 1
    with db.transaction() as conn:
        rows = conn.execute("SELECT carbon_kg_co2e, energy_kwh, occupancy_ratio, power_change_kw "
                            "FROM energy_records WHERE zone_id = 'Z1' ORDER BY event_time").fetchall()
    assert all(r[0] == pytest.approx(r[1] * 0.2) for r in rows)
    assert all(r[2] is not None for r in rows)          # zone registry used
    assert rows[0][3] is None and rows[-1][3] is not None

    # A restarted worker warm-starts its feature history from the database.
    restarted = EnergyProcessorHandler.create(db, LOGGER)
    assert restarted.processor.features.history_size("Z1") == 3
    assert restarted.processor.carbon_intensity_g_per_kwh is None  # no carbon rows stored by this worker


def test_record_handler_dispatches_by_topic(db):
    handler = RecordPersistenceHandler(db, LOGGER)
    now = T0.isoformat()
    action = {"action_id": str(uuid.uuid4()), "zone_id": "Z1", "created_at": now,
              "source": "optimizer", "action_type": "set_lighting",
              "parameters": {"lighting_level": 0.6}, "status": "approved"}
    handler(message({"event_id": str(uuid.uuid4()), "building_id": "b", "observed_at": now,
                     "source": "mock", "temperature_c": 3.0}, topic="weather_data"))
    handler(message({"event_id": str(uuid.uuid4()), "region": "GB", "observed_at": now,
                     "source": "mock", "intensity_g_per_kwh": 150}, topic="carbon_intensity"))
    handler(message({"alert_id": str(uuid.uuid4()), "zone_id": "Z3", "detected_at": now,
                     "alert_type": "drift", "severity": "low", "detector": "ks_test",
                     "description": "temperature drift"}, topic="security_alerts"))
    handler(message(action, topic="optimization_commands"))
    handler(message({**action, "status": "executed"}, topic="agent_actions"))
    handler(message({"alert_id": "x"}, topic="security_alerts", offset=5))
    assert handler.stats["weather_data.inserted"] == 1
    assert handler.stats["carbon_intensity.inserted"] == 1
    assert handler.stats["security_alerts.inserted"] == 1
    assert handler.stats["optimization_commands.inserted"] == 1
    assert handler.stats["agent_actions.updated"] == 1
    assert handler.stats["rejected"] == 1
    with db.transaction() as conn:
        assert conn.execute("SELECT status, kafka_topic FROM optimization_actions").fetchone() == (
            "executed", "agent_actions")


def test_database_outage_raises_transient_error(db):
    unreachable = Database(dataclasses.replace(db.settings, port=1, connect_timeout=1), name="down")
    unreachable.pool.open(wait=False)
    handler = SensorPersistenceHandler(unreachable, LOGGER)
    with pytest.raises(TransientProcessingError):
        handler(message(sim_events(ticks=1)[0]))
    assert handler.stats["db_unavailable"] == 1
    unreachable.close()


@pytest.mark.kafka
def test_full_flow_kafka_to_postgres(db, fresh_kafka, clean_env):
    """Simulator -> Kafka -> three independent workers -> PostgreSQL."""
    clean_env.setenv("APP_ENV", "testing")
    clean_env.setenv("KAFKA_TOPIC_PREFIX", fresh_kafka.topic_prefix)
    clean_env.setenv("KAFKA_CONSUMER_GROUP_PREFIX", f"{fresh_kafka.consumer_group_prefix}_flow")
    clean_env.setenv("POSTGRES_PASSWORD", db.settings.password)
    from workers import energy_processor, record_persistence, sensor_persistence, sensor_publisher
    from streaming.producer import JsonProducer

    assert sensor_publisher.main(["--fast", "--ticks", "4", "--seed", "8", "--broker-wait", "5"],
                                 stop_event=threading.Event()) == 0
    with JsonProducer(fresh_kafka, name="flow") as producer:
        producer.send("carbon_intensity", {"event_id": str(uuid.uuid4()), "region": "GB",
                                           "observed_at": T0.isoformat(), "source": "test",
                                           "intensity_g_per_kwh": 220.0}, key="building-01")

    results = {}

    def run(name, module):
        results[name] = module.main(["--idle-timeout", "6", "--broker-wait", "5", "--db-wait", "5"],
                                    stop_event=threading.Event())

    threads = [threading.Thread(target=run, args=(n, m)) for n, m in (
        ("sensor", sensor_persistence), ("energy", energy_processor), ("records", record_persistence))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    assert results == {"sensor": 0, "energy": 0, "records": 0}
    assert count(db, "sensor_readings") == 20
    assert count(db, "energy_records") == 20
    assert count(db, "carbon_intensity_records") == 1
    with db.transaction() as conn:
        missing = conn.execute("SELECT count(*) FROM sensor_readings s LEFT JOIN energy_records e "
                               "USING (event_id) WHERE e.event_id IS NULL").fetchone()[0]
    assert missing == 0
