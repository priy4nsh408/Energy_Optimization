"""Integration tests for the PostgreSQL layer (needs PostgreSQL, else skipped)."""

from __future__ import annotations

import dataclasses
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from config import SimulatorSettings
from database.db import SCHEMA_VERSION, Database, DatabaseUnavailableError
from database.models import (
    CarbonIntensityRecord,
    InvalidEventRecord,
    OptimizationActionRecord,
    SecurityAlertRecord,
    WeatherRecord,
)
from database.repositories import (
    fetch_recent_energy,
    fetch_zones,
    insert_record,
    latest_carbon_intensity,
    table_counts,
    upsert_optimization_action,
    upsert_security_alert,
)
from processors.data_validator import validate_sensor_event
from processors.energy_processor import EnergyProcessor
from processors.feature_engineering import FeatureEngineer
from simulator.engine import BuildingSimulator

pytestmark = pytest.mark.postgres

T0 = datetime(2026, 1, 14, 9, 0, tzinfo=timezone.utc)


def sensor_records(ticks=3, seed=1):
    sim = BuildingSimulator(SimulatorSettings(interval_seconds=2, speed_factor=30,
                                              random_seed=seed, start_time=T0))
    return [validate_sensor_event(e) for _ in range(ticks) for e in sim.tick()]


def count(db, table, where="TRUE"):
    with db.transaction() as conn:
        return conn.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]


def test_schema_is_idempotent_and_versioned(db):
    db.apply_schema()
    assert db.schema_version() == SCHEMA_VERSION == 4
    db.require_schema()
    with db.transaction() as conn:
        assert set(fetch_zones(conn)) == {"Z1", "Z2", "Z3", "Z4", "Z5"}
        # v3: agent sensor isolation is a valid action type
        check = conn.execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                             "WHERE conname = 'optimization_actions_action_type_check'").fetchone()[0]
        assert "isolate_sensor" in check and "restore_sensor" in check


def test_sensor_insert_is_idempotent(db):
    records = sensor_records()
    with db.transaction() as conn:
        assert all(insert_record(conn, r) for r in records)
        assert not any(insert_record(conn, r) for r in records)   # redelivery
    assert count(db, "sensor_readings") == len(records)
    with db.transaction() as conn:
        row = conn.execute("SELECT event_time, zone_id, temperature_c FROM sensor_readings "
                           "WHERE event_id = %s", (records[0].event_id,)).fetchone()
    assert row == (records[0].event_time, records[0].zone_id, records[0].temperature_c)


def test_energy_records_and_hourly_view(db):
    processor = EnergyProcessor(FeatureEngineer())
    records = [processor.process(r) for r in sensor_records(ticks=4)]
    with db.transaction() as conn:
        for record in records:
            assert insert_record(conn, record)
        hourly = conn.execute("SELECT sum(readings), sum(energy_kwh) FROM hourly_zone_energy").fetchone()
        recent = fetch_recent_energy(conn, limit_per_zone=2)
    assert hourly[0] == len(records)
    assert hourly[1] == pytest.approx(sum(r.energy_kwh for r in records))
    assert len(recent) == 10 and all(r["zone_id"] for r in recent)
    z1 = [r for r in recent if r["zone_id"] == "Z1"]
    assert z1[0]["event_time"] < z1[1]["event_time"]    # oldest first


def test_check_constraints_reject_bad_data(db):
    record = replace(sensor_records(ticks=1)[0], humidity_pct=150)   # bypassing the validator
    with pytest.raises(psycopg.errors.CheckViolation):
        with db.transaction() as conn:
            insert_record(conn, record)
    assert count(db, "sensor_readings") == 0   # transaction rolled back


def test_transaction_rolls_back_on_error(db):
    first, second = sensor_records(ticks=1)[:2]
    with pytest.raises(RuntimeError):
        with db.transaction() as conn:
            insert_record(conn, first)
            insert_record(conn, second)
            raise RuntimeError("boom")
    assert count(db, "sensor_readings") == 0


def test_weather_deduplicates_on_natural_key(db):
    base = WeatherRecord(uuid.uuid4(), "building-01", T0, "open-meteo", False, 4.5, 80)
    with db.transaction() as conn:
        assert insert_record(conn, base)
        # Same observation re-published with a new event id -> still one row.
        assert not insert_record(conn, replace(base, event_id=uuid.uuid4()))
        assert insert_record(conn, replace(base, event_id=uuid.uuid4(), is_forecast=True))
    assert count(db, "weather_records") == 2


def test_latest_carbon_intensity_ignores_forecasts(db):
    with db.transaction() as conn:
        assert latest_carbon_intensity(conn) is None
        insert_record(conn, CarbonIntensityRecord(uuid.uuid4(), "GB", T0, "api", False, 250))
        insert_record(conn, CarbonIntensityRecord(uuid.uuid4(), "GB", T0 + timedelta(hours=1),
                                                  "api", False, 180))
        insert_record(conn, CarbonIntensityRecord(uuid.uuid4(), "GB", T0 + timedelta(hours=2),
                                                  "api", True, 999))
        assert latest_carbon_intensity(conn, T0 + timedelta(minutes=30)) == 250
        assert latest_carbon_intensity(conn, T0 + timedelta(hours=3)) == 180


def test_alert_status_only_moves_forward(db):
    alert = SecurityAlertRecord(uuid.uuid4(), "Z1", None, T0, "attack", "high", "detector",
                                0.97, "replay attack suspected", {"window": 10})
    with db.transaction() as conn:
        assert upsert_security_alert(conn, alert) == "inserted"
        assert upsert_security_alert(conn, alert) == "unchanged"               # duplicate
        resolved = replace(alert, status="resolved", resolved_at=T0 + timedelta(minutes=5),
                           details={"resolution": "sensor reset"})
        assert upsert_security_alert(conn, resolved) == "updated"
        assert upsert_security_alert(conn, replace(alert, status="acknowledged")) == "unchanged"
        status, details, resolved_at = conn.execute(
            "SELECT status, details, resolved_at FROM security_alerts").fetchone()
    assert status == "resolved" and resolved_at == T0 + timedelta(minutes=5)
    assert details == {"window": 10, "resolution": "sensor reset"}


def test_action_lifecycle_with_out_of_order_messages(db):
    action = OptimizationActionRecord(uuid.uuid4(), "Z2", T0, T0, "agent", "set_setpoint",
                                      {"setpoint_c": 21.0}, "proposed", reason="pre-cooling")
    with db.transaction() as conn:
        assert upsert_optimization_action(conn, action) == "inserted"
        executed = replace(action, status="executed", updated_at=T0 + timedelta(minutes=2),
                           safety_reason="within comfort limits",
                           parameters={"setpoint_c": 21.0, "result": "applied in simulator"})
        assert upsert_optimization_action(conn, executed) == "updated"
        late = replace(action, status="approved", updated_at=T0 + timedelta(minutes=1),
                       parameters={"setpoint_c": 21.0, "result": "dispatched"})
        assert upsert_optimization_action(conn, late) == "unchanged"
        row = conn.execute("SELECT status, reason, safety_reason, updated_at, parameters "
                           "FROM optimization_actions").fetchone()
    # the execution result is kept; the late, older message changes nothing
    assert row == ("executed", "pre-cooling", "within comfort limits", T0 + timedelta(minutes=2),
                   {"setpoint_c": 21.0, "result": "applied in simulator"})


def test_invalid_events_are_idempotent_per_offset(db):
    record = InvalidEventRecord("w", "weather_data", 0, 17, "bad", None, ["event_id: invalid"],
                                {"event_id": "bad"})
    with db.transaction() as conn:
        assert insert_record(conn, record)
        assert not insert_record(conn, record)
        errors, payload = conn.execute("SELECT errors, payload FROM invalid_events").fetchone()
    assert errors == ["event_id: invalid"] and payload == {"event_id": "bad"}


def test_table_counts(db):
    with db.transaction() as conn:
        counts = table_counts(conn)
    assert counts["zones"] == 5 and counts["sensor_readings"] == 0


def test_unreachable_database_raises_transient_error(db):
    settings = dataclasses.replace(db.settings, port=1, connect_timeout=1)
    unreachable = Database(settings, name="unreachable")
    with pytest.raises(DatabaseUnavailableError):
        unreachable.open(timeout_s=1.5)
    with pytest.raises(DatabaseUnavailableError):
        with unreachable.transaction(timeout_s=1):
            pass
    assert unreachable.ping() is False
    unreachable.close()
