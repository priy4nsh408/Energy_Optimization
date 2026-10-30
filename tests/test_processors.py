"""Unit tests for validation, feature engineering and energy processing (no I/O)."""

from __future__ import annotations

import math
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from config import SimulatorSettings
from database.models import CarbonIntensityRecord, ZoneRecord
from processors.data_validator import (
    ValidationError,
    validate_action_message,
    validate_alert_message,
    validate_carbon_message,
    validate_sensor_event,
    validate_weather_message,
)
from processors.energy_processor import EnergyProcessor
from processors.feature_engineering import FeatureEngineer
from processors.sensor_processor import validate_or_reject
from simulator.engine import BuildingSimulator
from streaming.consumer import ConsumedMessage

START = datetime(2026, 1, 14, 9, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def sim_events() -> list[dict]:
    simulator = BuildingSimulator(SimulatorSettings(interval_seconds=2, speed_factor=30,
                                                    random_seed=5, start_time=START))
    return [e for _ in range(6) for e in simulator.tick()]


def z1(events):
    return [e for e in events if e["zone_id"] == "Z1"]


ZONES = {"Z1": ZoneRecord("Z1", "building-01", "Open-plan office", "office", 300, 30, 22.0)}


# --------------------------------------------------------------------------- sensor validation
def test_simulator_events_are_valid(sim_events):
    for event in sim_events:
        record = validate_sensor_event(event, kafka_partition=2, kafka_offset=10)
        assert str(record.event_id) == event["event_id"] and record.kafka_offset == 10
        assert record.event_time == datetime.fromisoformat(event["timestamp"])


@pytest.mark.parametrize(("change", "message"), [
    ({"event_id": "abc"}, "event_id"),
    ({"zone_id": "Z 1; DROP"}, "zone_id"),
    ({"temperature_c": 150}, "temperature_c"),
    ({"humidity_pct": -5}, "humidity_pct"),
    ({"occupancy_count": 2.5}, "occupancy_count"),
    ({"occupancy_count": True}, "occupancy_count"),
    ({"hvac_mode": "turbo"}, "hvac_mode"),
    ({"timestamp": "2026-01-14T09:00:00"}, "UTC offset"),
    ({"timestamp": "yesterday"}, "ISO 8601"),
    ({"co2_ppm": float("nan")}, "finite"),
    ({"total_power_kw": 99.0}, "sum of components"),
    ({"schema_version": 2}, "schema_version"),
])
def test_invalid_sensor_events(sim_events, change, message):
    with pytest.raises(ValidationError, match=message):
        validate_sensor_event({**sim_events[0], **change})


def test_all_errors_are_reported_together(sim_events):
    event = {**sim_events[0], "temperature_c": "hot", "lighting_lux": -1}
    del event["zone_id"]
    with pytest.raises(ValidationError) as info:
        validate_sensor_event(event)
    assert len(info.value.errors) == 3


def test_hvac_status_must_match_mode(sim_events):
    with pytest.raises(ValidationError, match="hvac_status"):
        validate_sensor_event({**sim_events[0], "hvac_mode": "auto", "hvac_status": "off"})


def test_non_object_message_is_invalid():
    with pytest.raises(ValidationError, match="JSON object"):
        validate_sensor_event(["not", "an", "object"])


# --------------------------------------------------------------------------- other contracts
NOW = "2026-01-14T09:00:00+00:00"


def test_weather_and_carbon_contracts():
    weather = validate_weather_message({"event_id": str(uuid.uuid4()), "building_id": "b",
                                        "observed_at": NOW, "source": "mock", "temperature_c": 5.5})
    assert weather.is_forecast is False and weather.humidity_pct is None
    carbon = validate_carbon_message({"event_id": str(uuid.uuid4()), "region": "GB",
                                      "observed_at": NOW, "source": "api",
                                      "intensity_g_per_kwh": 210, "is_forecast": True})
    assert carbon.intensity_g_per_kwh == 210 and carbon.is_forecast
    with pytest.raises(ValidationError, match="intensity_g_per_kwh"):
        validate_carbon_message({"event_id": str(uuid.uuid4()), "region": "GB",
                                 "observed_at": NOW, "source": "api", "intensity_g_per_kwh": -1})
    with pytest.raises(ValidationError, match="is_forecast"):
        validate_weather_message({"event_id": str(uuid.uuid4()), "building_id": "b",
                                  "observed_at": NOW, "source": "m", "temperature_c": 1,
                                  "is_forecast": "yes"})


def test_alert_contract():
    alert = validate_alert_message({"alert_id": str(uuid.uuid4()), "detected_at": NOW,
                                    "alert_type": "attack", "severity": "high",
                                    "detector": "isolation_forest", "description": "spoofing",
                                    "details": {"score": 0.9}})
    assert alert.status == "open" and alert.zone_id is None and alert.details == {"score": 0.9}
    with pytest.raises(ValidationError, match="severity"):
        validate_alert_message({"alert_id": str(uuid.uuid4()), "detected_at": NOW,
                                "alert_type": "attack", "severity": "extreme",
                                "detector": "x", "description": "y"})


def test_action_contract():
    action = validate_action_message({"action_id": str(uuid.uuid4()), "zone_id": "Z2",
                                      "created_at": NOW, "source": "optimizer",
                                      "action_type": "set_setpoint",
                                      "parameters": {"setpoint_c": 21}, "status": "approved"},
                                     topic="optimization_commands")
    assert action.updated_at == action.created_at and action.kafka_topic == "optimization_commands"
    with pytest.raises(ValidationError, match="parameters"):
        validate_action_message({"action_id": str(uuid.uuid4()), "zone_id": "Z2",
                                 "created_at": NOW, "source": "agent", "action_type": "set_lighting",
                                 "parameters": [1], "status": "proposed"})


def test_validate_or_reject_builds_rejection_record():
    message = ConsumedMessage(topic="weather_data", key=None, value={"event_id": "bad"},
                              partition=0, offset=42, timestamp_ms=None)
    record, rejected = validate_or_reject("worker", message, validate_weather_message)
    assert record is None and rejected.kafka_offset == 42 and rejected.event_id == "bad"
    assert any("event_id" in e for e in rejected.errors) and rejected.payload == {"event_id": "bad"}


# --------------------------------------------------------------------------- features
def readings(events):
    return [validate_sensor_event(e) for e in events]


def test_calendar_and_context_features(sim_events):
    reading = readings(z1(sim_events))[0]
    features = FeatureEngineer(ZONES).compute(reading)
    assert features["hour_of_day"] == 9 and features["day_of_week"] == 2
    assert features["is_weekend"] is False
    assert features["hour_sin"] == pytest.approx(math.sin(2 * math.pi * (9 + 1 / 60) / 24))
    assert features["setpoint_deviation_c"] == pytest.approx(reading.temperature_c - reading.hvac_setpoint_c)
    assert features["occupancy_ratio"] == pytest.approx(reading.occupancy_count / 30)
    assert features["power_density_w_m2"] == pytest.approx(reading.total_power_kw * 1000 / 300)
    assert features["power_change_kw"] is None and features["rolling_window_size"] == 1


def test_local_time_features_use_event_offset(sim_events):
    reading = readings(z1(sim_events))[0]
    shifted = replace(reading, event_time=reading.event_time.astimezone(timezone(timedelta(hours=5, minutes=30))))
    assert FeatureEngineer().compute(shifted)["hour_of_day"] == 14


def test_lag_and_rolling_features(sim_events):
    zone_readings = readings(z1(sim_events))
    engineer = FeatureEngineer(window=3)
    results = [engineer.compute(r) for r in zone_readings]
    last, prev = zone_readings[-1], zone_readings[-2]
    assert results[-1]["power_change_kw"] == pytest.approx(last.total_power_kw - prev.total_power_kw)
    hours = (last.event_time - prev.event_time).total_seconds() / 3600
    assert results[-1]["temperature_rate_c_per_h"] == pytest.approx(
        (last.temperature_c - prev.temperature_c) / hours)
    window = [r.total_power_kw for r in zone_readings[-3:]]
    assert results[-1]["rolling_mean_power_kw"] == pytest.approx(sum(window) / 3)
    assert results[-1]["rolling_max_power_kw"] == max(window)
    assert results[-1]["rolling_window_size"] == 3


def test_duplicates_do_not_change_state(sim_events):
    zone_readings = readings(z1(sim_events))
    engineer = FeatureEngineer()
    for r in zone_readings[:3]:
        engineer.compute(r)
    again = engineer.compute(zone_readings[1])
    assert engineer.history_size("Z1") == 3 and again["power_change_kw"] is None
    after = engineer.compute(zone_readings[3])
    assert after["power_change_kw"] == pytest.approx(
        zone_readings[3].total_power_kw - zone_readings[2].total_power_kw)


def test_time_going_backwards_starts_new_window(sim_events):
    zone_readings = readings(z1(sim_events))
    engineer = FeatureEngineer()
    for r in zone_readings:
        engineer.compute(r)
    restarted = replace(zone_readings[0], event_id=uuid.uuid4())   # new session, earlier time
    features = engineer.compute(restarted)
    assert features["rolling_window_size"] == 1 and engineer.history_size("Z1") == 1
    follow_up = replace(zone_readings[1], event_id=uuid.uuid4())
    assert engineer.compute(follow_up)["power_change_kw"] is not None


def test_long_gap_resets_history(sim_events):
    first, second = readings(z1(sim_events))[:2]
    engineer = FeatureEngineer(max_gap_s=600)
    engineer.compute(first)
    late = replace(second, event_time=first.event_time + timedelta(hours=2))
    assert engineer.compute(late)["power_change_kw"] is None
    assert engineer.history_size("Z1") == 1


def test_unknown_zone_features_are_null(sim_events):
    features = FeatureEngineer({}).compute(readings(z1(sim_events))[0])
    assert features["occupancy_ratio"] is None and features["power_density_w_m2"] is None


def test_warm_start_restores_history(sim_events):
    zone_readings = readings(z1(sim_events))
    rows = [{"event_id": r.event_id, "zone_id": r.zone_id, "event_time": r.event_time,
             "total_power_kw": r.total_power_kw, "temperature_c": r.temperature_c}
            for r in zone_readings[:-1]]
    engineer = FeatureEngineer()
    assert engineer.warm_start(rows) == len(rows)
    features = engineer.compute(zone_readings[-1])
    assert features["power_change_kw"] is not None
    assert features["rolling_window_size"] == len(zone_readings)


# --------------------------------------------------------------------------- energy processor
def carbon(intensity, *, minutes=0, forecast=False):
    return CarbonIntensityRecord(uuid.uuid4(), "GB", START + timedelta(minutes=minutes), "test",
                                 forecast, intensity)


def test_energy_split_and_carbon(sim_events):
    processor = EnergyProcessor(FeatureEngineer(ZONES))
    reading = readings(sim_events)[0]
    without = processor.process(reading)
    assert without.carbon_intensity_g_per_kwh is None and without.carbon_kg_co2e is None
    assert (without.hvac_energy_kwh + without.lighting_energy_kwh + without.appliance_energy_kwh
            == pytest.approx(reading.energy_kwh))
    assert without.hvac_energy_kwh == pytest.approx(
        reading.energy_kwh * reading.hvac_power_kw / reading.total_power_kw)

    assert processor.update_carbon_intensity(carbon(300.0))
    with_carbon = processor.process(readings(sim_events)[5])
    assert with_carbon.carbon_kg_co2e == pytest.approx(with_carbon.energy_kwh * 0.3)


def test_carbon_updates_ignore_forecasts_and_older_values():
    processor = EnergyProcessor(FeatureEngineer())
    assert processor.update_carbon_intensity(carbon(200, minutes=10))
    assert not processor.update_carbon_intensity(carbon(999, minutes=20, forecast=True))
    assert not processor.update_carbon_intensity(carbon(100, minutes=5))
    assert processor.carbon_intensity_g_per_kwh == 200


def test_zero_power_reading(sim_events):
    reading = replace(readings(sim_events)[0], hvac_power_kw=0, lighting_power_kw=0,
                      appliance_power_kw=0, total_power_kw=0, energy_kwh=0)
    record = EnergyProcessor(FeatureEngineer()).process(reading)
    assert record.hvac_energy_kwh == 0 and record.hvac_share is None
