"""Tests for the simulation engine, sensors, events, settings and CLI."""

from __future__ import annotations

import io
import json
import random
import statistics
import threading
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from config import ConfigurationError, SimulatorSettings, get_config
from simulator import __main__ as cli
from simulator.engine import BuildingSimulator, json_lines_sink
from simulator.sensor_simulator import SENSORS, SensorModel, SensorSpec

START = datetime(2026, 1, 14, 9, 0, tzinfo=timezone.utc)  # Wednesday morning
REQUIRED_FIELDS = {
    "event_id", "event_type", "schema_version", "source", "building_id", "zone_id", "zone_name",
    "zone_type", "sequence", "timestamp", "emitted_at", "interval_seconds",
    "temperature_c", "humidity_pct", "co2_ppm", "occupancy_count", "lighting_lux",
    "lighting_level_pct", "hvac_mode", "hvac_status", "hvac_setpoint_c", "hvac_power_kw",
    "lighting_power_kw", "appliance_power_kw", "total_power_kw", "energy_kwh",
    "outdoor_temperature_c", "outdoor_humidity_pct",
}


def make_settings(**overrides) -> SimulatorSettings:
    values = {"interval_seconds": 2.0, "speed_factor": 30.0, "random_seed": 42, "start_time": START}
    values.update(overrides)
    return SimulatorSettings(**values)


def strip_wall_clock(events):
    return [{k: v for k, v in e.items() if k != "emitted_at"} for e in events]


# --------------------------------------------------------------------------- events
def test_tick_emits_one_complete_event_per_zone():
    events = BuildingSimulator(make_settings()).tick()
    assert [e["zone_id"] for e in events] == ["Z1", "Z2", "Z3", "Z4", "Z5"]
    for event in events:
        assert REQUIRED_FIELDS <= event.keys()
        assert event["event_type"] == "sensor_reading" and event["schema_version"] == 1
        assert event["hvac_status"] in ("heating", "cooling", "idle", "off")
        json.loads(json.dumps(event))  # JSON-serialisable
        datetime.fromisoformat(event["timestamp"])
        datetime.fromisoformat(event["emitted_at"])
    assert len({e["event_id"] for e in events}) == 5
    # One rooftop weather station: identical outdoor readings for all zones.
    assert len({e["outdoor_temperature_c"] for e in events}) == 1


def test_event_ids_are_reproducible_but_differ_between_start_times():
    """A live run must not reuse the ids of stored history (the duplicate check would drop its readings)."""
    def ids(start):
        sim = BuildingSimulator(make_settings(random_seed=2026, start_time=start))
        return [e["event_id"] for _ in range(3) for e in sim.tick()]

    history, live = datetime(2026, 1, 5, tzinfo=timezone.utc), datetime(2026, 2, 9, tzinfo=timezone.utc)
    assert ids(live) == ids(live)
    assert not set(ids(history)) & set(ids(live))


def test_timestamps_follow_speed_factor_and_sequences_increment():
    sim = BuildingSimulator(make_settings(interval_seconds=2, speed_factor=60))
    first, second = sim.tick(), sim.tick()
    assert first[0]["timestamp"] == "2026-01-14T09:02:00+00:00"  # start + one interval
    t1, t2 = (datetime.fromisoformat(e[0]["timestamp"]) for e in (first, second))
    assert t2 - t1 == timedelta(seconds=120)
    assert first[0]["interval_seconds"] == 120
    assert [e["sequence"] for e in second] == [2] * 5


def test_energy_matches_power_over_interval():
    sim = BuildingSimulator(make_settings(speed_factor=1))  # 2 s interval -> single physics step
    for event in sim.tick():
        expected = event["total_power_kw"] * event["interval_seconds"] / 3600
        assert event["energy_kwh"] == pytest.approx(expected, rel=0.05, abs=1e-5)


def test_same_seed_is_reproducible_and_different_seed_differs():
    run = lambda seed: strip_wall_clock(
        [e for _ in range(5) for e in BuildingSimulator(make_settings(random_seed=seed)).tick()])
    assert run(1) == run(1)
    assert run(1) != run(2)


def test_readings_are_temporally_correlated_not_random():
    sim = BuildingSimulator(make_settings(speed_factor=30))
    temps = [sim.tick()[0]["temperature_c"] for _ in range(60)]
    steps = [abs(b - a) for a, b in zip(temps, temps[1:])]
    assert max(steps) < 0.6  # physical inertia: no jumps between 1-minute readings


def test_simulated_day_is_plausible():
    sim = BuildingSimulator(make_settings(interval_seconds=5, speed_factor=360,
                                          start_time=START.replace(hour=0)))
    events = [e for _ in range(48) for e in sim.tick()]  # 30 min per tick, 24 h
    by_hour = lambda h: [e for e in events if datetime.fromisoformat(e["timestamp"]).hour == h]
    office_working = [e for e in events if e["zone_id"] == "Z1"
                      and 10 <= datetime.fromisoformat(e["timestamp"]).hour < 17]
    assert all(20 <= e["temperature_c"] <= 24.5 for e in office_working)
    assert max(e["occupancy_count"] for e in office_working) > 15
    assert all(e["occupancy_count"] == 0 for e in by_hour(3))
    assert all(380 <= e["co2_ppm"] <= 2000 for e in events)
    assert all(0 <= e["humidity_pct"] <= 100 for e in events)
    noon_power = sum(e["total_power_kw"] for e in by_hour(12))
    night_power = sum(e["total_power_kw"] for e in by_hour(3))
    assert noon_power > 1.5 * night_power


def test_commands_change_emitted_state():
    sim = BuildingSimulator(make_settings())
    sim.apply_command("Z1", hvac_mode="off", lighting_level=0.0)
    z1 = sim.tick()[0]
    assert z1["hvac_status"] == "off" and z1["hvac_power_kw"] == 0
    assert z1["lighting_level_pct"] == 0 and z1["lighting_power_kw"] == 0


# --------------------------------------------------------------------------- run loop
def test_run_fast_delivers_all_events():
    received = []
    executed = BuildingSimulator(make_settings()).run(received.append, max_ticks=4, realtime=False)
    assert executed == 4 and len(received) == 20


def test_run_paces_ticks_in_real_time():
    started = time.monotonic()
    BuildingSimulator(make_settings(interval_seconds=1)).run(lambda e: None, max_ticks=2)
    assert 1.5 <= time.monotonic() - started < 4


def test_stop_event_stops_run():
    stop = threading.Event()
    received = []

    def sink(event):
        received.append(event)
        stop.set()

    executed = BuildingSimulator(make_settings(interval_seconds=5)).run(sink, stop_event=stop)
    assert executed == 1 and len(received) == 5


def test_failing_sink_does_not_stop_simulation():
    def sink(event):
        raise ConnectionError("broker down")

    sim = BuildingSimulator(make_settings())
    assert sim.run(sink, max_ticks=3, realtime=False) == 3
    assert sim.sink_failures == 15


def test_json_lines_sink():
    buffer = io.StringIO()
    BuildingSimulator(make_settings()).run(json_lines_sink(buffer), max_ticks=2, realtime=False)
    lines = buffer.getvalue().splitlines()
    assert len(lines) == 10 and all(json.loads(line)["zone_id"] for line in lines)


# --------------------------------------------------------------------------- sensors
def test_sensor_noise_resolution_and_range():
    rng = random.Random(0)
    temperature = SENSORS["temperature_c"]
    samples = [temperature.measure(21.0, rng) for _ in range(2000)]
    assert statistics.mean(samples) == pytest.approx(21.0, abs=0.02)
    assert statistics.stdev(samples) == pytest.approx(0.1, rel=0.2)
    assert all(round(s * 10) == pytest.approx(s * 10) for s in samples)
    spec = SensorSpec(noise_std=5, resolution=1, minimum=0, maximum=100)
    assert all(0 <= spec.measure(99, rng) <= 100 for _ in range(200))


def test_occupancy_sensor_miscounts_by_at_most_one():
    model = SensorModel(random.Random(0))
    readings = [model._occupancy(5, 10) for _ in range(2000)]
    assert set(readings) <= {4, 5, 6}
    assert 0.02 < sum(r != 5 for r in readings) / len(readings) < 0.09
    assert all(model._occupancy(0, 10) == 0 for _ in range(100))


# --------------------------------------------------------------------------- settings
@pytest.mark.parametrize("overrides", [
    {"interval_seconds": 0.5}, {"interval_seconds": 6}, {"speed_factor": 0},
    {"speed_factor": 5000}, {"latitude_deg": 95}, {"building_id": ""},
])
def test_invalid_simulator_settings(overrides):
    with pytest.raises(ConfigurationError):
        make_settings(**overrides)


def test_simulator_settings_from_env(clean_env):
    clean_env.setenv("SIM_INTERVAL_SECONDS", "3")
    clean_env.setenv("SIM_SPEED_FACTOR", "120")
    clean_env.setenv("SIM_RANDOM_SEED", "7")
    clean_env.setenv("SIM_UTC_OFFSET_HOURS", "5.5")
    clean_env.setenv("SIM_START_TIME", "2026-03-01T08:00")
    sim = get_config("testing", load_dotenv_file=False).SIMULATOR
    assert sim.interval_seconds == 3 and sim.speed_factor == 120 and sim.random_seed == 7
    assert sim.simulated_seconds_per_tick == 360
    assert sim.start_time.utcoffset() == timedelta(hours=5, minutes=30)


@pytest.mark.parametrize(("name", "value"), [
    ("SIM_INTERVAL_SECONDS", "fast"), ("SIM_INTERVAL_SECONDS", "10"), ("SIM_START_TIME", "yesterday"),
])
def test_invalid_simulator_env(clean_env, name, value):
    clean_env.setenv(name, value)
    with pytest.raises(ConfigurationError):
        get_config("testing", load_dotenv_file=False)


def test_local_time_zone_is_used_for_schedules():
    # 09:00 at UTC+5:30 is a working hour locally, so the office should be lit.
    tz = timezone(timedelta(hours=5, minutes=30))
    settings = make_settings(utc_offset_hours=5.5, start_time=datetime(2026, 1, 14, 9, tzinfo=tz))
    event = BuildingSimulator(settings).tick()[0]
    assert event["timestamp"].endswith("+05:30")
    assert event["lighting_level_pct"] == 100


# --------------------------------------------------------------------------- CLI
def test_cli_writes_json_lines_file(clean_env, tmp_path):
    clean_env.setenv("APP_ENV", "testing")
    output = tmp_path / "out" / "events.jsonl"
    code = cli.main(["--interval", "1", "--speed", "60", "--ticks", "3", "--seed", "1",
                     "--start", "2026-01-14T09:00", "--fast", "--output", str(output)])
    assert code == 0
    events = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert len(events) == 15 and events[0]["timestamp"] == "2026-01-14T09:01:00+00:00"


def test_cli_rejects_invalid_interval(clean_env, capsys):
    clean_env.setenv("APP_ENV", "testing")
    assert cli.main(["--interval", "10", "--ticks", "1", "--fast"]) == 2
    assert "interval_seconds" in capsys.readouterr().err


def test_settings_are_immutable_copies():
    base = make_settings()
    changed = replace(base, speed_factor=10)
    assert base.speed_factor == 30 and changed.speed_factor == 10
