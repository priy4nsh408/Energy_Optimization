"""Tests for the deterministic HVAC/lighting optimizer, its constraints,
the closed-loop experiment and the Kafka optimization service."""

from __future__ import annotations

import logging
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from config import ConfigurationError, OptimizationSettings, SimulatorSettings
from experiments.scenarios import compare, make_optimizer, run_scenario
from optimization.constraints import comfort_band, setpoint_band, validate_decision
from optimization.hvac_optimizer import ZoneConditions, building_models
from optimization.lighting_optimizer import (
    LIGHTING_TARGET_MARGIN,
    illuminance,
    is_lighting_short,
    lighting_requirement,
)
from optimization.optimizer import DecisionScheduler, ZoneOptimizer
from processors.data_validator import validate_action_message
from simulator.engine import BuildingSimulator
from streaming.consumer import ConsumedMessage

SETTINGS = OptimizationSettings()
MODELS = building_models()
WINTER_NOON = datetime(2026, 1, 14, 11, 0, tzinfo=timezone.utc)   # Wednesday


def conditions(zone="Z1", *, when=WINTER_NOON, occupancy=20, temperature=22.0, outdoor=5.0,
               setpoint=22.0, lighting=1.0, lux=800.0, appliance=3.0, total=9.0, mode="auto"):
    return ZoneConditions(zone, when, occupancy, temperature, outdoor, setpoint, lighting, lux,
                          appliance, 2.0, total, mode, 300.0)


@pytest.fixture
def optimizer():
    return ZoneOptimizer(SETTINGS, MODELS)


# --------------------------------------------------------------------------- settings
def test_optimization_settings_validate_limits():
    with pytest.raises(ConfigurationError):
        OptimizationSettings(comfort_min_c=26, comfort_max_c=25)
    with pytest.raises(ConfigurationError):
        OptimizationSettings(setpoint_min_c=18, setback_min_c=17)
    with pytest.raises(ConfigurationError):
        OptimizationSettings(comfort_weight=-1)


def test_optimization_settings_from_env(clean_env):
    from config import get_config

    clean_env.setenv("OPT_COMFORT_MIN_C", "21")
    clean_env.setenv("OPT_CARBON_WEIGHT_PER_KG", "0.5")
    settings = get_config("testing", load_dotenv_file=False).OPTIMIZATION
    assert settings.comfort_min_c == 21 and settings.carbon_weight_per_kg == 0.5


# --------------------------------------------------------------------------- energy model
def test_hvac_model_heating_and_cooling_directions():
    model = MODELS["Z1"]
    cold = conditions(outdoor=0.0, occupancy=0, lux=0, lighting=0.0, appliance=1.0)
    assert model.hvac_power_kw(23, 0.0, cold) > model.hvac_power_kw(21, 0.0, cold)  # heating
    hot = conditions(outdoor=33.0, lux=1500)
    assert model.hvac_power_kw(21, 1.0, hot) > model.hvac_power_kw(24, 1.0, hot)    # cooling
    # Lighting heat reduces heating demand and increases cooling demand.
    assert model.hvac_power_kw(22, 1.0, cold) < model.hvac_power_kw(22, 0.0, cold)
    assert model.hvac_power_kw(22, 1.0, hot) > model.hvac_power_kw(22, 0.0, hot)


def test_server_room_has_no_heating():
    model = MODELS["Z5"]
    cold = conditions("Z5", outdoor=-10.0, occupancy=0, appliance=0.0, lighting=0.0, lux=0)
    assert model.hvac_power_kw(24, 0.0, cold) < 0.05


# --------------------------------------------------------------------------- constraints
def test_bands_depend_on_occupancy_schedule_and_zone():
    occupied = comfort_band(SETTINGS, MODELS["Z1"], conditions())
    assert occupied[:2] == (20.5, 24.5) and occupied[2] == "occupied comfort band"
    standby = comfort_band(SETTINGS, MODELS["Z1"], conditions(occupancy=0))
    assert standby[:2] == (19.0, 26.0) and standby[2] == "scheduled standby band"
    preconditioning = comfort_band(SETTINGS, MODELS["Z1"], conditions(
        occupancy=0, when=WINTER_NOON.replace(hour=6, minute=30)))
    assert preconditioning[2] == "scheduled standby band"
    night = comfort_band(SETTINGS, MODELS["Z1"], conditions(occupancy=0, when=WINTER_NOON.replace(hour=2)))
    assert night[:2] == (17.0, 27.0)
    assert comfort_band(SETTINGS, MODELS["Z5"], conditions("Z5", occupancy=0))[:2] == (19.0, 23.0)


def test_step_limit_allows_recovery_into_band():
    band = setpoint_band(SETTINGS, MODELS["Z1"], conditions(occupancy=0, setpoint=22.0,
                                                           when=WINTER_NOON.replace(hour=2)))
    assert band.low == 20.5 and band.high == 23.5
    recovering = setpoint_band(SETTINGS, MODELS["Z1"], conditions(setpoint=17.0))
    assert recovering.low == 20.5            # jump straight into the comfort band


def test_lighting_requirements():
    model = MODELS["Z1"]
    dark = lighting_requirement(model, conditions(lighting=1.0, lux=500))      # no daylight
    assert not dark.achievable and dark.minimum_level == 1.0
    sunny = lighting_requirement(model, conditions(lighting=0.0, lux=400))
    assert sunny.lux_level == pytest.approx((500 * (1 + LIGHTING_TARGET_MARGIN) - 400) / 500)
    lobby_night = lighting_requirement(MODELS["Z4"], conditions("Z4", occupancy=0, lighting=0.2, lux=60))
    assert lobby_night.minimum_level == 0.2                                    # egress lighting
    assert is_lighting_short(model, conditions(lighting=0.3, lux=400))
    assert not is_lighting_short(model, conditions(lighting=1.0, lux=400))     # already at maximum


def test_validator_detects_violations():
    c = conditions(lighting=0.5, lux=700)
    assert validate_decision(SETTINGS, MODELS["Z1"], c, 22.0, 0.5) == []
    assert any("absolute" in v for v in validate_decision(SETTINGS, MODELS["Z1"], c, 30.0, 0.5))
    assert any("comfort band" in v for v in validate_decision(SETTINGS, MODELS["Z1"], c, 19.0, 0.5))
    assert any("illuminance" in v for v in validate_decision(SETTINGS, MODELS["Z1"], c, 22.0, 0.0))
    lobby = conditions("Z4", occupancy=0, lighting=0.2, lux=60, when=WINTER_NOON.replace(hour=2))
    assert any("safety level" in v for v in validate_decision(SETTINGS, MODELS["Z4"], lobby, 20.0, 0.0))


# --------------------------------------------------------------------------- optimizer
def test_optimizer_is_deterministic(optimizer):
    c = conditions()
    assert optimizer.optimize(c, carbon_intensity=300) == optimizer.optimize(c, carbon_intensity=300)


def test_empty_zone_switches_lights_off_and_sets_back(optimizer):
    decision = optimizer.optimize(conditions(occupancy=0, when=WINTER_NOON.replace(hour=20),
                                             temperature=22.0, lux=500))
    assert decision.approved and decision.lighting_level == 0.0
    assert decision.setpoint_c == pytest.approx(21.5)          # >= T - deadband (no forced cooling)
    assert decision.predicted_power_optimized_kw < decision.predicted_power_baseline_kw


def test_occupied_zone_uses_daylight_and_respects_comfort(optimizer):
    c = conditions(lighting=1.0, lux=900)                      # 400 lux daylight
    decision = optimizer.optimize(c)
    assert decision.approved
    assert illuminance(MODELS["Z1"], c, decision.lighting_level) >= 500 * (1 + LIGHTING_TARGET_MARGIN) - 1
    assert decision.lighting_level < 1.0
    assert 20.5 <= decision.setpoint_c <= 24.5


def test_heating_season_lowers_setpoint_within_comfort(optimizer):
    c = conditions(outdoor=-2.0, occupancy=10, temperature=22.0, lux=500, appliance=1.5)
    decision = optimizer.optimize(c)
    assert 20.9 <= decision.setpoint_c < 22.0                  # edge of the neutral comfort zone


def test_cooling_season_raises_setpoint(optimizer):
    c = conditions(outdoor=33.0, temperature=22.5, lux=1500, when=WINTER_NOON.replace(month=7))
    decision = optimizer.optimize(c)
    assert 22.5 < decision.setpoint_c <= 23.1


def test_no_forced_cooling_in_heating_season(optimizer):
    c = conditions(occupancy=0, when=WINTER_NOON.replace(hour=2), temperature=21.0, outdoor=0.0,
                   setpoint=22.0, lighting=0.0, lux=0)
    assert optimizer.optimize(c).setpoint_c >= 20.5


def test_hvac_off_keeps_setpoint(optimizer):
    decision = optimizer.optimize(conditions(mode="off", setpoint=22.0))
    assert decision.setpoint_c == 22.0


def test_carbon_intensity_increases_cost(optimizer):
    c = conditions(occupancy=0, when=WINTER_NOON.replace(hour=20), lux=500)
    low = optimizer.optimize(c, carbon_intensity=100)
    high = optimizer.optimize(c, carbon_intensity=800)
    assert high.objective_baseline > low.objective_baseline


def test_forecast_basis(optimizer):
    c = conditions()
    assert optimizer.optimize(c).basis == "measured"
    ml = optimizer.optimize(c, predicted_power_kw=12.0)
    assert ml.basis == "ml" and ml.predicted_power_baseline_kw == pytest.approx(12.0)


def test_decision_scheduler():
    scheduler = DecisionScheduler(900)
    t = WINTER_NOON
    assert scheduler.due("Z1", t, 5)
    assert not scheduler.due("Z1", t + timedelta(minutes=5), 5)
    assert scheduler.due("Z1", t + timedelta(minutes=10), 0)          # became empty
    assert scheduler.due("Z1", t + timedelta(minutes=12), 3)          # became occupied again
    assert scheduler.due("Z1", t + timedelta(minutes=13), 3, lighting_short=True)   # too dark
    assert not scheduler.due("Z1", t + timedelta(minutes=14), 3)
    assert scheduler.due("Z1", t + timedelta(minutes=40), 3)          # interval elapsed
    assert scheduler.due("Z1", t, 3)                                   # time went backwards


# --------------------------------------------------------------------------- closed loop
def test_closed_loop_saves_energy_without_breaking_limits():
    sim = SimulatorSettings(interval_seconds=1, speed_factor=300, random_seed=5,
                            start_time=datetime(2026, 1, 14, tzinfo=timezone.utc))
    baseline = run_scenario("baseline", sim, 1)
    optimized = run_scenario("optimized", sim, 1, optimizer=make_optimizer(SETTINGS))
    assert optimized.decisions > 0 and optimized.decisions_rejected == 0
    assert compare(baseline, optimized)["energy_saving_pct"] > 0
    assert optimized.server_max_temperature_c <= 24.0
    assert baseline.occupied_readings == optimized.occupied_readings   # same people in both runs


def test_baseline_scenario_is_reproducible():
    sim = SimulatorSettings(interval_seconds=1, speed_factor=300, random_seed=6,
                            start_time=datetime(2026, 7, 14, tzinfo=timezone.utc))
    assert run_scenario("a", sim, 0.25).energy_kwh == run_scenario("b", sim, 0.25).energy_kwh


# --------------------------------------------------------------------------- Kafka service
class RecordingProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, value, key=None):
        self.sent.append((topic, key, value))


def stream(ticks, seed=7, start=datetime(2026, 1, 14, 20, tzinfo=timezone.utc)):
    sim = BuildingSimulator(SimulatorSettings(interval_seconds=1, speed_factor=300,
                                              random_seed=seed, start_time=start))
    return [e for _ in range(ticks) for e in sim.tick()]


def msg(topic, value, offset=0):
    return ConsumedMessage(topic, value.get("zone_id"), value, 0, offset, None)


def test_optimization_service_publishes_valid_commands():
    from workers.optimization_service import OptimizationHandler

    producer = RecordingProducer()
    handler = OptimizationHandler(SETTINGS, producer, logging.getLogger("test"))
    handler(msg("carbon_intensity", {"event_id": str(uuid.uuid4()), "region": "mock", "source": "mock",
                                     "observed_at": "2026-01-14T19:00:00+00:00",
                                     "intensity_g_per_kwh": 350.0}))
    for i, event in enumerate(stream(6, start=datetime(2026, 1, 14, 18, tzinfo=timezone.utc))):
        handler(msg("building_sensor_data", event, i))
    assert handler.carbon_intensity == 350.0 and handler.stats["decisions"] >= 5
    assert producer.sent, "evening setback should produce commands"
    for topic, key, value in producer.sent:
        record = validate_action_message(value, topic=topic)
        assert topic == "optimization_commands" and key == record.zone_id
        assert record.status == "approved" and record.source == "optimizer"
        assert value["parameters"]["forecast_basis"] == "measured"
    lighting = [v["parameters"] for _, _, v in producer.sent if v["action_type"] == "set_lighting"]
    assert any(p["lighting_level"] < p["previous_lighting_level"] for p in lighting)
    assert value["parameters"]["carbon_intensity_g_per_kwh"] == 350.0


def test_optimization_service_waits_for_ml_prediction():
    from workers.optimization_service import OptimizationHandler

    producer = RecordingProducer()
    handler = OptimizationHandler(SETTINGS, producer, logging.getLogger("test"))
    handler._predictions_seen = True                      # a prediction service is running
    event = [e for e in stream(1) if e["zone_id"] == "Z1"][0]
    handler(msg("building_sensor_data", event))
    assert handler.stats["decisions"] == 0 and "Z1" in handler._pending
    handler(msg("energy_predictions", {
        "prediction_id": str(uuid.uuid4()), "source_event_id": event["event_id"],
        "building_id": "building-01", "zone_id": "Z1", "event_time": event["timestamp"],
        "predicted_for": (datetime.fromisoformat(event["timestamp"]) + timedelta(minutes=5)).isoformat(),
        "horizon_seconds": 300, "predicted_power_kw": 7.5, "predicted_energy_kwh": 0.625,
        "model_name": "xgboost", "model_version": "v", "created_at": event["timestamp"]}))
    assert handler.stats["basis_ml"] == 1 and not handler._pending


def test_optimization_service_falls_back_without_prediction():
    from workers.optimization_service import OptimizationHandler

    handler = OptimizationHandler(SETTINGS, RecordingProducer(), logging.getLogger("test"))
    handler._predictions_seen = True
    z1 = [e for e in stream(2) if e["zone_id"] == "Z1"]
    handler(msg("building_sensor_data", z1[0]))
    handler(msg("building_sensor_data", z1[1]))           # next reading: stop waiting
    assert handler.stats["basis_measured"] == 1
