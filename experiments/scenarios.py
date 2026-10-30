"""Closed-loop scenarios: baseline control vs. optimized control.

Both scenarios run the same simulator with the same seed. The simulator uses
separate random streams for occupancy, weather, appliances and sensors, so
people and weather are identical and only the control differs.

* ``baseline``  - the building's own schedule-based controls (no commands).
* ``optimized`` - every ``decision_interval_s`` per zone the deterministic
  optimizer decides setpoint and lighting; approved decisions are applied to
  the simulator via ``apply_command`` (in-process, no Kafka).

Carbon accounting uses a time-varying grid intensity. By default this is the
SYNTHETIC mock profile (``data_sources.carbon.MockCarbonProvider``) so the
experiment is reproducible offline; the report states which profile was used.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

import pandas as pd

from config import OptimizationSettings, SimulatorSettings
from data_sources.carbon import MockCarbonProvider
from ml.dataset import BUILDING_COLUMNS, prepare_building_frame
from ml.predict import OnlinePredictor
from ml.registry import ModelBundle
from optimization.hvac_optimizer import REQUIRED_LUX, ZoneConditions, building_models
from optimization.lighting_optimizer import LUX_SENSOR_TOLERANCE, is_lighting_short
from optimization.optimizer import DecisionScheduler, ZoneOptimizer
from processors.data_validator import validate_sensor_event
from simulator.engine import BuildingSimulator

logger = logging.getLogger(__name__)

CarbonProfile = Callable[[datetime], float]


@dataclass
class ScenarioResult:
    name: str
    start: str
    days: float
    interval_seconds: float
    energy_kwh: float = 0.0
    carbon_kg: float = 0.0
    peak_power_kw: float = 0.0
    energy_by_zone_kwh: dict[str, float] = field(default_factory=dict)
    occupied_readings: int = 0
    occupied_in_comfort_band: int = 0
    discomfort_degree_hours: float = 0.0       # occupied, outside the comfort band
    lighting_shortfall_readings: int = 0       # occupied readings below required lux (5 % tolerance)
    lighting_shortfall_on_arrival: int = 0     # ... of which the first reading after people arrived
    server_max_temperature_c: float = float("-inf")
    decisions: int = 0
    commands_applied: int = 0
    decisions_rejected: int = 0
    prediction_basis: dict[str, int] = field(default_factory=dict)

    @property
    def comfort_compliance_pct(self) -> float:
        return 100 * self.occupied_in_comfort_band / self.occupied_readings if self.occupied_readings else 100.0

    def as_dict(self) -> dict[str, Any]:
        data = dict(vars(self))
        data["comfort_compliance_pct"] = self.comfort_compliance_pct
        return data


def run_scenario(name: str, sim_settings: SimulatorSettings, days: float, *,
                 optimizer: ZoneOptimizer | None = None, bundle: ModelBundle | None = None,
                 carbon_profile: CarbonProfile | None = None,
                 comfort_band: tuple[float, float] = (20.0, 25.0)) -> ScenarioResult:
    """Run one scenario and return its metrics (all computed from the simulation)."""
    carbon_profile = carbon_profile or MockCarbonProvider().value_at
    simulator = BuildingSimulator(sim_settings)
    ticks = int(days * 86400 / sim_settings.simulated_seconds_per_tick)
    result = ScenarioResult(name, sim_settings.start_time.isoformat() if sim_settings.start_time else "",
                            days, sim_settings.simulated_seconds_per_tick)
    energy_by_zone: dict[str, float] = defaultdict(float)
    basis: dict[str, int] = defaultdict(int)
    online = OnlinePredictor(bundle, prepare_building_frame) if (optimizer and bundle) else None
    scheduler = DecisionScheduler(optimizer.settings.decision_interval_s) if optimizer else None
    interval_h = sim_settings.simulated_seconds_per_tick / 3600
    was_occupied: dict[str, bool] = {}

    for _ in range(ticks):
        events = simulator.tick()
        building_power = 0.0
        for event in events:
            reading = validate_sensor_event(event)
            zone = reading.zone_id
            energy_by_zone[zone] += reading.energy_kwh
            result.carbon_kg += reading.energy_kwh * carbon_profile(reading.event_time) / 1000
            building_power += reading.total_power_kw
            if zone == "Z5":
                result.server_max_temperature_c = max(result.server_max_temperature_c, reading.temperature_c)
            elif reading.occupancy_count > 0:
                result.occupied_readings += 1
                low, high = comfort_band
                excess = max(0.0, low - reading.temperature_c, reading.temperature_c - high)
                result.occupied_in_comfort_band += excess == 0
                result.discomfort_degree_hours += excess * interval_h
                required = REQUIRED_LUX.get(reading.zone_type or "", 300.0)
                # 5 % tolerance for lux-sensor accuracy (sensor noise is 3 %).
                short = reading.lighting_lux < LUX_SENSOR_TOLERANCE * required
                result.lighting_shortfall_readings += short
                result.lighting_shortfall_on_arrival += short and not was_occupied.get(zone, False)

            was_occupied[zone] = reading.occupancy_count > 0
            if optimizer is None:
                continue
            prediction = None
            if online is not None:
                row = {c: getattr(reading, c) for c in BUILDING_COLUMNS}
                row["event_id"] = str(reading.event_id)
                prediction = online.update(row)
            conditions = ZoneConditions.from_reading(reading)
            short = is_lighting_short(optimizer.models[zone], conditions)
            if not scheduler.due(zone, reading.event_time, reading.occupancy_count, short):
                continue
            decision = optimizer.optimize(
                conditions,
                predicted_power_kw=prediction.value if prediction else None,
                carbon_intensity=carbon_profile(reading.event_time))
            result.decisions += 1
            basis[decision.basis] += 1
            if not decision.approved:
                result.decisions_rejected += 1
                continue
            simulator.apply_command(zone, setpoint_c=decision.setpoint_c,
                                    lighting_level=decision.lighting_level)
            result.commands_applied += 1
        result.peak_power_kw = max(result.peak_power_kw, building_power)

    result.energy_by_zone_kwh = {z: round(v, 3) for z, v in sorted(energy_by_zone.items())}
    result.energy_kwh = sum(energy_by_zone.values())
    result.prediction_basis = dict(basis)
    return result


def compare(baseline: ScenarioResult, optimized: ScenarioResult) -> dict[str, float]:
    def saving(b: float, o: float) -> float:
        return 100 * (b - o) / b if b else 0.0
    return {
        "energy_saving_pct": saving(baseline.energy_kwh, optimized.energy_kwh),
        "carbon_reduction_pct": saving(baseline.carbon_kg, optimized.carbon_kg),
        "peak_reduction_pct": saving(baseline.peak_power_kw, optimized.peak_power_kw),
    }


def result_frame(results: list[ScenarioResult]) -> pd.DataFrame:
    return pd.DataFrame([{
        "scenario": r.name, "energy_kwh": r.energy_kwh, "carbon_kg": r.carbon_kg,
        "peak_power_kw": r.peak_power_kw, "comfort_compliance_pct": r.comfort_compliance_pct,
        "discomfort_degree_hours": r.discomfort_degree_hours,
        "lighting_shortfall_readings": r.lighting_shortfall_readings,
        "server_max_temperature_c": r.server_max_temperature_c} for r in results])


def make_optimizer(settings: OptimizationSettings) -> ZoneOptimizer:
    return ZoneOptimizer(settings, building_models())
