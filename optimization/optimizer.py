"""Deterministic HVAC setpoint and lighting optimizer (SciPy SLSQP).

For one zone and one decision interval it chooses ``x = (setpoint, lighting)``
minimising

    J(x) = H * (w_energy + w_carbon * CI / 1000) * P(x)              energy + carbon
         + H * w_comfort * occupants * excess(|T - T_pref| - neutral)^2   discomfort
         + 1e-5 * (T - T_now)^2                                        tie-breaker

where ``P(x)`` is the predicted average power: the ML forecast for the
current controls plus the modelled change from the new setpoint/lighting
(:mod:`optimization.hvac_optimizer`). H is the decision interval in hours and
CI the grid carbon intensity (gCO2e/kWh).

Constraints: setpoint band with step limit (bounds), lighting safety level
(bound) and required illuminance when occupied (inequality constraint). The
solver always starts from the current controls projected into the feasible
set, so results are reproducible. Every decision is re-validated
(:func:`optimization.constraints.validate_decision`).
"""

from __future__ import annotations

import logging
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import numpy as np
from scipy.optimize import minimize

from config import OptimizationSettings
from optimization.constraints import setpoint_band, validate_decision
from optimization.hvac_optimizer import ZoneConditions, ZoneEnergyModel, smooth_max0
from optimization.lighting_optimizer import illuminance, lighting_requirement, target_lux

logger = logging.getLogger(__name__)

SETPOINT_RESOLUTION_C = 0.1
LIGHTING_RESOLUTION = 0.01
TIE_BREAK_WEIGHT = 1e-5   # only breaks ties where the objective is flat


@dataclass(frozen=True)
class Decision:
    zone_id: str
    timestamp: datetime
    setpoint_c: float
    lighting_level: float
    baseline_setpoint_c: float
    baseline_lighting_level: float
    predicted_power_baseline_kw: float
    predicted_power_optimized_kw: float
    basis: str                     # "ml" (forecast) or "measured" (no forecast available)
    carbon_intensity_g_per_kwh: float | None
    objective_baseline: float
    objective_optimized: float
    approved: bool
    violations: tuple[str, ...]
    message: str

    @property
    def predicted_saving_kw(self) -> float:
        return self.predicted_power_baseline_kw - self.predicted_power_optimized_kw

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["timestamp"] = self.timestamp.isoformat()
        data["violations"] = list(self.violations)
        return data


class ZoneOptimizer:
    """Optimizes each zone independently; stateless between calls."""

    def __init__(self, settings: OptimizationSettings, models: dict[str, ZoneEnergyModel]) -> None:
        self.settings = settings
        self.models = models

    # -- model pieces -----------------------------------------------------------------
    def predicted_power_kw(self, model: ZoneEnergyModel, conditions: ZoneConditions,
                           base_power_kw: float, setpoint: float, lighting: float) -> float:
        hvac_delta = (model.hvac_power_kw(setpoint, lighting, conditions)
                      - model.hvac_power_kw(conditions.setpoint_c, conditions.lighting_level, conditions))
        lighting_delta = (lighting - conditions.lighting_level) * model.lighting_capacity_kw
        return max(0.0, base_power_kw + hvac_delta + lighting_delta)

    def discomfort(self, model: ZoneEnergyModel, conditions: ZoneConditions, setpoint: float) -> float:
        if model.critical or conditions.occupancy <= 0:
            return 0.0
        deviation = math.sqrt((setpoint - model.preferred_setpoint_c) ** 2 + 1e-6)
        excess = smooth_max0(deviation - self.settings.comfort_neutral_c, eps=0.05)
        return conditions.occupancy * excess ** 2

    def objective(self, model: ZoneEnergyModel, conditions: ZoneConditions, base_power_kw: float,
                  carbon_intensity: float | None, x: np.ndarray) -> float:
        setpoint, lighting = float(x[0]), float(x[1])
        hours = self.settings.decision_interval_s / 3600
        price = self.settings.energy_weight_per_kwh
        if carbon_intensity is not None:
            price += self.settings.carbon_weight_per_kg * carbon_intensity / 1000
        power = self.predicted_power_kw(model, conditions, base_power_kw, setpoint, lighting)
        return (hours * price * power
                + hours * self.settings.comfort_weight * self.discomfort(model, conditions, setpoint)
                + TIE_BREAK_WEIGHT * (setpoint - conditions.setpoint_c) ** 2)

    def _mode_consistent_bounds(self, model: ZoneEnergyModel, conditions: ZoneConditions,
                                low: float, high: float) -> tuple[float, float]:
        """Avoid setpoints that make the thermostat work *against* the season.

        Lowering the setpoint of a heated zone far below the room temperature
        makes an ``auto`` thermostat actively cool the room (and vice versa);
        the steady-state model cannot see that transient cost. So in a zone
        that needs heating the setpoint stays >= T - deadband (the room cools
        down naturally and the setpoint follows), and mirrored for cooling.
        """
        deadband = self.settings.thermostat_deadband_c
        temperature = conditions.temperature_c
        demand = model.thermal_demand_w(temperature, conditions.lighting_level, conditions)
        if demand > 0 and model.spec.cooling_capacity_kw > 0:
            low = min(high, max(low, temperature - deadband))
        elif demand < 0 and model.spec.heating_capacity_kw > 0:
            high = max(low, min(high, temperature + deadband))
        return low, high

    # -- optimisation ---------------------------------------------------------------------
    def optimize(self, conditions: ZoneConditions, *, predicted_power_kw: float | None = None,
                 carbon_intensity: float | None = None) -> Decision:
        model = self.models[conditions.zone_id]
        base_power = predicted_power_kw if predicted_power_kw is not None else conditions.total_power_kw
        basis = "ml" if predicted_power_kw is not None else "measured"
        band = setpoint_band(self.settings, model, conditions)
        requirement = lighting_requirement(model, conditions)

        if conditions.hvac_mode == "off":
            setpoint_bounds = (conditions.setpoint_c, conditions.setpoint_c)   # HVAC off: leave it
        else:
            setpoint_bounds = self._mode_consistent_bounds(model, conditions, band.low, band.high)
        lighting_low = 1.0 if (conditions.occupancy > 0 and not requirement.achievable) \
            else requirement.safety_level
        bounds = [setpoint_bounds, (lighting_low, 1.0)]
        constraints = []
        if conditions.occupancy > 0 and requirement.achievable:
            constraints.append({"type": "ineq", "fun": lambda x: illuminance(
                model, conditions, float(x[1])) - target_lux(model)})

        def f(x: np.ndarray) -> float:
            return self.objective(model, conditions, base_power, carbon_intensity, x)

        x0 = np.array([np.clip(conditions.setpoint_c, *setpoint_bounds),
                       np.clip(max(conditions.lighting_level, requirement.minimum_level), lighting_low, 1.0)])
        result = minimize(f, x0, method="SLSQP", bounds=bounds, constraints=constraints,
                          options={"maxiter": 200, "ftol": 1e-9})
        x = result.x if result.success else x0
        message = "optimal" if result.success else f"solver failed ({result.message}); feasible start used"

        # Actuator resolution: round the setpoint; round lighting *up* so the
        # illuminance constraint still holds.
        setpoint = float(np.clip(round(x[0] / SETPOINT_RESOLUTION_C) * SETPOINT_RESOLUTION_C,
                                 *setpoint_bounds))
        lighting = float(np.clip(math.ceil(x[1] / LIGHTING_RESOLUTION - 1e-9) * LIGHTING_RESOLUTION,
                                 lighting_low, 1.0))
        violations = validate_decision(self.settings, model, conditions, setpoint, lighting)
        baseline_power = self.predicted_power_kw(model, conditions, base_power,
                                                 conditions.setpoint_c, conditions.lighting_level)
        optimized_power = self.predicted_power_kw(model, conditions, base_power, setpoint, lighting)
        current = np.array([conditions.setpoint_c, conditions.lighting_level])
        return Decision(
            zone_id=conditions.zone_id, timestamp=conditions.timestamp,
            setpoint_c=round(setpoint, 2), lighting_level=round(lighting, 3),
            baseline_setpoint_c=conditions.setpoint_c,
            baseline_lighting_level=round(conditions.lighting_level, 3),
            predicted_power_baseline_kw=round(baseline_power, 4),
            predicted_power_optimized_kw=round(optimized_power, 4),
            basis=basis, carbon_intensity_g_per_kwh=carbon_intensity,
            objective_baseline=round(f(current), 6),
            objective_optimized=round(f(np.array([setpoint, lighting])), 6),
            approved=not violations, violations=tuple(violations), message=message)


class DecisionScheduler:
    """Decides *when* to re-optimize a zone.

    Periodically (``decision_interval_s`` of event time), immediately when a
    zone becomes occupied or empty, and when an occupied zone is too dark, so
    lighting and comfort react to people and daylight instead of waiting for
    the next period.
    """

    def __init__(self, interval_s: float) -> None:
        self.interval_s = interval_s
        self._last: dict[str, tuple[datetime, bool]] = {}

    def due(self, zone_id: str, timestamp: datetime, occupancy: int,
            lighting_short: bool = False) -> bool:
        """``lighting_short``: an occupied zone currently measures too little light
        (e.g. daylight faded since the last decision)."""
        occupied = occupancy > 0
        last = self._last.get(zone_id)
        if (last is None or occupied != last[1] or (occupied and lighting_short)
                or (timestamp - last[0]).total_seconds() >= self.interval_s
                or timestamp < last[0]):                      # time went backwards: new session
            self._last[zone_id] = (timestamp, occupied)
            return True
        return False
