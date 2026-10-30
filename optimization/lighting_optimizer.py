"""Lighting requirements: daylight harvesting, occupancy and safety lighting.

* Occupied: the illuminance (daylight + artificial) must reach the zone's
  required lux; the controller targets 10 % more to absorb sensor errors, so the
  minimum level is ``(1.1 * required - daylight) / design_lux``. If even 100 %
  cannot reach the target, lighting is set to 100 %.
* Always: never below the zone's safety level (egress/emergency lighting,
  e.g. 20 % in the lobby at night).
* Unoccupied zones without a safety requirement may be switched off.
"""

from __future__ import annotations

from dataclasses import dataclass

from optimization.hvac_optimizer import ZoneConditions, ZoneEnergyModel


# Lighting is controlled to 10 % above the requirement so that daylight-estimate
# and sensor errors (about 3 %) do not leave occupied zones under-lit.
LIGHTING_TARGET_MARGIN = 0.10


def target_lux(model: ZoneEnergyModel) -> float:
    return model.required_lux * (1 + LIGHTING_TARGET_MARGIN)


@dataclass(frozen=True)
class LightingRequirement:
    safety_level: float          # hard minimum at all times
    lux_level: float             # minimum level for the required illuminance (0 if unoccupied)
    achievable: bool             # False if even 100 % cannot reach the required lux

    @property
    def minimum_level(self) -> float:
        return max(self.safety_level, min(1.0, self.lux_level))


def lighting_requirement(model: ZoneEnergyModel, conditions: ZoneConditions) -> LightingRequirement:
    safety = model.safety_lighting_level
    if conditions.occupancy <= 0:
        return LightingRequirement(safety, 0.0, True)
    daylight = model.daylight_lux(conditions)
    design = model.spec.design_lux
    needed = max(0.0, target_lux(model) - daylight) / design if design > 0 else 0.0
    return LightingRequirement(safety, needed, needed <= 1.0)


def illuminance(model: ZoneEnergyModel, conditions: ZoneConditions, level: float) -> float:
    """Predicted illuminance at ``level`` with the current daylight."""
    return model.daylight_lux(conditions) + level * model.spec.design_lux


LUX_SENSOR_TOLERANCE = 0.95   # lux sensors are accurate to a few percent


def is_lighting_short(model: ZoneEnergyModel, conditions: ZoneConditions) -> bool:
    """Occupied and the *measured* illuminance is clearly below the requirement."""
    return (conditions.occupancy > 0
            and conditions.lighting_lux < LUX_SENSOR_TOLERANCE * model.required_lux
            and conditions.lighting_level < 1.0)
