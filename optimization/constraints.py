"""Hard constraints of the optimizer and an independent validator.

The optimizer solves *within* these limits; :func:`validate_decision` then
re-checks every decision before it is published as ``approved``. The two are
deliberately separate so that a solver bug cannot produce an approved command
that breaks a limit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from config import OptimizationSettings
from optimization.hvac_optimizer import ZoneConditions, ZoneEnergyModel
from optimization.lighting_optimizer import illuminance, lighting_requirement

# Equipment rooms: allowed temperature range regardless of occupancy. Under
# load the thermostat settles above its band (proportional offset), so the
# setpoint keeps a larger margin from these hard limits.
CRITICAL_ZONE_BAND_C = (18.0, 24.0)
CRITICAL_ZONE_MARGIN_C = 1.0
TOLERANCE = 1e-6


@dataclass(frozen=True)
class SetpointBand:
    low: float
    high: float
    reason: str


def comfort_band(settings: OptimizationSettings, model: ZoneEnergyModel,
                 conditions: ZoneConditions) -> tuple[float, float, str]:
    """Allowed setpoint range before the step limit.

    Comfort and equipment limits apply to the *temperature*. The thermostat
    lets the temperature swing by the deadband around the setpoint, so the
    setpoint keeps that distance from those limits.
    """
    deadband = settings.thermostat_deadband_c
    if model.critical:
        low, high, reason = (*CRITICAL_ZONE_BAND_C, "equipment room limits")
    elif conditions.occupancy > 0:
        low, high, reason = settings.comfort_min_c, settings.comfort_max_c, "occupied comfort band"
    elif (model.spec.is_operating(conditions.timestamp) or model.spec.is_operating(
            conditions.timestamp + timedelta(seconds=settings.preconditioning_lead_s))):
        # Empty but people are expected (operating hours or pre-conditioning):
        # allow a moderate setback so comfort can be restored quickly.
        margin = settings.standby_margin_c
        low, high, reason = (settings.comfort_min_c - margin, settings.comfort_max_c + margin,
                             "scheduled standby band")
    else:
        low, high, reason = settings.setback_min_c, settings.setback_max_c, "unoccupied setback band"
    if model.critical:
        low, high = low + CRITICAL_ZONE_MARGIN_C, high - CRITICAL_ZONE_MARGIN_C
    elif reason != "unoccupied setback band":
        low, high = low + deadband, high - deadband
    return max(low, settings.setpoint_min_c), min(high, settings.setpoint_max_c), reason


def setpoint_band(settings: OptimizationSettings, model: ZoneEnergyModel,
                  conditions: ZoneConditions) -> SetpointBand:
    """Comfort/setback band intersected with the per-decision step limit.

    The step limit never blocks a move *into* the band (comfort recovery):
    the current setpoint is first clamped into the band.
    """
    low, high, reason = comfort_band(settings, model, conditions)
    current = min(max(conditions.setpoint_c, low), high)
    step = settings.max_setpoint_step_c
    return SetpointBand(max(low, current - step), min(high, current + step), reason)


def validate_decision(settings: OptimizationSettings, model: ZoneEnergyModel,
                      conditions: ZoneConditions, setpoint_c: float,
                      lighting_level: float) -> list[str]:
    """Return the list of violated constraints (empty = approved)."""
    violations = []
    if not settings.setpoint_min_c - TOLERANCE <= setpoint_c <= settings.setpoint_max_c + TOLERANCE:
        violations.append(f"setpoint {setpoint_c:.2f} outside absolute limits "
                          f"[{settings.setpoint_min_c}, {settings.setpoint_max_c}]")
    band = setpoint_band(settings, model, conditions)
    if not band.low - TOLERANCE <= setpoint_c <= band.high + TOLERANCE:
        violations.append(f"setpoint {setpoint_c:.2f} outside {band.reason} incl. step limit "
                          f"[{band.low:.2f}, {band.high:.2f}]")
    if not 0.0 - TOLERANCE <= lighting_level <= 1.0 + TOLERANCE:
        violations.append(f"lighting level {lighting_level:.3f} outside [0, 1]")
    requirement = lighting_requirement(model, conditions)
    if lighting_level < requirement.safety_level - TOLERANCE:
        violations.append(f"lighting level {lighting_level:.2f} below safety level "
                          f"{requirement.safety_level:.2f}")
    if conditions.occupancy > 0:
        lux = illuminance(model, conditions, lighting_level)
        if lux < model.required_lux - 1.0 and lighting_level < 1.0 - TOLERANCE:
            violations.append(f"illuminance {lux:.0f} lux below required {model.required_lux:.0f} lux "
                              "for an occupied zone")
    return violations
