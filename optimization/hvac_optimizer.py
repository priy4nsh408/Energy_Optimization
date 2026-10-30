"""Zone energy model used by the optimizer.

The ML model forecasts next-interval power under the *current* controls. To
evaluate *other* setpoints and lighting levels, the optimizer adds the change
predicted by a steady-state heat balance of the zone:

    Q_hvac(T) = UA_eff * (T - T_out) - gains        (> 0 heating, < 0 cooling)
    P_hvac(T) = max(Q, 0) / COP_heat + max(-Q, 0) / COP_cool

with gains from occupants, appliances, lighting (lighting heat raises cooling
and lowers heating demand) and solar radiation estimated from the measured
daylight. A smooth max keeps the function differentiable for SLSQP.

Zone parameters come from the building specification (the same engineering
values the simulator uses), not from the ML model, so the optimizer works
with any energy forecast.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime

from simulator.building_model import SENSIBLE_HEAT_PER_PERSON_W, ZoneSpec, default_zones
from simulator.outdoor_model import LUMINOUS_EFFICACY_LUX_PER_W_M2
from simulator.psychrometrics import AIR_VOLUMETRIC_HEAT_CAPACITY_J_M3K

# Illuminance required while occupied (EN 12464-1 style task values).
REQUIRED_LUX = {"office": 500.0, "meeting": 500.0, "cafeteria": 200.0, "server": 300.0}
SMOOTHING_W = 50.0


def smooth_max0(x: float, eps: float = SMOOTHING_W) -> float:
    """Differentiable approximation of max(x, 0)."""
    return 0.5 * (x + math.sqrt(x * x + eps * eps))


@dataclass(frozen=True)
class ZoneConditions:
    """Current state of a zone, taken from a validated sensor event."""

    zone_id: str
    timestamp: datetime
    occupancy: int
    temperature_c: float
    outdoor_temperature_c: float | None
    setpoint_c: float
    lighting_level: float          # 0..1
    lighting_lux: float
    appliance_power_kw: float
    hvac_power_kw: float
    total_power_kw: float
    hvac_mode: str
    interval_seconds: float

    @classmethod
    def from_reading(cls, reading) -> "ZoneConditions":
        """Build from a ``SensorReadingRecord`` (processors.data_validator)."""
        return cls(
            zone_id=reading.zone_id, timestamp=reading.event_time,
            occupancy=reading.occupancy_count, temperature_c=reading.temperature_c,
            outdoor_temperature_c=reading.outdoor_temperature_c,
            setpoint_c=reading.hvac_setpoint_c, lighting_level=reading.lighting_level_pct / 100,
            lighting_lux=reading.lighting_lux, appliance_power_kw=reading.appliance_power_kw,
            hvac_power_kw=reading.hvac_power_kw, total_power_kw=reading.total_power_kw,
            hvac_mode=reading.hvac_mode, interval_seconds=reading.interval_seconds)


@dataclass(frozen=True)
class ZoneEnergyModel:
    """Engineering parameters of one zone."""

    zone_id: str
    zone_type: str
    spec: ZoneSpec
    preferred_setpoint_c: float
    required_lux: float
    safety_lighting_level: float       # minimum level at all times (egress lighting)
    critical: bool                     # equipment room: fixed temperature limits, no comfort term

    @classmethod
    def from_spec(cls, spec: ZoneSpec) -> "ZoneEnergyModel":
        return cls(zone_id=spec.zone_id, zone_type=spec.zone_type, spec=spec,
                   preferred_setpoint_c=spec.occupied_setpoint_c,
                   required_lux=REQUIRED_LUX.get(spec.zone_type, 300.0),
                   safety_lighting_level=spec.standby_lighting_level,
                   critical=spec.zone_type == "server")

    @property
    def lighting_capacity_kw(self) -> float:
        return self.spec.lighting_capacity_kw

    def ua_effective_w_per_k(self, when: datetime) -> float:
        ach = self.spec.ventilation_ach if self.spec.is_operating(when) else self.spec.infiltration_ach
        return (self.spec.envelope_ua_w_per_k
                + AIR_VOLUMETRIC_HEAT_CAPACITY_J_M3K * ach * self.spec.volume_m3 / 3600)

    def daylight_lux(self, conditions: ZoneConditions) -> float:
        """Daylight part of the measured illuminance."""
        return max(0.0, conditions.lighting_lux - conditions.lighting_level * self.spec.design_lux)

    def solar_gain_w(self, conditions: ZoneConditions) -> float:
        if self.spec.daylight_factor <= 0 or self.spec.solar_aperture_m2 <= 0:
            return 0.0
        irradiance = self.daylight_lux(conditions) / (self.spec.daylight_factor
                                                      * LUMINOUS_EFFICACY_LUX_PER_W_M2)
        return self.spec.solar_aperture_m2 * irradiance

    def thermal_demand_w(self, temperature_c: float, lighting_level: float,
                         conditions: ZoneConditions) -> float:
        """Steady-state HVAC heat needed to hold ``temperature_c`` (> 0 heating, < 0 cooling)."""
        outdoor = (conditions.outdoor_temperature_c if conditions.outdoor_temperature_c is not None
                   else conditions.temperature_c)
        gains = (conditions.occupancy * SENSIBLE_HEAT_PER_PERSON_W
                 + 1000 * conditions.appliance_power_kw
                 + 1000 * lighting_level * self.lighting_capacity_kw
                 + self.solar_gain_w(conditions))
        return self.ua_effective_w_per_k(conditions.timestamp) * (temperature_c - outdoor) - gains

    def hvac_power_kw(self, setpoint_c: float, lighting_level: float,
                      conditions: ZoneConditions) -> float:
        """Steady-state electrical HVAC power needed to hold ``setpoint_c``."""
        demand_w = self.thermal_demand_w(setpoint_c, lighting_level, conditions)
        power = 0.0
        if self.spec.heating_capacity_kw > 0:
            power += smooth_max0(demand_w) / 1000 / self.spec.heating_cop
        if self.spec.cooling_capacity_kw > 0:
            power += smooth_max0(-demand_w) / 1000 / self.spec.cooling_cop
        return power


def building_models(zones: list[ZoneSpec] | None = None) -> dict[str, ZoneEnergyModel]:
    return {spec.zone_id: ZoneEnergyModel.from_spec(spec) for spec in (zones or default_zones())}
