"""Physical model of a five-zone building.

Each zone is modelled independently with lumped-parameter (RC) physics:

* Temperature: ``C dT/dt = UA (T_out - T) + ventilation + internal + solar + HVAC``
  where internal gains come from people, appliances and lighting.
* Humidity: moisture balance (occupants add moisture, ventilation exchanges
  with outdoor air, cooling coils dehumidify).
* CO2: mass balance between occupant generation and ventilation.
* Lighting: daylight through windows plus dimmable artificial lighting.
* Power: HVAC (thermal output / COP + fan), lighting and appliances.

Baseline controls are deliberately simple, like a conventional building
management system: a fixed thermostat schedule with night setback and
lights switched by schedule or occupancy. Later modules (optimisation,
agent) change behaviour only through :meth:`Building.apply_command`.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import TYPE_CHECKING, Protocol

from simulator.psychrometrics import (
    AIR_DENSITY_KG_M3,
    AIR_VOLUMETRIC_HEAT_CAPACITY_J_M3K,
    LATENT_HEAT_VAPORISATION_J_KG,
    humidity_ratio,
    relative_humidity_pct,
)

if TYPE_CHECKING:
    from simulator.occupancy_simulator import OccupancySimulator
    from simulator.outdoor_model import OutdoorConditions

HVAC_MODES = ("auto", "heat", "cool", "off")
HVAC_STATUSES = ("heating", "cooling", "idle", "off")

# Physical constants for occupants.
SENSIBLE_HEAT_PER_PERSON_W = 75.0
MOISTURE_PER_PERSON_KG_S = 0.06 / 3600        # ~60 g/h
CO2_PER_PERSON_M3_S = 5.2e-6                   # office activity

# Thermostat behaviour.
OCCUPIED_DEADBAND_C = 0.5     # half-width of the comfort band around the setpoint
SETBACK_DEADBAND_C = 4.0      # wider band outside operating hours (night setback)
PROPORTIONAL_BAND_C = 1.0     # error at which HVAC reaches full output
MIN_HVAC_STAGE = 0.25         # minimum compressor/heater output when running
LATENT_FRACTION_OF_COOLING = 0.15
COIL_SATURATION_TEMPERATURE_C = 12.0

APPLIANCE_FLUCTUATION_TAU_S = 600.0
APPLIANCE_FLUCTUATION_STD = 0.05  # relative std of appliance load

SETPOINT_LIMITS_C = (10.0, 35.0)


@dataclass(frozen=True)
class ZoneSpec:
    """Static description of one building zone."""

    zone_id: str
    name: str
    zone_type: str                        # office | meeting | cafeteria | server
    floor_area_m2: float
    max_occupancy: int
    envelope_ua_w_per_k: float            # envelope heat-loss coefficient
    thermal_capacitance_j_per_k: float    # air + furniture + structure
    solar_aperture_m2: float              # effective window area x SHGC
    daylight_factor: float                # indoor/outdoor illuminance ratio
    heating_capacity_kw: float
    cooling_capacity_kw: float
    heating_cop: float
    cooling_cop: float
    fan_power_kw: float
    lighting_power_density_w_m2: float
    design_lux: float                     # illuminance from lights at 100 %
    appliance_base_kw: float
    appliance_per_occupant_kw: float
    occupied_setpoint_c: float
    operating_start_hour: int = 7
    operating_end_hour: int = 19
    operates_weekends: bool = False
    always_on: bool = False               # 24/7 operation (e.g. server room)
    lights_follow_schedule: bool = False  # lights on all operating hours
    standby_lighting_level: float = 0.0   # e.g. security lighting
    ceiling_height_m: float = 3.0
    ventilation_ach: float = 1.0          # mechanical air changes/h when operating
    infiltration_ach: float = 0.15        # air changes/h otherwise

    @property
    def volume_m3(self) -> float:
        return self.floor_area_m2 * self.ceiling_height_m

    @property
    def lighting_capacity_kw(self) -> float:
        return self.lighting_power_density_w_m2 * self.floor_area_m2 / 1000.0

    def is_operating(self, when: datetime) -> bool:
        """Whether the building schedule treats this zone as in use at ``when``."""
        if self.always_on:
            return True
        if when.weekday() >= 5 and not self.operates_weekends:
            return False
        return self.operating_start_hour <= when.hour < self.operating_end_hour


def default_zones() -> list[ZoneSpec]:
    """The five zones of the reference building.

    Parameters are typical engineering values for a small office building
    (about 100 W/m² HVAC capacity, 8-10 W/m² LED lighting, 60 kJ/(m²K)
    effective thermal mass).
    """
    return [
        ZoneSpec(
            zone_id="Z1", name="Open-plan office", zone_type="office",
            floor_area_m2=300, max_occupancy=30,
            envelope_ua_w_per_k=300, thermal_capacitance_j_per_k=300 * 60e3,
            solar_aperture_m2=12, daylight_factor=0.015,
            heating_capacity_kw=30, cooling_capacity_kw=30, heating_cop=3.5, cooling_cop=3.0,
            fan_power_kw=0.8, lighting_power_density_w_m2=9, design_lux=500,
            appliance_base_kw=1.0, appliance_per_occupant_kw=0.12,
            occupied_setpoint_c=22.0, lights_follow_schedule=True,
        ),
        ZoneSpec(
            zone_id="Z2", name="Private offices", zone_type="office",
            floor_area_m2=120, max_occupancy=8,
            envelope_ua_w_per_k=140, thermal_capacitance_j_per_k=120 * 60e3,
            solar_aperture_m2=6, daylight_factor=0.02,
            heating_capacity_kw=12, cooling_capacity_kw=12, heating_cop=3.5, cooling_cop=3.0,
            fan_power_kw=0.3, lighting_power_density_w_m2=9, design_lux=500,
            appliance_base_kw=0.4, appliance_per_occupant_kw=0.15,
            occupied_setpoint_c=22.0,
        ),
        ZoneSpec(
            zone_id="Z3", name="Meeting room", zone_type="meeting",
            floor_area_m2=60, max_occupancy=12,
            envelope_ua_w_per_k=60, thermal_capacitance_j_per_k=60 * 60e3,
            solar_aperture_m2=2, daylight_factor=0.008,
            heating_capacity_kw=8, cooling_capacity_kw=8, heating_cop=3.5, cooling_cop=3.0,
            fan_power_kw=0.2, lighting_power_density_w_m2=10, design_lux=500,
            appliance_base_kw=0.15, appliance_per_occupant_kw=0.05,
            occupied_setpoint_c=22.0, operating_start_hour=8, operating_end_hour=18,
            ventilation_ach=2.0,
        ),
        ZoneSpec(
            zone_id="Z4", name="Cafeteria and lobby", zone_type="cafeteria",
            floor_area_m2=200, max_occupancy=60,
            envelope_ua_w_per_k=250, thermal_capacitance_j_per_k=200 * 60e3,
            solar_aperture_m2=15, daylight_factor=0.025,
            heating_capacity_kw=25, cooling_capacity_kw=25, heating_cop=3.5, cooling_cop=3.0,
            fan_power_kw=0.6, lighting_power_density_w_m2=8, design_lux=300,
            appliance_base_kw=2.0, appliance_per_occupant_kw=0.08,
            occupied_setpoint_c=21.0, operating_end_hour=20,
            lights_follow_schedule=True, standby_lighting_level=0.2,
        ),
        ZoneSpec(
            zone_id="Z5", name="Server room", zone_type="server",
            floor_area_m2=40, max_occupancy=2,
            envelope_ua_w_per_k=40, thermal_capacitance_j_per_k=40 * 60e3,
            solar_aperture_m2=0, daylight_factor=0.0,
            heating_capacity_kw=0, cooling_capacity_kw=10, heating_cop=1.0, cooling_cop=3.5,
            fan_power_kw=0.4, lighting_power_density_w_m2=8, design_lux=400,
            appliance_base_kw=4.5, appliance_per_occupant_kw=0.0,
            occupied_setpoint_c=20.0, always_on=True,
            ventilation_ach=0.2, infiltration_ach=0.2,
        ),
    ]


@dataclass
class ZoneOverrides:
    """Control overrides applied on top of the baseline schedule (``None`` = schedule)."""

    setpoint_c: float | None = None
    hvac_mode: str | None = None
    lighting_level: float | None = None


@dataclass
class ZoneState:
    """True (noise-free) physical state of a zone."""

    temperature_c: float
    humidity_ratio: float
    co2_ppm: float
    occupants: int = 0
    lighting_level: float = 0.0       # 0..1 artificial lighting dimming level
    lighting_lux: float = 0.0
    hvac_mode: str = "auto"
    hvac_status: str = "idle"
    hvac_output_fraction: float = 0.0
    setpoint_c: float = 22.0
    hvac_power_kw: float = 0.0
    lighting_power_kw: float = 0.0
    appliance_power_kw: float = 0.0
    energy_kwh: float = 0.0           # accumulated since the last reading
    _appliance_noise: float = field(default=0.0, repr=False)

    @property
    def relative_humidity_pct(self) -> float:
        return relative_humidity_pct(self.temperature_c, self.humidity_ratio)

    @property
    def total_power_kw(self) -> float:
        return self.hvac_power_kw + self.lighting_power_kw + self.appliance_power_kw


class OutdoorSource(Protocol):
    """Anything that can provide outdoor conditions (synthetic model or real data)."""

    def advance(self, when: datetime, dt_s: float) -> "OutdoorConditions": ...


class Zone:
    """One zone: baseline controls plus physics integration."""

    def __init__(self, spec: ZoneSpec, initial_temperature_c: float = 21.0,
                 initial_rh_pct: float = 45.0, initial_co2_ppm: float = 450.0) -> None:
        self.spec = spec
        self.state = ZoneState(
            temperature_c=initial_temperature_c,
            humidity_ratio=humidity_ratio(initial_temperature_c, initial_rh_pct),
            co2_ppm=initial_co2_ppm,
            setpoint_c=spec.occupied_setpoint_c,
        )
        self.overrides = ZoneOverrides()

    # -- control ---------------------------------------------------------------------
    def _thermostat(self, operating: bool) -> tuple[float, float, str]:
        """Return (setpoint, deadband half-width, mode) after applying overrides."""
        setpoint = self.overrides.setpoint_c
        if setpoint is not None:
            deadband = OCCUPIED_DEADBAND_C
        else:
            setpoint = self.spec.occupied_setpoint_c
            deadband = OCCUPIED_DEADBAND_C if operating else SETBACK_DEADBAND_C
        mode = self.overrides.hvac_mode or "auto"
        return setpoint, deadband, mode

    def _hvac_output(self, setpoint: float, deadband: float, mode: str) -> tuple[str, float]:
        """Proportional thermostat with a minimum stage; returns (status, signed fraction)."""
        if mode == "off":
            return "off", 0.0
        temperature = self.state.temperature_c
        heat_error = (setpoint - deadband) - temperature
        cool_error = temperature - (setpoint + deadband)
        if heat_error > 0 and mode in ("auto", "heat") and self.spec.heating_capacity_kw > 0:
            return "heating", min(1.0, MIN_HVAC_STAGE + heat_error / PROPORTIONAL_BAND_C)
        if cool_error > 0 and mode in ("auto", "cool") and self.spec.cooling_capacity_kw > 0:
            return "cooling", -min(1.0, MIN_HVAC_STAGE + cool_error / PROPORTIONAL_BAND_C)
        return "idle", 0.0

    def _lighting_level(self, operating: bool, occupants: int) -> float:
        if self.overrides.lighting_level is not None:
            return self.overrides.lighting_level
        if occupants > 0 or (operating and self.spec.lights_follow_schedule):
            return 1.0
        return self.spec.standby_lighting_level

    def _appliance_power_kw(self, dt_s: float, occupants: int, rng: random.Random) -> float:
        phi = math.exp(-dt_s / APPLIANCE_FLUCTUATION_TAU_S)
        self.state._appliance_noise = (phi * self.state._appliance_noise
                                       + math.sqrt(1 - phi * phi) * rng.gauss(0, 1))
        nominal = self.spec.appliance_base_kw + self.spec.appliance_per_occupant_kw * occupants
        return max(0.0, nominal * (1 + APPLIANCE_FLUCTUATION_STD * self.state._appliance_noise))

    # -- physics ---------------------------------------------------------------------
    def step(self, when: datetime, dt_s: float, outdoor: "OutdoorConditions",
             occupants: int, rng: random.Random) -> None:
        """Advance the zone by ``dt_s`` seconds (explicit Euler integration)."""
        spec, state = self.spec, self.state
        operating = spec.is_operating(when)
        state.occupants = occupants

        setpoint, deadband, mode = self._thermostat(operating)
        status, fraction = self._hvac_output(setpoint, deadband, mode)
        state.setpoint_c, state.hvac_mode, state.hvac_status = setpoint, mode, status
        state.hvac_output_fraction = abs(fraction)

        if fraction > 0:
            hvac_thermal_w = fraction * spec.heating_capacity_kw * 1000
            compressor_kw = hvac_thermal_w / 1000 / spec.heating_cop
        elif fraction < 0:
            hvac_thermal_w = fraction * spec.cooling_capacity_kw * 1000
            compressor_kw = -hvac_thermal_w / 1000 / spec.cooling_cop
        else:
            hvac_thermal_w, compressor_kw = 0.0, 0.0
        ventilating = operating and mode != "off"
        fan_kw = spec.fan_power_kw if (ventilating or status in ("heating", "cooling")) else 0.0

        ach = spec.ventilation_ach if ventilating else spec.infiltration_ach
        airflow_m3_s = ach * spec.volume_m3 / 3600
        # Mechanical ventilation air passes the air handler's cooling coil,
        # which dries humid outdoor air; the latent load costs compressor energy.
        coil_limit = humidity_ratio(COIL_SATURATION_TEMPERATURE_C, 100.0)
        supply_ratio = outdoor.humidity_ratio
        if (ventilating and mode in ("auto", "cool") and spec.cooling_capacity_kw > 0
                and outdoor.humidity_ratio > coil_limit):
            supply_ratio = coil_limit
            latent_w = (AIR_DENSITY_KG_M3 * airflow_m3_s * (outdoor.humidity_ratio - coil_limit)
                        * LATENT_HEAT_VAPORISATION_J_KG)
            compressor_kw += latent_w / 1000 / spec.cooling_cop
        state.hvac_power_kw = compressor_kw + fan_kw

        state.lighting_level = self._lighting_level(operating, occupants)
        state.lighting_power_kw = state.lighting_level * spec.lighting_capacity_kw
        state.lighting_lux = (outdoor.illuminance_lux * spec.daylight_factor
                              + state.lighting_level * spec.design_lux)
        state.appliance_power_kw = self._appliance_power_kw(dt_s, occupants, rng)

        # Heat balance (W). All appliance and lighting electricity ends up as heat.
        q_envelope = spec.envelope_ua_w_per_k * (outdoor.temperature_c - state.temperature_c)
        q_ventilation = (AIR_VOLUMETRIC_HEAT_CAPACITY_J_M3K * airflow_m3_s
                         * (outdoor.temperature_c - state.temperature_c))
        q_internal = (occupants * SENSIBLE_HEAT_PER_PERSON_W
                      + 1000 * (state.appliance_power_kw + state.lighting_power_kw))
        q_solar = spec.solar_aperture_m2 * outdoor.solar_irradiance_w_m2
        net_w = q_envelope + q_ventilation + q_internal + q_solar + hvac_thermal_w
        state.temperature_c += net_w * dt_s / spec.thermal_capacitance_j_per_k

        # Moisture balance (kg/kg).
        air_mass_kg = AIR_DENSITY_KG_M3 * spec.volume_m3
        exchange = airflow_m3_s / spec.volume_m3
        d_ratio = (occupants * MOISTURE_PER_PERSON_KG_S / air_mass_kg
                   + exchange * (supply_ratio - state.humidity_ratio))
        if hvac_thermal_w < 0 and state.humidity_ratio > coil_limit:
            latent_kg_s = LATENT_FRACTION_OF_COOLING * -hvac_thermal_w / LATENT_HEAT_VAPORISATION_J_KG
            d_ratio -= latent_kg_s / air_mass_kg
        state.humidity_ratio = max(1e-4, state.humidity_ratio + d_ratio * dt_s)

        # CO2 balance (ppm).
        generation_ppm_s = occupants * CO2_PER_PERSON_M3_S * 1e6 / spec.volume_m3
        state.co2_ppm += (generation_ppm_s + exchange * (outdoor.co2_ppm - state.co2_ppm)) * dt_s

        state.energy_kwh += state.total_power_kw * dt_s / 3600


class Building:
    """A collection of independent zones sharing one outdoor environment."""

    def __init__(self, zones: list[ZoneSpec], occupancy: "OccupancySimulator",
                 outdoor: OutdoorSource, rng: random.Random) -> None:
        if not zones:
            raise ValueError("A building needs at least one zone.")
        ids = [z.zone_id for z in zones]
        if len(set(ids)) != len(ids):
            raise ValueError(f"Duplicate zone ids: {ids}")
        self.zones: dict[str, Zone] = {spec.zone_id: Zone(spec) for spec in zones}
        self.occupancy = occupancy
        self.outdoor_source = outdoor
        self.rng = rng
        self.outdoor: "OutdoorConditions | None" = None

    def step(self, when: datetime, dt_s: float) -> None:
        """Advance every zone by one physics step starting at ``when``."""
        self.outdoor = self.outdoor_source.advance(when, dt_s)
        for zone in self.zones.values():
            occupants = self.occupancy.step(zone.spec, when, dt_s, zone.state.occupants)
            zone.step(when, dt_s, self.outdoor, occupants, self.rng)

    def apply_command(self, zone_id: str, *, setpoint_c: float | None = None,
                      hvac_mode: str | None = None, lighting_level: float | None = None) -> None:
        """Override baseline controls for a zone.

        This is the only way other modules change the building. Values are
        checked against physical limits only; policy checks belong to the
        safety layer.

        Raises:
            KeyError: unknown zone.
            ValueError: value outside physical limits.
        """
        zone = self.zones[zone_id]
        overrides = zone.overrides
        if setpoint_c is not None:
            low, high = SETPOINT_LIMITS_C
            if not low <= setpoint_c <= high:
                raise ValueError(f"setpoint_c must be within {SETPOINT_LIMITS_C}, got {setpoint_c}")
            overrides = replace(overrides, setpoint_c=float(setpoint_c))
        if hvac_mode is not None:
            if hvac_mode not in HVAC_MODES:
                raise ValueError(f"hvac_mode must be one of {HVAC_MODES}, got '{hvac_mode}'")
            overrides = replace(overrides, hvac_mode=hvac_mode)
        if lighting_level is not None:
            if not 0.0 <= lighting_level <= 1.0:
                raise ValueError(f"lighting_level must be within [0, 1], got {lighting_level}")
            overrides = replace(overrides, lighting_level=float(lighting_level))
        zone.overrides = overrides

    def clear_overrides(self, zone_id: str | None = None) -> None:
        """Return one zone (or all zones) to baseline schedule control."""
        targets = [self.zones[zone_id]] if zone_id else list(self.zones.values())
        for zone in targets:
            zone.overrides = ZoneOverrides()
