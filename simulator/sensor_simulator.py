"""Sensor layer: turns the true zone state into realistic sensor readings.

Real sensors are imperfect: they add measurement noise, have limited
resolution and a fixed measuring range, and people counters occasionally
miscount. Noise is applied to the physically simulated values, so readings
are never purely random.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any

from simulator.building_model import Zone
from simulator.outdoor_model import OutdoorConditions


@dataclass(frozen=True)
class SensorSpec:
    """Noise model of one sensor type."""

    noise_std: float          # absolute noise (or relative if ``relative``)
    resolution: float         # reported precision
    minimum: float
    maximum: float
    relative: bool = False

    def measure(self, true_value: float, rng: random.Random) -> float:
        std = self.noise_std * abs(true_value) if self.relative else self.noise_std
        value = min(self.maximum, max(self.minimum, true_value + rng.gauss(0, std)))
        return round(round(value / self.resolution) * self.resolution, 4)


# Typical commercial sensor characteristics.
SENSORS: dict[str, SensorSpec] = {
    "temperature_c": SensorSpec(noise_std=0.1, resolution=0.1, minimum=-40, maximum=85),
    "humidity_pct": SensorSpec(noise_std=1.0, resolution=0.1, minimum=0, maximum=100),
    "co2_ppm": SensorSpec(noise_std=15.0, resolution=1, minimum=0, maximum=5000),
    "lighting_lux": SensorSpec(noise_std=0.03, resolution=1, minimum=0, maximum=100_000, relative=True),
    "power_kw": SensorSpec(noise_std=0.005, resolution=0.001, minimum=0, maximum=1000, relative=True),
    "outdoor_temperature_c": SensorSpec(noise_std=0.2, resolution=0.1, minimum=-40, maximum=60),
    "outdoor_humidity_pct": SensorSpec(noise_std=2.0, resolution=0.1, minimum=0, maximum=100),
}
OCCUPANCY_MISCOUNT_PROBABILITY = 0.05


class SensorModel:
    """Produces noisy readings for a zone."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def _occupancy(self, true_count: int, capacity: int) -> int:
        if true_count > 0 and self.rng.random() < OCCUPANCY_MISCOUNT_PROBABILITY:
            return max(0, min(capacity, true_count + self.rng.choice((-1, 1))))
        return true_count

    def read_outdoor(self, outdoor: OutdoorConditions) -> dict[str, float]:
        """Readings of the building's single rooftop weather station."""
        return {
            "outdoor_temperature_c": SENSORS["outdoor_temperature_c"].measure(
                outdoor.temperature_c, self.rng),
            "outdoor_humidity_pct": SENSORS["outdoor_humidity_pct"].measure(
                outdoor.relative_humidity_pct, self.rng),
        }

    def read_zone(self, zone: Zone) -> dict[str, Any]:
        """Return the sensor readings of one zone (without event metadata)."""
        state, rng = zone.state, self.rng
        power = SENSORS["power_kw"]
        hvac_kw = power.measure(state.hvac_power_kw, rng)
        lighting_kw = power.measure(state.lighting_power_kw, rng)
        appliance_kw = power.measure(state.appliance_power_kw, rng)
        return {
            "temperature_c": SENSORS["temperature_c"].measure(state.temperature_c, rng),
            "humidity_pct": SENSORS["humidity_pct"].measure(state.relative_humidity_pct, rng),
            "co2_ppm": SENSORS["co2_ppm"].measure(state.co2_ppm, rng),
            "occupancy_count": self._occupancy(state.occupants, zone.spec.max_occupancy),
            "lighting_lux": SENSORS["lighting_lux"].measure(state.lighting_lux, rng),
            "lighting_level_pct": round(state.lighting_level * 100, 1),
            "hvac_mode": state.hvac_mode,
            "hvac_status": state.hvac_status,
            "hvac_setpoint_c": round(state.setpoint_c, 1),
            "hvac_power_kw": hvac_kw,
            "lighting_power_kw": lighting_kw,
            "appliance_power_kw": appliance_kw,
            "total_power_kw": round(hvac_kw + lighting_kw + appliance_kw, 3),
        }
