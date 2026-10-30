"""Moist-air helper functions (standard engineering approximations).

Humidity is tracked internally as a humidity ratio (kg water / kg dry air)
because it is conserved when air is heated or cooled; relative humidity is
derived from it for sensor readings.
"""

from __future__ import annotations

import math

ATMOSPHERIC_PRESSURE_PA = 101_325.0
AIR_DENSITY_KG_M3 = 1.2
AIR_VOLUMETRIC_HEAT_CAPACITY_J_M3K = 1_200.0  # rho * cp of air
LATENT_HEAT_VAPORISATION_J_KG = 2.45e6


def saturation_vapour_pressure_pa(temperature_c: float) -> float:
    """Magnus formula, valid roughly -40..60 °C."""
    return 610.94 * math.exp(17.625 * temperature_c / (temperature_c + 243.04))


def humidity_ratio(temperature_c: float, relative_humidity_pct: float) -> float:
    """Humidity ratio (kg/kg) from temperature and relative humidity."""
    vapour_pressure = saturation_vapour_pressure_pa(temperature_c) * relative_humidity_pct / 100.0
    return 0.622 * vapour_pressure / (ATMOSPHERIC_PRESSURE_PA - vapour_pressure)


def relative_humidity_pct(temperature_c: float, ratio: float) -> float:
    """Relative humidity (0-100 %) from temperature and humidity ratio."""
    vapour_pressure = ratio * ATMOSPHERIC_PRESSURE_PA / (0.622 + ratio)
    rh = 100.0 * vapour_pressure / saturation_vapour_pressure_pa(temperature_c)
    return max(0.0, min(100.0, rh))
