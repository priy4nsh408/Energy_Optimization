"""Synthetic outdoor climate model.

SYNTHETIC DATA: this model produces plausible outdoor conditions from a
seasonal + daily temperature cycle, a sun-position model and slowly varying
random weather (autoregressive processes). It stands in for real weather data
until the weather API module is added; the simulator accepts any object with
the same ``advance`` method, so a real data source can replace it later.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime

from simulator.psychrometrics import humidity_ratio, saturation_vapour_pressure_pa

OUTDOOR_CO2_PPM = 420.0
LUMINOUS_EFFICACY_LUX_PER_W_M2 = 110.0


@dataclass(frozen=True)
class OutdoorConditions:
    """Outdoor state at one instant."""

    temperature_c: float
    relative_humidity_pct: float
    humidity_ratio: float
    solar_irradiance_w_m2: float
    illuminance_lux: float
    cloud_cover: float
    co2_ppm: float = OUTDOOR_CO2_PPM


@dataclass
class _Ar1:
    """Zero-mean AR(1) process with time constant ``tau_s`` and std ``sigma``."""

    tau_s: float
    sigma: float
    value: float = 0.0

    def step(self, dt_s: float, rng: random.Random) -> float:
        phi = math.exp(-dt_s / self.tau_s)
        self.value = phi * self.value + self.sigma * math.sqrt(1 - phi * phi) * rng.gauss(0, 1)
        return self.value


class OutdoorModel:
    """Seasonal/diurnal outdoor climate with stochastic weather variation."""

    def __init__(self, *, latitude_deg: float, annual_mean_c: float,
                 seasonal_amplitude_c: float, diurnal_amplitude_c: float,
                 rng: random.Random, hottest_day_of_year: int | None = None) -> None:
        self.latitude_rad = math.radians(latitude_deg)
        self.annual_mean_c = annual_mean_c
        self.seasonal_amplitude_c = seasonal_amplitude_c
        self.diurnal_amplitude_c = diurnal_amplitude_c
        # Hottest day is ~3 weeks after the summer solstice.
        self.hottest_day = hottest_day_of_year or (200 if latitude_deg >= 0 else 17)
        self.rng = rng
        self._temperature_anomaly = _Ar1(tau_s=6 * 3600, sigma=1.5)
        self._dew_point_anomaly = _Ar1(tau_s=12 * 3600, sigma=1.5)
        self._cloud = _Ar1(tau_s=3 * 3600, sigma=0.35)

    # -- deterministic components ------------------------------------------------
    def seasonal_mean_c(self, when: datetime) -> float:
        """Daily mean temperature for the day of year."""
        day = when.timetuple().tm_yday
        return self.annual_mean_c + self.seasonal_amplitude_c * math.cos(
            2 * math.pi * (day - self.hottest_day) / 365)

    def base_temperature_c(self, when: datetime) -> float:
        """Seasonal + diurnal temperature without weather noise (min 03:00, max 15:00)."""
        hour = when.hour + when.minute / 60 + when.second / 3600
        diurnal = self.diurnal_amplitude_c * math.cos(2 * math.pi * (hour - 15) / 24)
        return self.seasonal_mean_c(when) + diurnal

    def clear_sky_irradiance_w_m2(self, when: datetime) -> float:
        """Global horizontal irradiance under clear sky from solar elevation.

        Uses local clock time as solar time (adequate for a simulation).
        """
        day = when.timetuple().tm_yday
        hour = when.hour + when.minute / 60 + when.second / 3600
        declination = math.radians(23.44) * math.sin(2 * math.pi * (284 + day) / 365)
        hour_angle = math.radians(15 * (hour - 12))
        sin_elevation = (math.sin(self.latitude_rad) * math.sin(declination)
                         + math.cos(self.latitude_rad) * math.cos(declination) * math.cos(hour_angle))
        if sin_elevation <= 0:
            return 0.0
        return 1000.0 * sin_elevation ** 1.15

    # -- stateful update ---------------------------------------------------------
    def advance(self, when: datetime, dt_s: float) -> OutdoorConditions:
        """Advance the weather processes by ``dt_s`` and return conditions at ``when``."""
        temperature = self.base_temperature_c(when) + self._temperature_anomaly.step(dt_s, self.rng)

        # Dew point follows the daily mean temperature, so relative humidity
        # naturally peaks at night and drops in the afternoon.
        dew_point = min(temperature, self.seasonal_mean_c(when) - 5.0 + self._dew_point_anomaly.step(dt_s, self.rng))
        rh = 100.0 * saturation_vapour_pressure_pa(dew_point) / saturation_vapour_pressure_pa(temperature)
        rh = max(5.0, min(100.0, rh))

        cloud = min(1.0, max(0.0, 0.4 + self._cloud.step(dt_s, self.rng)))
        irradiance = self.clear_sky_irradiance_w_m2(when) * (1 - 0.75 * cloud ** 3)

        return OutdoorConditions(
            temperature_c=temperature,
            relative_humidity_pct=rh,
            humidity_ratio=humidity_ratio(temperature, rh),
            solar_irradiance_w_m2=irradiance,
            illuminance_lux=irradiance * LUMINOUS_EFFICACY_LUX_PER_W_M2,
            cloud_cover=cloud,
        )
