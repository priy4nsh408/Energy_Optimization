"""Weather providers: Open-Meteo (live) and a synthetic mock for offline work.

Open-Meteo (https://open-meteo.com) needs no API key for non-commercial use.
One request returns the *current* conditions (15-minute resolution) plus an
hourly series covering ``past_hours`` and ``forecast_hours``.

Variables: temperature, relative humidity, wind speed, cloud cover and
shortwave (solar) radiation; the last is useful for predicting solar heat gains.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from config import ExternalDataSettings, SimulatorSettings
from data_sources.http_client import ApiRequestError, CachedHttpClient
from data_sources.observations import WeatherObservation
from simulator.outdoor_model import OutdoorModel
from simulator.psychrometrics import saturation_vapour_pressure_pa

OPEN_METEO_VARIABLES = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "humidity_pct",
    "wind_speed_10m": "wind_speed_ms",
    "cloud_cover": "cloud_cover_pct",
    "shortwave_radiation": "solar_irradiance_w_m2",
}
OPEN_METEO_TTL_S = 900          # current values update every 15 minutes
OPEN_METEO_MIN_INTERVAL_S = 60


class WeatherProvider(Protocol):
    name: str

    def fetch(self, *, past_hours: int = 0) -> list[WeatherObservation]: ...


def _parse_utc(value: str) -> datetime:
    """Open-Meteo times are ISO without offset; we request timezone=GMT."""
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


class OpenMeteoProvider:
    """Live weather from the Open-Meteo forecast API."""

    name = "open-meteo"

    def __init__(self, settings: ExternalDataSettings, client: CachedHttpClient) -> None:
        self.settings = settings
        self.client = client

    def fetch(self, *, past_hours: int = 0, now: datetime | None = None) -> list[WeatherObservation]:
        now = now or datetime.now(timezone.utc)
        variables = ",".join(OPEN_METEO_VARIABLES)
        params: dict[str, Any] = {
            "latitude": self.settings.latitude,
            "longitude": self.settings.longitude,
            "current": variables,
            "hourly": variables,
            "forecast_hours": max(1, self.settings.weather_forecast_hours),
            "past_hours": past_hours,
            "wind_speed_unit": "ms",
            "timezone": "GMT",
        }
        secret = {"apikey": self.settings.open_meteo_api_key} if self.settings.open_meteo_api_key else None
        response = self.client.get_json(f"{self.settings.open_meteo_base_url}/forecast",
                                        params=params, secret_params=secret,
                                        ttl_s=OPEN_METEO_TTL_S)
        return self.parse(response.data, now=now)

    @classmethod
    def parse(cls, data: dict[str, Any], *, now: datetime) -> list[WeatherObservation]:
        """Convert an Open-Meteo response into observations.

        Raises:
            ApiRequestError: the response does not have the documented structure.
        """
        try:
            observations = []
            current = data.get("current")
            if current and current.get("temperature_2m") is not None:
                observations.append(cls._observation(
                    _parse_utc(current["time"]), False,
                    {key: current.get(key) for key in OPEN_METEO_VARIABLES}))
            hourly = data.get("hourly") or {}
            times = hourly.get("time", [])
            for i, time_str in enumerate(times):
                values = {key: (hourly.get(key) or [None] * len(times))[i] for key in OPEN_METEO_VARIABLES}
                if values["temperature_2m"] is None:
                    continue
                observed_at = _parse_utc(time_str)
                observations.append(cls._observation(observed_at, observed_at > now, values))
            return observations
        except (KeyError, TypeError, ValueError, IndexError) as exc:
            raise ApiRequestError(f"open-meteo: unexpected response format ({exc})") from exc

    @staticmethod
    def _observation(observed_at: datetime, is_forecast: bool,
                     values: dict[str, Any]) -> WeatherObservation:
        mapped = {OPEN_METEO_VARIABLES[k]: (float(v) if v is not None else None)
                  for k, v in values.items()}
        return WeatherObservation(observed_at=observed_at, source="open-meteo",
                                  is_forecast=is_forecast, **mapped)


class MockWeatherProvider:
    """SYNTHETIC weather for offline development (``source="mock"``).

    Uses the simulator's climate model (seasonal + daily cycle, sun position)
    with hour-seeded variation, so values are plausible and reproducible but
    are NOT real observations.
    """

    name = "mock"

    def __init__(self, settings: ExternalDataSettings, climate: SimulatorSettings,
                 *, seed: int = 0) -> None:
        self.settings = settings
        self.seed = seed
        self.model = OutdoorModel(
            latitude_deg=settings.latitude, annual_mean_c=climate.outdoor_annual_mean_c,
            seasonal_amplitude_c=climate.outdoor_seasonal_amplitude_c,
            diurnal_amplitude_c=climate.outdoor_diurnal_amplitude_c, rng=random.Random(seed))

    def _at(self, when: datetime, is_forecast: bool) -> WeatherObservation:
        rng = random.Random(f"{self.seed}:{when.strftime('%Y%m%d%H')}")
        # The climate model works in local solar time (UTC + longitude / 15 h).
        solar_time = when + timedelta(hours=self.settings.longitude / 15)
        temperature = self.model.base_temperature_c(solar_time) + rng.gauss(0, 1.0)
        dew_point = min(temperature, self.model.seasonal_mean_c(solar_time) - 5 + rng.gauss(0, 1.0))
        humidity = 100 * saturation_vapour_pressure_pa(dew_point) / saturation_vapour_pressure_pa(temperature)
        cloud = min(1.0, max(0.0, rng.gauss(0.4, 0.25)))
        solar = self.model.clear_sky_irradiance_w_m2(solar_time) * (1 - 0.75 * cloud ** 3)
        return WeatherObservation(
            observed_at=when, source="mock", is_forecast=is_forecast,
            temperature_c=round(temperature, 1), humidity_pct=round(min(100.0, humidity), 1),
            wind_speed_ms=round(abs(rng.gauss(3.0, 1.5)), 1),
            solar_irradiance_w_m2=round(solar, 1), cloud_cover_pct=round(cloud * 100, 1))

    def fetch(self, *, past_hours: int = 0, now: datetime | None = None) -> list[WeatherObservation]:
        now = now or datetime.now(timezone.utc)
        current = now.replace(minute=now.minute - now.minute % 15, second=0, microsecond=0)
        hour = now.replace(minute=0, second=0, microsecond=0)
        observations = [self._at(current, False)]
        observations += [self._at(hour - timedelta(hours=h), False) for h in range(past_hours, 0, -1)]
        observations += [self._at(hour + timedelta(hours=h), True)
                         for h in range(1, self.settings.weather_forecast_hours + 1)]
        return observations
