"""Carbon-intensity providers (gCO2e/kWh of grid electricity).

* :class:`ElectricityMapsProvider` - worldwide, needs a (free) API key.
  Uses ``/carbon-intensity/latest`` and, for back-fill, ``/carbon-intensity/history``
  (last 24 h). Forecasts are not used because they need a paid plan.
* :class:`UkCarbonIntensityProvider` - National Grid ESO API for Great Britain,
  no key; provides actual values and a 24 h forecast.
* :class:`MockCarbonProvider` - SYNTHETIC values for offline development.
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from config import ConfigurationError, ExternalDataSettings
from data_sources.http_client import ApiRequestError, CachedHttpClient
from data_sources.observations import CarbonObservation

ELECTRICITY_MAPS_TTL_S = 900          # values are hourly; avoid wasting free-tier quota
ELECTRICITY_MAPS_MIN_INTERVAL_S = 300
UK_CARBON_BASE_URL = "https://api.carbonintensity.org.uk"
UK_CARBON_TTL_S = 900                 # half-hourly settlement periods
UK_CARBON_MIN_INTERVAL_S = 60
# Values are labelled with the *start* of their period, so the period that
# contains "now" (or the start of a back-fill window) starts up to one period earlier.
ELECTRICITY_MAPS_PERIOD = timedelta(hours=1)
UK_CARBON_PERIOD = timedelta(minutes=30)


class CarbonProvider(Protocol):
    name: str

    def fetch(self, *, past_hours: int = 0) -> list[CarbonObservation]: ...


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class ElectricityMapsProvider:
    name = "electricitymaps"

    def __init__(self, settings: ExternalDataSettings, client: CachedHttpClient) -> None:
        if not settings.electricity_maps_api_key:
            raise ConfigurationError(
                "CARBON_PROVIDER=electricitymaps requires ELECTRICITY_MAPS_API_KEY "
                "(or use CARBON_PROVIDER=uk-carbon-intensity or mock).")
        self.settings = settings
        self.client = client

    def _location(self) -> dict[str, Any]:
        if self.settings.electricity_maps_zone:
            return {"zone": self.settings.electricity_maps_zone}
        return {"lat": self.settings.latitude, "lon": self.settings.longitude}

    def _get(self, endpoint: str, ttl_s: float) -> Any:
        return self.client.get_json(
            f"{self.settings.electricity_maps_base_url}/carbon-intensity/{endpoint}",
            params=self._location(), headers={"auth-token": self.settings.electricity_maps_api_key},
            ttl_s=ttl_s).data

    def fetch(self, *, past_hours: int = 0, now: datetime | None = None) -> list[CarbonObservation]:
        now = now or datetime.now(timezone.utc)
        observations = []
        if past_hours > 0:
            history = self._get("history", ttl_s=3600)
            try:
                entries = history["history"]
            except (KeyError, TypeError) as exc:
                raise ApiRequestError("electricitymaps: unexpected history format") from exc
            cutoff = now - timedelta(hours=past_hours) - ELECTRICITY_MAPS_PERIOD
            for entry in entries:
                observation = self.parse_entry(entry)
                if observation and observation.observed_at >= cutoff:
                    observations.append(observation)
        latest = self.parse_entry(self._get("latest", ttl_s=ELECTRICITY_MAPS_TTL_S))
        if latest:
            observations.append(latest)
        return observations

    @staticmethod
    def parse_entry(entry: dict[str, Any]) -> CarbonObservation | None:
        """Parse one ``latest``/``history`` entry; ``None`` if no value is available."""
        try:
            value = entry.get("carbonIntensity")
            if value is None:
                return None
            return CarbonObservation(
                observed_at=_parse_time(entry["datetime"]), source="electricitymaps",
                region=str(entry.get("zone") or "unknown"), is_forecast=False,
                intensity_g_per_kwh=float(value), is_estimated=entry.get("isEstimated"))
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise ApiRequestError(f"electricitymaps: unexpected response format ({exc})") from exc


class UkCarbonIntensityProvider:
    name = "uk-carbon-intensity"
    region = "GB"

    def __init__(self, client: CachedHttpClient, *, forecast_hours: int = 24) -> None:
        self.client = client
        self.forecast_hours = forecast_hours

    def _get(self, path: str) -> list[dict[str, Any]]:
        data = self.client.get_json(f"{UK_CARBON_BASE_URL}{path}", ttl_s=UK_CARBON_TTL_S).data
        try:
            return list(data["data"])
        except (KeyError, TypeError) as exc:
            raise ApiRequestError("uk-carbon-intensity: unexpected response format") from exc

    def fetch(self, *, past_hours: int = 0, now: datetime | None = None) -> list[CarbonObservation]:
        now = now or datetime.now(timezone.utc)
        stamp = now.strftime("%Y-%m-%dT%H:%MZ")
        periods = self._get("/intensity")
        if past_hours > 0:
            periods = self._get(f"/intensity/{stamp}/pt24h") + periods
        if self.forecast_hours > 0:
            periods += self._get(f"/intensity/{stamp}/fw24h")
        observations: dict[tuple[datetime, bool], CarbonObservation] = {}
        cutoff = now - timedelta(hours=past_hours) - UK_CARBON_PERIOD
        horizon = now + timedelta(hours=self.forecast_hours)
        for period in periods:
            observation = self.parse_period(period)
            if observation and cutoff <= observation.observed_at <= horizon:
                observations[(observation.observed_at, observation.is_forecast)] = observation
        return sorted(observations.values(), key=lambda o: o.observed_at)

    @classmethod
    def parse_period(cls, period: dict[str, Any]) -> CarbonObservation | None:
        """Prefer the measured ``actual`` value; fall back to ``forecast``."""
        try:
            intensity = period["intensity"]
            actual, forecast = intensity.get("actual"), intensity.get("forecast")
            if actual is None and forecast is None:
                return None
            return CarbonObservation(
                observed_at=_parse_time(period["from"]), source="uk-carbon-intensity",
                region=cls.region, is_forecast=actual is None,
                intensity_g_per_kwh=float(actual if actual is not None else forecast))
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise ApiRequestError(f"uk-carbon-intensity: unexpected response format ({exc})") from exc


class MockCarbonProvider:
    """SYNTHETIC carbon intensity for offline development (``source="mock"``).

    Shape: higher in the morning and evening demand peaks, lower around midday
    (solar), plus hour-seeded variation. NOT real grid data.
    """

    name = "mock"
    region = "mock"

    def __init__(self, *, forecast_hours: int = 24, base_g_per_kwh: float = 300.0,
                 longitude: float = 0.0, seed: int = 0) -> None:
        self.forecast_hours = forecast_hours
        self.base = base_g_per_kwh
        self.longitude = longitude
        self.seed = seed

    def value_at(self, when: datetime) -> float:
        local = when.astimezone(timezone.utc) + timedelta(hours=self.longitude / 15)
        hour = local.hour + local.minute / 60
        evening = 80 * math.exp(-((hour - 19) ** 2) / 6)
        morning = 40 * math.exp(-((hour - 8) ** 2) / 4)
        solar = 70 * max(0.0, math.cos(2 * math.pi * (hour - 13) / 24)) ** 2
        noise = random.Random(f"{self.seed}:{when.strftime('%Y%m%d%H')}").gauss(0, 15)
        return round(max(50.0, self.base + evening + morning - solar + noise), 1)

    def fetch(self, *, past_hours: int = 0, now: datetime | None = None) -> list[CarbonObservation]:
        now = now or datetime.now(timezone.utc)
        hour = now.replace(minute=0, second=0, microsecond=0)
        offsets = [(h, False) for h in range(-past_hours, 1)]
        offsets += [(h, True) for h in range(1, self.forecast_hours + 1)]
        return [CarbonObservation(observed_at=hour + timedelta(hours=h), source="mock",
                                  region=self.region, is_forecast=forecast,
                                  intensity_g_per_kwh=self.value_at(hour + timedelta(hours=h)))
                for h, forecast in offsets]
