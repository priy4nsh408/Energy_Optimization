"""Provider-independent observation records and their Kafka message format.

Event ids are deterministic (UUID5 of source, location, time and forecast flag),
so re-fetching or re-publishing the same observation never creates a duplicate
row downstream.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

_NAMESPACE = uuid.UUID("5b0f6a43-7c47-4c55-9a51-5f2d3c1a8e10")


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _event_id(*parts: object) -> str:
    return str(uuid.uuid5(_NAMESPACE, "|".join(str(p) for p in parts)))


@dataclass(frozen=True)
class WeatherObservation:
    observed_at: datetime
    source: str
    is_forecast: bool
    temperature_c: float
    humidity_pct: float | None = None
    wind_speed_ms: float | None = None
    solar_irradiance_w_m2: float | None = None
    cloud_cover_pct: float | None = None

    def to_message(self, building_id: str) -> dict[str, Any]:
        """Message for the ``weather_data`` topic (contract in processors.data_validator)."""
        return {
            "event_id": _event_id("weather", building_id, self.source, self.is_forecast,
                                  _utc_iso(self.observed_at)),
            "event_type": "weather_observation",
            "building_id": building_id,
            "observed_at": _utc_iso(self.observed_at),
            "source": self.source,
            "is_forecast": self.is_forecast,
            "temperature_c": self.temperature_c,
            "humidity_pct": self.humidity_pct,
            "wind_speed_ms": self.wind_speed_ms,
            "solar_irradiance_w_m2": self.solar_irradiance_w_m2,
            "cloud_cover_pct": self.cloud_cover_pct,
            "emitted_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }


@dataclass(frozen=True)
class CarbonObservation:
    observed_at: datetime
    source: str
    region: str
    is_forecast: bool
    intensity_g_per_kwh: float
    is_estimated: bool | None = None

    def to_message(self, building_id: str) -> dict[str, Any]:
        """Message for the ``carbon_intensity`` topic."""
        return {
            "event_id": _event_id("carbon", self.region, self.source, self.is_forecast,
                                  _utc_iso(self.observed_at)),
            "event_type": "carbon_intensity_observation",
            "building_id": building_id,
            "region": self.region,
            "observed_at": _utc_iso(self.observed_at),
            "source": self.source,
            "is_forecast": self.is_forecast,
            "intensity_g_per_kwh": self.intensity_g_per_kwh,
            "is_estimated": self.is_estimated,
            "emitted_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }
