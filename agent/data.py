"""Where the agent's read tools get building data.

* :class:`InMemoryDataSource` - fed directly by a running simulation
  (``python -m agent.demo``). Weather and carbon come from the configured
  providers, evaluated at simulated time.
* :class:`PostgresDataSource` - reads the history persisted by the Kafka
  workers (``workers.agent_service``).

Sensor readings are returned as event dictionaries (the ``building_sensor_data``
contract) so the Module 8 detectors can be replayed on them.
"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any, Protocol

from data_sources.carbon import CarbonProvider
from data_sources.weather import WeatherProvider

SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2, "critical": 3}
_EVENT_FIELDS = ("event_id", "building_id", "zone_id", "zone_type", "sequence", "interval_seconds",
                 "temperature_c", "humidity_pct", "co2_ppm", "occupancy_count", "lighting_lux",
                 "lighting_level_pct", "hvac_mode", "hvac_status", "hvac_setpoint_c", "hvac_power_kw",
                 "lighting_power_kw", "appliance_power_kw", "total_power_kw", "energy_kwh",
                 "outdoor_temperature_c", "outdoor_humidity_pct")


class BuildingDataSource(Protocol):
    def zones(self) -> list[str]: ...
    def now(self) -> datetime | None: ...
    def recent_events(self, zone_id: str, minutes: float) -> list[dict[str, Any]]: ...
    def latest_event(self, zone_id: str) -> dict[str, Any] | None: ...
    def alerts(self, zone_id: str | None, hours: float) -> list[dict[str, Any]]: ...
    def weather(self) -> dict[str, Any] | None: ...
    def carbon(self) -> dict[str, Any] | None: ...


def _parse_time(value: Any) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))


def _latest_observation(observations: list, now: datetime) -> dict[str, Any] | None:
    past = [o for o in observations if not o.is_forecast and o.observed_at <= now]
    if not past:
        return None
    latest = max(past, key=lambda o: o.observed_at)
    data = asdict(latest)
    data["observed_at"] = latest.observed_at.isoformat()
    return data


class InMemoryDataSource:
    """Rolling in-memory history of one building (thread-safe)."""

    def __init__(self, *, weather: WeatherProvider | None = None, carbon: CarbonProvider | None = None,
                 max_events_per_zone: int = 2016) -> None:
        self._events: dict[str, deque] = defaultdict(lambda: deque(maxlen=max_events_per_zone))
        self._alerts: deque = deque(maxlen=500)
        self._weather, self._carbon = weather, carbon
        self._lock = threading.Lock()

    # -- feeding ----------------------------------------------------------------------
    def add_event(self, event: dict[str, Any]) -> None:
        with self._lock:
            self._events[event["zone_id"]].append(event)

    def add_alert(self, alert: dict[str, Any]) -> None:
        with self._lock:
            self._alerts.append(alert)

    # -- queries ----------------------------------------------------------------------
    def zones(self) -> list[str]:
        with self._lock:
            return sorted(self._events)

    def now(self) -> datetime | None:
        with self._lock:
            times = [_parse_time(q[-1]["timestamp"]) for q in self._events.values() if q]
        return max(times) if times else None

    def recent_events(self, zone_id: str, minutes: float) -> list[dict[str, Any]]:
        with self._lock:
            events = list(self._events.get(zone_id, ()))
        if not events:
            return []
        cutoff = _parse_time(events[-1]["timestamp"]) - timedelta(minutes=minutes)
        return [e for e in events if _parse_time(e["timestamp"]) > cutoff]

    def latest_event(self, zone_id: str) -> dict[str, Any] | None:
        with self._lock:
            events = self._events.get(zone_id)
            return events[-1] if events else None

    def alerts(self, zone_id: str | None, hours: float) -> list[dict[str, Any]]:
        now = self.now()
        with self._lock:
            alerts = list(self._alerts)
        cutoff = (now - timedelta(hours=hours)) if now else None
        return [a for a in alerts if (zone_id is None or a.get("zone_id") == zone_id)
                and (cutoff is None or _parse_time(a["detected_at"]) >= cutoff)]

    def weather(self) -> dict[str, Any] | None:
        now = self.now()
        if self._weather is None or now is None:
            return None
        return _latest_observation(self._weather.fetch(past_hours=2, now=now), now)

    def carbon(self) -> dict[str, Any] | None:
        now = self.now()
        if self._carbon is None or now is None:
            return None
        return _latest_observation(self._carbon.fetch(past_hours=2, now=now), now)


class PostgresDataSource:
    """Reads persisted history (sensor_readings, security_alerts, weather, carbon)."""

    def __init__(self, database, building_id: str | None = None) -> None:
        self.db = database
        self.building_id = building_id

    def zones(self) -> list[str]:
        with self.db.transaction() as conn:
            return [r[0] for r in conn.execute("SELECT zone_id FROM zones ORDER BY zone_id")]

    def now(self) -> datetime | None:
        with self.db.transaction() as conn:
            return conn.execute("SELECT max(event_time) FROM sensor_readings").fetchone()[0]

    def recent_events(self, zone_id: str, minutes: float) -> list[dict[str, Any]]:
        columns = ", ".join(_EVENT_FIELDS)
        query = (f"SELECT {columns}, event_time, emitted_at FROM sensor_readings "
                 "WHERE zone_id = %s AND event_time > "
                 "(SELECT max(event_time) FROM sensor_readings WHERE zone_id = %s) - %s "
                 "ORDER BY event_time")
        with self.db.transaction() as conn:
            cursor = conn.execute(query, (zone_id, zone_id, timedelta(minutes=minutes)))
            names = [d.name for d in cursor.description]
            rows = [dict(zip(names, row)) for row in cursor]
        return [self._to_event(r) for r in rows]

    @staticmethod
    def _to_event(row: dict[str, Any]) -> dict[str, Any]:
        event = {k: row[k] for k in _EVENT_FIELDS}
        event["event_id"] = str(event["event_id"])
        event.update(schema_version=1, event_type="sensor_reading", source="postgresql",
                     timestamp=row["event_time"].isoformat(),
                     emitted_at=row["emitted_at"].isoformat() if row["emitted_at"] else None)
        return event

    def latest_event(self, zone_id: str) -> dict[str, Any] | None:
        events = self.recent_events(zone_id, minutes=60)
        return events[-1] if events else None

    def alerts(self, zone_id: str | None, hours: float) -> list[dict[str, Any]]:
        query = ("SELECT alert_id, zone_id, source_event_id, detected_at, alert_type, severity, detector, "
                 "score, description, details, status FROM security_alerts "
                 "WHERE detected_at >= (SELECT coalesce(max(event_time), now()) FROM sensor_readings) - %s "
                 "AND (%s::text IS NULL OR zone_id = %s) ORDER BY detected_at")
        with self.db.transaction() as conn:
            cursor = conn.execute(query, (timedelta(hours=hours), zone_id, zone_id))
            names = [d.name for d in cursor.description]
            rows = [dict(zip(names, row)) for row in cursor]
        for row in rows:
            row["alert_id"] = str(row["alert_id"])
            row["source_event_id"] = str(row["source_event_id"]) if row["source_event_id"] else None
            row["detected_at"] = row["detected_at"].isoformat()
        return rows

    def weather(self) -> dict[str, Any] | None:
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT observed_at, source, temperature_c, humidity_pct, wind_speed_ms, "
                "solar_irradiance_w_m2, cloud_cover_pct FROM weather_records WHERE NOT is_forecast "
                "ORDER BY observed_at DESC LIMIT 1").fetchone()
        if row is None:
            return None
        keys = ("observed_at", "source", "temperature_c", "humidity_pct", "wind_speed_ms",
                "solar_irradiance_w_m2", "cloud_cover_pct")
        data = dict(zip(keys, row))
        data["observed_at"] = data["observed_at"].isoformat()
        return data

    def carbon(self) -> dict[str, Any] | None:
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT observed_at, source, region, intensity_g_per_kwh FROM carbon_intensity_records "
                "WHERE NOT is_forecast ORDER BY observed_at DESC LIMIT 1").fetchone()
        if row is None:
            return None
        return {"observed_at": row[0].isoformat(), "source": row[1], "region": row[2],
                "intensity_g_per_kwh": row[3]}
