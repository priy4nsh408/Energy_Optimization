"""Software isolation (quarantine) of compromised sensors.

Isolating a sensor does not touch any equipment. Its readings are no longer
trusted: tools show them as ``None``, the safety validator makes conservative
assumptions instead, and executors fall back to the zone's baseline schedule.
"""

from __future__ import annotations

import copy
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from agent.actions import SENSOR_FIELDS


@dataclass(frozen=True)
class Isolation:
    zone_id: str
    sensor: str
    since: datetime
    reason: str
    alert_id: str | None = None


class SensorIsolationRegistry:
    """Thread-safe set of isolated (zone, sensor) pairs."""

    def __init__(self) -> None:
        self._items: dict[tuple[str, str], Isolation] = {}
        self._lock = threading.Lock()

    def isolate(self, zone_id: str, sensor: str, *, since: datetime, reason: str,
                alert_id: str | None = None) -> Isolation:
        if sensor not in SENSOR_FIELDS:
            raise ValueError(f"Unknown sensor '{sensor}'. Known: {list(SENSOR_FIELDS)}")
        with self._lock:
            item = self._items.setdefault((zone_id, sensor), Isolation(zone_id, sensor, since, reason, alert_id))
        return item

    def restore(self, zone_id: str, sensor: str) -> bool:
        with self._lock:
            return self._items.pop((zone_id, sensor), None) is not None

    def is_isolated(self, zone_id: str, sensor: str) -> bool:
        with self._lock:
            return (zone_id, sensor) in self._items

    def isolated(self, zone_id: str | None = None) -> list[Isolation]:
        with self._lock:
            return [i for (z, _), i in sorted(self._items.items()) if zone_id is None or z == zone_id]

    def mask(self, event: dict[str, Any]) -> dict[str, Any]:
        """Copy of a reading with the values of isolated sensors replaced by ``None``."""
        sensors = [i.sensor for i in self.isolated(event.get("zone_id"))]
        if not sensors:
            return event
        masked = copy.copy(event)
        for sensor in sensors:
            for name in SENSOR_FIELDS[sensor]:
                masked[name] = None
        masked["isolated_sensors"] = sensors
        return masked
