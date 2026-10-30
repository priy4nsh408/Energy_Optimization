"""Feature engineering for energy records.

Computes per-reading features used by the ML and optimisation modules:

* Calendar: hour of day, day of week, weekend flag, cyclic hour encoding
  (local building time, taken from the event timestamp's UTC offset).
* Thermal: indoor/outdoor temperature difference, deviation from setpoint,
  temperature rate of change.
* Usage: occupancy ratio, power density (W/m²), HVAC share of power.
* Temporal: power change since the previous reading and rolling mean/std/max
  of power over the last ``window`` readings of the same zone.

State is kept per zone. Duplicates (same ``event_id``) do not modify the
state. The history is reset when time jumps forward by more than
``max_gap_s`` or goes backwards (a new simulator session, or replayed data,
which the security module flags separately), so rolling features never mix
unrelated periods. After a restart the state is rebuilt from the database
(:meth:`FeatureEngineer.warm_start`).
"""

from __future__ import annotations

import math
import statistics
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable
from uuid import UUID

from database.models import SensorReadingRecord, ZoneRecord

DEFAULT_WINDOW = 12
DEFAULT_MAX_GAP_S = 3600.0


@dataclass(frozen=True)
class _Point:
    event_id: UUID
    event_time: datetime
    power_kw: float
    temperature_c: float


class FeatureEngineer:
    """Stateful per-zone feature computation."""

    def __init__(self, zones: dict[str, ZoneRecord] | None = None, *,
                 window: int = DEFAULT_WINDOW, max_gap_s: float = DEFAULT_MAX_GAP_S) -> None:
        if window < 1:
            raise ValueError("window must be >= 1")
        self.zones = dict(zones or {})
        self.window = window
        self.max_gap_s = max_gap_s
        self._history: dict[str, deque[_Point]] = {}

    def warm_start(self, rows: Iterable[dict[str, Any]]) -> int:
        """Load recent points (oldest first) with keys event_id, zone_id, event_time,
        total_power_kw, temperature_c. Returns the number of points loaded."""
        count = 0
        for row in rows:
            self._history.setdefault(row["zone_id"], deque(maxlen=self.window)).append(_Point(
                UUID(str(row["event_id"])), row["event_time"],
                float(row["total_power_kw"]), float(row["temperature_c"])))
            count += 1
        return count

    def history_size(self, zone_id: str) -> int:
        return len(self._history.get(zone_id, ()))

    def compute(self, reading: SensorReadingRecord) -> dict[str, Any]:
        """Return the feature dict for ``reading`` and update the zone state."""
        history = self._history.setdefault(reading.zone_id, deque(maxlen=self.window))
        duplicate = any(p.event_id == reading.event_id for p in history)
        previous = history[-1] if history else None
        gap_s = (reading.event_time - previous.event_time).total_seconds() if previous else None

        if not duplicate and gap_s is not None and (gap_s <= 0 or gap_s > self.max_gap_s):
            history.clear()          # time went backwards or long gap: start a new window
            previous, gap_s = None, None

        power_change = temperature_rate = None
        if previous is not None and not duplicate and gap_s:
            power_change = reading.total_power_kw - previous.power_kw
            temperature_rate = (reading.temperature_c - previous.temperature_c) / (gap_s / 3600)

        window_values = [p.power_kw for p in history]
        if not duplicate:
            window_values.append(reading.total_power_kw)
            history.append(_Point(reading.event_id, reading.event_time,
                                  reading.total_power_kw, reading.temperature_c))
        window_values = window_values[-self.window:]

        local_time = reading.event_time
        hour = local_time.hour + local_time.minute / 60 + local_time.second / 3600
        zone = self.zones.get(reading.zone_id)
        total = reading.total_power_kw
        return {
            "hour_of_day": local_time.hour,
            "day_of_week": local_time.weekday(),
            "is_weekend": local_time.weekday() >= 5,
            "hour_sin": math.sin(2 * math.pi * hour / 24),
            "hour_cos": math.cos(2 * math.pi * hour / 24),
            "indoor_outdoor_delta_c": (reading.temperature_c - reading.outdoor_temperature_c
                                       if reading.outdoor_temperature_c is not None else None),
            "setpoint_deviation_c": reading.temperature_c - reading.hvac_setpoint_c,
            "occupancy_ratio": (reading.occupancy_count / zone.max_occupancy
                                if zone and zone.max_occupancy > 0 else None),
            "power_density_w_m2": total * 1000 / zone.floor_area_m2 if zone else None,
            "hvac_share": reading.hvac_power_kw / total if total > 0 else None,
            "power_change_kw": power_change,
            "temperature_rate_c_per_h": temperature_rate,
            "rolling_mean_power_kw": statistics.fmean(window_values),
            "rolling_std_power_kw": statistics.pstdev(window_values),
            "rolling_max_power_kw": max(window_values),
            "rolling_window_size": len(window_values),
        }
