"""Per-reading security features (shared by training, detection and evaluation).

Each zone keeps its previous reading so that rates of change can be computed.
Rates are per minute, so they do not depend on the reading interval.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from database.models import SensorReadingRecord

FEATURES = (
    "temperature_c", "humidity_pct", "co2_ppm", "occupancy_count", "lighting_lux",
    "hvac_power_kw", "lighting_power_kw", "appliance_power_kw", "total_power_kw",
    "temp_minus_setpoint", "temp_minus_outdoor",
    "d_temperature_per_min", "d_co2_per_min", "d_occupancy", "d_total_power_per_min",
    "hour_sin", "hour_cos", "weekend",
)
# Feature groups used to explain Isolation Forest anomalies (attack classification).
FEATURE_GROUPS = {
    "temperature": ("temperature_c", "temp_minus_setpoint", "temp_minus_outdoor", "d_temperature_per_min"),
    "occupancy": ("occupancy_count", "d_occupancy", "co2_ppm", "d_co2_per_min"),
    "power": ("hvac_power_kw", "lighting_power_kw", "appliance_power_kw", "total_power_kw",
              "d_total_power_per_min"),
}
MAX_GAP_FACTOR = 1.5   # deltas are only computed between consecutive readings
CO2_TIME_CONSTANT_S = 1800.0   # CO2 follows occupancy with roughly this lag


@dataclass(frozen=True)
class FeatureRow:
    zone_id: str
    values: dict[str, Any]
    previous: SensorReadingRecord | None     # previous reading of the zone (None after a gap)


class FeatureTracker:
    """Keeps the last reading per zone and turns readings into feature rows."""

    def __init__(self) -> None:
        self._last: dict[str, SensorReadingRecord] = {}
        self._occupancy_ema: dict[str, float] = {}

    def update(self, reading: SensorReadingRecord) -> FeatureRow:
        previous = self._last.get(reading.zone_id)
        minutes = None
        if previous is not None:
            gap_s = (reading.event_time - previous.event_time).total_seconds()
            if 0 < gap_s <= MAX_GAP_FACTOR * reading.interval_seconds:
                minutes = gap_s / 60
        in_order = previous is None or reading.event_time > previous.event_time
        if in_order:
            self._last[reading.zone_id] = reading      # replayed (older) readings do not reset state
        ema = self._occupancy_ema.get(reading.zone_id)
        if ema is None or minutes is None:
            ema = float(reading.occupancy_count)
        else:
            alpha = 1 - math.exp(-minutes * 60 / CO2_TIME_CONSTANT_S)
            ema += alpha * (reading.occupancy_count - ema)
        if in_order:
            self._occupancy_ema[reading.zone_id] = ema
        active = reading.hvac_status in ("heating", "cooling")
        local = reading.event_time
        hour = local.hour + local.minute / 60
        outdoor = reading.outdoor_temperature_c if reading.outdoor_temperature_c is not None else reading.temperature_c

        def rate(name: str) -> float:
            if minutes is None:
                return 0.0
            return (getattr(reading, name) - getattr(previous, name)) / minutes

        values = {
            "temperature_c": reading.temperature_c,
            "humidity_pct": reading.humidity_pct,
            "co2_ppm": reading.co2_ppm,
            "occupancy_count": float(reading.occupancy_count),
            "lighting_lux": reading.lighting_lux,
            "hvac_power_kw": reading.hvac_power_kw,
            "lighting_power_kw": reading.lighting_power_kw,
            "appliance_power_kw": reading.appliance_power_kw,
            "total_power_kw": reading.total_power_kw,
            "temp_minus_setpoint": reading.temperature_c - reading.hvac_setpoint_c,
            "temp_minus_outdoor": reading.temperature_c - outdoor,
            "d_temperature_per_min": rate("temperature_c"),
            "d_co2_per_min": rate("co2_ppm"),
            "d_occupancy": (float(reading.occupancy_count - previous.occupancy_count)
                            if minutes is not None else 0.0),
            "d_total_power_per_min": rate("total_power_kw"),
            "hour_sin": math.sin(2 * math.pi * hour / 24),
            "hour_cos": math.cos(2 * math.pi * hour / 24),
            "weekend": float(local.weekday() >= 5),
            # Not Isolation Forest inputs (see FEATURES); used by rules and drift detection.
            "occupancy_ema": ema,
            "lighting_level_pct": reading.lighting_level_pct,
            "hvac_status": reading.hvac_status,
            "control_error_active": (reading.temperature_c - reading.hvac_setpoint_c) if active else math.nan,
        }
        return FeatureRow(reading.zone_id, values, previous if minutes is not None else None)
