"""Typed records written to PostgreSQL.

Field names match the table columns in ``schema.sql``. Processors build these
records from validated Kafka messages; repositories persist them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class ZoneRecord:
    zone_id: str
    building_id: str
    name: str
    zone_type: str
    floor_area_m2: float
    max_occupancy: int
    occupied_setpoint_c: float | None = None


@dataclass(frozen=True)
class SensorReadingRecord:
    event_id: UUID
    building_id: str
    zone_id: str
    zone_type: str | None
    sequence: int
    event_time: datetime
    emitted_at: datetime | None
    interval_seconds: float
    temperature_c: float
    humidity_pct: float
    co2_ppm: float
    occupancy_count: int
    lighting_lux: float
    lighting_level_pct: float
    hvac_mode: str
    hvac_status: str
    hvac_setpoint_c: float
    hvac_power_kw: float
    lighting_power_kw: float
    appliance_power_kw: float
    total_power_kw: float
    energy_kwh: float
    outdoor_temperature_c: float | None
    outdoor_humidity_pct: float | None
    kafka_partition: int | None = None
    kafka_offset: int | None = None


@dataclass(frozen=True)
class EnergyRecord:
    event_id: UUID
    building_id: str
    zone_id: str
    event_time: datetime
    interval_seconds: float
    energy_kwh: float
    hvac_energy_kwh: float
    lighting_energy_kwh: float
    appliance_energy_kwh: float
    total_power_kw: float
    carbon_intensity_g_per_kwh: float | None
    carbon_kg_co2e: float | None
    temperature_c: float
    outdoor_temperature_c: float | None
    occupancy_count: int
    hvac_status: str
    hour_of_day: int
    day_of_week: int
    is_weekend: bool
    hour_sin: float
    hour_cos: float
    indoor_outdoor_delta_c: float | None
    setpoint_deviation_c: float
    occupancy_ratio: float | None
    power_density_w_m2: float | None
    hvac_share: float | None
    power_change_kw: float | None
    temperature_rate_c_per_h: float | None
    rolling_mean_power_kw: float
    rolling_std_power_kw: float
    rolling_max_power_kw: float
    rolling_window_size: int


@dataclass(frozen=True)
class WeatherRecord:
    event_id: UUID
    building_id: str
    observed_at: datetime
    source: str
    is_forecast: bool
    temperature_c: float
    humidity_pct: float | None = None
    wind_speed_ms: float | None = None
    solar_irradiance_w_m2: float | None = None
    cloud_cover_pct: float | None = None


@dataclass(frozen=True)
class CarbonIntensityRecord:
    event_id: UUID
    region: str
    observed_at: datetime
    source: str
    is_forecast: bool
    intensity_g_per_kwh: float


@dataclass(frozen=True)
class SecurityAlertRecord:
    alert_id: UUID
    zone_id: str | None
    source_event_id: UUID | None
    detected_at: datetime
    alert_type: str
    severity: str
    detector: str
    score: float | None
    description: str
    details: dict[str, Any] = field(default_factory=dict)
    status: str = "open"
    resolved_at: datetime | None = None


@dataclass(frozen=True)
class OptimizationActionRecord:
    action_id: UUID
    zone_id: str
    created_at: datetime
    updated_at: datetime
    source: str
    action_type: str
    parameters: dict[str, Any]
    status: str
    reason: str | None = None
    safety_reason: str | None = None
    related_alert_id: UUID | None = None
    kafka_topic: str | None = None


@dataclass(frozen=True)
class InvalidEventRecord:
    worker: str
    topic: str
    kafka_partition: int
    kafka_offset: int
    event_id: str | None
    zone_id: str | None
    errors: list[str]
    payload: dict[str, Any]


@dataclass(frozen=True)
class EnergyPredictionRecord:
    prediction_id: UUID
    source_event_id: UUID
    building_id: str
    zone_id: str
    event_time: datetime
    predicted_for: datetime
    horizon_seconds: float
    predicted_power_kw: float
    predicted_energy_kwh: float
    model_name: str
    model_version: str
    created_at: datetime
