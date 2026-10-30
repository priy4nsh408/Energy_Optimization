"""Validation of incoming Kafka messages.

Each ``validate_*`` function checks one message type against its contract
(required fields, types, physical ranges, consistency) and returns a typed
record ready for the database. All problems are collected and raised
together in a :class:`ValidationError`.

Scope: these checks reject malformed or physically impossible data. Values
that are *plausible but wrong* (e.g. a spoofed but in-range temperature) are
the job of the security/anomaly-detection module, not of this validator.

Message contracts for the other topics (``weather_data``, ``carbon_intensity``,
``security_alerts``, ``optimization_commands``, ``agent_actions``) are defined
here so that later modules publish compatible messages; see README.
"""

from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any
from uuid import UUID

from database.models import (
    CarbonIntensityRecord,
    EnergyPredictionRecord,
    OptimizationActionRecord,
    SecurityAlertRecord,
    SensorReadingRecord,
    WeatherRecord,
)

ZONE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
HVAC_MODES = {"auto", "heat", "cool", "off"}
HVAC_STATUSES = {"heating", "cooling", "idle", "off"}
ALERT_TYPES = {"anomaly", "attack", "drift", "validation"}
SEVERITIES = {"low", "medium", "high", "critical"}
ALERT_STATUSES = {"open", "acknowledged", "resolved"}
ACTION_SOURCES = {"optimizer", "agent", "operator", "baseline"}
ACTION_TYPES = {"set_setpoint", "set_hvac_mode", "set_lighting", "clear_overrides"}
ACTION_STATUSES = {"proposed", "approved", "rejected", "executed", "failed"}
POWER_SUM_TOLERANCE_KW = 0.01
POWER_SUM_TOLERANCE_REL = 0.01


class ValidationError(ValueError):
    """A message violates its contract."""

    def __init__(self, errors: list[str]) -> None:
        super().__init__("; ".join(errors))
        self.errors = errors


class _Checker:
    """Collects errors while extracting typed fields from a message."""

    def __init__(self, message: Any) -> None:
        self.errors: list[str] = []
        if not isinstance(message, dict):
            self.errors.append(f"message must be a JSON object, got {type(message).__name__}")
            message = {}
        self.message = message

    def _get(self, name: str, required: bool) -> Any:
        value = self.message.get(name)
        if value is None and required:
            self.errors.append(f"{name}: missing")
        return value

    def number(self, name: str, low: float | None = None, high: float | None = None,
               *, required: bool = True) -> float | None:
        value = self._get(name, required)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            self.errors.append(f"{name}: must be a number, got {type(value).__name__}")
            return None
        if not math.isfinite(value):
            self.errors.append(f"{name}: must be finite")
            return None
        if (low is not None and value < low) or (high is not None and value > high):
            self.errors.append(f"{name}: {value} outside [{low}, {high}]")
            return None
        return float(value)

    def integer(self, name: str, low: int | None = None, high: int | None = None,
                *, required: bool = True) -> int | None:
        value = self._get(name, required)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            if isinstance(value, float) and value.is_integer():
                value = int(value)
            else:
                self.errors.append(f"{name}: must be an integer")
                return None
        if (low is not None and value < low) or (high is not None and value > high):
            self.errors.append(f"{name}: {value} outside [{low}, {high}]")
            return None
        return value

    def text(self, name: str, *, required: bool = True, choices: set[str] | None = None,
             pattern: re.Pattern | None = None, max_length: int = 500) -> str | None:
        value = self._get(name, required)
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            self.errors.append(f"{name}: must be a non-empty string")
            return None
        if len(value) > max_length:
            self.errors.append(f"{name}: longer than {max_length} characters")
            return None
        if choices is not None and value not in choices:
            self.errors.append(f"{name}: '{value}' not in {sorted(choices)}")
            return None
        if pattern is not None and not pattern.match(value):
            self.errors.append(f"{name}: '{value}' has an invalid format")
            return None
        return value

    def uuid(self, name: str, *, required: bool = True) -> UUID | None:
        value = self._get(name, required)
        if value is None:
            return None
        try:
            return UUID(str(value))
        except ValueError:
            self.errors.append(f"{name}: '{value}' is not a valid UUID")
            return None

    def timestamp(self, name: str, *, required: bool = True) -> datetime | None:
        value = self._get(name, required)
        if value is None:
            return None
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            self.errors.append(f"{name}: '{value}' is not an ISO 8601 timestamp")
            return None
        if parsed.tzinfo is None:
            self.errors.append(f"{name}: timestamp must include a UTC offset")
            return None
        return parsed

    def boolean(self, name: str, default: bool) -> bool:
        value = self.message.get(name, default)
        if not isinstance(value, bool):
            self.errors.append(f"{name}: must be true or false")
            return default
        return value

    def mapping(self, name: str) -> dict[str, Any]:
        value = self.message.get(name, {})
        if value is None:
            return {}
        if not isinstance(value, dict):
            self.errors.append(f"{name}: must be a JSON object")
            return {}
        return value

    def raise_if_invalid(self) -> None:
        if self.errors:
            raise ValidationError(self.errors)


def validate_sensor_event(message: Any, *, kafka_partition: int | None = None,
                          kafka_offset: int | None = None) -> SensorReadingRecord:
    """Validate a ``building_sensor_data`` event (schema version 1)."""
    c = _Checker(message)
    c.integer("schema_version", 1, 1)
    record = dict(
        event_id=c.uuid("event_id"),
        building_id=c.text("building_id", max_length=64),
        zone_id=c.text("zone_id", pattern=ZONE_ID_PATTERN),
        zone_type=c.text("zone_type", required=False, max_length=64),
        sequence=c.integer("sequence", 1),
        event_time=c.timestamp("timestamp"),
        emitted_at=c.timestamp("emitted_at", required=False),
        interval_seconds=c.number("interval_seconds", 0.001, 86_400),
        temperature_c=c.number("temperature_c", -40, 85),
        humidity_pct=c.number("humidity_pct", 0, 100),
        co2_ppm=c.number("co2_ppm", 0, 10_000),
        occupancy_count=c.integer("occupancy_count", 0, 10_000),
        lighting_lux=c.number("lighting_lux", 0, 200_000),
        lighting_level_pct=c.number("lighting_level_pct", 0, 100),
        hvac_mode=c.text("hvac_mode", choices=HVAC_MODES),
        hvac_status=c.text("hvac_status", choices=HVAC_STATUSES),
        hvac_setpoint_c=c.number("hvac_setpoint_c", 5, 40),
        hvac_power_kw=c.number("hvac_power_kw", 0, 10_000),
        lighting_power_kw=c.number("lighting_power_kw", 0, 10_000),
        appliance_power_kw=c.number("appliance_power_kw", 0, 10_000),
        total_power_kw=c.number("total_power_kw", 0, 10_000),
        energy_kwh=c.number("energy_kwh", 0, 1_000_000),
        outdoor_temperature_c=c.number("outdoor_temperature_c", -60, 60, required=False),
        outdoor_humidity_pct=c.number("outdoor_humidity_pct", 0, 100, required=False),
        kafka_partition=kafka_partition,
        kafka_offset=kafka_offset,
    )
    parts = [record["hvac_power_kw"], record["lighting_power_kw"], record["appliance_power_kw"]]
    total = record["total_power_kw"]
    if total is not None and None not in parts:
        tolerance = POWER_SUM_TOLERANCE_KW + POWER_SUM_TOLERANCE_REL * total
        if abs(sum(parts) - total) > tolerance:
            c.errors.append(f"total_power_kw: {total} does not equal the sum of components "
                            f"({sum(parts):.3f})")
    if record["hvac_status"] == "off" and record["hvac_mode"] not in (None, "off"):
        c.errors.append("hvac_status: 'off' is only valid when hvac_mode is 'off'")
    c.raise_if_invalid()
    return SensorReadingRecord(**record)


def validate_weather_message(message: Any) -> WeatherRecord:
    """Contract for ``weather_data``."""
    c = _Checker(message)
    record = WeatherRecord(
        event_id=c.uuid("event_id"),
        building_id=c.text("building_id", max_length=64),
        observed_at=c.timestamp("observed_at"),
        source=c.text("source", max_length=64),
        is_forecast=c.boolean("is_forecast", False),
        temperature_c=c.number("temperature_c", -60, 60),
        humidity_pct=c.number("humidity_pct", 0, 100, required=False),
        wind_speed_ms=c.number("wind_speed_ms", 0, 120, required=False),
        solar_irradiance_w_m2=c.number("solar_irradiance_w_m2", 0, 1_500, required=False),
        cloud_cover_pct=c.number("cloud_cover_pct", 0, 100, required=False),
    )
    c.raise_if_invalid()
    return record


def validate_carbon_message(message: Any) -> CarbonIntensityRecord:
    """Contract for ``carbon_intensity``."""
    c = _Checker(message)
    record = CarbonIntensityRecord(
        event_id=c.uuid("event_id"),
        region=c.text("region", max_length=64),
        observed_at=c.timestamp("observed_at"),
        source=c.text("source", max_length=64),
        is_forecast=c.boolean("is_forecast", False),
        intensity_g_per_kwh=c.number("intensity_g_per_kwh", 0, 3_000),
    )
    c.raise_if_invalid()
    return record


def validate_alert_message(message: Any) -> SecurityAlertRecord:
    """Contract for ``security_alerts``."""
    c = _Checker(message)
    status = c.text("status", required=False, choices=ALERT_STATUSES) or "open"
    record = SecurityAlertRecord(
        alert_id=c.uuid("alert_id"),
        zone_id=c.text("zone_id", required=False, pattern=ZONE_ID_PATTERN),
        source_event_id=c.uuid("source_event_id", required=False),
        detected_at=c.timestamp("detected_at"),
        alert_type=c.text("alert_type", choices=ALERT_TYPES),
        severity=c.text("severity", choices=SEVERITIES),
        detector=c.text("detector", max_length=100),
        score=c.number("score", required=False),
        description=c.text("description", max_length=2_000),
        details=c.mapping("details"),
        status=status,
        resolved_at=c.timestamp("resolved_at", required=False),
    )
    c.raise_if_invalid()
    return record


def validate_action_message(message: Any, *, topic: str | None = None) -> OptimizationActionRecord:
    """Contract for ``optimization_commands`` and ``agent_actions``."""
    c = _Checker(message)
    created_at = c.timestamp("created_at")
    updated_at = c.timestamp("updated_at", required=False) or created_at
    record = OptimizationActionRecord(
        action_id=c.uuid("action_id"),
        zone_id=c.text("zone_id", pattern=ZONE_ID_PATTERN),
        created_at=created_at,
        updated_at=updated_at,
        source=c.text("source", choices=ACTION_SOURCES),
        action_type=c.text("action_type", choices=ACTION_TYPES),
        parameters=c.mapping("parameters"),
        status=c.text("status", choices=ACTION_STATUSES),
        reason=c.text("reason", required=False, max_length=2_000),
        safety_reason=c.text("safety_reason", required=False, max_length=2_000),
        related_alert_id=c.uuid("related_alert_id", required=False),
        kafka_topic=topic,
    )
    c.raise_if_invalid()
    return record


def validate_prediction_message(message: Any) -> EnergyPredictionRecord:
    """Contract for ``energy_predictions``."""
    c = _Checker(message)
    record = dict(
        prediction_id=c.uuid("prediction_id"),
        source_event_id=c.uuid("source_event_id"),
        building_id=c.text("building_id", max_length=64),
        zone_id=c.text("zone_id", pattern=ZONE_ID_PATTERN),
        event_time=c.timestamp("event_time"),
        predicted_for=c.timestamp("predicted_for"),
        horizon_seconds=c.number("horizon_seconds", 0.001, 86_400),
        predicted_power_kw=c.number("predicted_power_kw", 0, 10_000),
        predicted_energy_kwh=c.number("predicted_energy_kwh", 0, 1_000_000),
        model_name=c.text("model_name", max_length=100),
        model_version=c.text("model_version", max_length=100),
        created_at=c.timestamp("created_at"),
    )
    if record["event_time"] and record["predicted_for"] and record["predicted_for"] <= record["event_time"]:
        c.errors.append("predicted_for: must be after event_time")
    c.raise_if_invalid()
    return EnergyPredictionRecord(**record)
