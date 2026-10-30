"""Security monitor: runs every detector on one sensor event.

Used unchanged by the Kafka service (``workers.security_service``) and the
offline evaluation (``experiments.cyberattack_experiment``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from processors.data_validator import ValidationError, validate_sensor_event
from security.alert_manager import AlertManager
from security.anomaly_detector import AnomalyDetector
from security.attack_detector import RuleEngine, RuleHit, ZoneRuleLimits, classify
from security.drift_detector import (
    DATA_DRIFT_FEATURES,
    SECURITY_DRIFT_FEATURES,
    DriftDetector,
    DriftReference,
    DriftResult,
)
from security.features import FeatureTracker

_ZONE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
SECURITY_DRIFT_EFFECT_SIZE = 0.5


@dataclass
class SecurityModels:
    anomaly: AnomalyDetector
    limits: dict[str, ZoneRuleLimits]
    reference: DriftReference
    metadata: dict[str, Any] = field(default_factory=dict)
    disabled_drift_monitors: frozenset = frozenset()   # (zone, feature) pairs


@dataclass(frozen=True)
class PointDetection:
    attack_type: str | None          # None = reading looks normal
    detector: str
    hits: tuple[RuleHit, ...]
    anomaly: tuple[bool, float, float] | None
    top_feature: tuple[str, str, float] | None

    @property
    def flagged(self) -> bool:
        return self.attack_type is not None


@dataclass
class MonitorOutput:
    valid: bool
    detection: PointDetection | None
    drift: DriftResult | None            # security drift (possible manipulation)
    alerts: list[dict[str, Any]]
    data_drift: DriftResult | None = None   # input-distribution drift (model monitoring)


class SecurityMonitor:
    def __init__(self, models: SecurityModels, *, cooldown_s: float = 900.0, **drift_options: Any) -> None:
        self.models = models
        self.features = FeatureTracker()
        self.rules = RuleEngine(models.limits)
        self.drift = DriftDetector(models.reference, features=SECURITY_DRIFT_FEATURES,
                                   min_effect_size=SECURITY_DRIFT_EFFECT_SIZE,
                                   disabled=models.disabled_drift_monitors, **drift_options)
        self.data_drift = DriftDetector(models.reference, features=DATA_DRIFT_FEATURES, **drift_options)
        self.alerts = AlertManager(cooldown_s)

    def process(self, event: dict[str, Any]) -> MonitorOutput:
        try:
            reading = validate_sensor_event(event)
        except ValidationError as exc:
            zone = event.get("zone_id") if isinstance(event, dict) else None
            alert = self.alerts.validation_alert(
                zone_id=zone if isinstance(zone, str) and _ZONE.match(zone) else None,
                event_id=str(event.get("event_id"))[:100] if isinstance(event, dict) else None,
                when=datetime.now(timezone.utc), errors=exc.errors)
            return MonitorOutput(False, None, None, [alert] if alert else [])


        row = self.features.update(reading)
        limits = self.models.limits.get(reading.zone_id)
        if limits is not None and limits.co2_model is not None:
            row.values["co2_residual"] = limits.co2_model.residual(row.values)
        hits = self.rules.check(reading, row)
        anomaly = self.models.anomaly.evaluate(reading.zone_id, row.values)
        model = self.models.anomaly.models.get(reading.zone_id)
        top = model.top_group(row.values) if (model and anomaly and anomaly[0]) else None
        attack_type, detector = classify(hits, anomaly, top)
        detection = PointDetection(attack_type, detector, tuple(hits), anomaly, top)

        alerts = []
        if attack_type:
            alert = self.alerts.point_alert(
                zone_id=reading.zone_id, event_id=str(reading.event_id), when=reading.event_time,
                attack_type=attack_type, detector=detector, hits=hits, anomaly=anomaly, top_feature=top)
            if alert:
                alerts.append(alert)

        drift = data_drift = None
        replayed_old = any(h.rule == "timestamp_regression" for h in hits)
        if not replayed_old:          # out-of-order messages would corrupt the windows
            drift = self.drift.update(reading.zone_id, reading.event_time, row.values)
            if drift is not None and drift.is_drift:
                alert = self.alerts.drift_alert(drift, "gradual_manipulation")
                if alert:
                    alerts.append(alert)
            data_drift = self.data_drift.update(reading.zone_id, reading.event_time, row.values)
            if data_drift is not None and data_drift.is_drift:
                alert = self.alerts.drift_alert(data_drift, "data_drift")
                if alert:
                    alerts.append(alert)
        return MonitorOutput(True, detection, drift, alerts, data_drift)
