"""Security alert generation (``security_alerts`` contract) with rate limiting.

Detections are evaluated per reading; *alerts* are what operators and the
agent see. To avoid alert storms, at most one alert per zone and alert kind
is emitted per ``cooldown_s`` of event time; suppressed detections are
counted in the next alert's details.

Alert types:
    attack      classified attack on individual readings (rules and/or Isolation Forest)
    anomaly     Isolation Forest anomaly without a clear attack signature
    drift       distributional drift of a window (KS + PSI), not a single reading:
                classification ``gradual_manipulation`` (security residuals shifted) or
                ``data_drift`` (raw input distributions changed, e.g. season/weather;
                relevant for retraining ML models, not an attack)
    validation  message rejected by schema/physical validation
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from security.attack_detector import RuleHit
from security.drift_detector import DriftResult

_NAMESPACE = uuid.UUID("9a4c2e71-0b3d-4f6a-8e5c-1d7b9f3a2c40")
SEVERITY = {"replay": "high", "temperature_spoofing": "high", "occupancy_manipulation": "medium",
            "abnormal_power": "medium", "unknown_anomaly": "low", "gradual_manipulation": "medium",
            "data_drift": "low", "invalid_message": "medium"}
DATA_DRIFT_COOLDOWN = timedelta(hours=6)
DESCRIPTIONS = {
    "replay": "Replay attack suspected: readings repeat earlier data or go back in time",
    "temperature_spoofing": "Temperature readings inconsistent with physics (possible spoofing)",
    "occupancy_manipulation": "Occupancy readings inconsistent with CO2/history (possible manipulation)",
    "abnormal_power": "Abnormal power readings for this zone",
    "unknown_anomaly": "Anomalous reading without a known attack signature",
}


class AlertManager:
    def __init__(self, cooldown_s: float = 900.0) -> None:
        self.cooldown = timedelta(seconds=cooldown_s)
        self._last: dict[tuple[str, str], datetime] = {}
        self._suppressed: dict[tuple[str, str], int] = defaultdict(int)
        self.emitted = 0
        self.suppressed = 0

    def _allow(self, zone_id: str, kind: str, when: datetime) -> tuple[bool, int]:
        key = (zone_id, kind)
        last = self._last.get(key)
        cooldown = DATA_DRIFT_COOLDOWN if kind == "data_drift" else self.cooldown
        if last is not None and abs(when - last) < cooldown:
            self._suppressed[key] += 1
            self.suppressed += 1
            return False, 0
        self._last[key] = when
        count, self._suppressed[key] = self._suppressed[key], 0
        self.emitted += 1
        return True, count

    def point_alert(self, *, zone_id: str, event_id: str, when: datetime, attack_type: str,
                    detector: str, hits: list[RuleHit], anomaly: tuple[bool, float, float] | None,
                    top_feature: tuple[str, str, float] | None) -> dict[str, Any] | None:
        allowed, suppressed = self._allow(zone_id, attack_type, when)
        if not allowed:
            return None
        score = anomaly[1] if anomaly else None
        return {
            "alert_id": str(uuid.uuid5(_NAMESPACE, f"{zone_id}|{event_id}|{attack_type}")),
            "zone_id": zone_id, "source_event_id": event_id, "detected_at": when.isoformat(),
            "alert_type": "anomaly" if attack_type == "unknown_anomaly" else "attack",
            "severity": SEVERITY[attack_type], "detector": detector, "score": score,
            "description": DESCRIPTIONS[attack_type],
            "details": {
                "classification": attack_type, "scope": "individual_reading",
                "rules": [{"rule": h.rule, "detail": h.detail} for h in hits],
                "isolation_forest": ({"score": anomaly[1], "threshold": anomaly[2], "anomalous": anomaly[0]}
                                     if anomaly else None),
                "most_unusual_feature": ({"group": top_feature[0], "feature": top_feature[1],
                                          "robust_z": round(top_feature[2], 2)} if top_feature else None),
                "suppressed_since_last_alert": suppressed,
            },
            "status": "open",
        }

    def drift_alert(self, result: DriftResult, classification: str = "gradual_manipulation") -> dict[str, Any] | None:
        allowed, suppressed = self._allow(result.zone_id, classification, result.window_end)
        if not allowed:
            return None
        drifting = [f for f in result.features if f.feature in result.drifting]
        worst = max(drifting, key=lambda f: f.psi)
        return {
            "alert_id": str(uuid.uuid5(_NAMESPACE, f"{result.zone_id}|{classification}|"
                                                   f"{result.window_end.isoformat()}")),
            "zone_id": result.zone_id, "source_event_id": None,
            "detected_at": result.window_end.isoformat(),
            "alert_type": "drift", "severity": SEVERITY[classification],
            "detector": "ks_test+psi", "score": worst.psi,
            "description": (
                f"Physical-consistency residuals ({', '.join(result.drifting)}) have shifted: possible "
                "gradual sensor manipulation or sensor fault" if classification == "gradual_manipulation"
                else f"Input distribution of {', '.join(result.drifting)} differs from the training data "
                     "(season, weather or usage change); review ML model validity"),
            "details": {
                "classification": classification, "scope": "distribution_window",
                "window_start": result.window_start.isoformat(), "window_end": result.window_end.isoformat(),
                "features": [{"feature": f.feature, "ks_statistic": round(f.ks_statistic, 4),
                              "ks_p_value": f.ks_p_value, "psi": round(f.psi, 4),
                              "mean_shift": round(f.mean_shift, 3)} for f in result.features],
                "suppressed_since_last_alert": suppressed,
            },
            "status": "open",
        }

    def validation_alert(self, *, zone_id: str | None, event_id: str | None, when: datetime,
                         errors: list[str]) -> dict[str, Any] | None:
        allowed, suppressed = self._allow(zone_id or "unknown", "validation", when)
        if not allowed:
            return None
        return {
            "alert_id": str(uuid.uuid5(_NAMESPACE, f"{zone_id}|{event_id}|validation|{when.isoformat()}")),
            "zone_id": zone_id, "source_event_id": None, "detected_at": when.isoformat(),
            "alert_type": "validation", "severity": SEVERITY["invalid_message"], "detector": "schema_validation",
            "score": None, "description": "Sensor message failed validation",
            "details": {"errors": errors[:10], "event_id": event_id,
                        "suppressed_since_last_alert": suppressed},
            "status": "open",
        }
