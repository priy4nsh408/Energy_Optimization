"""Which alerts start an agent investigation.

Alerts below ``AGENT_MIN_ALERT_SEVERITY`` are not investigated, and the same
zone + classification is investigated at most once per
``AGENT_INCIDENT_COOLDOWN_S`` (alert time), so a persistent attack does not
trigger a new agent run - and new LLM costs - for every reading (an
investigation that took no action is re-checked after
``AGENT_UNCONFIRMED_RECHECK_S`` if alerts continue). An incident
whose sensor isolations are still in place is *contained* and is not
re-investigated until the sensors are restored.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Any

from agent.data import SEVERITY_RANK
from agent.isolation import SensorIsolationRegistry
from config import AgentSettings
from database.models import AgentDecisionRecord


def incident_key(alert: dict[str, Any]) -> tuple[str | None, str]:
    return alert.get("zone_id"), str((alert.get("details") or {}).get("classification", alert.get("alert_type")))


class IncidentDispatcher:
    def __init__(self, settings: AgentSettings, isolation: SensorIsolationRegistry | None = None) -> None:
        self.settings, self.isolation = settings, isolation
        self._last: dict[tuple[str | None, str], datetime] = {}
        self._contained: dict[tuple[str | None, str], list[tuple[str, str]]] = {}
        self._acted: dict[tuple[str | None, str], bool] = {}
        self._lock = threading.Lock()

    def remember(self, alert: dict[str, Any], decision: AgentDecisionRecord) -> None:
        """Note which isolations an investigation executed (they contain the incident)."""
        isolated = [(a["zone_id"], a["parameters"]["sensor"]) for a in decision.actions
                    if a["action_type"] == "isolate_sensor" and a["status"] == "executed"]
        with self._lock:
            self._acted[incident_key(alert)] = any(a["status"] in ("executed", "approved") for a in decision.actions)
            if isolated:
                self._contained[incident_key(alert)] = isolated

    def should_investigate(self, alert: dict[str, Any]) -> tuple[bool, str]:
        severity = str(alert.get("severity", "low"))
        if SEVERITY_RANK.get(severity, 0) < SEVERITY_RANK[self.settings.min_alert_severity]:
            return False, f"severity {severity} below {self.settings.min_alert_severity}"
        key = incident_key(alert)
        when = datetime.fromisoformat(str(alert["detected_at"]))
        with self._lock:
            contained = self._contained.get(key)
            if contained and self.isolation and all(self.isolation.is_isolated(z, s) for z, s in contained):
                return False, "contained: sensor already isolated"
            last = self._last.get(key)
            cooldown = (self.settings.incident_cooldown_s if self._acted.get(key, True)
                        else min(self.settings.incident_cooldown_s, self.settings.unconfirmed_recheck_s))
            if last is not None and timedelta(0) <= when - last < timedelta(seconds=cooldown):
                return False, f"same incident investigated at {last.isoformat()}"
            self._last[key] = when
        return True, "investigate"
