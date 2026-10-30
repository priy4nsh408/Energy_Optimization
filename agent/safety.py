"""Deterministic safety validator for every action the agent proposes.

The LLM never decides whether an action is safe. This module does, with fixed
rules and no randomness, so the same proposal in the same state always gets
the same verdict:

* well-formed: known zone, known action type, only expected parameters;
* fresh data: control actions need a zone reading at most ``max_data_age_s`` old;
* the Module 7 constraints (``optimization.constraints.validate_decision``):
  absolute setpoint limits, comfort/setback/equipment bands, step limit,
  egress safety lighting and illuminance for occupied zones;
* untrusted inputs are treated conservatively: an isolated or attacked
  occupancy sensor means "assume the zone is occupied";
* equipment rooms (server room) cannot have HVAC switched off, occupied zones
  cannot be switched off either;
* rate limit per zone; sensor isolation requires a matching alert as evidence
  and is capped per zone; restoring a sensor requires no recent alert.

Returning a zone to its baseline schedule (``clear_overrides``) is always
allowed: it is the safe fallback.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any

from agent.actions import ACTION_TYPES, SENSORS, ProposedAction
from agent.isolation import SensorIsolationRegistry
from config import AgentSettings, OptimizationSettings
from optimization.constraints import validate_decision
from optimization.hvac_optimizer import ZoneConditions, ZoneEnergyModel
from processors.data_validator import ValidationError, validate_sensor_event
from simulator.building_model import HVAC_MODES

PARAMETERS: dict[str, tuple[set[str], set[str]]] = {          # action -> (required, optional)
    "set_setpoint": ({"setpoint_c"}, set()),
    "set_hvac_mode": ({"hvac_mode"}, set()),
    "set_lighting": ({"lighting_level"}, set()),
    "clear_overrides": (set(), set()),
    "isolate_sensor": ({"sensor"}, set()),
    "restore_sensor": ({"sensor"}, set()),
}
OCCUPANCY_ATTACKS = {"occupancy_manipulation", "replay"}


def security_relevant(alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Alerts that can indicate manipulation (data drift is a model-validity signal, not an attack)."""
    return [a for a in alerts if (a.get("details") or {}).get("classification") != "data_drift"]


@dataclass(frozen=True)
class SafetyVerdict:
    approved: bool
    reasons: tuple[str, ...]

    @property
    def text(self) -> str:
        return "; ".join(self.reasons)


class SafetyValidator:
    def __init__(self, agent: AgentSettings, optimization: OptimizationSettings,
                 models: dict[str, ZoneEnergyModel], isolation: SensorIsolationRegistry) -> None:
        self.agent, self.optimization = agent, optimization
        self.models, self.isolation = models, isolation
        self._history: list[tuple[str, datetime]] = []         # (zone, time) of approved control actions
        self._lock = threading.Lock()

    # -- bookkeeping ------------------------------------------------------------------
    def record_approved(self, action: ProposedAction, at: datetime) -> None:
        with self._lock:
            self._history.append((action.zone_id, at))

    def _recent_count(self, zone_id: str, now: datetime) -> int:
        with self._lock:
            return sum(1 for z, t in self._history if z == zone_id and now - timedelta(hours=1) < t <= now)

    # -- validation -------------------------------------------------------------------
    def validate(self, action: ProposedAction, *, latest_event: dict[str, Any] | None, now: datetime,
                 zone_alerts: list[dict[str, Any]]) -> SafetyVerdict:
        problems = self._structure(action)
        if problems:
            return SafetyVerdict(False, tuple(problems))
        if action.action_type in ("isolate_sensor", "restore_sensor"):
            problems = self._sensor_action(action, zone_alerts, now)
        elif action.action_type == "clear_overrides":
            problems = []
        else:
            problems = self._control_action(action, latest_event, now, zone_alerts)
        if not problems and self._recent_count(action.zone_id, now) >= self.agent.max_actions_per_zone_per_hour:
            problems.append(f"rate limit: already {self.agent.max_actions_per_zone_per_hour} approved "
                            f"actions for {action.zone_id} in the last hour")
        if problems:
            return SafetyVerdict(False, tuple(problems))
        return SafetyVerdict(True, (self._approval_text(action),))

    def _structure(self, action: ProposedAction) -> list[str]:
        if action.action_type not in ACTION_TYPES:
            return [f"unknown action type '{action.action_type}'"]
        if action.zone_id not in self.models:
            return [f"unknown zone '{action.zone_id}'"]
        required, optional = PARAMETERS[action.action_type]
        given = set(action.parameters)
        problems = [f"missing parameter '{p}'" for p in sorted(required - given)]
        problems += [f"unexpected parameter '{p}'" for p in sorted(given - required - optional)]
        if not action.reason.strip():
            problems.append("a reason is required")
        return problems

    def _sensor_action(self, action: ProposedAction, zone_alerts: list[dict[str, Any]],
                       now: datetime) -> list[str]:
        sensor = action.parameters["sensor"]
        if sensor not in SENSORS:
            return [f"unknown sensor '{sensor}' (known: {', '.join(SENSORS)})"]
        isolated = self.isolation.is_isolated(action.zone_id, sensor)
        if action.action_type == "restore_sensor":
            if not isolated:
                return [f"{sensor} sensor in {action.zone_id} is not isolated"]
            recent = [a for a in security_relevant(zone_alerts)
                      if datetime.fromisoformat(str(a["detected_at"])) >= now - timedelta(hours=1)]
            return [f"{len(recent)} security alert(s) for {action.zone_id} in the last hour; "
                    "restore only after the zone has been quiet for an hour"] if recent else []
        if isolated:
            return [f"{sensor} sensor in {action.zone_id} is already isolated"]
        alert_ids = {str(a.get("alert_id")) for a in zone_alerts}
        if not action.related_alert_id or action.related_alert_id not in alert_ids:
            return ["sensor isolation needs a related alert for this zone as evidence"]
        count = len(self.isolation.isolated(action.zone_id))
        if count >= self.agent.max_isolated_sensors_per_zone:
            return [f"{count} sensors already isolated in {action.zone_id} "
                    f"(maximum {self.agent.max_isolated_sensors_per_zone})"]
        return []

    def _conditions(self, event: dict[str, Any], zone_alerts: list[dict[str, Any]]) -> ZoneConditions:
        conditions = ZoneConditions.from_reading(validate_sensor_event(event))
        attacked = {(a.get("details") or {}).get("classification") for a in zone_alerts}
        if (self.isolation.is_isolated(conditions.zone_id, "occupancy") or attacked & OCCUPANCY_ATTACKS) \
                and conditions.occupancy == 0:
            conditions = replace(conditions, occupancy=1)          # conservative: assume occupied
        return conditions

    def _control_action(self, action: ProposedAction, event: dict[str, Any] | None, now: datetime,
                        zone_alerts: list[dict[str, Any]]) -> list[str]:
        if event is None:
            return [f"no reading for {action.zone_id}"]
        age = (now - datetime.fromisoformat(str(event["timestamp"]))).total_seconds()
        if age > self.agent.max_data_age_s:
            return [f"latest reading is {age / 60:.0f} min old (maximum {self.agent.max_data_age_s / 60:.0f} min)"]
        try:
            conditions = self._conditions(event, zone_alerts)
        except ValidationError as exc:
            return [f"latest reading is invalid: {'; '.join(exc.errors)}"]
        model, p = self.models[action.zone_id], action.parameters
        if action.action_type == "set_setpoint":
            value = p["setpoint_c"]
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                return ["setpoint_c must be a number"]
            return validate_decision(self.optimization, model, conditions, float(value), conditions.lighting_level)
        if action.action_type == "set_lighting":
            value = p["lighting_level"]
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                return ["lighting_level must be a number between 0 and 1"]
            return [v for v in validate_decision(self.optimization, model, conditions, conditions.setpoint_c,
                                                 float(value)) if not v.startswith("setpoint")]
        mode = p["hvac_mode"]
        if mode not in HVAC_MODES:
            return [f"hvac_mode must be one of {list(HVAC_MODES)}"]
        if mode == "off" and model.critical:
            return ["HVAC cannot be switched off in an equipment room"]
        if mode == "off" and conditions.occupancy > 0:
            return ["HVAC cannot be switched off in an occupied zone (or one with untrusted occupancy)"]
        return []

    @staticmethod
    def _approval_text(action: ProposedAction) -> str:
        if action.action_type == "clear_overrides":
            return "return to baseline schedule is always permitted (safe fallback)"
        if action.action_type in ("isolate_sensor", "restore_sensor"):
            return "sensor quarantine rules satisfied (software only, no equipment change)"
        return "within absolute limits, comfort/equipment band, step limit, lighting safety and rate limit"
