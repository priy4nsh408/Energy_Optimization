"""Plain-language explanation of an action that waits for operator approval.

Built from facts only (the alert, the agent's assessment, the zone's latest
reading and the action itself). A deterministic template always produces the
full explanation; if a small language model is configured
(``SLM_PROVIDER=ollama``) it rewrites the facts as a short summary for the
operator. The SLM explains; it does not decide: the safety validator has
already approved the action and the operator makes the final call.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from agent.actions import ProposedAction
from agent.slm import OllamaSLM, SLMError
from simulator.building_model import default_zones

logger = logging.getLogger(__name__)

CLASSIFICATIONS = {
    "temperature_spoofing": "temperature spoofing (fake temperature readings)",
    "occupancy_manipulation": "occupancy manipulation (fake people counts)",
    "replay": "a replay attack (old readings sent again)",
    "abnormal_power": "abnormal power readings",
    "gradual_manipulation": "gradual manipulation (readings pushed off slowly)",
    "data_drift": "data drift (conditions changed since training)",
    "unknown_anomaly": "an unusual reading",
}
SENSORS = {"temperature": "temperature", "occupancy": "people-count", "power_meter": "power-meter", "co2": "CO2",
           "humidity": "humidity", "lighting": "light"}
SLM_SYSTEM = (
    "You explain a building-security action to a non-expert operator, who must approve or decline it. "
    "Use only the facts you are given; never add numbers or claims that are not in them. Write 3 or 4 short, "
    "plain sentences: what was detected, what the action does, how it affects energy use and comfort, and what "
    "happens if it is declined. End with the question: Do you want to allow this action? No markdown, no lists.")


def zone_names() -> dict[str, str]:
    return {z.zone_id: z.name for z in default_zones()}


def _num(value: Any, digits: int = 1) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "unknown"


class ActionExplainer:
    def __init__(self, data=None, slm: OllamaSLM | None = None) -> None:
        self.data, self.slm = data, slm
        self.names = zone_names()

    def explain(self, action: ProposedAction, alert: dict[str, Any] | None = None,
                assessment: dict[str, Any] | None = None) -> dict[str, Any]:
        facts = self.facts(action, alert, assessment)
        explanation = {**self._template(action, facts), "explained_by": "built-in template"}
        if self.slm is not None:
            try:
                explanation["summary"] = self.slm.generate(SLM_SYSTEM, json.dumps(facts, ensure_ascii=False))[:1500]
                explanation["explained_by"] = f"small language model {self.slm.name}"
            except SLMError as exc:
                logger.warning("SLM explanation failed; using the template", extra={"error": str(exc)})
        return explanation

    def facts(self, action: ProposedAction, alert: dict[str, Any] | None,
              assessment: dict[str, Any] | None) -> dict[str, Any]:
        details = (alert or {}).get("details") or {}
        event = self.data.latest_event(action.zone_id) if self.data is not None else None
        state = {k: event.get(k) for k in ("temperature_c", "hvac_setpoint_c", "occupancy_count",
                                           "lighting_level_pct", "total_power_kw")} if event else {}
        return {
            "zone": f"{action.zone_id} ({self.names.get(action.zone_id, 'zone')})",
            "detected": CLASSIFICATIONS.get(details.get("classification", ""), "a security alert"),
            "alert_severity": (alert or {}).get("severity"),
            "agent_conclusion": (assessment or {}).get("likely_cause"),
            "agent_confidence": (assessment or {}).get("confidence"),
            "action": action.action_type, "action_parameters": action.parameters,
            "reason_given_by_agent": action.reason,
            "safety_validator": action.safety_reason,
            "zone_now": state,
        }

    def _template(self, action: ProposedAction, facts: dict[str, Any]) -> dict[str, Any]:
        zone, p, now = facts["zone"], action.parameters, facts["zone_now"]
        sensor = SENSORS.get(str(p.get("sensor")), str(p.get("sensor")))
        conclusion = facts["agent_conclusion"]
        happened = f"The detectors flagged {facts['detected']} in zone {zone}"
        happened += f" ({facts['alert_severity']} severity)." if facts["alert_severity"] else "."
        if conclusion:
            happened += f" The AI agent re-checked the data and concluded: {conclusion}"
            happened += f" (confidence {facts['agent_confidence']})." if facts["agent_confidence"] else "."
        kind = action.action_type
        if kind == "isolate_sensor":
            title = f"Quarantine the {sensor} sensor in {action.zone_id}"
            will = (f"The system stops trusting the {sensor} sensor in {zone}. Nothing in the building is switched; "
                    "only the sensor's readings are ignored.")
            effect = (f"While the sensor is quarantined, the energy optimizer stops adjusting {action.zone_id}, so no "
                      "decision is based on fake readings. Savings in this zone pause until the sensor is trusted "
                      "again, which happens automatically after one quiet hour.")
            declined = (f"The {sensor} readings stay trusted. If they are fake, the optimizer and the agent keep "
                        "using them, which can waste energy or make the zone uncomfortable.")
        elif kind == "clear_overrides":
            title = f"Put {action.zone_id} back on its normal schedule"
            will = (f"{zone} returns to its fixed schedule: the normal target temperature and light levels for the "
                    "time of day.")
            effect = (f"Setpoint and lighting changes the optimizer made for {action.zone_id}, possibly based on "
                      "manipulated readings, are undone. The zone may use slightly more energy for a while, but its "
                      "settings are safe and known.")
            declined = "The current settings stay, even though they may have been computed from manipulated readings."
        elif kind == "set_setpoint":
            title = f"Change the target temperature in {action.zone_id}"
            will = (f"The target temperature in {zone} changes from {_num(now.get('hvac_setpoint_c'))} °C to "
                    f"{_num(p.get('setpoint_c'))} °C (the room is {_num(now.get('temperature_c'))} °C now).")
            effect = "Heating/cooling power changes accordingly; the new value is inside the comfort and safety limits."
            declined = "The current target temperature stays."
        elif kind == "set_lighting":
            title = f"Change the lights in {action.zone_id}"
            will = (f"The lights in {zone} change from {_num(now.get('lighting_level_pct'), 0)} % to "
                    f"{_num(100 * float(p.get('lighting_level', 0)), 0)} %.")
            effect = "Lighting power changes accordingly; minimum safety lighting is always kept."
            declined = "The current light level stays."
        elif kind == "set_hvac_mode":
            title = f"Switch heating/cooling in {action.zone_id} to {p.get('hvac_mode')}"
            will = f"Heating/cooling in {zone} switches to “{p.get('hvac_mode')}”."
            effect = "Heating/cooling power changes accordingly; occupied zones and equipment rooms are never switched off."
            declined = "The current heating/cooling mode stays."
        else:
            title = kind.replace("_", " ").capitalize()
            will, effect, declined = action.reason, "", "Nothing changes."
        power = now.get("total_power_kw")
        if power is not None:
            effect += f" The zone currently uses {_num(power, 2)} kW."
        question = f"Allow the AI agent to {title[0].lower()}{title[1:]}?"
        sure = {"high": " and is highly confident", "medium": " and is fairly confident"}.get(facts["agent_confidence"], "")
        reason = f" It found {facts['detected']} in zone {zone}{sure}."
        return {"title": title, "what_happened": happened, "what_will_happen": will, "energy_effect": effect.strip(),
                "if_declined": declined, "safety_check": action.safety_reason or "",
                "summary": question + reason}
