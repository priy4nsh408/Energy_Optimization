"""LLM providers for the incident agent (selected with ``AGENT_LLM_PROVIDER``).

* :class:`AnthropicLLM` - Claude through the official Anthropic SDK. The
  credentials come from the environment (``ANTHROPIC_API_KEY``) and are never
  logged. Adaptive thinking; effort from ``AGENT_LLM_EFFORT``. The stable
  system prompt and tool list are cached. On the models that support it, a
  server-side fallback model answers if the primary model declines a request
  (``AGENT_LLM_REFUSAL_FALLBACK``).
* :class:`OfflinePolicy` - **not an LLM**. A deterministic, rule-based
  playbook that drives the same tools, so the whole pipeline (tools, safety
  validator, execution, records) can run and be tested without an API key.
  Every decision it makes is recorded with provider ``offline``.

Both return one :class:`LLMTurn` per call; conversation history uses the
Anthropic Messages format.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

from config import AgentSettings

logger = logging.getLogger(__name__)

# Models that accept adaptive thinking + effort and the "default" server-side fallback.
CURRENT_MODELS = ("claude-fable-5-1", "claude-opus-5-5", "claude-opus-5", "claude-sonnet-5-5")
FALLBACK_BETA = "server-side-fallback-2026-07-01"


class LLMError(RuntimeError):
    """The provider could not produce a usable turn (network, API error, refusal, truncation)."""


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    input: dict[str, Any]


@dataclass
class LLMTurn:
    assistant_message: dict[str, Any]       # appended to the history as-is
    tool_calls: list[ToolCall] = field(default_factory=list)
    text: str = ""
    stop_reason: str = "end_turn"
    usage: dict[str, int] = field(default_factory=dict)


class LLMClient(Protocol):
    provider: str
    model: str

    def next_turn(self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> LLMTurn: ...


# --------------------------------------------------------------------------- Claude
class AnthropicLLM:
    provider = "anthropic"

    def __init__(self, settings: AgentSettings, client: Any | None = None) -> None:
        import anthropic                                      # optional dependency, imported lazily

        self._anthropic = anthropic
        self.settings = settings
        self.model = settings.llm_model
        self.client = client or anthropic.Anthropic(timeout=settings.llm_timeout_s, max_retries=3)
        self.use_fallback = settings.llm_refusal_fallback and self.model in CURRENT_MODELS

    def _request(self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> dict[str, Any]:
        request: dict[str, Any] = {
            "model": self.model,
            "max_tokens": self.settings.llm_max_tokens,
            # Tools + system prompt are identical for every incident: cache them.
            "system": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            "messages": messages,
            "tools": tools,
        }
        if self.model in CURRENT_MODELS:
            request["thinking"] = {"type": "adaptive"}
            request["output_config"] = {"effort": self.settings.llm_effort}
        return request

    def next_turn(self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> LLMTurn:
        anthropic = self._anthropic
        request = self._request(system, messages, tools)
        try:
            if self.use_fallback:
                response = self.client.beta.messages.create(betas=[FALLBACK_BETA], fallbacks="default", **request)
            else:
                response = self.client.messages.create(**request)
        except anthropic.AuthenticationError as exc:
            raise LLMError("Anthropic authentication failed (check ANTHROPIC_API_KEY)") from exc
        except anthropic.BadRequestError as exc:
            raise LLMError(f"Anthropic rejected the request: {exc.message}") from exc
        except anthropic.RateLimitError as exc:
            raise LLMError("Anthropic rate limit reached (after SDK retries)") from exc
        except anthropic.APIStatusError as exc:
            raise LLMError(f"Anthropic API error {exc.status_code}") from exc
        except anthropic.APIConnectionError as exc:
            raise LLMError("cannot reach the Anthropic API") from exc
        except anthropic.AnthropicError as exc:
            raise LLMError(f"Anthropic client error: {exc}") from exc
        except TypeError as exc:                           # raised by the SDK when no credentials resolve
            raise LLMError(f"Anthropic client not configured: {exc}") from exc

        if response.stop_reason == "refusal":
            category = getattr(response.stop_details, "category", None) if response.stop_details else None
            raise LLMError(f"model declined the request (category: {category})")
        if response.stop_reason == "max_tokens":
            raise LLMError("response truncated at AGENT_LLM_MAX_TOKENS")
        calls = [ToolCall(b.id, b.name, dict(b.input)) for b in response.content if b.type == "tool_use"]
        text = "\n".join(b.text for b in response.content if b.type == "text")
        usage = {k: getattr(response.usage, k, 0) or 0 for k in
                 ("input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens")}
        logger.info("LLM turn", extra={"model": response.model, "stop_reason": response.stop_reason,
                                       "tool_calls": [c.name for c in calls], "usage": usage,
                                       "request_id": getattr(response, "_request_id", None)})
        # Append the full content (incl. thinking blocks) so the history stays append-only.
        return LLMTurn({"role": "assistant", "content": response.content}, calls, text,
                       response.stop_reason, usage)


# --------------------------------------------------------------------------- offline policy
PLAYBOOK: dict[str, dict[str, Any]] = {
    "temperature_spoofing": {"isolate": ["temperature"], "fallback": True,
                             "cause": "manipulated (spoofed) temperature readings"},
    "occupancy_manipulation": {"isolate": ["occupancy"], "fallback": True,
                               "cause": "manipulated occupancy readings"},
    "replay": {"isolate": ["temperature", "occupancy"], "fallback": True,
               "cause": "replayed (repeated old) sensor data"},
    "abnormal_power": {"isolate": ["power_meter"], "fallback": False,
                       "cause": "manipulated or faulty power-meter readings"},
    "gradual_manipulation": {"isolate": [], "fallback": True,
                             "cause": "slow drift of physically linked readings (gradual manipulation or a "
                                      "drifting sensor)"},
    "data_drift": {"isolate": [], "fallback": False,
                   "cause": "input data differ from the training period (e.g. season); not an attack"},
}
DRIFT_FEATURE_SENSOR = {"control_error": "temperature", "temp": "temperature", "co2": "co2",
                        "occupancy": "occupancy", "power": "power_meter"}
OFFLINE_ID = "offline-{turn}-{index}"
MIN_CONFIRMING_READINGS = 2


def _alert_from(messages: list[dict[str, Any]]) -> dict[str, Any]:
    text = messages[0]["content"] if isinstance(messages[0]["content"], str) else ""
    start, end = text.find("<incident>"), text.find("</incident>")
    return json.loads(text[start + len("<incident>"):end])["alert"] if start >= 0 else {}


def _tool_results(messages: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Map tool name -> last result, using the offline policy's own tool-use ids."""
    names: dict[str, str] = {}
    results: dict[str, dict[str, Any]] = {}
    for message in messages:
        content = message["content"] if isinstance(message["content"], list) else []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                names[block["id"]] = block["name"]
            elif isinstance(block, dict) and block.get("type") == "tool_result":
                results[names.get(block["tool_use_id"], "?")] = json.loads(block["content"])
    return results


class OfflinePolicy:
    """Deterministic rule-based stand-in for the LLM (clearly labelled ``offline``)."""

    provider = "offline"
    model = "rule-based-playbook-v1"

    def next_turn(self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> LLMTurn:
        alert = _alert_from(messages)
        results = _tool_results(messages)
        turn = sum(1 for m in messages if m["role"] == "assistant")
        zone = alert.get("zone_id")
        if zone is None:
            return self._turn(turn, [("submit_assessment", self._building_wide(alert))])
        if turn == 0:
            return self._turn(turn, [
                ("get_sensor_data", {"zone_id": zone, "minutes": 120}),
                ("get_energy_data", {"zone_id": zone, "hours": 3}),
                ("get_weather_and_carbon", {}),
                ("run_anomaly_detection", {"zone_id": zone, "minutes": 120}),
                ("run_drift_analysis", {"zone_id": zone}),
            ])
        if turn == 1:
            calls = self._actions(alert, results)
            return self._turn(turn, calls) if calls else self._turn(
                turn, [("submit_assessment", self._assessment(alert, results, []))])
        if "submit_assessment" not in results:
            proposals = [r for name, r in results.items() if name in ("isolate_sensor", "adjust_hvac")]
            return self._turn(turn, [("submit_assessment", self._assessment(alert, results, proposals))])
        return LLMTurn({"role": "assistant", "content": [{"type": "text", "text": "Assessment submitted."}]},
                       [], "Assessment submitted.")

    @staticmethod
    def _turn(turn: int, calls: list[tuple[str, dict[str, Any]]]) -> LLMTurn:
        tool_calls = [ToolCall(OFFLINE_ID.format(turn=turn, index=i), name, args)
                      for i, (name, args) in enumerate(calls)]
        content = [{"type": "tool_use", "id": c.id, "name": c.name, "input": c.input} for c in tool_calls]
        return LLMTurn({"role": "assistant", "content": content}, tool_calls, "", "tool_use")

    @staticmethod
    def _confirmed(classification: str, results: dict[str, dict[str, Any]]) -> bool:
        anomaly = results.get("run_anomaly_detection", {})
        drift = results.get("run_drift_analysis", {})
        if classification in ("gradual_manipulation", "data_drift"):
            key = "security_drift" if classification == "gradual_manipulation" else "data_drift"
            return bool(drift.get(key, {}).get("windows_with_drift"))
        # A single re-flagged reading is weak evidence: require two of this type, or one of this type
        # corroborated by other flagged readings in the same zone.
        same = anomaly.get("flagged_by_type", {}).get(classification, 0)
        return same >= MIN_CONFIRMING_READINGS or (same >= 1 and anomaly.get("flagged", 0) >= 3)

    def _actions(self, alert: dict[str, Any], results: dict[str, dict[str, Any]]) -> list[tuple[str, dict]]:
        classification = (alert.get("details") or {}).get("classification", "")
        play = PLAYBOOK.get(classification)
        if play is None or not self._confirmed(classification, results):
            return []
        zone, sensors = alert["zone_id"], list(play["isolate"])
        if classification == "gradual_manipulation":
            drifting = results.get("run_drift_analysis", {}).get("security_drift", {}).get(
                "latest_drifting_features", []) or (alert.get("details") or {}).get("drifting_features", [])
            for feature in drifting:
                sensor = next((s for k, s in DRIFT_FEATURE_SENSOR.items() if k in feature), None)
                if sensor and sensor not in sensors:
                    sensors.append(sensor)
        calls = [("isolate_sensor", {"zone_id": zone, "sensor": s, "alert_id": alert["alert_id"],
                                     "reason": f"{classification} confirmed by re-running the detectors; "
                                               f"stop trusting the {s} sensor until it is inspected"})
                 for s in sensors[:2]]
        if play["fallback"]:
            calls.append(("adjust_hvac", {"zone_id": zone, "action": "clear_overrides", "setpoint_c": None,
                                          "hvac_mode": None,
                                          "reason": "optimizer overrides were computed from untrusted readings; "
                                                    "return the zone to its baseline schedule (safe fallback)"}))
        return calls

    def _assessment(self, alert: dict[str, Any], results: dict[str, dict[str, Any]],
                    proposals: list[dict[str, Any]]) -> dict[str, Any]:
        details = alert.get("details") or {}
        classification = details.get("classification", "unknown_anomaly")
        play = PLAYBOOK.get(classification, {"cause": "unclassified anomaly"})
        zone = alert.get("zone_id")
        anomaly = results.get("run_anomaly_detection", {})
        drift = results.get("run_drift_analysis", {})
        energy = results.get("get_energy_data", {})
        confirmed = self._confirmed(classification, results)
        evidence = [f"alert '{classification}' ({alert.get('severity')}) from {alert.get('detector')}"]
        if anomaly:
            evidence.append(f"re-running the point detectors flagged {anomaly.get('flagged', 0)} of "
                            f"{anomaly.get('readings_evaluated', 0)} readings in the last 2 h "
                            f"{anomaly.get('flagged_by_type') or ''}".strip())
            if anomaly.get("rules_triggered"):
                evidence.append("rules: " + ", ".join(anomaly["rules_triggered"]))
        security = drift.get("security_drift", {})
        if security.get("windows_evaluated"):
            evidence.append(f"security drift in {security.get('windows_with_drift', 0)} of "
                            f"{security['windows_evaluated']} windows (24 h)")
        current, previous = energy.get("current") or {}, energy.get("same_hours_yesterday") or {}
        if current:
            energy_text = (f"{zone} used {current['energy_kwh']:.2f} kWh in the last {energy['window_hours']:g} h "
                           f"(mean {current['mean_power_kw']:.2f} kW, peak {current['peak_power_kw']:.2f} kW)")
            if previous:
                change = (current["energy_kwh"] - previous["energy_kwh"]) / max(previous["energy_kwh"], 1e-6)
                energy_text += f"; same hours yesterday {previous['energy_kwh']:.2f} kWh ({change:+.0%})"
            if "estimated_kg_co2e" in energy:
                energy_text += (f"; about {energy['estimated_kg_co2e']:.2f} kg CO2e at "
                                f"{energy['carbon_intensity_g_per_kwh']:.0f} g/kWh ({energy['carbon_source']})")
            energy_text += "."
            if confirmed:
                energy_text += (" Control decisions based on manipulated readings can waste energy (wrong "
                                "setpoints) or misreport consumption; isolating the sensor stops that.")
        else:
            energy_text = "No energy data available for the zone."
        operational = {
            "temperature_spoofing": "Comfort and control decisions for the zone relied on false temperatures.",
            "occupancy_manipulation": "Occupancy-based setbacks and lighting could leave occupants in the dark "
                                      "or condition an empty zone.",
            "replay": "The zone's live state is unknown while old data is replayed.",
            "abnormal_power": "Energy accounting and anomaly baselines are corrupted; no comfort impact.",
            "gradual_manipulation": "Slowly biased readings can shift control decisions without obvious jumps.",
            "data_drift": "ML forecasts and detectors are less reliable until retrained on recent data.",
        }.get(classification, "Unclear; operator review needed.")
        recommendations = [f"Inspect the {', '.join(PLAYBOOK.get(classification, {}).get('isolate', [])) or 'zone'} "
                           f"sensor(s) and the network path of {zone}"]
        if proposals:
            recommendations.append("Keep the isolated sensors quarantined until inspected; restore them only after "
                                   "the zone has been free of alerts for an hour")
        if classification == "data_drift":
            recommendations = ["Retrain the energy model and detectors on recent data",
                               "No control action needed: drift is not evidence of an attack"]
        if not confirmed:
            recommendations = ["Re-running the detectors did not confirm the alert: treat as a possible false "
                               "alarm and monitor the zone", "No automatic action taken"]
        return {
            "diagnosis": ("; ".join(evidence)) + ("" if confirmed else " - not confirmed on re-analysis"),
            "likely_cause": play["cause"] if confirmed else f"possible false alarm (alert said: {play['cause']})",
            "confidence": "high" if confirmed and classification != "gradual_manipulation" else
                          "medium" if confirmed else "low",
            "energy_impact": energy_text,
            "operational_impact": operational,
            "recommendations": recommendations,
        }

    @staticmethod
    def _building_wide(alert: dict[str, Any]) -> dict[str, Any]:
        return {"diagnosis": f"building-wide alert: {alert.get('description', '')}",
                "likely_cause": "not zone-specific (e.g. invalid messages)", "confidence": "low",
                "energy_impact": "unknown", "operational_impact": "data quality issue",
                "recommendations": ["Check the message producer and data contracts"]}


def create_llm(settings: AgentSettings) -> LLMClient:
    if settings.resolved_provider == "anthropic":
        return AnthropicLLM(settings)
    logger.warning("Agent running with the OFFLINE rule-based policy (no LLM); set AGENT_LLM_PROVIDER=anthropic "
                   "and ANTHROPIC_API_KEY to use Claude")
    return OfflinePolicy()
