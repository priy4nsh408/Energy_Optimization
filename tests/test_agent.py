"""Tests for the agentic AI module: safety validator, isolation, tools, dispatcher,
LLM providers (offline policy and a mocked Anthropic client), the LangGraph
incident graph, simulated execution and decision records."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from agent.actions import ProposedAction
from agent.data import InMemoryDataSource
from agent.dispatch import IncidentDispatcher
from agent.executor import SimulatorActuator, SimulatorGateway, apply_to_simulator
from agent.graph import IncidentAgent
from agent.isolation import SensorIsolationRegistry
from agent.llm import AnthropicLLM, LLMError, LLMTurn, OfflinePolicy, ToolCall
from agent.recorder import JsonlRecorder
from agent.safety import SafetyValidator
from agent.tools import TOOL_SPECS, AgentToolbox, IncidentContext
from config import AgentSettings, OptimizationSettings, SimulatorSettings
from optimization.hvac_optimizer import building_models
from optimization.optimizer import ZoneOptimizer
from processors.data_validator import validate_action_message
from simulator.engine import BuildingSimulator
from streaming.consumer import ConsumedMessage

MONDAY_9 = datetime(2026, 1, 12, 9, 0, tzinfo=timezone.utc)


class Building:
    """A simulator with an in-memory data source and the agent's components."""

    def __init__(self, start: datetime = MONDAY_9, hours: float = 1.0, **agent_settings) -> None:
        self.simulator = BuildingSimulator(SimulatorSettings(interval_seconds=1, speed_factor=300, random_seed=7,
                                                             start_time=start))
        self.data = InMemoryDataSource()
        self.tick(hours)
        self.settings = AgentSettings(llm_provider="offline", **agent_settings)
        self.isolation = SensorIsolationRegistry()
        self.models = building_models()
        self.validator = SafetyValidator(self.settings, OptimizationSettings(), self.models, self.isolation)
        self.toolbox = AgentToolbox(self.data, self.isolation, self.validator,
                                    ZoneOptimizer(OptimizationSettings(), self.models))

    def tick(self, hours: float) -> None:
        for _ in range(int(hours * 12)):
            for event in self.simulator.tick():
                self.data.add_event(event)

    def alert(self, zone: str = "Z1", classification: str = "temperature_spoofing", severity: str = "high") -> dict:
        alert = {"alert_id": str(uuid.uuid4()), "zone_id": zone, "source_event_id": None,
                 "detected_at": self.data.now().isoformat(), "alert_type": "attack", "severity": severity,
                 "detector": "rules", "score": None, "description": "test alert",
                 "details": {"classification": classification}, "status": "open"}
        self.data.add_alert(alert)
        return alert

    def action(self, action_type: str, zone: str = "Z1", alert_id: str | None = None, **parameters):
        return ProposedAction(zone_id=zone, action_type=action_type, parameters=parameters, reason="test",
                              created_at=self.data.now(), decision_id="d1", related_alert_id=alert_id)

    def check(self, action: ProposedAction, now: datetime | None = None):
        return self.validator.validate(action, latest_event=self.data.latest_event(action.zone_id),
                                       now=now or self.data.now(), zone_alerts=self.data.alerts(action.zone_id, 24))


@pytest.fixture
def building() -> Building:
    return Building()


# --------------------------------------------------------------------------- safety validator
class TestSafetyValidator:
    def test_setpoint_within_band_and_step_is_approved(self, building):
        current = building.data.latest_event("Z1")["hvac_setpoint_c"]
        verdict = building.check(building.action("set_setpoint", setpoint_c=min(current + 1.0, 24.0)))
        assert verdict.approved, verdict.text

    @pytest.mark.parametrize("setpoint, fragment", [(30.0, "absolute limits"), (15.0, "absolute limits")])
    def test_setpoint_outside_limits_is_rejected(self, building, setpoint, fragment):
        verdict = building.check(building.action("set_setpoint", setpoint_c=setpoint))
        assert not verdict.approved and fragment in verdict.text

    def test_step_limit(self, building):
        current = building.data.latest_event("Z1")["hvac_setpoint_c"]
        verdict = building.check(building.action("set_setpoint", setpoint_c=current - 3.0))
        assert not verdict.approved and "step limit" in verdict.text

    def test_stale_data_blocks_control_but_not_fallback(self, building):
        later = building.data.now() + timedelta(hours=2)
        assert "old" in building.check(building.action("set_lighting", lighting_level=1.0), later).text
        assert building.check(building.action("clear_overrides"), later).approved

    def test_hvac_off(self, building):
        server = building.check(building.action("set_hvac_mode", zone="Z5", hvac_mode="off"))
        assert not server.approved and "equipment room" in server.text
        assert not building.check(building.action("set_hvac_mode", hvac_mode="cooler")).approved

    def test_lighting_too_dark_for_occupants_rejected(self):
        evening = Building(start=datetime(2026, 1, 12, 17, 0, tzinfo=timezone.utc), hours=0.5)
        assert evening.data.latest_event("Z1")["occupancy_count"] > 0          # dark and occupied
        verdict = evening.check(evening.action("set_lighting", lighting_level=0.0))
        assert not verdict.approved and "illuminance" in verdict.text

    def test_structure(self, building):
        assert "unknown zone" in building.check(building.action("clear_overrides", zone="Z9")).text
        assert "unexpected parameter" in building.check(building.action("clear_overrides", force=True)).text
        assert "unknown action" in building.check(building.action("reboot_plc")).text

    def test_isolation_needs_evidence_and_is_capped(self, building):
        assert "evidence" in building.check(building.action("isolate_sensor", sensor="temperature")).text
        alert = building.alert()
        assert building.check(building.action("isolate_sensor", alert_id=alert["alert_id"],
                                              sensor="temperature")).approved
        assert "unknown sensor" in building.check(building.action("isolate_sensor", alert_id=alert["alert_id"],
                                                                  sensor="camera")).text
        now = building.data.now()
        building.isolation.isolate("Z1", "temperature", since=now, reason="t")
        building.isolation.isolate("Z1", "co2", since=now, reason="t")
        assert "already isolated" in building.check(building.action(
            "isolate_sensor", alert_id=alert["alert_id"], sensor="temperature")).text
        assert "maximum" in building.check(building.action(
            "isolate_sensor", alert_id=alert["alert_id"], sensor="occupancy")).text

    def test_restore_requires_a_quiet_hour(self, building):
        building.isolation.isolate("Z1", "temperature", since=building.data.now(), reason="t")
        building.alert()
        assert "quiet" in building.check(building.action("restore_sensor", sensor="temperature")).text
        assert building.check(building.action("restore_sensor", sensor="temperature"),
                              building.data.now() + timedelta(hours=2)).approved

    def test_rate_limit(self):
        b = Building(max_actions_per_zone_per_hour=2)
        for _ in range(2):
            b.validator.record_approved(b.action("clear_overrides"), b.data.now())
        assert "rate limit" in b.check(b.action("clear_overrides")).text

    def test_untrusted_occupancy_is_treated_as_occupied(self):
        night = Building(start=datetime(2026, 1, 12, 2, 0, tzinfo=timezone.utc), hours=0.5)
        event = night.data.latest_event("Z1")
        assert event["occupancy_count"] == 0
        assert night.validator._conditions(event, []).occupancy == 0
        night.isolation.isolate("Z1", "occupancy", since=night.data.now(), reason="t")
        assert night.validator._conditions(event, []).occupancy == 1

    def test_deterministic(self, building):
        action = building.action("set_setpoint", setpoint_c=21.0)
        assert building.check(action) == building.check(action)


# --------------------------------------------------------------------------- isolation, actions, dispatcher
def test_isolation_masks_readings(building):
    event = building.data.latest_event("Z2")
    building.isolation.isolate("Z2", "power_meter", since=building.data.now(), reason="t")
    masked = building.isolation.mask(event)
    assert masked["total_power_kw"] is None and masked["energy_kwh"] is None
    assert masked["temperature_c"] == event["temperature_c"] and masked["isolated_sensors"] == ["power_meter"]
    assert event["total_power_kw"] is not None                 # original untouched
    assert building.isolation.restore("Z2", "power_meter") and not building.isolation.restore("Z2", "power_meter")


def test_action_message_matches_the_kafka_contract(building):
    action = building.action("isolate_sensor", alert_id=str(uuid.uuid4()), sensor="temperature")
    record = validate_action_message(action.to_message(), topic="agent_actions")
    assert record.action_type == "isolate_sensor" and record.source == "agent"
    again = building.action("isolate_sensor", alert_id=action.related_alert_id, sensor="temperature")
    assert again.action_id == action.action_id                 # deterministic id


class TestDispatcher:
    def alert(self, minutes: float, classification: str = "temperature_spoofing", severity: str = "high") -> dict:
        return {"alert_id": str(uuid.uuid4()), "zone_id": "Z1", "severity": severity,
                "detected_at": (MONDAY_9 + timedelta(minutes=minutes)).isoformat(),
                "details": {"classification": classification}}

    def decision(self, *actions: dict) -> SimpleNamespace:
        return SimpleNamespace(actions=list(actions))

    def test_severity_and_cooldown(self):
        d = IncidentDispatcher(AgentSettings(min_alert_severity="medium"))
        assert not d.should_investigate(self.alert(0, severity="low"))[0]
        first = self.alert(0)
        assert d.should_investigate(first)[0]
        d.remember(first, self.decision({"zone_id": "Z1", "action_type": "clear_overrides", "status": "executed",
                                         "parameters": {}}))
        assert not d.should_investigate(self.alert(30))[0]
        assert d.should_investigate(self.alert(30, classification="replay"))[0]
        assert d.should_investigate(self.alert(61))[0]

    def test_unconfirmed_incident_is_rechecked_sooner(self):
        d = IncidentDispatcher(AgentSettings(unconfirmed_recheck_s=900))
        first = self.alert(0)
        d.should_investigate(first)
        d.remember(first, self.decision())                     # nothing done: not confirmed
        assert not d.should_investigate(self.alert(10))[0]
        assert d.should_investigate(self.alert(16))[0]

    def test_contained_incident_waits_for_restore(self):
        isolation = SensorIsolationRegistry()
        d = IncidentDispatcher(AgentSettings(incident_cooldown_s=0), isolation)
        first = self.alert(0)
        d.should_investigate(first)
        isolation.isolate("Z1", "temperature", since=MONDAY_9, reason="t")
        d.remember(first, self.decision({"zone_id": "Z1", "action_type": "isolate_sensor", "status": "executed",
                                         "parameters": {"sensor": "temperature"}}))
        assert d.should_investigate(self.alert(120)) == (False, "contained: sensor already isolated")
        isolation.restore("Z1", "temperature")
        assert d.should_investigate(self.alert(121))[0]


# --------------------------------------------------------------------------- tools
class TestTools:
    def test_specs_are_well_formed(self):
        names = [t["name"] for t in TOOL_SPECS]
        assert len(names) == len(set(names)) == 10
        for spec in TOOL_SPECS:
            schema = spec["input_schema"]
            assert schema["additionalProperties"] is False and set(schema["required"]) == set(schema["properties"])

    def test_sensor_and_energy_data(self, building):
        ctx = IncidentContext("d", None)
        building.isolation.isolate("Z1", "temperature", since=building.data.now(), reason="t")
        sensor = building.toolbox.call("get_sensor_data", {"zone_id": "Z1", "minutes": 30}, ctx)
        assert sensor["readings"] == 6 and sensor["latest"]["temperature_c"] is None
        assert sensor["isolated_sensors"] == ["temperature"] and "Z2" in sensor["other_zones_latest_temperature_c"]
        energy = building.toolbox.call("get_energy_data", {"zone_id": "Z1", "hours": 1}, ctx)
        assert energy["current"]["readings"] == 12 and energy["current"]["energy_kwh"] > 0
        assert energy["same_hours_yesterday"] is None

    def test_errors_are_returned_not_raised(self, building):
        ctx = IncidentContext("d", None)
        assert "unknown zone" in building.toolbox.call("get_sensor_data", {"zone_id": "Z9", "minutes": 5}, ctx)["error"]
        assert "unknown tool" in building.toolbox.call("format_disk", {}, ctx)["error"]
        assert "no trained security detectors" in building.toolbox.call(
            "run_anomaly_detection", {"zone_id": "Z1", "minutes": 60}, ctx)["error"]
        assert "setpoint_c is required" in building.toolbox.call(
            "adjust_hvac", {"zone_id": "Z1", "action": "set_setpoint", "setpoint_c": None, "hvac_mode": None,
                            "reason": "x"}, ctx)["error"]

    def test_proposals_are_prechecked_and_not_executed(self, building):
        alert = building.alert()
        ctx = IncidentContext("d", alert)
        ok = building.toolbox.call("isolate_sensor", {"zone_id": "Z1", "sensor": "temperature",
                                                      "alert_id": alert["alert_id"], "reason": "spoofing"}, ctx)
        bad = building.toolbox.call("adjust_hvac", {"zone_id": "Z5", "action": "set_hvac_mode", "setpoint_c": None,
                                                    "hvac_mode": "off", "reason": "save energy"}, ctx)
        assert ok["pre_check"] == "passed" and bad["pre_check"] == "rejected"
        assert [p.status for p in ctx.proposals] == ["proposed", "rejected"]
        assert not building.isolation.isolated()               # proposing does not execute
        dup = building.toolbox.call("isolate_sensor", {"zone_id": "Z1", "sensor": "temperature",
                                                       "alert_id": alert["alert_id"], "reason": "again"}, ctx)
        assert dup["note"] == "duplicate proposal" and len(ctx.proposals) == 2

    def test_optimization_tool_is_read_only(self, building):
        before = building.simulator.building.zones["Z1"].overrides
        result = building.toolbox.call("run_energy_optimization", {"zone_id": "Z1"}, IncidentContext("d", None))
        assert "recommended_setpoint_c" in result and building.simulator.building.zones["Z1"].overrides == before


# --------------------------------------------------------------------------- graph
class ScriptedLLM:
    provider, model = "scripted", "test-script"

    def __init__(self, script: list[list[tuple[str, dict]]]) -> None:
        self.script, self.calls = script, []

    def next_turn(self, system, messages, tools) -> LLMTurn:
        self.calls.append(messages)
        turn = len([m for m in messages if m["role"] == "assistant"])
        if turn >= len(self.script):
            return LLMTurn({"role": "assistant", "content": [{"type": "text", "text": "done"}]}, [], "done")
        calls = [ToolCall(f"s{turn}-{i}", name, args) for i, (name, args) in enumerate(self.script[turn])]
        return LLMTurn({"role": "assistant", "content": [{"type": "tool_use", "id": c.id, "name": c.name,
                                                          "input": c.input} for c in calls]}, calls, "", "tool_use")


class FailingLLM:
    provider, model = "anthropic", "claude-opus-5-5"

    def next_turn(self, system, messages, tools):
        raise LLMError("cannot reach the Anthropic API")


def make_agent(b: Building, llm, tmp_path) -> IncidentAgent:
    return IncidentAgent(llm=llm, toolbox=b.toolbox, data=b.data, isolation=b.isolation, validator=b.validator,
                         gateway=SimulatorGateway(b.simulator, b.isolation),
                         recorder=JsonlRecorder(tmp_path / "audit.jsonl"), max_iterations=6)


ASSESSMENT = {"diagnosis": "spoofed", "likely_cause": "spoofing", "confidence": "high", "energy_impact": "x",
              "operational_impact": "y", "recommendations": ["inspect"]}


class TestIncidentGraph:
    def test_only_approved_actions_are_executed_and_recorded(self, building, tmp_path):
        alert = building.alert()
        building.simulator.apply_command("Z1", setpoint_c=23.0)
        llm = ScriptedLLM([
            [("get_sensor_data", {"zone_id": "Z1", "minutes": 60}), ("get_weather_and_carbon", {})],
            [("isolate_sensor", {"zone_id": "Z1", "sensor": "temperature", "alert_id": alert["alert_id"],
                                 "reason": "spoofing"}),
             ("adjust_hvac", {"zone_id": "Z1", "action": "clear_overrides", "setpoint_c": None, "hvac_mode": None,
                              "reason": "fallback"}),
             ("adjust_hvac", {"zone_id": "Z5", "action": "set_hvac_mode", "setpoint_c": None, "hvac_mode": "off",
                              "reason": "unsafe"})],
            [("submit_assessment", ASSESSMENT)],
        ])
        decision = make_agent(building, llm, tmp_path).run(alert)
        statuses = {(a["zone_id"], a["action_type"]): a["status"] for a in decision.actions}
        assert statuses == {("Z1", "isolate_sensor"): "executed", ("Z1", "clear_overrides"): "executed",
                            ("Z5", "set_hvac_mode"): "rejected"}
        assert building.isolation.is_isolated("Z1", "temperature")
        assert building.simulator.building.zones["Z1"].overrides.setpoint_c is None
        assert building.simulator.building.zones["Z5"].overrides.hvac_mode is None
        assert decision.llm_provider == "scripted" and decision.confidence == "high" and not decision.errors
        assert [t["tool"] for t in decision.tool_calls][:2] == ["get_sensor_data", "get_weather_and_carbon"]
        # tool results go back to the LLM as JSON tool_result blocks
        results = llm.calls[1][2]["content"]                      # [user, assistant, tool results, ...]
        assert results[0]["type"] == "tool_result" and json.loads(results[0]["content"])["zone_id"] == "Z1"
        record = json.loads((tmp_path / "audit.jsonl").read_text().splitlines()[0])
        assert record["alert_id"] == alert["alert_id"] and record["outcome"] == "2 executed, 1 rejected"

    def test_final_safety_gate_uses_the_latest_state(self, building, tmp_path):
        alert = building.alert()
        current = building.data.latest_event("Z1")["hvac_setpoint_c"]

        class ChangingLLM(ScriptedLLM):
            def next_turn(inner, system, messages, tools):
                turn = super().next_turn(system, messages, tools)
                if turn.tool_calls and turn.tool_calls[0].name == "submit_assessment":
                    building.tick(1)                     # time passes before the gate: data now stale?
                    building.data._events["Z1"].clear()  # no fresh Z1 reading at all
                return turn

        llm = ChangingLLM([[("adjust_hvac", {"zone_id": "Z1", "action": "set_setpoint",
                                             "setpoint_c": min(current + 0.5, 24.0), "hvac_mode": None,
                                             "reason": "comfort"})],
                           [("submit_assessment", ASSESSMENT)]])
        decision = make_agent(building, llm, tmp_path).run(alert)
        assert decision.actions[0]["status"] == "rejected" and "no reading" in decision.actions[0]["safety_reason"]

    def test_llm_failure_falls_back_to_offline_policy(self, building, tmp_path):
        decision = make_agent(building, FailingLLM(), tmp_path).run(building.alert())
        assert decision.llm_provider == "anthropic -> offline (fallback)"
        assert "cannot reach" in decision.errors[0]
        # without trained detectors the offline policy cannot confirm the alert -> no actions
        assert decision.confidence == "low" and decision.actions == []

    def test_missing_assessment_is_recorded(self, building, tmp_path):
        llm = ScriptedLLM([[("get_weather_and_carbon", {})]])
        decision = make_agent(building, llm, tmp_path).run(building.alert())
        assert "did not submit an assessment" in decision.errors[0] and decision.confidence == "low"

    def test_iteration_limit(self, building, tmp_path):
        llm = ScriptedLLM([[("get_weather_and_carbon", {})]] * 20)
        decision = make_agent(building, llm, tmp_path).run(building.alert())
        assert any("stopped after 6" in e for e in decision.errors) and len(llm.calls) == 6


def test_offline_policy_playbook():
    alert = {"alert_id": "a1", "zone_id": "Z2", "severity": "high", "detector": "rules",
             "details": {"classification": "replay"}}
    first = {"role": "user", "content": f"<incident>{json.dumps({'alert': alert})}</incident>"}
    policy = OfflinePolicy()
    turn = policy.next_turn("", [first], [])
    assert {c.name for c in turn.tool_calls} >= {"get_sensor_data", "run_anomaly_detection", "run_drift_analysis"}
    results = {"run_anomaly_detection": {"flagged": 5, "readings_evaluated": 24, "flagged_by_type": {"replay": 5}}}
    history = [first, turn.assistant_message, {"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": c.id, "content": json.dumps(results.get(c.name, {}))}
        for c in turn.tool_calls]}]
    actions = policy.next_turn("", history, [])
    assert [(c.name, c.input.get("sensor") or c.input.get("action")) for c in actions.tool_calls] == [
        ("isolate_sensor", "temperature"), ("isolate_sensor", "occupancy"), ("adjust_hvac", "clear_overrides")]
    # cold start: the same evidence with only 10 h of history is not trusted -> assessment, no actions
    results["run_anomaly_detection"]["history_hours"] = 10.0
    cold = [first, turn.assistant_message, {"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": c.id, "content": json.dumps(results.get(c.name, {}))}
        for c in turn.tool_calls]}]
    final = policy.next_turn("", cold, [])
    assert [c.name for c in final.tool_calls] == ["submit_assessment"]
    assert "10.0 h of history" in final.tool_calls[0].input["diagnosis"]


# --------------------------------------------------------------------------- Anthropic provider (mocked client)
def _response(content, stop_reason="tool_use", stop_details=None):
    usage = SimpleNamespace(input_tokens=100, output_tokens=20, cache_read_input_tokens=80,
                            cache_creation_input_tokens=0)
    return SimpleNamespace(content=content, stop_reason=stop_reason, stop_details=stop_details, usage=usage,
                           model="claude-opus-5-5", _request_id="req_test")


class FakeClient:
    def __init__(self, responses):
        self.responses, self.requests = list(responses), []
        create = self.create
        self.messages = SimpleNamespace(create=lambda **kw: create("messages", kw))
        self.beta = SimpleNamespace(messages=SimpleNamespace(create=lambda **kw: create("beta", kw)))

    def create(self, kind, kwargs):
        self.requests.append((kind, kwargs))
        return self.responses.pop(0)


class TestAnthropicLLM:
    def test_request_and_parsing(self):
        tool_use = SimpleNamespace(type="tool_use", id="toolu_1", name="get_weather_and_carbon", input={})
        client = FakeClient([_response([SimpleNamespace(type="thinking", thinking=""), tool_use])])
        llm = AnthropicLLM(AgentSettings(llm_provider="anthropic", llm_effort="high"), client=client)
        turn = llm.next_turn("system", [{"role": "user", "content": "hi"}], TOOL_SPECS)
        kind, request = client.requests[0]
        assert kind == "beta" and request["betas"] == ["server-side-fallback-2026-07-01"]
        assert request["fallbacks"] == "default" and request["model"] == "claude-opus-5-5"
        assert request["thinking"] == {"type": "adaptive"} and request["output_config"] == {"effort": "high"}
        assert request["system"][0]["cache_control"] == {"type": "ephemeral"}
        assert turn.tool_calls == [ToolCall("toolu_1", "get_weather_and_carbon", {})]
        assert turn.assistant_message["content"][0].type == "thinking"     # full content kept (append-only)
        assert turn.usage["cache_read_input_tokens"] == 80

    def test_no_fallback_or_thinking_options_for_other_models(self):
        client = FakeClient([_response([SimpleNamespace(type="text", text="ok")], stop_reason="end_turn")])
        llm = AnthropicLLM(AgentSettings(llm_provider="anthropic", llm_model="claude-haiku-4-5"), client=client)
        turn = llm.next_turn("s", [{"role": "user", "content": "hi"}], [])
        kind, request = client.requests[0]
        assert kind == "messages" and "thinking" not in request and "output_config" not in request
        assert turn.text == "ok" and not turn.tool_calls

    @pytest.mark.parametrize("stop_reason, message", [("refusal", "declined"), ("max_tokens", "truncated")])
    def test_unusable_turns_raise(self, stop_reason, message):
        details = SimpleNamespace(category="cyber") if stop_reason == "refusal" else None
        client = FakeClient([_response([], stop_reason=stop_reason, stop_details=details)])
        llm = AnthropicLLM(AgentSettings(llm_provider="anthropic"), client=client)
        with pytest.raises(LLMError, match=message):
            llm.next_turn("s", [{"role": "user", "content": "hi"}], [])

    def test_provider_selection(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        assert AgentSettings(llm_provider="auto").resolved_provider == "offline"
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
        assert AgentSettings(llm_provider="auto").resolved_provider == "anthropic"
        assert AgentSettings(llm_provider="offline").resolved_provider == "offline"


# --------------------------------------------------------------------------- execution over Kafka (simulator side)
class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, value, key=None, headers=None):
        self.sent.append((topic, value))


def _command(building, source="agent", status="approved", **parameters) -> ConsumedMessage:
    action = building.action("set_setpoint" if "setpoint_c" in parameters else "clear_overrides", **parameters)
    action.source, action.status, action.action_id = source, status, str(uuid.uuid4())
    return ConsumedMessage("optimization_commands", "Z1", action.to_message(), 0, 0, None)


def test_simulator_actuator_applies_only_approved_commands_once(building):
    producer = FakeProducer()
    actuator = SimulatorActuator(building.simulator, producer, "agent_actions", {"agent"})
    message = _command(building, setpoint_c=22.5)
    actuator.handle(message)
    actuator.handle(message)                                     # redelivery
    actuator.handle(_command(building, source="optimizer", setpoint_c=21.0))
    actuator.handle(_command(building, status="rejected", setpoint_c=21.0))
    assert actuator.drain() == 1
    assert building.simulator.building.zones["Z1"].overrides.setpoint_c == 22.5
    topic, result = producer.sent[0]
    assert topic == "agent_actions" and result["status"] == "executed"
    assert validate_action_message(result).status == "executed"
    actuator.handle(_command(building, setpoint_c=50.0))         # outside physical limits
    actuator.drain()
    assert producer.sent[-1][1]["status"] == "failed" and actuator.stats["ignored"] == 2


def test_apply_to_simulator_rejects_sensor_actions(building):
    with pytest.raises(ValueError):
        apply_to_simulator(building.simulator, "Z1", "isolate_sensor", {"sensor": "temperature"})


# --------------------------------------------------------------------------- PostgreSQL records
@pytest.mark.postgres
def test_postgres_recorder_and_data_source(db, building, tmp_path):
    from agent.data import PostgresDataSource
    from agent.recorder import PostgresRecorder
    from database.repositories import insert_record, upsert_security_alert
    from processors.data_validator import validate_alert_message, validate_sensor_event

    with db.transaction() as conn:
        for zone in ("Z1", "Z2"):
            for event in building.data.recent_events(zone, 60):
                insert_record(conn, validate_sensor_event(event))
    alert = building.alert()
    with db.transaction() as conn:
        upsert_security_alert(conn, validate_alert_message(alert))
    source = PostgresDataSource(db)
    assert source.latest_event("Z1")["event_id"] == building.data.latest_event("Z1")["event_id"]
    assert len(source.recent_events("Z1", 30)) == 6 and source.alerts("Z1", 1)[0]["alert_id"] == alert["alert_id"]

    llm = ScriptedLLM([[("isolate_sensor", {"zone_id": "Z1", "sensor": "temperature",
                                            "alert_id": alert["alert_id"], "reason": "spoofing"})],
                       [("submit_assessment", ASSESSMENT)]])
    agent = IncidentAgent(llm=llm, toolbox=AgentToolbox(source, building.isolation, building.validator,
                                                        ZoneOptimizer(OptimizationSettings(), building.models)),
                          data=source, isolation=building.isolation, validator=building.validator,
                          gateway=SimulatorGateway(building.simulator, building.isolation),
                          recorder=PostgresRecorder(db))
    decision = agent.run(alert)
    with db.transaction() as conn:
        row = conn.execute("SELECT likely_cause, outcome, llm_provider FROM agent_decisions WHERE decision_id = %s",
                           (decision.decision_id,)).fetchone()
        action = conn.execute("SELECT action_type, status, source FROM optimization_actions").fetchone()
    assert row == ("spoofing", "1 executed", "scripted")
    assert action == ("isolate_sensor", "executed", "agent")
