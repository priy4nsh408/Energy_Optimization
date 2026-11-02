"""Operator approval (agent.approval), the action explainer and the optional small language model."""

from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

import pytest

from agent.approval import ApprovalGate
from agent.executor import SimulatorGateway
from agent.explainer import ActionExplainer
from agent.graph import IncidentAgent
from agent.recorder import JsonlRecorder
from agent.slm import OllamaSLM, SLMError
from config import ConfigurationError, SLMSettings
from processors.data_validator import ValidationError, validate_action_message, validate_operator_decision
from streaming.consumer import ConsumedMessage
from tests.test_agent import ASSESSMENT, Building, ScriptedLLM


class Recorder:
    def __init__(self) -> None:
        self.actions = []

    def record_actions(self, actions) -> None:
        self.actions.extend((a.action_id, a.status) for a in actions)


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def make_gate(b: Building, explainer=None, timeout_s: float = 60.0):
    recorder, clock = Recorder(), Clock()
    gate = ApprovalGate(SimulatorGateway(b.simulator, b.isolation), b.validator, b.data, recorder,
                        explainer or ActionExplainer(b.data), timeout_s=timeout_s, clock=clock)
    return gate, recorder, clock


def approved(b: Building, action):
    """An action that passed the safety validator, as the graph hands it to the gateway."""
    verdict = b.check(action)
    assert verdict.approved, verdict.text
    action.status, action.safety_reason = "approved", verdict.text
    b.validator.record_approved(action, b.data.now())
    return action


@pytest.fixture
def building() -> Building:
    return Building()


class TestApprovalGate:
    def test_nothing_runs_until_the_operator_approves(self, building):
        alert = building.alert()
        gate, recorder, _ = make_gate(building)
        action = approved(building, building.action("isolate_sensor", alert_id=alert["alert_id"], sensor="temperature"))
        status, result = gate.execute_incident(action, building.data.now(), alert, ASSESSMENT)
        assert status == "awaiting_approval" and "waiting for operator approval" in result
        assert not building.isolation.is_isolated("Z1", "temperature")
        assert action.explanation["title"] == "Quarantine the temperature sensor in Z1"
        assert "spoofing" in action.explanation["what_happened"] and action.explanation["explained_by"] == "built-in template"
        assert gate.pending() == [action]
        decided = gate.decide(action.action_id, "approve", "alice")
        assert decided.status == "executed" and decided.result.startswith("approved by alice")
        assert decided.explanation["decision"]["by"] == "alice"
        assert building.isolation.is_isolated("Z1", "temperature")
        assert recorder.actions == [(action.action_id, "executed")] and gate.pending() == []
        assert gate.decide(action.action_id, "approve") is None                # already decided

    def test_decline(self, building):
        gate, recorder, _ = make_gate(building)
        building.simulator.apply_command("Z1", setpoint_c=23.0)
        action = approved(building, building.action("clear_overrides"))
        gate.execute(action, building.data.now())
        decided = gate.decide(action.action_id, "decline", "bob", "I will check it myself")
        assert decided.status == "declined" and "I will check it myself" in decided.result
        assert building.simulator.building.zones["Z1"].overrides.setpoint_c == 23.0      # unchanged
        assert recorder.actions == [(action.action_id, "declined")]

    def test_approval_rechecks_safety_with_the_latest_state(self, building):
        alert = building.alert()
        gate, recorder, _ = make_gate(building)
        action = approved(building, building.action("isolate_sensor", alert_id=alert["alert_id"], sensor="temperature"))
        gate.execute(action, building.data.now())
        building.isolation.isolate("Z1", "temperature", since=building.data.now(), reason="meanwhile")
        decided = gate.decide(action.action_id, "approve")
        assert decided.status == "rejected" and "already isolated" in decided.safety_reason
        assert "re-check failed" in decided.result

    def test_unanswered_actions_expire(self, building):
        gate, recorder, clock = make_gate(building, timeout_s=60)
        action = approved(building, building.action("clear_overrides"))
        gate.execute(action, building.data.now())
        clock.t += 59
        assert gate.expire() == []
        clock.t += 2
        assert [a.status for a in gate.expire()] == ["expired"] and gate.pending() == []
        assert recorder.actions == [(action.action_id, "expired")]

    def test_identical_waiting_action_is_not_queued_twice(self, building):
        gate, _, _ = make_gate(building)
        first = approved(building, building.action("clear_overrides", decision_id="d1"))
        second = approved(building, building.action("clear_overrides", decision_id="d2"))
        assert gate.execute(first, building.data.now())[0] == "awaiting_approval"
        status, result = gate.execute(second, building.data.now())
        assert status == "rejected" and first.action_id in result and len(gate.pending()) == 1

    def test_message_carries_the_explanation(self, building):
        gate, _, _ = make_gate(building)
        action = approved(building, building.action("clear_overrides"))
        gate.execute(action, building.data.now())
        action.status = "awaiting_approval"
        record = validate_action_message(action.to_message(), topic="agent_actions")
        assert record.status == "awaiting_approval" and record.parameters["approval"]["title"].startswith("Put Z1")

    def test_incident_graph_waits_for_the_operator(self, building, tmp_path):
        alert = building.alert()
        building.simulator.apply_command("Z1", setpoint_c=23.0)
        gate, _, _ = make_gate(building)
        llm = ScriptedLLM([
            [("isolate_sensor", {"zone_id": "Z1", "sensor": "temperature", "alert_id": alert["alert_id"],
                                 "reason": "spoofing"}),
             ("adjust_hvac", {"zone_id": "Z1", "action": "clear_overrides", "setpoint_c": None, "hvac_mode": None,
                              "reason": "fallback"})],
            [("submit_assessment", ASSESSMENT)],
        ])
        agent = IncidentAgent(llm=llm, toolbox=building.toolbox, data=building.data, isolation=building.isolation,
                              validator=building.validator, gateway=gate,
                              recorder=JsonlRecorder(tmp_path / "audit.jsonl"), max_iterations=6)
        decision = agent.run(alert)
        assert {a["status"] for a in decision.actions} == {"awaiting_approval"}
        assert decision.outcome == "2 awaiting_approval"
        assert not building.isolation.is_isolated("Z1", "temperature")
        assert building.simulator.building.zones["Z1"].overrides.setpoint_c == 23.0
        # the explanation uses the alert and the agent's own assessment
        assert all("spoofing" in a.explanation["what_happened"] for a in gate.pending())
        for action in gate.pending():
            assert gate.decide(action.action_id, "approve").status == "executed"
        assert building.isolation.is_isolated("Z1", "temperature")
        assert building.simulator.building.zones["Z1"].overrides.setpoint_c is None


class TestExplainer:
    def test_templates_cover_every_action(self, building):
        explainer = ActionExplainer(building.data)
        for kind, params in [("set_setpoint", {"setpoint_c": 22.0}), ("set_lighting", {"lighting_level": 0.6}),
                             ("set_hvac_mode", {"hvac_mode": "auto"}), ("clear_overrides", {}),
                             ("isolate_sensor", {"sensor": "occupancy"})]:
            e = explainer.explain(building.action(kind, **params))
            assert all(e[k] for k in ("title", "what_happened", "what_will_happen", "if_declined", "summary")), kind
            assert e["summary"].startswith("Allow the AI agent to ")
        assert "people-count" in explainer.explain(building.action("isolate_sensor", sensor="occupancy"))["title"]

    def test_slm_phrases_the_summary_and_falls_back(self, building):
        class FakeSLM:
            name = "tiny (Ollama)"

            def __init__(self, fail: bool) -> None:
                self.fail, self.prompts = fail, []

            def generate(self, system, prompt, max_tokens=300):
                self.prompts.append(json.loads(prompt))
                if self.fail:
                    raise SLMError("not running")
                return "Someone faked the temperature. Allow the quarantine?"

        good = FakeSLM(False)
        e = ActionExplainer(building.data, good).explain(building.action("clear_overrides"))
        assert e["summary"] == "Someone faked the temperature. Allow the quarantine?"
        assert e["explained_by"] == "small language model tiny (Ollama)"
        assert good.prompts[0]["action"] == "clear_overrides" and "zone_now" in good.prompts[0]
        e = ActionExplainer(building.data, FakeSLM(True)).explain(building.action("clear_overrides"))
        assert e["explained_by"] == "built-in template" and e["summary"].startswith("Allow the AI agent to put Z1")


# --------------------------------------------------------------------------- small language model client
class FakeOllama(BaseHTTPRequestHandler):
    reply = {"message": {"role": "assistant", "content": "<think>hmm</think> Plain answer."}}
    requests: list = []

    def do_POST(self):  # noqa: N802
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        FakeOllama.requests.append((self.path, body))
        self._send(FakeOllama.reply)

    def do_GET(self):  # noqa: N802
        self._send({"models": [{"name": "qwen2.5:1.5b"}]})

    def _send(self, payload):
        data = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture
def ollama():
    server = HTTPServer(("127.0.0.1", 0), FakeOllama)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


class TestOllamaSLM:
    def test_generate_and_availability(self, ollama):
        slm = OllamaSLM(SLMSettings(provider="ollama", base_url=ollama, model="qwen2.5:1.5b", timeout_s=5))
        assert slm.generate("system", "facts") == "Plain answer."
        path, body = FakeOllama.requests[-1]
        assert path == "/api/chat" and body["model"] == "qwen2.5:1.5b" and body["stream"] is False
        assert [m["role"] for m in body["messages"]] == ["system", "user"]
        assert slm.available()[0]
        assert not OllamaSLM(SLMSettings(provider="ollama", base_url=ollama, model="llama3.2")).available()[0]

    def test_unreachable_server_raises(self):
        slm = OllamaSLM(SLMSettings(provider="ollama", base_url="http://127.0.0.1:9", timeout_s=1))
        with pytest.raises(SLMError):
            slm.generate("s", "p")
        assert not slm.available()[0]

    def test_settings(self):
        assert not SLMSettings().enabled
        with pytest.raises(ConfigurationError):
            SLMSettings(provider="gpt")


# --------------------------------------------------------------------------- operator decisions
def decision_message(**overrides) -> dict:
    return {"decision_id": str(uuid.uuid4()), "action_id": str(uuid.uuid4()), "decision": "approve",
            "requested_at": datetime.now(timezone.utc).isoformat(), **overrides}


def test_operator_decision_contract():
    assert validate_operator_decision(decision_message())["operator"] == "dashboard user"
    for bad in ({"decision": "maybe"}, {"action_id": "x"}, {"note": "n" * 600}):
        with pytest.raises(ValidationError):
            validate_operator_decision(decision_message(**bad))


def test_operator_decision_handler():
    from workers.agent_service import OperatorDecisionHandler

    calls = []
    gate = SimpleNamespace(decide=lambda *a: calls.append(a) or (SimpleNamespace() if a[1] == "approve" else None))
    handler = OperatorDecisionHandler(gate, threading.Lock(), SimpleNamespace(warning=lambda *a, **k: None, info=lambda *a, **k: None))
    message = decision_message(note="ok")
    handler(ConsumedMessage("operator_decisions", None, message, 0, 0, None))
    handler(ConsumedMessage("operator_decisions", None, decision_message(decision="decline"), 0, 1, None))
    handler(ConsumedMessage("operator_decisions", None, {"bad": 1}, 0, 2, None))
    assert calls[0] == (message["action_id"], "approve", "dashboard user", "ok")
    assert handler.stats == {"approve": 1, "not_waiting": 1, "invalid": 1}


def test_operator_decision_is_published(monkeypatch):
    from services.simulation_control import SimulationControl
    from tests.test_dashboard import FakeProducer

    producer = FakeProducer()
    message = SimulationControl(lambda: producer, "building-01").send_operator_decision(
        str(uuid.uuid4()), "decline", note="not now")
    topic, key, value = producer.sent[-1]
    assert topic == "operator_decisions" and value == message and key == message["action_id"]
    with pytest.raises(ValidationError):
        SimulationControl(lambda: producer, "building-01").send_operator_decision(str(uuid.uuid4()), "yes")


# --------------------------------------------------------------------------- database
@pytest.mark.postgres
def test_status_lifecycle_and_restart(db, building):
    from database.repositories import upsert_optimization_action

    gate, _, _ = make_gate(building)
    action = approved(building, building.action("clear_overrides"))
    gate.execute(action, building.data.now())
    action.status = "awaiting_approval"

    def store(status):
        action.status = status
        with db.transaction() as conn:
            return upsert_optimization_action(conn, validate_action_message(action.to_message(), topic="agent_actions"))

    assert store("awaiting_approval") == "inserted"
    restarted, _, _ = make_gate(building)
    assert restarted.load_from_database(db) == 1
    waiting = restarted.pending()[0]
    assert waiting.action_id == action.action_id and waiting.parameters == {} and waiting.explanation["title"]
    assert store("approved") == "updated" and store("awaiting_approval") == "unchanged"     # forward only
    assert store("executed") == "updated" and store("declined") == "unchanged"
