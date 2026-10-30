"""Tests for the dashboard (Module 10): control contract, simulation manager, control client,
Flask pages and JSON API, live updates, and the PostgreSQL data service."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app import create_app
from config import SimulatorSettings, get_config
from processors.data_validator import ValidationError, validate_control_message
from services.dashboard_data import DashboardData, jsonable
from services.live_updates import LiveUpdater
from services.registry import DashboardServices, ServiceUnavailableError
from services.simulation_control import ControlUnavailableError, SimulationControl
from simulator.engine import BuildingSimulator
from workers.simulation_manager import SimulationManager

NOW = datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc)
START = datetime(2026, 2, 9, 9, 0, tzinfo=timezone.utc)


def command(kind: str, attack: dict | None = None, *, building: str = "building-01", age_min: float = 0) -> dict:
    message = {"command_id": str(uuid.uuid4()), "command": kind, "building_id": building,
               "requested_at": (NOW - timedelta(minutes=age_min)).isoformat()}
    if attack is not None:
        message["attack"] = attack
    return validate_control_message(message)


SPOOF = {"attack_type": "temperature_spoofing", "zone_id": "Z1", "duration_min": 30, "params": {"offset_c": 4}}


# --------------------------------------------------------------------------- control contract
class TestControlContract:
    def test_valid_attack(self):
        result = command("attack", SPOOF)
        assert result["attack"] == {**SPOOF, "duration_min": 30.0}

    @pytest.mark.parametrize("attack, fragment", [
        ({**SPOOF, "attack_type": "ransomware"}, "attack_type"),
        ({**SPOOF, "zone_id": "Z1; DROP TABLE"}, "zone_id"),
        ({**SPOOF, "duration_min": 2}, "duration_min"),
        ({**SPOOF, "params": {"offset_c": 50}}, "params.offset_c"),
        ({**SPOOF, "params": {"factor": 2}}, "not a parameter"),
        ({**SPOOF, "attack_type": "occupancy_manipulation", "params": {"mode": "explode"}}, "params.mode"),
    ])
    def test_invalid_attack(self, attack, fragment):
        with pytest.raises(ValidationError) as exc:
            command("attack", attack)
        assert any(fragment in e for e in exc.value.errors)

    def test_unknown_command(self):
        with pytest.raises(ValidationError):
            validate_control_message({"command_id": str(uuid.uuid4()), "command": "reboot", "building_id": "b",
                                      "requested_at": NOW.isoformat()})


# --------------------------------------------------------------------------- simulation manager
class FakeStore:
    def __init__(self):
        self.statuses, self.attacks = [], []

    def status(self, status):
        self.statuses.append(dict(status))

    def attack(self, attack):
        self.attacks.append(dict(attack))


@pytest.fixture
def manager():
    simulator = BuildingSimulator(SimulatorSettings(interval_seconds=1, speed_factor=300, random_seed=3,
                                                    start_time=START))
    published = []
    return SimulationManager(simulator, published.append, store=FakeStore()), published


class TestSimulationManager:
    def test_paused_until_started(self, manager):
        m, published = manager
        assert not m.step() and published == []
        m.handle(command("start"), now=NOW)
        assert m.step() and len(published) == 5 and m.status()["state"] == "running"
        m.handle(command("stop"), now=NOW)
        sim_time = m.simulator.sim_time
        assert not m.step() and m.simulator.sim_time == sim_time and m.status()["state"] == "paused"

    def test_attack_manipulates_the_next_readings_and_is_counted(self, manager):
        m, published = manager
        m.handle(command("start"), now=NOW)
        m.step()
        clean = {e["zone_id"]: e["temperature_c"] for e in published[-5:]}
        m.handle(command("attack", SPOOF), now=NOW)
        m.step()
        attacked = {e["zone_id"]: e["temperature_c"] for e in published[-5:]}
        assert attacked["Z1"] - clean["Z1"] > 3.0                # +4 °C offset (minus 5 minutes of physics)
        assert abs(attacked["Z2"] - clean["Z2"]) < 1.0
        for _ in range(6):                                       # 30 simulated minutes = 6 readings in total
            m.step()
        assert m.attacks[0]["readings_manipulated"] == 6 and m.injector.labels == []
        m.flush_store()
        stored = m.store.attacks[-1]
        assert stored["attack_type"] == "temperature_spoofing" and stored["readings_manipulated"] == 6
        assert m.store.statuses[-1]["state"] == "running"

    def test_ignores_stale_foreign_and_unknown_zone_commands(self, manager):
        m, _ = manager
        assert m.handle(command("start", age_min=30), now=NOW) == "ignored: stale command"
        assert m.handle(command("start", building="other"), now=NOW).startswith("ignored: command for building")
        assert m.handle(command("attack", {**SPOOF, "zone_id": "Z9"}), now=NOW) == "ignored: unknown zone Z9"
        assert not m.running and m.attacks == []


# --------------------------------------------------------------------------- control client
class FakeProducer:
    def __init__(self, *, undelivered: int = 0):
        self.sent, self.undelivered, self.stats = [], undelivered, SimpleNamespace(failed=0)

    def send(self, topic, value, key=None, headers=None):
        self.sent.append((topic, key, value))

    def flush(self, timeout_s=10):
        return self.undelivered


class TestSimulationControl:
    def test_sends_validated_message(self):
        producer = FakeProducer()
        control = SimulationControl(lambda: producer, "building-01")
        message = control.send("attack", SPOOF)
        topic, key, value = producer.sent[0]
        assert topic == "simulation_control" and key == "building-01" and value == message
        assert validate_control_message(value)["attack"]["zone_id"] == "Z1"

    def test_invalid_command_is_not_sent(self):
        producer = FakeProducer()
        with pytest.raises(ValidationError):
            SimulationControl(lambda: producer, "building-01").send("attack", {**SPOOF, "duration_min": 0})
        assert producer.sent == []

    def test_unconfirmed_delivery_raises(self):
        with pytest.raises(ControlUnavailableError):
            SimulationControl(lambda: FakeProducer(undelivered=1), "building-01").send("start")


# --------------------------------------------------------------------------- Flask app and API
class FakeData:
    """Stand-in for DashboardData with fixed content."""

    def status(self):
        return {"simulation": {"state": "running", "ticks": 3}, "latest_reading": START.isoformat(), "counts": {}}

    def live_readings(self):
        return [{"zone_id": "Z1", "temperature_c": 21.5, "isolated_sensors": ["temperature"]}]

    def readings_since(self, watermark, limit=200):
        return [] if watermark else [{"zone_id": "Z1", "ingested_at": "2026-10-30T12:00:00+00:00"}]

    def power_timeseries(self, hours=6):
        return {"zones": {"Z1": {"t": [START.isoformat()], "power_kw": [1.0], "temperature_c": [21.0]}}}

    def hourly_energy(self, hours=48):
        return []

    def energy_summary(self, hours=24):
        return {"energy_kwh": 10.0}

    def predictions(self, hours=12, zone_id=None):
        return {"rows": [], "accuracy": [], "zone": zone_id}

    def optimizer_savings(self, hours=24):
        return []

    def comparison(self):
        return None

    def __init__(self):
        self.alert_rows = [{"alert_id": "a1", "status": "open", "zone_id": "Z1"}]

    def alerts(self, limit=100, alert_type=None, zone_id=None):
        return self.alert_rows

    def alert_timeline(self, hours=48):
        return []

    def drift(self, limit=50):
        return []

    def attacks(self, limit=50):
        return []

    def isolated_sensors(self):
        return []

    def decisions(self, limit=30):
        return [{"decision_id": "d1"}]

    def actions(self, limit=100, source=None):
        return [{"action_id": "x1", "status": "approved", "source": source}]

    def action_summary(self):
        return []


class FakeServices(DashboardServices):
    def __init__(self, config, data=None, control=None):
        super().__init__(config, control=control)
        self.fake = data

    def data(self):
        if self.fake is None:
            raise ServiceUnavailableError("PostgreSQL not reachable")
        return self.fake


@pytest.fixture
def client_factory(clean_env):
    def make(data=FakeData(), control=None, controls_enabled=True):
        clean_env.setenv("DASHBOARD_CONTROLS_ENABLED", "true" if controls_enabled else "false")
        config = get_config("testing", load_dotenv_file=False)
        app = create_app(config, services=FakeServices(config, data, control))
        return app.test_client()
    return make


class TestFlaskDashboard:
    @pytest.mark.parametrize("path, text", [("/", "Live sensor readings"), ("/energy", "ML prediction vs. reality"),
                                            ("/security", "Simulated attacks"), ("/agent", "Investigations")])
    def test_pages_render(self, client_factory, path, text):
        response = client_factory().get(path)
        html = response.get_data(as_text=True)
        assert response.status_code == 200 and text in html
        for page in ("Energy Analytics", "Security Monitoring", "Agentic AI Activity"):
            assert page in html                                  # navigation
        assert 'id="attack-form"' in html or path in ("/energy", "/agent")

    def test_controls_hidden_when_disabled(self, client_factory):
        html = client_factory(controls_enabled=False).get("/").get_data(as_text=True)
        assert 'id="attack-form"' not in html and "Controls are disabled" in html

    @pytest.mark.parametrize("path", ["/api/status", "/api/live", "/api/energy/power?hours=abc", "/api/energy/hourly",
                                      "/api/energy/summary", "/api/energy/predictions?zone_id=Z2",
                                      "/api/energy/optimization", "/api/security/alerts", "/api/security/timeline",
                                      "/api/security/drift", "/api/security/attacks", "/api/security/isolated",
                                      "/api/agent/decisions", "/api/agent/actions?source=agent"])
    def test_api_endpoints(self, client_factory, path):
        response = client_factory().get(path)
        assert response.status_code == 200 and response.is_json

    def test_api_passes_filters(self, client_factory):
        client = client_factory()
        assert client.get("/api/energy/predictions?zone_id=Z2").get_json()["zone"] == "Z2"
        assert client.get("/api/agent/actions?source=agent").get_json()["rows"][0]["source"] == "agent"

    def test_api_returns_503_without_database(self, client_factory):
        response = client_factory(data=None).get("/api/status")
        assert response.status_code == 503 and response.get_json()["error"] == "database unavailable"
        assert client_factory(data=None).get("/").status_code == 200       # pages still render

    def test_simulation_controls(self, client_factory):
        producer = FakeProducer()
        client = client_factory(control=SimulationControl(lambda: producer, "building-01"))
        assert client.post("/api/simulation/start", json={}).status_code == 202
        assert client.post("/api/simulation/stop", data="x").status_code == 415      # JSON only
        response = client.post("/api/simulation/attack", json=SPOOF)
        assert response.status_code == 202 and producer.sent[-1][2]["attack"]["zone_id"] == "Z1"
        bad = client.post("/api/simulation/attack", json={**SPOOF, "params": {"offset_c": 99}})
        assert bad.status_code == 400 and "params.offset_c" in bad.get_json()["details"][0]
        assert [m[2]["command"] for m in producer.sent] == ["start", "attack"]

    def test_controls_disabled_and_kafka_down(self, client_factory):
        assert client_factory(controls_enabled=False).post("/api/simulation/start", json={}).status_code == 403
        down = client_factory(control=SimulationControl(lambda: FakeProducer(undelivered=1), "building-01"))
        assert down.post("/api/simulation/start", json={}).status_code == 503

    def test_socketio_connects_without_starting_the_poller_in_tests(self, clean_env):
        from app import socketio

        config = get_config("testing", load_dotenv_file=False)
        app = create_app(config, services=FakeServices(config, FakeData()))
        client = socketio.test_client(app)
        assert client.is_connected() and app.extensions["live_updater"].polls == 0
        client.disconnect()


# --------------------------------------------------------------------------- live updates
class FakeSocket:
    def __init__(self):
        self.emitted = []

    def emit(self, event, payload):
        self.emitted.append((event, payload))


def test_live_updater_emits_only_new_items():
    data, socket = FakeData(), FakeSocket()
    updater = LiveUpdater(socket, lambda: data, 1.0)
    first = updater.poll_once()
    assert first == {"readings": 1} and [e for e, _ in socket.emitted] == ["status", "readings"]
    data.alert_rows = [{"alert_id": "a2", "status": "open"}, *data.alert_rows]
    second = updater.poll_once()
    assert second == {"alerts": 1}
    assert socket.emitted[-1] == ("alerts", [{"alert_id": "a2", "status": "open"}])
    data.alert_rows[1] = {"alert_id": "a1", "status": "resolved", "zone_id": "Z1"}     # status change
    assert updater.poll_once() == {"alerts": 1}


def test_live_updater_survives_database_outage():
    socket = FakeSocket()

    def broken():
        raise ServiceUnavailableError("down")

    assert LiveUpdater(socket, broken, 1.0).poll_once() == {}
    assert socket.emitted == [("status", {"error": "database unavailable"})]


def test_jsonable():
    value = {"t": START, "id": uuid.UUID(int=1), "nan": float("nan"), "nested": [START.date()]}
    assert jsonable(value) == {"t": START.isoformat(), "id": str(uuid.UUID(int=1)), "nan": None,
                               "nested": ["2026-02-09"]}


# --------------------------------------------------------------------------- PostgreSQL data service
@pytest.mark.postgres
def test_dashboard_data_on_postgres(db, tmp_path):
    import json as _json

    from agent.actions import ProposedAction
    from database.repositories import insert_record, upsert_optimization_action, upsert_security_alert
    from processors.data_validator import validate_action_message, validate_alert_message, validate_sensor_event
    from workers.simulation_manager import PostgresStatusStore

    simulator = BuildingSimulator(SimulatorSettings(interval_seconds=1, speed_factor=300, random_seed=5,
                                                    start_time=START))
    store = PostgresStatusStore(db, SimpleNamespace(warning=print))
    manager = SimulationManager(simulator, lambda e: None, store=store)
    fresh = {"requested_at": datetime.now(timezone.utc)}
    manager.handle(command("start") | fresh)
    manager.handle(command("attack", SPOOF) | fresh)
    events = []
    for _ in range(12):
        events += simulator.tick()
    with db.transaction() as conn:
        for event in events:
            insert_record(conn, validate_sensor_event(event))
        alert = {"alert_id": str(uuid.uuid4()), "zone_id": "Z1", "source_event_id": None,
                 "detected_at": events[-1]["timestamp"], "alert_type": "attack", "severity": "high",
                 "detector": "rules", "score": None, "description": "spoofing <script>",
                 "details": {"classification": "temperature_spoofing"}, "status": "open"}
        upsert_security_alert(conn, validate_alert_message(alert))
        action = ProposedAction("Z1", "isolate_sensor", {"sensor": "temperature"}, "evidence",
                                datetime.fromisoformat(events[-1]["timestamp"]), "d1", alert["alert_id"])
        action.status = "executed"
        upsert_optimization_action(conn, validate_action_message(action.to_message()))
    manager.flush_store()

    comparison = tmp_path / "comparison.json"
    comparison.write_text(_json.dumps({"winter week": [{"name": "baseline", "energy_kwh": 1.0}]}))
    data = DashboardData(db, comparison_path=comparison)
    status = data.status()
    assert status["simulation"]["state"] == "running" and status["latest_reading"] == events[-1]["timestamp"]
    assert status["counts"]["open_alerts"] == 1 and status["counts"]["simulated_attacks"] == 1
    live = {r["zone_id"]: r for r in data.live_readings()}
    assert set(live) == {"Z1", "Z2", "Z3", "Z4", "Z5"} and live["Z1"]["isolated_sensors"] == ["temperature"]
    power = data.power_timeseries(hours=0.5)["zones"]
    assert len(power["Z1"]["t"]) == 6                                       # last 30 minutes
    assert data.readings_since(None) and not data.readings_since(data.readings_since(None)[-1]["ingested_at"])
    attack = data.attacks()[0]
    assert attack["attack_type"] == "temperature_spoofing" and attack["alerts_during_attack"] == 1
    assert data.alerts(alert_type="attack")[0]["description"] == "spoofing <script>"   # stored as data
    assert data.actions(source="agent")[0]["action_type"] == "isolate_sensor"
    assert data.comparison()["cases"]["winter week"][0]["energy_kwh"] == 1.0
    assert data.alert_timeline()[0]["classification"] == "temperature_spoofing"
    for query in (data.hourly_energy, data.energy_summary, data.predictions, data.optimizer_savings, data.drift,
                  data.decisions, data.action_summary):
        query()                                                              # run without errors on real SQL
