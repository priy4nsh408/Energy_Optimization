"""Architecture and integration checks: imports, consumer groups, configuration, supervisor,
shared quarantine state, closed-loop behaviour of the optimizer, agent recovery loop."""

from __future__ import annotations

import ast
import importlib
import re
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from config import BASE_DIR, ConfigurationError, SystemSettings

PACKAGES = ("agent", "data_sources", "database", "experiments", "ml", "optimization", "processors", "routes",
            "scripts", "security", "services", "simulator", "streaming", "utils", "workers")


def project_modules() -> dict[str, Path]:
    modules = {"app": BASE_DIR / "app.py", "config": BASE_DIR / "config.py"}
    for package in PACKAGES:
        for path in (BASE_DIR / package).rglob("*.py"):
            parts = path.relative_to(BASE_DIR).with_suffix("").parts
            name = ".".join(parts[:-1] if parts[-1] == "__init__" else parts)
            modules[name] = path
    return modules


def module_level_imports(path: Path, known: set[str]) -> set[str]:
    """Project modules imported when ``path`` is imported (function-level imports are lazy and excluded)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: set[str] = set()

    def visit(nodes):
        for node in nodes:
            if isinstance(node, ast.Import):
                found.update(a.name for a in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                found.add(node.module)
                found.update(f"{node.module}.{a.name}" for a in node.names)
            elif isinstance(node, (ast.If, ast.Try)):
                if isinstance(node, ast.If) and "TYPE_CHECKING" in ast.dump(node.test):
                    continue
                visit(node.body)
                visit(getattr(node, "orelse", []))
                for handler in getattr(node, "handlers", []):
                    visit(handler.body)
    visit(tree.body)
    return {m for m in found if m in known}


def test_no_circular_imports():
    modules = project_modules()
    graph = {name: module_level_imports(path, set(modules)) - {name} for name, path in modules.items()}
    cycles, state, stack = [], {}, []

    def dfs(node):
        state[node] = "active"
        stack.append(node)
        for nxt in graph[node]:
            if state.get(nxt) == "active":
                cycles.append(stack[stack.index(nxt):] + [nxt])
            elif nxt not in state:
                dfs(nxt)
        stack.pop()
        state[node] = "done"

    for node in graph:
        if node not in state:
            dfs(node)
    assert cycles == [], f"circular module-level imports: {cycles}"


def test_every_module_imports():
    failures = {}
    for name in sorted(project_modules()):
        if name.startswith("tests"):
            continue
        try:
            importlib.import_module(name)
        except Exception as exc:  # noqa: BLE001
            failures[name] = repr(exc)
    assert failures == {}


def test_consumer_groups_are_unique():
    """Every service consumes with its own group (shared groups would split, i.e. lose, messages)."""
    groups: dict[str, list[str]] = {}
    for path in (BASE_DIR / "workers").glob("*.py"):
        text = path.read_text(encoding="utf-8")
        service = re.search(r'^SERVICE = "([^"]+)"', text, re.M)
        for match in re.finditer(r'JsonConsumer\(config\.KAFKA,\s*([^,]+),', text):
            raw = match.group(1).strip()
            if raw == "SERVICE":
                group = service.group(1)
            elif raw.startswith('f"{SERVICE}'):
                group = service.group(1) + raw[len('f"{SERVICE}'):-1]
            elif raw.startswith('"'):
                group = raw.strip('"')
            else:
                group = f"<{raw}>"                        # e.g. args.group (topic monitor), dynamic
            groups.setdefault(group, []).append(path.stem)
        call = re.search(r"run_worker\(service=\w+,\s*group=(\w+)", text)
        if call:
            assert call.group(1) == "SERVICE", path
            groups.setdefault(service.group(1), []).append(path.stem)
    duplicates = {g: files for g, files in groups.items() if len(files) > 1}
    assert duplicates == {}
    from scripts.run_stack import SERVICES
    producer_only = {"external_data_publisher"}            # polls HTTP APIs, consumes nothing
    assert set(SERVICES) - producer_only <= set(groups), "every consuming service has a group named after it"


def test_every_environment_variable_is_documented():
    config_text = (BASE_DIR / "config.py").read_text(encoding="utf-8")
    used = set(re.findall(r'_get_\w+\(\s*"([A-Z0-9_]+)"', config_text))
    documented = set(re.findall(r"^#?\s*([A-Z0-9_]+)=", (BASE_DIR / ".env.example").read_text(encoding="utf-8"),
                                re.M))
    assert used - documented == set(), f"variables read in config.py but missing from .env.example: {used - documented}"


def test_stack_services_have_entry_points():
    from scripts.run_stack import SERVICES

    for name in SERVICES:
        module = importlib.import_module(f"workers.{name}")
        assert callable(getattr(module, "main", None)), name


# --------------------------------------------------------------------------- configuration
class TestSystemSettings:
    def test_detectors_follow_the_control_mode(self):
        assert SystemSettings().detector_set == "baseline"
        closed = SystemSettings(control_mode="closed_loop")
        assert closed.detector_set == "optimizer_control" and closed.executed_command_sources == ("agent", "optimizer")
        assert SystemSettings(control_mode="closed_loop", security_detectors="baseline").detector_set == "baseline"

    @pytest.mark.parametrize("kwargs", [{"control_mode": "autopilot"}, {"security_detectors": "magic"}])
    def test_invalid_values(self, kwargs):
        with pytest.raises(ConfigurationError):
            SystemSettings(**kwargs)

    def test_preflight_reports_bad_configuration(self, clean_env):
        from scripts.preflight import run_checks

        clean_env.setenv("CONTROL_MODE", "autopilot")
        results = run_checks()
        assert results[0].status == "fail" and "CONTROL_MODE" in results[0].detail


# --------------------------------------------------------------------------- supervisor
def test_supervisor_restarts_then_gives_up(tmp_path):
    from scripts.run_stack import Supervisor

    supervisor = Supervisor({"crasher": [sys.executable, "-c", "import sys; sys.exit(3)"]},
                            {**__import__("os").environ}, tmp_path, max_restarts=1)
    supervisor.start_all()
    try:
        supervisor.processes["crasher"].wait(timeout=20)
        assert supervisor.watch() is True and supervisor.restarts["crasher"] == 1      # restart scheduled
        supervisor.restart_at["crasher"] = 0                                           # skip the backoff
        assert supervisor.watch() is True                                              # restarted
        supervisor.processes["crasher"].wait(timeout=20)
        assert supervisor.watch() is False                                             # gave up
    finally:
        supervisor.stop_all()
    assert (tmp_path / "crasher.log").is_file()


# --------------------------------------------------------------------------- shared quarantine
def action(kind: str, sensor: str = "temperature", status: str = "executed", zone: str = "Z1") -> dict:
    return {"action_type": kind, "status": status, "zone_id": zone, "parameters": {"sensor": sensor},
            "created_at": "2026-02-10T10:15:00+00:00", "updated_at": "2026-02-10T10:15:00+00:00"}


def test_registry_follows_agent_actions():
    from agent.isolation import SensorIsolationRegistry

    registry = SensorIsolationRegistry()
    assert not registry.apply_action_message(action("isolate_sensor", status="approved"))
    assert not registry.apply_action_message(action("set_setpoint"))
    assert registry.apply_action_message(action("isolate_sensor")) and registry.is_isolated("Z1", "temperature")
    assert not registry.apply_action_message(action("isolate_sensor"))                # idempotent (redelivery)
    assert registry.apply_action_message(action("restore_sensor")) and not registry.isolated()
    assert not registry.apply_action_message(action("isolate_sensor", sensor="camera"))


@pytest.mark.postgres
def test_registry_loads_quarantine_from_database(db):
    from agent.actions import ProposedAction
    from agent.isolation import SensorIsolationRegistry
    from database.repositories import upsert_optimization_action
    from processors.data_validator import validate_action_message

    t0 = datetime(2026, 2, 10, 10, tzinfo=timezone.utc)
    with db.transaction() as conn:
        for i, (kind, zone, sensor) in enumerate([("isolate_sensor", "Z1", "temperature"),
                                                  ("isolate_sensor", "Z2", "occupancy"),
                                                  ("restore_sensor", "Z2", "occupancy")]):
            a = ProposedAction(zone, kind, {"sensor": sensor}, "test", t0 + timedelta(minutes=i), f"d{i}")
            a.status = "executed"
            upsert_optimization_action(conn, validate_action_message(a.to_message(updated_at=a.created_at)))
    registry = SensorIsolationRegistry()
    assert registry.load_from_database(db) == 1
    assert registry.is_isolated("Z1", "temperature") and not registry.is_isolated("Z2", "occupancy")


def test_optimizer_skips_quarantined_zones():
    from config import OptimizationSettings
    from simulator.engine import BuildingSimulator
    from config import SimulatorSettings
    from streaming.consumer import ConsumedMessage
    from workers.optimization_service import OptimizationHandler

    sent = []
    producer = SimpleNamespace(send=lambda topic, value, key=None: sent.append(value))
    handler = OptimizationHandler(OptimizationSettings(), producer, SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None, debug=lambda *a, **k: None))
    simulator = BuildingSimulator(SimulatorSettings(interval_seconds=1, speed_factor=300, random_seed=1,
                                                    start_time=datetime(2026, 2, 10, 9, tzinfo=timezone.utc)))

    def feed(ticks: int) -> list[str]:
        sent.clear()
        for _ in range(ticks):
            for event in simulator.tick():
                handler(ConsumedMessage("building_sensor_data", event["zone_id"], event, 0, 0, None))
        return sorted({m["zone_id"] for m in sent})

    handler(ConsumedMessage("agent_actions", "Z1", action("isolate_sensor"), 0, 0, None))
    zones = feed(6)
    assert "Z1" not in zones and len(zones) >= 3 and handler.stats["skipped_isolated_zone"] == 6
    handler(ConsumedMessage("agent_actions", "Z1", action("restore_sensor"), 0, 0, None))
    simulator.apply_command("Z1", setpoint_c=24.5)                    # far from the optimum: next decision moves it
    handler.scheduler._last.pop("Z1", None)
    assert "Z1" in feed(3)


# --------------------------------------------------------------------------- agent service recovery loop
def test_agent_recovery_loop_restores_and_records():
    from agent.executor import SimulatorGateway
    from agent.recovery import RecoveryPolicy
    from tests.test_agent import Building
    from workers.agent_service import RecoveryLoop

    b = Building(hours=2)
    b.isolation.isolate("Z1", "temperature", since=b.data.now() - timedelta(hours=2), reason="t")
    recorded = []
    recorder = SimpleNamespace(record_actions=lambda actions: recorded.extend(actions))
    loop = RecoveryLoop(RecoveryPolicy(b.data, b.isolation, b.validator, SimulatorGateway(b.simulator, b.isolation)),
                        b.data, recorder, threading.Lock(), SimpleNamespace(info=lambda *a, **k: None), 0.01)
    assert loop.run_once() == 1 and loop.restored == 1
    assert [(a.action_type, a.status) for a in recorded] == [("restore_sensor", "executed")]
    assert loop.run_once() == 0


@pytest.mark.postgres
def test_second_simulation_manager_is_refused(db):
    from workers.simulation_manager import _live_manager

    with db.transaction() as conn:
        conn.execute("INSERT INTO simulation_status (building_id, state, interval_seconds, speed_factor) "
                     "VALUES ('b1', 'running', 1, 300)")
    assert _live_manager(db, "b1") is not None
    assert _live_manager(db, "other") is None
    with db.transaction() as conn:
        conn.execute("UPDATE simulation_status SET updated_at = now() - interval '1 minute'")
    assert _live_manager(db, "b1") is None                            # stale heartbeat: may start
