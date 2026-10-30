"""End-to-end verification of the integrated system.

    python -m scripts.verify_system --launch                       # isolated run, advisory mode
    python -m scripts.verify_system --launch --mode closed_loop
    python -m scripts.verify_system --url http://127.0.0.1:5000    # against a stack you started

``--launch`` starts every service (``scripts.run_stack``'s supervisor) in an
isolated environment: a fresh PostgreSQL database, its own Kafka topic and
consumer-group prefix and its own dashboard port, so it never touches your
data. The trained models are shared. Everything is driven through the
dashboard's HTTP API, exactly as a user would:

 1. dashboard health + Socket.IO connection
 2. start the simulation (POST /api/simulation/start)
 3. simulator -> Kafka            (topic end offsets grow)
 4. Kafka -> processing -> PostgreSQL (readings, energy records with CO2e, weather/carbon)
 5. ML predictions                 (energy_predictions, accuracy view)
 6. energy optimization            (optimizer commands; closed loop: executed by the simulator)
 7. dashboard live updates         (Socket.IO 'status' and 'readings' events)
 8. after ~26 simulated hours (detector/agent warm-up): trigger an attack (POST /api/simulation/attack)
 9. cyberattack detection          (alert for the attacked zone, pushed over Socket.IO)
10. drift detection                (drift alerts from the KS/PSI monitors)
11. agent diagnosis + validated actions (decision, isolation executed, safety verdicts recorded)
12. building feedback loop         (agent command executed by the simulator; quarantine shown on the
                                    dashboard; closed loop: optimizer stops controlling the zone)
13. recovery                       (sensor restored after the zone is quiet; reported, not required)
14. stop the simulation            (POST /api/simulation/stop)

Results (pass/fail, timings, evidence) go to ``--report`` (Markdown) and a JSON file next to it.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import requests

from config import BASE_DIR

ATTACK = {"attack_type": "temperature_spoofing", "zone_id": "Z1", "duration_min": 60, "params": {"offset_c": 4}}
WARMUP_SIM_HOURS = 26.0


@dataclass
class Result:
    step: str
    required: bool
    passed: bool = False
    seconds: float = 0.0
    detail: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)


class Verifier:
    def __init__(self, url: str, mode: str, timeout_scale: float = 1.0) -> None:
        from config import get_config
        from database.db import Database

        self.url, self.mode, self.scale = url.rstrip("/"), mode, timeout_scale
        self.config = get_config()
        self.db = Database(self.config.DATABASE, name="verify")
        self.db.open(timeout_s=30)
        self.results: list[Result] = []
        self.events: dict[str, list] = defaultdict(list)
        self.socket = None
        self.attack: dict[str, Any] = {}

    # -- helpers ----------------------------------------------------------------------
    def q(self, sql: str, params: tuple = ()) -> list[tuple]:
        with self.db.transaction() as conn:
            return conn.execute(sql, params).fetchall()

    def one(self, sql: str, params: tuple = ()) -> Any:
        rows = self.q(sql, params)
        return rows[0][0] if rows else None

    def wait(self, predicate: Callable[[], Any], timeout_s: float, interval: float = 2.0) -> Any:
        deadline = time.monotonic() + timeout_s * self.scale
        while True:
            value = predicate()
            if value or time.monotonic() > deadline:
                return value
            time.sleep(interval)

    def check(self, step: str, required: bool, fn: Callable[[Result], bool]) -> Result:
        result = Result(step, required)
        started = time.monotonic()
        try:
            result.passed = bool(fn(result))
        except Exception as exc:  # noqa: BLE001 - recorded as a failed check
            result.passed, result.detail = False, f"{type(exc).__name__}: {exc}"
        result.seconds = round(time.monotonic() - started, 1)
        self.results.append(result)
        mark = "PASS" if result.passed else ("FAIL" if required else "n/a ")
        print(f"[{mark}] {step} ({result.seconds:.0f} s) {result.detail}", flush=True)
        return result

    def sim_time(self) -> datetime | None:
        return self.one("SELECT event_time FROM sensor_readings ORDER BY ingested_at DESC, event_time DESC LIMIT 1")

    # -- steps ------------------------------------------------------------------------
    def dashboard(self, r: Result) -> bool:
        import socketio

        health = self.wait(lambda: _get(f"{self.url}/health"), 90)
        if not health:
            r.detail = "dashboard did not answer /health"
            return False
        client = socketio.Client(reconnection=True)
        for name in ("status", "readings", "alerts", "decisions", "actions"):
            client.on(name, lambda payload, name=name: self.events[name].append((time.monotonic(), payload)))
        client.connect(self.url, transports=["polling"], wait_timeout=20)
        self.socket = client
        r.detail = f"health {health}; Socket.IO connected"
        return client.connected

    def start(self, r: Result) -> bool:
        response = requests.post(f"{self.url}/api/simulation/start", json={}, timeout=15)
        state = self.wait(lambda: self.one("SELECT state FROM simulation_status WHERE building_id = %s "
                                           "AND updated_at > now() - interval '15 seconds'",
                                           (self.config.SIMULATOR.building_id,)) == "running", 60)
        r.evidence = {"http_status": response.status_code}
        r.detail = f"POST start -> {response.status_code}; simulation_status running: {bool(state)}"
        return response.status_code == 202 and bool(state)

    def kafka(self, r: Result) -> bool:
        from confluent_kafka import Consumer, TopicPartition

        from streaming.topics import BUILDING_SENSOR_DATA, TOPICS

        topic = self.config.KAFKA.topic(BUILDING_SENSOR_DATA)
        consumer = Consumer({**self.config.KAFKA.consumer_config(f"verify_{uuid.uuid4().hex[:6]}")})

        def offsets() -> int:
            return sum(consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
                       for p in range(TOPICS[BUILDING_SENSOR_DATA].partitions))
        try:
            first = self.wait(offsets, 60)
            time.sleep(5)
            second = offsets()
        finally:
            consumer.close()
        r.evidence = {"topic": topic, "end_offsets": [first, second]}
        r.detail = f"{topic}: {first} -> {second} messages"
        return second > first > 0

    def storage(self, r: Result) -> bool:
        counts = self.wait(lambda: (lambda c: c if all(c.values()) else None)({
            "sensor_readings": self.one("SELECT count(*) FROM sensor_readings"),
            "energy_records_with_co2e": self.one("SELECT count(*) FROM energy_records "
                                                 "WHERE carbon_kg_co2e IS NOT NULL"),
            "weather_records": self.one("SELECT count(*) FROM weather_records"),
            "carbon_intensity_records": self.one("SELECT count(*) FROM carbon_intensity_records"),
        }), 120)
        if not counts:
            counts = {t: self.one(f"SELECT count(*) FROM {t}") for t in
                      ("sensor_readings", "energy_records", "weather_records", "carbon_intensity_records")}
        r.evidence = counts
        r.detail = ", ".join(f"{k} {v}" for k, v in counts.items())
        return all(counts.values())

    def predictions(self, r: Result) -> bool:
        n = self.wait(lambda: self.one("SELECT count(*) FROM prediction_accuracy"), 180)
        row = self.q("SELECT count(*), avg(abs_error_kw), avg(actual_power_kw) FROM prediction_accuracy")[0]
        r.evidence = {"scored_predictions": row[0], "mae_kw": row[1], "mean_actual_kw": row[2],
                      "predictions": self.one("SELECT count(*) FROM energy_predictions")}
        r.detail = f"{r.evidence['predictions']} predictions, {row[0]} scored, MAE {row[1] or 0:.3f} kW"
        return bool(n)

    def optimization(self, r: Result) -> bool:
        def found():
            approved = self.one("SELECT count(*) FROM optimization_actions WHERE source = 'optimizer'")
            executed = self.one("SELECT count(*) FROM optimization_actions WHERE source = 'optimizer' "
                                "AND status = 'executed'")
            return (approved, executed) if approved and (executed or self.mode == "advisory") else None
        result = self.wait(found, 240)
        approved = self.one("SELECT count(*) FROM optimization_actions WHERE source = 'optimizer'")
        executed = self.one("SELECT count(*) FROM optimization_actions WHERE source = 'optimizer' "
                            "AND status = 'executed'")
        r.evidence = {"optimizer_commands": approved, "executed_by_simulator": executed}
        r.detail = f"{approved} optimizer commands, {executed} executed by the simulator ({self.mode})"
        return bool(result) and (executed == 0 if self.mode == "advisory" else executed > 0)

    def live(self, r: Result) -> bool:
        self.wait(lambda: self.events["readings"] and self.events["status"], 30)
        r.evidence = {k: len(v) for k, v in self.events.items()}
        latest = self.events["status"][-1][1] if self.events["status"] else {}
        r.detail = (f"events received: {r.evidence}; simulation state in pushed status: "
                    f"{(latest.get('simulation') or {}).get('state')}")
        return bool(self.events["readings"] and self.events["status"])

    def warmup(self, r: Result) -> bool:
        start = self.one("SELECT min(event_time) FROM sensor_readings")
        target = start + timedelta(hours=WARMUP_SIM_HOURS)
        last = [None]

        def reached():
            now = self.sim_time()
            if now and (last[0] is None or (now - last[0]) >= timedelta(hours=4)):
                print(f"       simulated time {now:%Y-%m-%d %H:%M} (waiting for {target:%Y-%m-%d %H:%M})", flush=True)
                last[0] = now
            return now and now >= target
        ok = self.wait(reached, WARMUP_SIM_HOURS * 3600 / 300 * 2 + 120, interval=5)
        r.detail = f"simulated {start:%m-%d %H:%M} -> {self.sim_time():%m-%d %H:%M}"
        return bool(ok)

    def attack_detection(self, r: Result) -> bool:
        before_alerts = {a["alert_id"] for _, batch in self.events["alerts"] for a in batch}
        response = requests.post(f"{self.url}/api/simulation/attack", json=ATTACK, timeout=15)
        attack = self.wait(lambda: self.q("SELECT attack_id, starts_at, ends_at FROM simulated_attacks "
                                          "ORDER BY requested_at DESC LIMIT 1"), 30)
        if response.status_code != 202 or not attack:
            r.detail = f"POST attack -> {response.status_code}; attack recorded: {bool(attack)}"
            return False
        attack_id, starts, ends = attack[0]
        self.attack = {"id": str(attack_id), "start": starts, "end": ends}
        alert = self.wait(lambda: self.q(
            "SELECT alert_id, detected_at, detector FROM security_alerts WHERE zone_id = %s "
            "AND details->>'classification' = %s AND detected_at >= %s ORDER BY detected_at LIMIT 1",
            (ATTACK["zone_id"], ATTACK["attack_type"], starts)), 120)
        manipulated = self.one("SELECT readings_manipulated FROM simulated_attacks WHERE attack_id = %s", (attack_id,))
        pushed = self.wait(lambda: any(a.get("zone_id") == ATTACK["zone_id"] and a["alert_id"] not in before_alerts
                                       for _, batch in self.events["alerts"] for a in batch), 20)
        if not alert:
            r.detail = f"no {ATTACK['attack_type']} alert for {ATTACK['zone_id']} ({manipulated} readings manipulated)"
            return False
        alert_id, detected, detector = alert[0]
        self.attack.update(alert_id=str(alert_id), detected=detected)
        latency = (detected - starts).total_seconds() / 60
        r.evidence = {"attack_id": str(attack_id), "alert_id": str(alert_id), "detector": detector,
                      "detection_latency_sim_min": latency, "readings_manipulated": manipulated,
                      "alert_pushed_over_socketio": bool(pushed)}
        r.detail = (f"attack {ATTACK['attack_type']} on {ATTACK['zone_id']} detected after {latency:.0f} simulated "
                    f"min by {detector}; pushed to the dashboard: {bool(pushed)}")
        return bool(pushed)

    def drift(self, r: Result) -> bool:
        rows = self.wait(lambda: self.q("SELECT details->>'classification', count(*) FROM security_alerts "
                                        "WHERE alert_type = 'drift' GROUP BY 1"), 60)
        evaluated = self.one("SELECT count(*) FROM security_alerts WHERE alert_type = 'drift'")
        r.evidence = dict(rows or [])
        r.detail = f"drift alerts by classification: {r.evidence or 'none'}"
        return bool(evaluated)

    def agent(self, r: Result) -> bool:
        def decision():
            return self.q("SELECT decision_id, llm_provider, likely_cause, confidence, outcome, actions, tool_calls "
                          "FROM agent_decisions WHERE zone_id = %s AND started_at >= now() - interval '10 minutes' "
                          "AND actions::text LIKE '%%isolate_sensor%%' ORDER BY started_at LIMIT 1",
                          (ATTACK["zone_id"],))
        rows = self.wait(decision, 180)
        if not rows:
            any_decision = self.q("SELECT likely_cause, outcome FROM agent_decisions ORDER BY started_at DESC LIMIT 3")
            r.detail = f"no agent decision with an isolation for {ATTACK['zone_id']}; latest: {any_decision}"
            return False
        decision_id, provider, cause, confidence, outcome, actions, tools = rows[0]
        isolated = self.wait(lambda: self.q("SELECT updated_at FROM optimization_actions WHERE zone_id = %s AND "
                                            "action_type = 'isolate_sensor' AND status = 'executed' "
                                            "AND parameters->>'sensor' = 'temperature'", (ATTACK["zone_id"],)), 30)
        verdicts = [(a["action_type"], a["status"], a["safety_reason"]) for a in actions]
        self.attack["isolated_at"] = isolated[0][0] if isolated else None
        r.evidence = {"decision_id": str(decision_id), "provider": provider, "likely_cause": cause,
                      "confidence": confidence, "outcome": outcome, "tools": [t["tool"] for t in tools],
                      "actions": verdicts, "decision_pushed": bool(self.events["decisions"])}
        r.detail = f"{provider}: '{cause}' ({confidence}); {outcome}; temperature isolated: {bool(isolated)}"
        return bool(isolated) and all(v[2] for v in verdicts)

    def feedback(self, r: Result) -> bool:
        executed = self.wait(lambda: self.q(
            "SELECT parameters->>'result', parameters->>'applied_at_sim_time' FROM optimization_actions "
            "WHERE zone_id = %s AND source = 'agent' AND action_type = 'clear_overrides' AND status = 'executed' "
            "AND parameters ? 'applied_at_sim_time'", (ATTACK["zone_id"],)), 60)
        live = _get(f"{self.url}/api/live", as_json=True) or {}
        zone = next((z for z in live.get("readings", []) if z["zone_id"] == ATTACK["zone_id"]), {})
        evidence = {"agent_command_executed_by_simulator": executed[0] if executed else None,
                    "dashboard_isolated_sensors": zone.get("isolated_sensors")}
        ok = bool(executed) and "temperature" in (zone.get("isolated_sensors") or [])
        if self.mode == "closed_loop" and self.attack.get("isolated_at"):
            # Commands already in flight may still arrive briefly; after a 15-minute margin the optimizer
            # must leave the quarantined zone alone.
            time.sleep(15)
            margin = self.attack["isolated_at"] + timedelta(minutes=15)
            later = self.one("SELECT count(*) FROM optimization_actions WHERE source = 'optimizer' AND zone_id = %s "
                             "AND created_at > %s AND created_at < COALESCE((SELECT min(updated_at) FROM "
                             "optimization_actions WHERE zone_id = %s AND action_type = 'restore_sensor' AND "
                             "status = 'executed'), 'infinity')",
                             (ATTACK["zone_id"], margin, ATTACK["zone_id"]))
            evidence["optimizer_commands_for_quarantined_zone"] = later
            ok = ok and later == 0
        r.evidence = evidence
        r.detail = "; ".join(f"{k}: {v}" for k, v in evidence.items())
        return ok

    def recovery(self, r: Result) -> bool:
        rows = self.wait(lambda: self.q("SELECT updated_at FROM optimization_actions WHERE zone_id = %s AND "
                                        "action_type = 'restore_sensor' AND status = 'executed'",
                                        (ATTACK["zone_id"],)), 240, interval=5)
        if rows and self.attack.get("end"):
            minutes = (rows[0][0] - self.attack["end"]).total_seconds() / 60
            r.evidence = {"restored_at": rows[0][0], "minutes_after_attack_end": minutes}
            r.detail = f"temperature sensor restored {minutes:.0f} simulated min after the attack ended"
        else:
            r.detail = "not restored within the waiting time (zone not quiet yet)"
        return bool(rows)

    def stop(self, r: Result) -> bool:
        response = requests.post(f"{self.url}/api/simulation/stop", json={}, timeout=15)
        paused = self.wait(lambda: self.one("SELECT state FROM simulation_status WHERE building_id = %s",
                                            (self.config.SIMULATOR.building_id,)) == "paused", 30)
        frozen_at = self.sim_time()
        time.sleep(6)
        r.evidence = {"http_status": response.status_code, "sim_time": [frozen_at, self.sim_time()]}
        r.detail = f"POST stop -> {response.status_code}; paused: {bool(paused)}; simulated time frozen: " \
                   f"{frozen_at == self.sim_time()}"
        return response.status_code == 202 and bool(paused)

    def run(self) -> bool:
        steps = [("dashboard health + Socket.IO", True, self.dashboard),
                 ("start simulation from the dashboard API", True, self.start),
                 ("simulator -> Kafka", True, self.kafka),
                 ("Kafka -> processing workers -> PostgreSQL", True, self.storage),
                 ("ML energy predictions", True, self.predictions),
                 ("energy optimization", True, self.optimization),
                 ("dashboard live updates (Socket.IO)", True, self.live),
                 (f"warm-up to {WARMUP_SIM_HOURS:g} simulated hours", True, self.warmup),
                 ("cyberattack detection (+ live alert)", True, self.attack_detection),
                 ("drift detection", True, self.drift),
                 ("agent diagnosis and validated actions", True, self.agent),
                 ("building feedback loop", True, self.feedback),
                 ("recovery (sensor restored)", False, self.recovery),
                 ("stop simulation from the dashboard API", True, self.stop)]
        for name, required, fn in steps:
            result = self.check(name, required, fn)
            if required and not result.passed and name in ("dashboard health + Socket.IO",
                                                           "start simulation from the dashboard API"):
                break
        if self.socket is not None:
            self.socket.disconnect()
        self.db.close()
        return all(r.passed for r in self.results if r.required) and len(self.results) == len(steps)


def _get(url: str, *, as_json: bool = False):
    try:
        response = requests.get(url, timeout=5)
        return (response.json() if as_json else response.status_code) if response.ok else None
    except (requests.RequestException, ValueError):
        return None


# --------------------------------------------------------------------------- isolated launch
def isolated_environment(mode: str, port: int) -> dict[str, str]:
    tag = f"verify{datetime.now(timezone.utc):%m%d%H%M%S}"
    return {"POSTGRES_DB": f"smart_building_{tag}", "KAFKA_TOPIC_PREFIX": f"{tag}_",
            "KAFKA_CONSUMER_GROUP_PREFIX": tag, "FLASK_PORT": str(port), "CONTROL_MODE": mode,
            "WEATHER_PROVIDER": "mock", "CARBON_PROVIDER": "mock", "AGENT_LLM_PROVIDER": "offline",
            "AGENT_AUDIT_LOG": f"logs/{tag}_agent_decisions.jsonl", "LOG_TO_FILE": "false"}


def launch(env: dict[str, str], log_dir: Path):
    import subprocess

    from scripts.run_stack import SERVICES, Supervisor

    for step in (["-m", "database", "init"], ["-m", "streaming.topics"]):
        subprocess.run([sys.executable, *step], cwd=BASE_DIR, env=env, check=True, stdout=subprocess.DEVNULL)
    simulation = ["--start", "2026-02-09T00:00+00:00", "--interval", "1", "--speed", "300", "--seed", "2026"]
    commands = {n: [sys.executable, "-m", f"workers.{n}", *(simulation if n == "simulation_manager" else [])]
                for n in SERVICES}
    commands["dashboard"] = [sys.executable, "app.py"]
    supervisor = Supervisor(commands, env, log_dir, max_restarts=1)
    supervisor.start_all()
    return supervisor


def cleanup(env: dict[str, str]) -> None:
    """Drop the verification database and topics."""
    import psycopg
    from confluent_kafka.admin import AdminClient

    from config import get_config

    config = get_config()
    s = config.DATABASE
    with psycopg.connect(host=s.host, port=s.port, user=s.user, password=s.password, dbname="postgres",
                         autocommit=True, connect_timeout=5) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{env["POSTGRES_DB"]}" WITH (FORCE)')
    admin = AdminClient({"bootstrap.servers": config.KAFKA.bootstrap_servers})
    topics = [t for t in admin.list_topics(timeout=10).topics if t.startswith(env["KAFKA_TOPIC_PREFIX"])]
    if topics:
        for future in admin.delete_topics(topics).values():
            future.result()


def render(results: list[Result], mode: str, env: dict[str, str] | None, passed: bool, started: datetime) -> str:
    lines = [f"# System verification ({mode})", "",
             f"Run by `python -m scripts.verify_system{' --launch' if env else ''} --mode {mode}` from "
             f"{started:%Y-%m-%d %H:%M} UTC; result: **{'PASSED' if passed else 'FAILED'}**.", ""]
    if env:
        lines += [f"Isolated environment: database `{env['POSTGRES_DB']}`, topic prefix `{env['KAFKA_TOPIC_PREFIX']}`, "
                  f"dashboard port {env['FLASK_PORT']}, mock weather/carbon, offline agent policy.", ""]
    lines += ["| # | Check | Required | Result | Time (s) | Details |", "|---|---|---|---|---|---|"]
    for i, r in enumerate(results, 1):
        status = "PASS" if r.passed else ("FAIL" if r.required else "not observed")
        lines.append(f"| {i} | {r.step} | {'yes' if r.required else 'no'} | {status} | {r.seconds:.0f} | "
                     f"{r.detail.replace('|', '/')} |")
    lines += ["", "Evidence for every check is in the JSON file next to this report."]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m scripts.verify_system", description=__doc__.splitlines()[0])
    parser.add_argument("--launch", action="store_true", help="start an isolated stack for the verification")
    parser.add_argument("--mode", choices=["advisory", "closed_loop"], default="advisory")
    parser.add_argument("--url", default=None, help="dashboard URL (default: the launched one / FLASK_PORT)")
    parser.add_argument("--port", type=int, default=5055, help="dashboard port of the launched stack")
    parser.add_argument("--keep", action="store_true", help="keep the verification database and topics")
    parser.add_argument("--report", type=Path, help="write a Markdown report (JSON next to it)")
    parser.add_argument("--timeout-scale", type=float, default=1.0, help="multiply all waiting times")
    args = parser.parse_args(argv)

    started = datetime.now(timezone.utc)
    env = supervisor = None
    if args.launch:
        env = isolated_environment(args.mode, args.port)
        os.environ.update(env)                       # this process uses the same database/topics
        log_dir = BASE_DIR / "logs" / env["KAFKA_TOPIC_PREFIX"].rstrip("_")
        log_dir.mkdir(parents=True, exist_ok=True)
        supervisor = launch({**os.environ}, log_dir)
        print(f"isolated stack started (logs in {log_dir.relative_to(BASE_DIR)})", flush=True)
    url = args.url or f"http://127.0.0.1:{args.port if args.launch else os.environ.get('FLASK_PORT', '5000')}"
    passed = False
    results: list[Result] = []
    try:
        verifier = Verifier(url, args.mode, args.timeout_scale)
        passed = verifier.run()
        results = verifier.results
    finally:
        if supervisor is not None:
            supervisor.stop_all()
            # 0 = graceful stop; -15 = SIGTERM default action (the dashboard's web server)
            crashed = {n: p.returncode for n, p in supervisor.processes.items() if p.returncode not in (0, -15)}
            if crashed or any(supervisor.restarts.values()):
                print(f"services that crashed or restarted: {crashed} restarts {supervisor.restarts}", flush=True)
                passed = False
                results.append(Result("no service crashed or restarted", True, False,
                                      detail=f"exit codes {crashed}, restarts {supervisor.restarts}"))
            else:
                results.append(Result("no service crashed or restarted", True, True,
                                      detail=f"{len(supervisor.processes)} processes stopped cleanly"))
            if not args.keep:
                cleanup(env)
    print(f"\nSYSTEM VERIFICATION {'PASSED' if passed else 'FAILED'}")
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(render(results, args.mode, env, passed, started), encoding="utf-8")
        args.report.with_suffix(".json").write_text(json.dumps(
            {"mode": args.mode, "passed": passed, "started": started.isoformat(), "environment": env,
             "results": [asdict(r) for r in results]}, indent=2, default=str), encoding="utf-8")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
