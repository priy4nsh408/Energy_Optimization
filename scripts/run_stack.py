"""Start every service of the system as separate processes (development helper).

    python -m scripts.run_stack                       # all workers + dashboard (CONTROL_MODE from .env)
    python -m scripts.run_stack --mode closed_loop    # simulator also executes optimizer commands
    python -m scripts.run_stack --no-dashboard
    python -m scripts.run_stack --only simulation_manager,sensor_persistence

Each service runs as its own process (``python -m workers.<name>``) with its
output in ``logs/stack/<name>.log``. A service that exits unexpectedly is
restarted (at most ``--max-restarts`` times, with increasing delay); Ctrl+C
stops them all. Works on Windows and Linux. ``scripts.preflight`` runs first
and blocks the start if Kafka, PostgreSQL, the schema or a model is missing
(``python -m scripts.bootstrap`` prepares everything once).

The simulation starts paused; press Start on the dashboard. Without
``--start`` it begins on 2026-02-09 (inside the security detectors' training
season) or, if the database already holds later data from an earlier run,
right after that data, so restarts continue the timeline instead of
overlapping it.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from config import BASE_DIR

DEFAULT_START = "2026-02-09T00:00:00+00:00"
SERVICES: dict[str, list[str]] = {
    "sensor_persistence": [],
    "energy_processor": [],
    "record_persistence": [],
    "external_data_publisher": [],
    "prediction_service": [],
    "optimization_service": [],
    "security_service": [],
    "agent_service": [],
    "simulation_manager": [],         # last: consumers are ready before readings flow
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m scripts.run_stack", description=__doc__.splitlines()[0])
    parser.add_argument("--only", help="comma-separated subset of: " + ", ".join(SERVICES))
    parser.add_argument("--no-dashboard", action="store_true")
    parser.add_argument("--start", help=f"simulated start time (default {DEFAULT_START} or after the stored data)")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between readings")
    parser.add_argument("--speed", type=float, default=300.0, help="simulated seconds per real second")
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--autostart", action="store_true", help="start the simulation immediately")
    parser.add_argument("--mock-external", action="store_true",
                        help="use mock weather/carbon providers (no API keys or internet)")
    parser.add_argument("--mode", choices=["advisory", "closed_loop"], help="override CONTROL_MODE")
    parser.add_argument("--max-restarts", type=int, default=3, help="per service (default 3)")
    parser.add_argument("--skip-preflight", action="store_true")
    return parser.parse_args(argv)


def continue_after_stored_data(step_s: float) -> str:
    """Start time for the simulation: DEFAULT_START, or one step after the latest stored reading.

    Reusing simulated dates that are already stored would mix two runs in the same hours of every
    chart and aggregate, so a restart continues where the previous run stopped.
    """
    from config import get_config
    from database.db import Database

    default = datetime.fromisoformat(DEFAULT_START)
    try:
        database = Database(get_config().DATABASE, name="run_stack")
        database.open(timeout_s=10)
        try:
            with database.transaction() as conn:
                latest = conn.execute("SELECT greatest((SELECT max(event_time) FROM sensor_readings), "
                                      "(SELECT max(event_time) FROM energy_records))").fetchone()[0]
        finally:
            database.close()
    except Exception as exc:  # noqa: BLE001 - preflight reports database problems
        print(f"could not read the latest stored reading ({exc}); starting at {DEFAULT_START}", flush=True)
        return DEFAULT_START
    if latest is None or latest < default:
        return DEFAULT_START
    start = latest + timedelta(seconds=step_s)
    print(f"database already holds simulated data up to {latest.isoformat()}; continuing from "
          f"{start.isoformat()}", flush=True)
    return start.isoformat()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    names = [n.strip() for n in args.only.split(",")] if args.only else list(SERVICES)
    unknown = [n for n in names if n not in SERVICES]
    if unknown:
        print(f"Unknown services: {unknown}", file=sys.stderr)
        return 2
    log_dir = BASE_DIR / "logs" / "stack"
    log_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    if args.mock_external:
        env.update(WEATHER_PROVIDER="mock", CARBON_PROVIDER="mock")
    if args.mode:
        env["CONTROL_MODE"] = args.mode
    if not args.skip_preflight:
        print("running preflight checks ...", flush=True)
        if subprocess.run([sys.executable, "-m", "scripts.preflight"], cwd=BASE_DIR, env=env).returncode != 0:
            print("Preflight failed: fix the problems above (or run python -m scripts.bootstrap).", file=sys.stderr)
            return 2
    start = args.start or continue_after_stored_data(args.interval * args.speed)
    simulation = ["--start", start, "--interval", str(args.interval), "--speed", str(args.speed),
                  "--seed", str(args.seed), *(["--autostart"] if args.autostart else [])]
    commands = {n: [sys.executable, "-m", f"workers.{n}", *SERVICES[n],
                    *(simulation if n == "simulation_manager" else [])] for n in names}
    if not args.no_dashboard:
        commands["dashboard"] = [sys.executable, "app.py"]

    supervisor = Supervisor(commands, env, log_dir, max_restarts=args.max_restarts)
    try:
        supervisor.start_all()
        if "dashboard" in commands:
            print(f"\nDashboard: http://127.0.0.1:{env.get('FLASK_PORT', '5000')}  (Ctrl+C stops everything)")
        print(f"control mode: {env.get('CONTROL_MODE', 'advisory')}; logs: logs/stack/", flush=True)
        while supervisor.watch():
            time.sleep(2)
        return 1
    except KeyboardInterrupt:
        print("\nstopping ...")
        return 0
    finally:
        supervisor.stop_all()


class Supervisor:
    """Starts the services, restarts crashed ones (bounded, with backoff) and stops them in reverse order."""

    def __init__(self, commands: dict[str, list[str]], env: dict[str, str], log_dir: Path, *,
                 max_restarts: int = 3) -> None:
        self.commands, self.env, self.log_dir, self.max_restarts = commands, env, log_dir, max_restarts
        self.processes: dict[str, subprocess.Popen] = {}
        self.handles: dict[str, object] = {}
        self.restarts: dict[str, int] = {name: 0 for name in commands}
        self.restart_at: dict[str, float] = {}

    def _start(self, name: str) -> None:
        handle = self.handles.get(name) or open(self.log_dir / f"{name}.log", "a", encoding="utf-8")  # noqa: SIM115
        self.handles[name] = handle
        # Windows: a process group of its own, so stop_all() can send Ctrl+Break (a graceful stop that
        # the workers handle) instead of TerminateProcess.
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        self.processes[name] = subprocess.Popen(self.commands[name], cwd=BASE_DIR, env=self.env, stdout=handle,
                                                stderr=subprocess.STDOUT, creationflags=flags)
        print(f"started {name:<24} pid {self.processes[name].pid}  log logs/stack/{name}.log", flush=True)

    def start_all(self) -> None:
        for name in self.commands:
            self._start(name)
            time.sleep(0.5)

    def watch(self) -> bool:
        """Restart crashed services; returns False when a service exceeded its restarts."""
        now = time.monotonic()
        for name, process in list(self.processes.items()):
            if process.poll() is None:
                continue
            if name in self.restart_at:
                if now >= self.restart_at.pop(name):
                    self._start(name)
                continue
            if self.restarts[name] >= self.max_restarts:
                print(f"{name} exited with code {process.returncode} {self.restarts[name] + 1} times; "
                      f"giving up (see logs/stack/{name}.log)", flush=True)
                return False
            self.restarts[name] += 1
            delay = 2 ** self.restarts[name]
            print(f"{name} exited with code {process.returncode}; restart {self.restarts[name]}/"
                  f"{self.max_restarts} in {delay} s", flush=True)
            self.restart_at[name] = now + delay
        return True

    def stop_all(self) -> None:
        for process in reversed(list(self.processes.values())):
            if process.poll() is None:
                if os.name == "nt":
                    process.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    process.terminate()                  # SIGTERM: graceful stop in every worker
        for process in self.processes.values():
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
        for handle in self.handles.values():
            handle.close()


if __name__ == "__main__":
    sys.exit(main())
