"""Start every service of the system as separate processes (development helper).

    python -m scripts.run_stack                       # all workers + dashboard
    python -m scripts.run_stack --no-dashboard
    python -m scripts.run_stack --only simulation_manager,sensor_persistence

Each service runs as its own process (``python -m workers.<name>``) with its
log in ``logs/stack/<name>.log``. Ctrl+C stops them all. Works on Windows and
Linux. Kafka and PostgreSQL must already be running (docker compose), the
schema initialised (``python -m database init``) and the models trained.

The simulation starts paused at ``--start`` (default 2026-02-09, inside the
security detectors' training season); press Start on the dashboard.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from config import BASE_DIR

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
    parser.add_argument("--start", default="2026-02-09T00:00+00:00", help="simulated start time")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between readings")
    parser.add_argument("--speed", type=float, default=300.0, help="simulated seconds per real second")
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--autostart", action="store_true", help="start the simulation immediately")
    parser.add_argument("--mock-external", action="store_true",
                        help="use mock weather/carbon providers (no API keys or internet)")
    return parser.parse_args(argv)


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
    simulation = ["--start", args.start, "--interval", str(args.interval), "--speed", str(args.speed),
                  "--seed", str(args.seed), *(["--autostart"] if args.autostart else [])]
    commands = {n: [sys.executable, "-m", f"workers.{n}", *SERVICES[n],
                    *(simulation if n == "simulation_manager" else [])] for n in names}
    if not args.no_dashboard:
        commands["dashboard"] = [sys.executable, "app.py"]

    processes: dict[str, tuple[subprocess.Popen, object]] = {}
    try:
        for name, command in commands.items():
            handle = open(log_dir / f"{name}.log", "a", encoding="utf-8")   # noqa: SIM115 - closed below
            processes[name] = (subprocess.Popen(command, cwd=BASE_DIR, env=env, stdout=handle,
                                                stderr=subprocess.STDOUT), handle)
            print(f"started {name:<24} pid {processes[name][0].pid}  log {Path('logs/stack') / (name + '.log')}")
            time.sleep(0.5)
        if "dashboard" in processes:
            config_port = os.environ.get("FLASK_PORT", "5000")
            print(f"\nDashboard: http://127.0.0.1:{config_port}  (Ctrl+C stops everything)")
        while True:
            for name, (process, _) in processes.items():
                if process.poll() is not None:
                    print(f"{name} exited with code {process.returncode}; see logs/stack/{name}.log")
                    return 1
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nstopping ...")
        return 0
    finally:
        for name, (process, _) in reversed(list(processes.items())):
            if process.poll() is None:
                process.terminate()
        for name, (process, handle) in processes.items():
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
            handle.close()


if __name__ == "__main__":
    sys.exit(main())
