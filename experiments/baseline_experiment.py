"""Run only the baseline scenario and print its metrics as JSON.

    python -m experiments.baseline_experiment --start 2026-01-19 --days 7 --seed 101
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from config import SimulatorSettings
from experiments.scenarios import run_scenario


def parse(argv: list[str] | None, description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--start", type=datetime.fromisoformat, default=datetime(2026, 1, 19))
    parser.add_argument("--days", type=float, default=7.0)
    parser.add_argument("--seed", type=int, default=101)
    return parser.parse_args(argv)


def simulator_settings(args: argparse.Namespace) -> SimulatorSettings:
    start = args.start if args.start.tzinfo else args.start.replace(tzinfo=timezone.utc)
    return SimulatorSettings(interval_seconds=1, speed_factor=300, random_seed=args.seed, start_time=start)


def main(argv: list[str] | None = None) -> int:
    args = parse(argv, "Baseline (schedule-based) control scenario.")
    result = run_scenario("baseline", simulator_settings(args), args.days)
    print(json.dumps(result.as_dict(), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
