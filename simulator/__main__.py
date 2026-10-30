"""Command-line runner for the building simulator.

Writes sensor events as JSON Lines to stdout (or a file); logs go to stderr.

Examples::

    python -m simulator                                  # real time, settings from .env
    python -m simulator --interval 1 --speed 60 --ticks 10
    python -m simulator --speed 3600 --ticks 24 --fast --seed 42 --output data/sample.jsonl
"""

from __future__ import annotations

import argparse
import dataclasses
import sys
from datetime import datetime
from pathlib import Path

from config import ConfigurationError, SimulatorSettings, get_config
from simulator.engine import BuildingSimulator, json_lines_sink
from utils.logging_config import configure_logging


def add_simulation_arguments(parser: argparse.ArgumentParser) -> None:
    """Options shared by every command that runs the simulator."""
    parser.add_argument("--interval", type=float, help="seconds between readings (1-5)")
    parser.add_argument("--speed", type=float, help="simulated seconds per real second")
    parser.add_argument("--ticks", type=int, help="stop after N readings per zone")
    parser.add_argument("--seed", type=int, help="random seed for a reproducible run")
    parser.add_argument("--start", help="simulated start time, ISO 8601 (e.g. 2026-01-15T08:00)")
    parser.add_argument("--fast", action="store_true", help="do not wait between ticks")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m simulator",
                                     description="Run the smart building simulator.")
    add_simulation_arguments(parser)
    parser.add_argument("--output", type=Path, help="write JSON Lines to this file instead of stdout")
    return parser.parse_args(argv)


def build_settings(base: SimulatorSettings, args: argparse.Namespace) -> SimulatorSettings:
    changes: dict = {}
    if args.interval is not None:
        changes["interval_seconds"] = args.interval
    if args.speed is not None:
        changes["speed_factor"] = args.speed
    if args.seed is not None:
        changes["random_seed"] = args.seed
    if args.start:
        start = datetime.fromisoformat(args.start)
        changes["start_time"] = start if start.tzinfo else start.replace(tzinfo=base.tzinfo)
    return dataclasses.replace(base, **changes)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = get_config()
        settings = build_settings(config.SIMULATOR, args)
    except (ConfigurationError, ValueError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    logger = configure_logging(config.LOGGING, "simulator", stream=sys.stderr)

    simulator = BuildingSimulator(settings)
    output = None
    try:
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            output = args.output.open("a", encoding="utf-8", newline="\n")
        sink = json_lines_sink(output or sys.stdout)
        simulator.run(sink, max_ticks=args.ticks, realtime=not args.fast)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except OSError as exc:
        logger.error("Cannot write simulator output", extra={"error": str(exc)})
        return 1
    finally:
        if output:
            output.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
