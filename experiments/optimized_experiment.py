"""Run only the optimized scenario and print its metrics as JSON.

    python -m experiments.optimized_experiment --start 2026-01-19 --days 7 --seed 101 [--ml]
"""

from __future__ import annotations

import json
import sys

from config import get_config
from experiments.baseline_experiment import parse, simulator_settings
from experiments.scenarios import make_optimizer, run_scenario
from ml.features import BUILDING_SPEC
from ml.registry import load_bundle


def main(argv: list[str] | None = None) -> int:
    import argparse

    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--ml", action="store_true", help="use the trained building model as forecast basis")
    known, rest = pre.parse_known_args(argv)
    args = parse(rest, "Optimized (SciPy) control scenario.")
    bundle = load_bundle(BUILDING_SPEC.name) if known.ml else None
    result = run_scenario("optimized", simulator_settings(args), args.days,
                          optimizer=make_optimizer(get_config().OPTIMIZATION), bundle=bundle)
    print(json.dumps(result.as_dict(), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
