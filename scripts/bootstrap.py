"""One-time setup of data and models (idempotent: finished steps are skipped).

    python -m scripts.bootstrap                 # everything needed for advisory mode
    python -m scripts.bootstrap --closed-loop   # also the optimizer-controlled detectors
    python -m scripts.bootstrap --force-models  # retrain even if models exist

Steps (each runs as its own process, so logs and failures stay separate):

1. ``python -m database init``                     schema + zone registry
2. ``python -m streaming.topics``                  Kafka topics
3. building history (28 simulated days) through the real pipeline:
   ``workers.sensor_publisher --fast`` -> Kafka -> ``workers.sensor_persistence`` -> PostgreSQL
4. ``python -m ml.train --dataset building ...``   ML model for the prediction service
5. ``python -m security.train``                    detectors for advisory mode
6. ``python -m security.train --control optimizer`` detectors for closed-loop mode (``--closed-loop``)
7. ``python -m scripts.preflight``
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time

from config import BASE_DIR, get_config

HISTORY_START = "2026-01-05T00:00+00:00"
HISTORY_END = "2026-02-02T00:00+00:00"
HISTORY_TICKS = 28 * 288                   # 5-minute readings
HISTORY_SEED = 2026


def step(title: str, command: list[str]) -> None:
    print(f"\n== {title}\n   $ python {' '.join(command)}", flush=True)
    started = time.monotonic()
    result = subprocess.run([sys.executable, *command], cwd=BASE_DIR)
    if result.returncode != 0:
        raise SystemExit(f"Step failed ({title}), exit code {result.returncode}")
    print(f"   done in {time.monotonic() - started:.0f} s", flush=True)


def history_rows() -> int:
    from database.db import Database

    db = Database(get_config().DATABASE, name="bootstrap")
    db.open(timeout_s=30)
    try:
        with db.transaction() as conn:
            return conn.execute("SELECT count(*) FROM sensor_readings WHERE event_time >= %s AND event_time < %s",
                                (HISTORY_START, HISTORY_END)).fetchone()[0]
    finally:
        db.close()


def generate_history() -> None:
    expected = HISTORY_TICKS * 5
    have = history_rows()
    if have >= expected * 0.99:
        print(f"\n== building history: {have} readings already stored, skipped")
        return
    print(f"\n== building history: {have} of {expected} readings stored; generating through Kafka", flush=True)
    persistence = subprocess.Popen([sys.executable, "-m", "workers.sensor_persistence"], cwd=BASE_DIR)
    try:
        time.sleep(5)
        step("publish 28 simulated days", ["-m", "workers.sensor_publisher", "--fast", "--interval", "1",
                                           "--speed", "300", "--ticks", str(HISTORY_TICKS), "--seed",
                                           str(HISTORY_SEED), "--start", HISTORY_START])
        deadline = time.monotonic() + 600
        while (have := history_rows()) < expected * 0.99 and time.monotonic() < deadline:
            print(f"   waiting for persistence: {have}/{expected}", flush=True)
            time.sleep(10)
        if have < expected * 0.99:
            raise SystemExit(f"Only {have} of {expected} readings were stored; check logs/sensor_persistence.log")
    finally:
        persistence.terminate()
        persistence.wait(timeout=30)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m scripts.bootstrap", description=__doc__.splitlines()[0])
    parser.add_argument("--closed-loop", action="store_true", help="also train optimizer-controlled detectors")
    parser.add_argument("--force-models", action="store_true", help="retrain models even if they exist")
    args = parser.parse_args(argv)

    from ml.features import BUILDING_SPEC
    from ml.registry import ModelNotFoundError, load_bundle
    from security.train import ARTIFACT_ROOT, OPTIMIZER_CONTROL_ROOT

    step("database schema", ["-m", "database", "init"])
    step("kafka topics", ["-m", "streaming.topics"])
    generate_history()
    try:
        load_bundle(BUILDING_SPEC.name)
        have_model = not args.force_models
    except ModelNotFoundError:
        have_model = False
    if have_model:
        print("\n== ML building model exists, skipped")
    else:
        step("train ML building model", ["-m", "ml.train", "--dataset", "building", "--start", HISTORY_START,
                                         "--end", HISTORY_END])
    if args.force_models or not (ARTIFACT_ROOT / "latest.json").is_file():
        step("train detectors (advisory mode)", ["-m", "security.train"])
    else:
        print("\n== detectors (advisory mode) exist, skipped")
    if args.closed_loop or get_config().SYSTEM.detector_set == "optimizer_control":
        if args.force_models or not (OPTIMIZER_CONTROL_ROOT / "latest.json").is_file():
            step("train detectors (closed-loop mode)", ["-m", "security.train", "--control", "optimizer"])
        else:
            print("\n== detectors (closed-loop mode) exist, skipped")
    step("preflight", ["-m", "scripts.preflight"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
