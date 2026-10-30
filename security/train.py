"""Train the security detectors on clean (attack-free) data.

    python -m security.train                                   # simulated clean history (default)
    python -m security.train --source db --start 2026-01-05T00:00+00:00 --end 2026-02-02T00:00+00:00

Steps:
    1. clean readings (simulator or the Kafka-derived ``sensor_readings`` table)
    2. chronological split: first 75 % train, last 25 % validation
    3. Isolation Forest per zone on train; threshold = 99.5th percentile of
       validation scores (expected false-positive rate about 0.5 %)
    4. rule limits from all clean data; security-drift monitors are checked on
       the clean validation period (reference = training period) and any
       (zone, feature) monitor with > 2 % false alarms there is disabled
    5. final drift reference from all clean data
    6. save ``security/artifacts/<version>/detectors.joblib`` + ``metadata.json``
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib

from config import BASE_DIR, SimulatorSettings, get_config
from database.models import SensorReadingRecord
from processors.data_validator import validate_sensor_event
from security.anomaly_detector import AnomalyDetector
from security.attack_detector import ZoneRuleLimits
from security.drift_detector import SECURITY_DRIFT_FEATURES, DriftDetector, DriftReference
from security.features import FeatureRow, FeatureTracker
from security.monitor import SECURITY_DRIFT_EFFECT_SIZE, SecurityModels
from simulator.engine import BuildingSimulator
from utils.logging_config import configure_logging

logger = logging.getLogger(__name__)

ARTIFACT_ROOT = BASE_DIR / "security" / "artifacts"
TRAIN_FRACTION = 0.75
MAX_DRIFT_FALSE_ALARM_RATE = 0.02


class SecurityModelNotFoundError(FileNotFoundError):
    pass


def simulate_clean_readings(days: float, seed: int, start: datetime) -> list[SensorReadingRecord]:
    simulator = BuildingSimulator(SimulatorSettings(interval_seconds=1, speed_factor=300,
                                                    random_seed=seed, start_time=start))
    ticks = int(days * 86400 / 300)
    return [validate_sensor_event(e) for _ in range(ticks) for e in simulator.tick()]


def load_db_readings(start: datetime | None, end: datetime | None) -> list[SensorReadingRecord]:
    from dataclasses import fields

    from database.db import Database

    config = get_config()
    names = [f.name for f in fields(SensorReadingRecord)]
    where, params = ["TRUE"], []
    if start:
        where.append("event_time >= %s"), params.append(start)
    if end:
        where.append("event_time < %s"), params.append(end)
    with Database(config.DATABASE, name="security_train") as db, db.transaction() as conn:
        rows = conn.execute(f"SELECT {', '.join(names)} FROM sensor_readings WHERE {' AND '.join(where)} "
                            "ORDER BY event_time, zone_id", params).fetchall()
    return [SensorReadingRecord(**dict(zip(names, row))) for row in rows]


def train_models(readings: list[SensorReadingRecord], *, random_state: int = 0,
                 source: dict[str, Any] | None = None) -> SecurityModels:
    readings = sorted(readings, key=lambda r: (r.event_time, r.zone_id))
    if len(readings) < 500:
        raise ValueError(f"Need more clean readings to train (got {len(readings)})")
    cutoff = readings[int(len(readings) * TRAIN_FRACTION)].event_time
    tracker = FeatureTracker()
    rows: dict[str, list[FeatureRow]] = defaultdict(list)
    train: dict[str, list[dict]] = defaultdict(list)
    validation: dict[str, list[dict]] = defaultdict(list)
    reference = DriftReference()
    ordered: list[tuple[SensorReadingRecord, FeatureRow]] = []
    for reading in readings:
        row = tracker.update(reading)
        rows[reading.zone_id].append(row)
        ordered.append((reading, row))
        (train if reading.event_time < cutoff else validation)[reading.zone_id].append(row.values)

    anomaly = AnomalyDetector().fit(train, validation, random_state=random_state)
    limits = {zone: ZoneRuleLimits.calibrate(zone_rows) for zone, zone_rows in rows.items()}
    train_reference = DriftReference()
    for reading, row in ordered:           # residuals need the fitted CO2 model
        row.values["co2_residual"] = limits[reading.zone_id].co2_model.residual(row.values)
        reference.add(reading.zone_id, reading.event_time, row.values)
        if reading.event_time < cutoff:
            train_reference.add(reading.zone_id, reading.event_time, row.values)
    disabled, alarm_rates = calibrate_drift_monitors(train_reference.freeze(), ordered, cutoff)
    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": source or {},
        "readings": len(readings),
        "start": readings[0].event_time.isoformat(), "end": readings[-1].event_time.isoformat(),
        "validation_start": cutoff.isoformat(),
        "isolation_forest_thresholds": {z: m.threshold for z, m in anomaly.models.items()},
        "threshold_quantile": anomaly.threshold_quantile,
        "rule_limits": {z: {k: v for k, v in vars(l).items()
                            if k not in ("co2_model", "control_error_bucket_range")}
                        | {"co2_max_abs_residual": l.co2_model.max_abs_residual} for z, l in limits.items()},
    }
    metadata["drift_monitor_false_alarm_rates"] = alarm_rates
    metadata["disabled_drift_monitors"] = sorted(f"{z}:{f}" for z, f in disabled)
    return SecurityModels(anomaly, limits, reference.freeze(), metadata, frozenset(disabled))


def calibrate_drift_monitors(train_reference: DriftReference, ordered, cutoff,
                             max_false_alarm_rate: float = MAX_DRIFT_FALSE_ALARM_RATE):
    """Run security-drift detection over the clean validation period; monitors
    that alarm too often on clean data are unreliable and get disabled."""
    detector = DriftDetector(train_reference, features=SECURITY_DRIFT_FEATURES,
                             min_effect_size=SECURITY_DRIFT_EFFECT_SIZE)
    evaluations: dict[tuple[str, str], int] = defaultdict(int)
    alarms: dict[tuple[str, str], int] = defaultdict(int)
    for reading, row in ordered:
        if reading.event_time < cutoff:
            continue
        result = detector.update(reading.zone_id, reading.event_time, row.values)
        if result is None:
            continue
        for feature in result.features:
            evaluations[(reading.zone_id, feature.feature)] += 1
        for feature in result.drifting:
            alarms[(reading.zone_id, feature)] += 1
    rates = {f"{z}:{f}": alarms[(z, f)] / n for (z, f), n in evaluations.items() if n}
    disabled = {(z, f) for (z, f), n in evaluations.items() if n and alarms[(z, f)] / n > max_false_alarm_rate}
    return disabled, rates


def save_models(models: SecurityModels, root: Path = ARTIFACT_ROOT) -> Path:
    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    directory = root / version
    directory.mkdir(parents=True, exist_ok=False)
    models.metadata["version"] = version
    joblib.dump(models, directory / "detectors.joblib")
    (directory / "metadata.json").write_text(json.dumps(models.metadata, indent=2, default=str), encoding="utf-8")
    (root / "latest.json").write_text(json.dumps({"version": version}), encoding="utf-8")
    return directory


def load_models(version: str | None = None, root: Path = ARTIFACT_ROOT) -> SecurityModels:
    """Load trained detectors (only load files you created: joblib can run code)."""
    if version is None:
        pointer = root / "latest.json"
        if not pointer.is_file():
            raise SecurityModelNotFoundError(f"No security models in {root}; run: python -m security.train")
        version = json.loads(pointer.read_text(encoding="utf-8"))["version"]
    path = root / version / "detectors.joblib"
    if not path.is_file():
        raise SecurityModelNotFoundError(f"Missing {path}")
    return joblib.load(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m security.train", description=__doc__.splitlines()[0])
    parser.add_argument("--source", choices=["simulator", "db"], default="simulator")
    parser.add_argument("--days", type=float, default=28.0, help="simulator: days of clean history")
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--start", type=datetime.fromisoformat, default=datetime(2026, 1, 5, tzinfo=timezone.utc))
    parser.add_argument("--end", type=datetime.fromisoformat)
    parser.add_argument("--artifacts", type=Path, default=ARTIFACT_ROOT)
    args = parser.parse_args(argv)
    config = get_config()
    configure_logging(config.LOGGING, "security_train", stream=sys.stderr)

    if args.source == "simulator":
        readings = simulate_clean_readings(args.days, args.seed, args.start)
        source = {"type": "simulator", "days": args.days, "seed": args.seed, "start": args.start.isoformat()}
    else:
        readings = load_db_readings(args.start, args.end)
        source = {"type": "postgresql:sensor_readings", "start": str(args.start), "end": str(args.end)}
    models = train_models(readings, source=source)
    directory = save_models(models, args.artifacts)
    print(json.dumps({k: models.metadata[k] for k in ("version", "readings", "start", "end",
                                                       "isolation_forest_thresholds")}, indent=2))
    print(f"Saved: {directory}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
