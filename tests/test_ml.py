"""Tests for the ML pipeline: data loading, leakage-safe features, split,
training/evaluation, persistence and online inference."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from config import SimulatorSettings
from ml.dataset import (
    BUILDING_COLUMNS,
    DEFAULT_KAG_PATH,
    DatasetError,
    DatasetInfo,
    load_kag,
    prepare_building_frame,
)
from ml.evaluate import regression_metrics
from ml.features import BUILDING_SPEC, KAG_SPEC, FeatureSpec, build_features
from ml.models import predict
from ml.predict import EnergyPredictor, IncompatibleModelError, OnlinePredictor
from ml.registry import ModelNotFoundError, load_bundle
from ml.train import chronological_split, render_report, train_and_evaluate
from processors.data_validator import validate_prediction_message, validate_sensor_event
from simulator.engine import BuildingSimulator

KAG_HEADER = ["date", "Appliances", "lights", *[f"T{i}" for i in range(1, 10)],
              *[f"RH_{i}" for i in range(1, 10)], "T_out", "Press_mm_hg", "RH_out", "Windspeed",
              "Visibility", "Tdewpoint", "rv1", "rv2"]


def write_kag_csv(path: Path, rows: int = 400, seed: int = 0) -> Path:
    """Small file in the same format as KAG_energydata_complete.csv (quoted, padded numbers)."""
    rng = np.random.default_rng(seed)
    times = pd.date_range("2016-01-11 17:00", periods=rows, freq="10min")
    daily = 60 + 40 * np.sin(2 * np.pi * (times.hour * 60 + times.minute) / 1440)
    appliances = np.round(np.asarray(daily) + rng.normal(0, 10, rows)).clip(10)
    lines = [",".join(f'"{h}"' for h in KAG_HEADER)]
    for i, t in enumerate(times):
        values = [f'"{t:%Y-%m-%d %H:%M:%S}"', f'"  {appliances[i]:.0f}"', '"10"']
        values += [f'"{20 + rng.normal():.4f}"' for _ in range(9)]
        values += [f'"{40 + rng.normal():.4f}"' for _ in range(9)]
        values += [f'"{v:.3f}"' for v in (5 + rng.normal(), 755, 80, 3, 40, 2)]
        values += [f'"{rng.random():.5f}"'] * 2
        lines.append(",".join(values))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.fixture(scope="module")
def kag_frame(tmp_path_factory):
    path = write_kag_csv(tmp_path_factory.mktemp("kag") / "KAG_energydata_complete.csv", rows=1500)
    return load_kag(path)


def small_spec(**overrides) -> FeatureSpec:
    return replace(KAG_SPEC, lags=(1, 2, 6), rolling_windows=(3, 6), **overrides)


def building_rows(ticks: int, seed: int = 1, start=datetime(2026, 1, 5, tzinfo=timezone.utc)):
    simulator = BuildingSimulator(SimulatorSettings(interval_seconds=1, speed_factor=300,
                                                    random_seed=seed, start_time=start))
    rows = []
    for _ in range(ticks):
        for event in simulator.tick():
            reading = validate_sensor_event(event)
            row = {c: getattr(reading, c) for c in BUILDING_COLUMNS}
            row["event_id"] = str(reading.event_id)
            row["building_id"] = reading.building_id
            rows.append(row)
    return rows


@pytest.fixture(scope="module")
def building_bundle(tmp_path_factory):
    rows = building_rows(ticks=600)
    frame = build_features(prepare_building_frame(pd.DataFrame(rows)), BUILDING_SPEC)
    info = DatasetInfo("building_test", "simulator", len(rows), "", "")
    root = tmp_path_factory.mktemp("artifacts")
    bundle, _ = train_and_evaluate(frame, BUILDING_SPEC, info, artifact_root=root)
    return bundle, root


# --------------------------------------------------------------------------- dataset
def test_load_kag_parses_cleans_and_records_provenance(tmp_path):
    path = write_kag_csv(tmp_path / "data.csv", rows=1200)
    with path.open("a", encoding="utf-8") as handle:            # duplicate last line
        handle.write(path.read_text(encoding="utf-8").splitlines()[-1] + "\n")
    df, info = load_kag(path)
    assert len(df) == 1200 and info.dropped_rows["duplicate_timestamps"] == 1
    assert "rv1" not in df.columns and "rv2" not in df.columns
    assert df["Appliances"].dtype == float and df["timestamp"].is_monotonic_increasing
    assert info.sha256 and len(info.sha256) == 64


def test_load_kag_errors(tmp_path):
    with pytest.raises(DatasetError, match="not found"):
        load_kag(tmp_path / "missing.csv")
    bad = tmp_path / "bad.csv"
    bad.write_text('"date","Appliances"\n"2016-01-01 00:00:00","50"\n', encoding="utf-8")
    with pytest.raises(DatasetError, match="missing columns"):
        load_kag(bad)


@pytest.mark.skipif(not DEFAULT_KAG_PATH.is_file(), reason="KAG_energydata_complete.csv not present")
def test_real_kag_dataset_structure():
    df, info = load_kag(DEFAULT_KAG_PATH)
    assert info.rows == 19735 and df["timestamp"].diff().dropna().eq(pd.Timedelta("10min")).all()


# --------------------------------------------------------------------------- features
def test_features_do_not_use_future_data(kag_frame):
    df, _ = kag_frame
    spec = small_spec()
    base = build_features(df, spec)
    cut = 300
    tampered = df.copy()
    tampered.loc[cut + 1:, ["Appliances", "T1", "lights"]] *= 7   # change only the future
    changed = build_features(tampered, spec)
    rows = base.timestamps <= df["timestamp"].iloc[cut]
    pd.testing.assert_frame_equal(base.X[rows], changed.X[rows])
    # ...but the target of row `cut` is the (changed) next value, as intended.
    at_cut = base.timestamps == df["timestamp"].iloc[cut]
    assert float(changed.y[at_cut].iloc[0]) == pytest.approx(7 * float(base.y[at_cut].iloc[0]))


def test_lag_rolling_and_target_values(kag_frame):
    df, _ = kag_frame
    frame = build_features(df, small_spec())
    i = 50
    t = frame.timestamps[i]
    position = df.index[df["timestamp"] == t][0]
    series = df["Appliances"]
    assert frame.X["target_lag0"][i] == series[position]
    assert frame.X["target_lag6"][i] == series[position - 6]
    assert frame.X["target_roll_mean3"][i] == pytest.approx(series[position - 2:position + 1].mean())
    assert frame.y[i] == series[position + 1]
    assert frame.target_times[i] == t + pd.Timedelta("10min")
    assert frame.X["tgt_hour"][i] == frame.target_times[i].hour


def test_gaps_invalidate_lags(kag_frame):
    df, _ = kag_frame
    gapped = df.drop(index=range(100, 103))            # 30 minutes missing
    frame = build_features(gapped, small_spec())
    after_gap = df["timestamp"].iloc[103]
    assert after_gap not in set(frame.timestamps)      # lag1 would span the gap
    assert frame.dropped_rows > build_features(df, small_spec()).dropped_rows


def test_history_required_and_spec_round_trip():
    assert KAG_SPEC.history_required == 145
    assert BUILDING_SPEC.history_required == 13
    assert FeatureSpec.from_dict(json.loads(json.dumps(BUILDING_SPEC.as_dict()))) == BUILDING_SPEC


def test_missing_columns_raise(kag_frame):
    df, _ = kag_frame
    with pytest.raises(ValueError, match="Missing columns"):
        build_features(df.drop(columns=["T1"]), small_spec())


# --------------------------------------------------------------------------- split / metrics
def test_chronological_split_has_no_overlap(kag_frame):
    df, _ = kag_frame
    frame = build_features(df, small_spec())
    split = chronological_split(frame)
    assert not (split.train & split.validation).any() and not (split.validation & split.test).any()
    assert frame.timestamps[split.train].max() < frame.timestamps[split.validation].min()
    assert frame.timestamps[split.validation].max() < frame.timestamps[split.test].min()
    assert frame.target_times[split.train].max() < split.validation_start
    assert frame.target_times[split.validation].max() < split.test_start
    assert split.purged_rows >= 1


def test_regression_metrics():
    m = regression_metrics([1, 2, 3, 4], [1, 3, 3, 6])
    assert m["mae"] == pytest.approx(0.75) and m["rmse"] == pytest.approx(np.sqrt(5 / 4))
    assert m["r2"] == pytest.approx(1 - 5 / 5) and m["n"] == 4
    with pytest.raises(ValueError):
        regression_metrics([], [])


# --------------------------------------------------------------------------- training
def test_training_reports_metrics_computed_from_predictions(kag_frame, tmp_path):
    df, info = kag_frame
    spec = small_spec()
    frame = build_features(df, spec)
    bundle, metadata = train_and_evaluate(frame, spec, info, artifact_root=tmp_path)
    split = chronological_split(frame)
    recomputed = regression_metrics(frame.y[split.test], predict(bundle.model, frame.X[split.test]))
    assert metadata["metrics"]["xgboost"]["test"] == pytest.approx(recomputed)
    assert set(metadata["metrics"]) == {"persistence (naive)", "linear_regression",
                                        "random_forest", "xgboost"}
    assert metadata["data_interval_seconds"] == 600 and metadata["dataset"]["sha256"]
    report = render_report(bundle.metadata)
    assert "| xgboost | test |" in report and info.sha256 in report


def test_bundle_round_trip(kag_frame, tmp_path):
    df, info = kag_frame
    spec = small_spec()
    frame = build_features(df, spec)
    bundle, _ = train_and_evaluate(frame, spec, info, artifact_root=tmp_path)
    loaded = load_bundle(spec.name, root=tmp_path)
    assert loaded.version == bundle.version and loaded.spec == spec
    np.testing.assert_allclose(predict(loaded.model, frame.X[:20]), predict(bundle.model, frame.X[:20]))
    with pytest.raises(ModelNotFoundError):
        load_bundle("unknown_spec", root=tmp_path)


def test_building_model_metrics_per_zone(building_bundle):
    bundle, _ = building_bundle
    by_group = bundle.metadata["test_metrics_by_group"]
    assert set(by_group) == {"Z1", "Z2", "Z3", "Z4", "Z5"}
    assert all("xgboost" in models for models in by_group.values())


# --------------------------------------------------------------------------- inference
def test_online_predictions_match_batch_predictions(building_bundle):
    bundle, _ = building_bundle
    rows = building_rows(ticks=30, seed=9, start=datetime(2026, 3, 2, 8, tzinfo=timezone.utc))
    online = OnlinePredictor(bundle, prepare_building_frame)
    streamed = [p for p in (online.update(r) for r in rows) if p is not None]
    batch = EnergyPredictor(bundle).predict_frame(prepare_building_frame(pd.DataFrame(rows)))
    assert len(streamed) == len(batch) == 5 * (30 - 12)
    batch_by_key = {(p.group, p.timestamp): p.value for p in batch}
    for p in streamed:
        assert p.value == pytest.approx(batch_by_key[(p.group, p.timestamp)], rel=1e-6)


def test_online_predictor_warm_up_duplicates_and_restarts(building_bundle):
    bundle, _ = building_bundle
    rows = [r for r in building_rows(ticks=14, seed=4) if r["zone_id"] == "Z1"]
    online = OnlinePredictor(bundle, prepare_building_frame)
    results = [online.update(r) for r in rows]
    assert results[:12] == [None] * 12 and results[12] is not None
    assert online.update(rows[13]) is None                    # duplicate delivery
    assert online.update(rows[0] | {"event_id": "new-session"}) is None   # time went back
    assert online.history_size("Z1") == 1


def test_prediction_service_messages(building_bundle):
    from workers.prediction_service import PredictionHandler
    from streaming.consumer import ConsumedMessage

    bundle, _ = building_bundle

    class Producer:
        def __init__(self):
            self.sent = []

        def send(self, topic, value, key=None):
            self.sent.append((topic, key, value))

    producer = Producer()
    import logging
    handler = PredictionHandler(bundle, producer, logging.getLogger("test"))
    simulator = BuildingSimulator(SimulatorSettings(interval_seconds=1, speed_factor=300, random_seed=3,
                                                    start_time=datetime(2026, 3, 2, tzinfo=timezone.utc)))
    for tick in range(14):
        for i, event in enumerate(simulator.tick()):
            handler(ConsumedMessage("building_sensor_data", event["zone_id"], event, 0, tick * 5 + i, None))
    handler(ConsumedMessage("building_sensor_data", "Z1", {"bad": 1}, 0, 999, None))
    assert handler.stats["predictions"] == 10 and handler.stats["invalid"] == 1
    topic, key, message = producer.sent[0]
    record = validate_prediction_message(message)
    assert topic == "energy_predictions" and key == record.zone_id
    assert (record.predicted_for - record.event_time).total_seconds() == 300
    assert record.predicted_energy_kwh == pytest.approx(record.predicted_power_kw * 300 / 3600, rel=1e-3)


def test_prediction_service_rejects_kag_model(kag_frame, tmp_path):
    from workers.prediction_service import PredictionHandler

    df, info = kag_frame
    frame = build_features(df, small_spec())
    bundle, _ = train_and_evaluate(frame, small_spec(), info, artifact_root=tmp_path)
    with pytest.raises(IncompatibleModelError, match="--dataset building"):
        PredictionHandler(bundle, producer=None, logger=None)


def test_prediction_contract_rejects_bad_times():
    from processors.data_validator import ValidationError

    message = {"prediction_id": "6f1c2c1e-6d6c-4b5f-9a3e-1d2b3c4d5e6f",
               "source_event_id": "6f1c2c1e-6d6c-4b5f-9a3e-1d2b3c4d5e60", "building_id": "b",
               "zone_id": "Z1", "event_time": "2026-01-01T10:00:00+00:00",
               "predicted_for": "2026-01-01T09:55:00+00:00", "horizon_seconds": 300,
               "predicted_power_kw": 1.0, "predicted_energy_kwh": 0.08, "model_name": "xgboost",
               "model_version": "v1", "created_at": "2026-01-01T10:00:01+00:00"}
    with pytest.raises(ValidationError, match="predicted_for"):
        validate_prediction_message(message)


# --------------------------------------------------------------------------- database
@pytest.mark.postgres
def test_building_dataset_from_postgres_and_accuracy_view(db, building_bundle):
    from database.repositories import insert_record
    from ml.dataset import load_building
    from workers.sensor_persistence import SensorPersistenceHandler
    from workers.record_persistence import RecordPersistenceHandler
    from streaming.consumer import ConsumedMessage
    from workers.prediction_service import PredictionHandler
    import logging

    logger = logging.getLogger("test")
    simulator = BuildingSimulator(SimulatorSettings(interval_seconds=1, speed_factor=300, random_seed=5,
                                                    start_time=datetime(2026, 3, 2, tzinfo=timezone.utc)))
    events = [e for _ in range(220) for e in simulator.tick()]
    store = SensorPersistenceHandler(db, logger)
    for i, event in enumerate(events):
        store(ConsumedMessage("building_sensor_data", event["zone_id"], event, 0, i, None))

    with db.transaction() as conn:
        df, info = load_building(conn)
    assert info.rows == 1100 and set(df["group"]) == {"Z1", "Z2", "Z3", "Z4", "Z5"}
    assert "avg_power_kw" in df.columns

    class Producer:
        sent: list = []

        def send(self, topic, value, key=None):
            self.sent.append(value)

    bundle, _ = building_bundle
    producer = Producer()
    predictor = PredictionHandler(bundle, producer, logger)
    for event in events[:100]:
        predictor(ConsumedMessage("building_sensor_data", event["zone_id"], event, 0, 0, None))
    persist = RecordPersistenceHandler(db, logger)
    for value in producer.sent:
        persist(ConsumedMessage("energy_predictions", value["zone_id"], value, 0, 0, None))
    with db.transaction() as conn:
        rows = conn.execute("SELECT count(*), avg(abs_error_kw) FROM prediction_accuracy").fetchone()
    assert rows[0] == len(producer.sent) == 5 * (20 - 12) and rows[1] is not None


# --------------------------------------------------------------------------- cross-sectional datasets
def write_building_csv(path: Path, rows: int = 120, noise: float = 0.0, seed: int = 0) -> Path:
    rng = np.random.default_rng(seed)
    kinds = rng.choice(["Residential", "Commercial", "Industrial"], rows)
    days = rng.choice(["Weekday", "Weekend"], rows)
    sqft, occ = rng.integers(1000, 50000, rows), rng.integers(1, 100, rows)
    appl, temp = rng.integers(1, 50, rows), rng.uniform(10, 35, rows).round(2)
    energy = (2000 + 0.05 * sqft + 10 * occ + 20 * appl - 5 * temp
              + np.select([kinds == "Industrial", kinds == "Residential"], [500, -500], 0)
              + np.where(days == "Weekday", 25, -25) + rng.normal(0, noise, rows)).round(2)
    pd.DataFrame({"Building Type": kinds, "Square Footage": sqft, "Number of Occupants": occ,
                  "Appliances Used": appl, "Average Temperature": temp, "Day of Week": days,
                  "Energy Consumption": energy}).to_csv(path, index=False)
    return path


def write_customer_csv(path: Path, rows: int = 300, seed: int = 0) -> Path:
    rng = np.random.default_rng(seed)
    occupants = rng.integers(1, 5, rows)
    df = pd.DataFrame({"customer_id": [f"CUSTOMER_{i:04d}" for i in range(rows)],
                       "customer_type": rng.choice(["residential", "commercial"], rows),
                       "regions": rng.choice(["North", "South", "Midwest"], rows),
                       "building_size_m2": rng.integers(17, 78, rows), "occupants": occupants})
    df["energy_cost_brl"] = (50 + 12 * occupants + rng.normal(0, 10, rows)).round(2)
    df = pd.concat([df, df.iloc[:3].assign(customer_id=["DUP1", "DUP2", "DUP3"])])   # duplicates without id
    df.to_csv(path, index=False)
    return path


def test_load_tabular_cleans_and_drops_identifiers(tmp_path):
    from ml.tabular import CUSTOMER_COST_SPEC, load_tabular

    df, info = load_tabular(CUSTOMER_COST_SPEC, write_customer_csv(tmp_path / "c.csv"))
    assert "customer_id" not in df.columns and info.dropped_rows["duplicate_rows"] == 3
    assert len(df) == 300 and info.sha256
    bad = tmp_path / "bad.csv"
    bad.write_text("customer_type,occupants\nresidential,2\n", encoding="utf-8")
    with pytest.raises(DatasetError, match="missing columns"):
        load_tabular(CUSTOMER_COST_SPEC, bad)


def test_cross_validation_and_linear_structure(tmp_path):
    from ml.tabular import BUILDING_ENERGY_SPEC, cross_validate, linear_structure, load_tabular

    exact, _ = load_tabular(BUILDING_ENERGY_SPEC, write_building_csv(tmp_path / "exact.csv"))
    assert linear_structure(BUILDING_ENERGY_SPEC, exact)["deterministic_linear"]
    noisy, _ = load_tabular(BUILDING_ENERGY_SPEC, write_building_csv(tmp_path / "noisy.csv", noise=300))
    assert not linear_structure(BUILDING_ENERGY_SPEC, noisy)["deterministic_linear"]

    cv = cross_validate(BUILDING_ENERGY_SPEC, noisy, folds=3, repeats=2)
    assert set(cv) == {"mean (naive)", "linear_regression", "random_forest", "xgboost"}
    assert all(m["folds"] == 6 for m in cv.values())
    assert cv["linear_regression"]["r2_mean"] > cv["mean (naive)"]["r2_mean"]
    assert cv["mean (naive)"]["r2_mean"] < 0.05


def test_tabular_training_report_and_bundle(tmp_path):
    from ml.tabular import BUILDING_ENERGY_SPEC, TabularPredictor, TabularSpec
    from ml.train import render_tabular_report, train_tabular

    path = write_building_csv(tmp_path / "b.csv", rows=60)
    spec = replace(BUILDING_ENERGY_SPEC, path=path)
    bundle = train_tabular(spec, None, random_state=0, artifact_root=tmp_path / "artifacts", folds=3, repeats=1)
    report = render_tabular_report(bundle.metadata)
    assert "exact linear function" in report and "| xgboost |" in report
    loaded = load_bundle(spec.name, root=tmp_path / "artifacts")
    assert isinstance(loaded.spec, TabularSpec) and loaded.spec.target == "Energy Consumption"
    record = {"Building Type": "Commercial", "Square Footage": 20000, "Number of Occupants": 30,
              "Appliances Used": 10, "Average Temperature": 20.0, "Day of Week": "Weekday"}
    value = TabularPredictor(loaded.model, loaded.spec).predict([record])[0]
    assert 2000 < value < 6000
    with pytest.raises(ValueError, match="Missing input fields"):
        TabularPredictor(loaded.model, loaded.spec).predict([{"Building Type": "Commercial"}])
