"""Train and evaluate next-interval energy prediction models.

    python -m ml.train --dataset kag                       # primary: KAG_energydata_complete.csv
    python -m ml.train --dataset kag --data path/to/file.csv
    python -m ml.train --dataset building --start 2026-01-05 --end 2026-02-02
    python -m ml.train --dataset building_energy    # test_energy_data.csv (cross-sectional)
    python -m ml.train --dataset customer_cost      # energy_consumption.csv (cross-sectional)
    python -m ml.train --dataset all --report docs/results/energy_prediction_combined.md

Pipeline:
    1. load + preprocess (ml.dataset)
    2. leakage-safe features (ml.features)
    3. chronological split: train 70 % | validation 15 % | test 15 %, with
       boundary rows removed whose target lies in the next split
    4. fit Linear Regression (baseline), Random Forest and XGBoost on train;
       XGBoost early-stops on validation; test is used once, for the final report
    5. compare with a naive persistence forecast (next value = current value)
    6. save the primary model (XGBoost) with metrics and provenance

The two cross-sectional datasets (no timestamps, different targets and units)
are evaluated with repeated K-fold cross-validation instead (``ml.tabular``).
``--dataset all`` trains the three project datasets with the same model
families and writes one comparison report; their rows are never pooled
because targets and units differ.

All reported numbers are computed from the actual run.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from config import BASE_DIR, ConfigurationError, get_config
from ml.dataset import DEFAULT_KAG_PATH, DatasetError, DatasetInfo, load_building, load_kag
from ml.evaluate import metrics_table, regression_metrics
from ml.features import BUILDING_SPEC, KAG_SPEC, FeatureFrame, FeatureSpec, build_features
from ml.models import PRIMARY_MODEL, fit_model, make_models, predict
from ml.registry import ARTIFACT_ROOT, ModelBundle, save_bundle
from utils.logging_config import configure_logging

logger = logging.getLogger(__name__)

TRAIN_FRACTION = 0.70
VALIDATION_FRACTION = 0.15


@dataclass
class Split:
    train: np.ndarray
    validation: np.ndarray
    test: np.ndarray
    validation_start: pd.Timestamp
    test_start: pd.Timestamp
    purged_rows: int

    def describe(self, frame: FeatureFrame) -> dict[str, Any]:
        def period(mask: np.ndarray) -> dict[str, Any]:
            ts = frame.timestamps[mask]
            return {"rows": int(mask.sum()), "start": str(ts.min()), "end": str(ts.max())}
        return {"train": period(self.train), "validation": period(self.validation),
                "test": period(self.test), "purged_boundary_rows": self.purged_rows}


def chronological_split(frame: FeatureFrame, train_fraction: float = TRAIN_FRACTION,
                        validation_fraction: float = VALIDATION_FRACTION) -> Split:
    """Split by time. Every training row (and its target) precedes every
    validation row, which precede every test row."""
    times = np.sort(frame.timestamps.unique())
    if len(times) < 20:
        raise DatasetError("Not enough time steps to split")
    validation_start = pd.Timestamp(times[int(len(times) * train_fraction)])
    test_start = pd.Timestamp(times[int(len(times) * (train_fraction + validation_fraction))])
    ts, target_ts = frame.timestamps, frame.target_times
    in_train = (ts < validation_start).to_numpy()
    in_validation = ((ts >= validation_start) & (ts < test_start)).to_numpy()
    train = in_train & (target_ts < validation_start).to_numpy()
    validation = in_validation & (target_ts < test_start).to_numpy()
    test = (ts >= test_start).to_numpy()
    purged = int(in_train.sum() - train.sum() + in_validation.sum() - validation.sum())
    return Split(train, validation, test, validation_start, test_start, purged)


def train_and_evaluate(frame: FeatureFrame, spec: FeatureSpec, info: DatasetInfo, *,
                       random_state: int = 42, artifact_root: Path = ARTIFACT_ROOT,
                       model_names: list[str] | None = None) -> tuple[ModelBundle, dict[str, Any]]:
    split = chronological_split(frame)
    X, y = frame.X, frame.y
    X_train, y_train = X[split.train], y[split.train]
    X_val, y_val = X[split.validation], y[split.validation]
    X_test, y_test = X[split.test], y[split.test]
    logger.info("Data split", extra=split.describe(frame))

    results: dict[str, dict[str, dict[str, float]]] = {
        "persistence (naive)": {
            "validation": regression_metrics(y_val, frame.current[split.validation]),
            "test": regression_metrics(y_test, frame.current[split.test]),
        }
    }
    fitted: dict[str, Any] = {}
    timings: dict[str, float] = {}
    for name, model in make_models(random_state).items():
        if model_names and name not in model_names and name != PRIMARY_MODEL:
            continue
        started = time.perf_counter()
        fitted[name] = fit_model(name, model, X_train, y_train, X_val, y_val)
        timings[name] = round(time.perf_counter() - started, 2)
        results[name] = {
            "train": regression_metrics(y_train, predict(model, X_train)),
            "validation": regression_metrics(y_val, predict(model, X_val)),
            "test": regression_metrics(y_test, predict(model, X_test)),
        }
        logger.info("Model evaluated", extra={"model": name, "fit_seconds": timings[name],
                                              **{f"test_{k}": v for k, v in results[name]["test"].items()}})

    # Per-group test metrics: with several zones, pooled R² is dominated by the
    # differences *between* zones and overstates accuracy within a zone.
    by_group: dict[str, dict[str, dict[str, float]]] = {}
    test_groups = frame.groups[split.test]
    if test_groups.nunique() > 1:
        for group in sorted(test_groups.unique()):
            mask = split.test & (frame.groups == group).to_numpy()
            by_group[str(group)] = {"persistence (naive)": regression_metrics(y[mask], frame.current[mask])}
            for name, model in fitted.items():
                by_group[str(group)][name] = regression_metrics(y[mask], predict(model, X[mask]))

    primary = fitted[PRIMARY_MODEL]
    importances = sorted(zip(frame.feature_names, primary.feature_importances_.tolist()),
                         key=lambda item: item[1], reverse=True)
    steps = frame.timestamps.groupby(frame.groups).diff().dropna()
    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data_interval_seconds": float(steps.median().total_seconds()) if len(steps) else None,
        "task": f"predict {spec.target} {spec.horizon} step(s) ahead",
        "target_unit": spec.target_unit,
        "dataset": info.as_dict(),
        "split": split.describe(frame),
        "feature_rows_dropped_incomplete_history": frame.dropped_rows,
        "metrics": results,
        "test_metrics_by_group": by_group,
        "fit_seconds": timings,
        "xgboost_best_iteration": int(getattr(primary, "best_iteration", -1)),
        "feature_importance_gain_top20": importances[:20],
        "random_state": random_state,
    }
    bundle = save_bundle(primary, PRIMARY_MODEL, spec, frame.feature_names, metadata,
                         root=artifact_root)
    return bundle, metadata


def render_report(metadata: dict[str, Any]) -> str:
    dataset, split = metadata["dataset"], metadata["split"]
    lines = [
        f"# Energy prediction results: {metadata['spec']['name']}",
        "",
        f"Generated by `python -m ml.train` on {metadata['created_at']} (model version "
        f"`{metadata['version']}`). All values are computed from this run.",
        "",
        f"* **Task:** {metadata['task']} (unit: {metadata['target_unit']})",
        f"* **Dataset:** `{dataset['source']}` ({dataset['rows']} rows, {dataset['start']} to "
        f"{dataset['end']})" + (f", SHA-256 `{dataset['sha256']}`" if dataset.get("sha256") else ""),
        f"* **Split (chronological):** train {split['train']['rows']} rows "
        f"({split['train']['start']} to {split['train']['end']}), validation "
        f"{split['validation']['rows']}, test {split['test']['rows']} "
        f"({split['test']['start']} to {split['test']['end']}). "
        f"{split['purged_boundary_rows']} boundary rows were removed so that no target crosses into the next split.",
        f"* **Features:** {len(metadata['feature_names'])}. Rows dropped for incomplete history: "
        f"{metadata['feature_rows_dropped_incomplete_history']}.",
        f"* **XGBoost:** early stopping on validation; best iteration "
        f"{metadata['xgboost_best_iteration']}.",
        "",
        "## Metrics",
        "",
        metrics_table(metadata["metrics"], metadata["target_unit"].split(" ")[0]),
        "",
        *(["## Test metrics per group", "",
           "Pooled R² across groups mixes between-group differences into the score; these "
           "per-group values show accuracy *within* each group.", "",
           "| Group | Model | MAE | RMSE | R² | n |", "|---|---|---|---|---|---|",
           *[f"| {g} | {m} | {v['mae']:.3f} | {v['rmse']:.3f} | {v['r2']:.3f} | {v['n']} |"
             for g, models in metadata.get("test_metrics_by_group", {}).items()
             for m, v in models.items()], ""]
          if metadata.get("test_metrics_by_group") else []),
        "## Top XGBoost features (gain)",
        "",
        "| Feature | Importance |",
        "|---|---|",
        *[f"| {name} | {value:.4f} |" for name, value in metadata["feature_importance_gain_top20"][:12]],
        "",
        f"Library versions: {metadata['library_versions']}.",
        "",
    ]
    return "\n".join(lines)


def train_tabular(spec, path: Path | None, *, random_state: int, artifact_root: Path,
                  folds: int = 5, repeats: int = 10) -> ModelBundle:
    from ml.tabular import cross_validate, fit_final, linear_structure, load_tabular

    df, info = load_tabular(spec, path)
    started = time.perf_counter()
    cv = cross_validate(spec, df, folds=folds, repeats=repeats, random_state=random_state)
    model = fit_final(spec, df, PRIMARY_MODEL, random_state)
    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "task": f"predict {spec.target} from building/customer characteristics",
        "target_unit": spec.target_unit, "dataset": info.as_dict(),
        "evaluation": f"repeated K-fold cross-validation ({folds} folds x {repeats} repeats); "
                      "preprocessing fitted inside each fold",
        "cv_metrics": cv, "cv_seconds": round(time.perf_counter() - started, 1),
        "cv_folds": folds, "cv_repeats": repeats,
        "linear_structure": linear_structure(spec, df),
        "final_model": f"{PRIMARY_MODEL} fitted on all {info.rows} rows", "random_state": random_state,
        "target_summary": {"mean": float(df[spec.target].mean()), "std": float(df[spec.target].std()),
                           "min": float(df[spec.target].min()), "max": float(df[spec.target].max())},
    }
    return save_bundle(model, PRIMARY_MODEL, spec, [*spec.numeric, *spec.categorical], metadata,
                       root=artifact_root)


def render_tabular_report(metadata: dict[str, Any]) -> str:
    dataset, target = metadata["dataset"], metadata["target_summary"]
    lines = [
        f"# Energy prediction results: {metadata['spec']['name']}", "",
        f"Generated by `python -m ml.train` on {metadata['created_at']} (model version `{metadata['version']}`). "
        "All values are computed from this run.", "",
        f"* **Task:** {metadata['task']} (target `{metadata['spec']['target']}`, unit: {metadata['target_unit']})",
        f"* **Dataset:** `{dataset['source']}`, {dataset['rows']} rows, SHA-256 `{dataset['sha256']}`; "
        f"dropped rows: {dataset['dropped_rows']}; {'; '.join(dataset['notes'])}",
        f"* **Target:** mean {target['mean']:.2f}, std {target['std']:.2f}, range {target['min']:.2f} - {target['max']:.2f}",
        f"* **Evaluation:** {metadata['evaluation']}. {metadata['final_model']}.",
        _linear_note(metadata), "",
        "| Model | MAE (mean ± std) | RMSE (mean ± std) | R² (mean ± std) | folds |", "|---|---|---|---|---|",
        *[f"| {name} | {m['mae_mean']:.3f} ± {m['mae_std']:.3f} | {m['rmse_mean']:.3f} ± {m['rmse_std']:.3f} | "
          f"{m['r2_mean']:.3f} ± {m['r2_std']:.3f} | {m['folds']} |" for name, m in metadata["cv_metrics"].items()],
        "",
    ]
    return "\n".join(lines)


def _linear_note(metadata: dict[str, Any]) -> str:
    structure = metadata.get("linear_structure", {})
    if structure.get("deterministic_linear"):
        return ("* **Finding:** the target is an exact linear function of the inputs (in-sample R² "
                f"{structure['in_sample_r2']:.6f}, max residual {structure['max_abs_residual']:.3f}), i.e. "
                "synthetic data generated by a formula. Linear Regression recovers it; the scores do not "
                "measure real-world predictive skill.")
    return (f"* Linear fit on all rows: in-sample R² {structure.get('in_sample_r2', float('nan')):.3f} "
            "(target is not a deterministic linear formula).")


def render_combined_report(entries: list[tuple[str, dict[str, Any]]]) -> str:
    lines = [
        "# Energy prediction: all project datasets", "",
        f"Generated by `python -m ml.train --dataset all` on {datetime.now(timezone.utc).isoformat()}. "
        "All values are computed from this run.", "",
        "The three datasets have different targets, units and structure (a 10-minute time series of one "
        "house; per-building consumption; per-customer cost in BRL), so their rows are **not pooled**. "
        "Each is modelled with the same model families and evaluated with the protocol that fits its "
        "structure. Metrics are only comparable *within* a dataset.", "",
        "| Dataset | Rows | Target (unit) | Evaluation | Model | MAE | RMSE | R² |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for label, meta in entries:
        unit = meta["target_unit"]
        if "cv_metrics" in meta:
            for name, m in meta["cv_metrics"].items():
                lines.append(f"| {label} | {meta['dataset']['rows']} | {meta['spec']['target']} ({unit}) | "
                             f"{meta['cv_folds']}-fold CV x {meta['cv_repeats']} | {name} | {m['mae_mean']:.3f} ± {m['mae_std']:.3f} | "
                             f"{m['rmse_mean']:.3f} ± {m['rmse_std']:.3f} | {m['r2_mean']:.3f} ± {m['r2_std']:.3f} |")
        else:
            for name, splits in meta["metrics"].items():
                m = splits["test"]
                lines.append(f"| {label} | {meta['dataset']['rows']} | {meta['spec']['target']} ({unit}) | "
                             f"chronological test split | {name} | {m['mae']:.3f} | {m['rmse']:.3f} | {m['r2']:.3f} |")
    lines += ["", "## Findings", ""]
    for label, meta in entries:
        if "linear_structure" in meta:
            lines.append(f"* **{label}:** " + _linear_note(meta)[2:].replace("**Finding:** ", ""))
        dropped = {k: v for k, v in meta["dataset"].get("dropped_rows", {}).items() if v}
        if dropped:
            lines.append(f"* **{label}:** rows removed during cleaning: {dropped}.")
    lines += ["", "Detailed reports per dataset are written next to each model in `ml/artifacts/` "
              "and in `docs/results/`.", ""]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m ml.train", description=__doc__.splitlines()[0])
    parser.add_argument("--dataset", choices=["kag", "building", "building_energy", "customer_cost", "all"],
                        default="kag")
    parser.add_argument("--data", type=Path, default=DEFAULT_KAG_PATH, help="KAG CSV path")
    parser.add_argument("--start", type=datetime.fromisoformat, help="building data: from (ISO)")
    parser.add_argument("--end", type=datetime.fromisoformat, help="building data: until (ISO)")
    parser.add_argument("--report", type=Path, help="also write the Markdown report here")
    parser.add_argument("--artifacts", type=Path, default=ARTIFACT_ROOT)
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = get_config()
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    configure_logging(config.LOGGING, "ml_train", stream=sys.stderr)

    if args.dataset in ("building_energy", "customer_cost", "all"):
        return train_project_datasets(args)

    try:
        if args.dataset == "kag":
            df, info = load_kag(args.data)
            spec = KAG_SPEC
        else:
            from dataclasses import replace

            from database.db import Database

            with Database(config.DATABASE, name="ml_train") as db, db.transaction() as conn:
                df, info = load_building(conn, start=args.start, end=args.end,
                                         building_id=config.SIMULATOR.building_id)
            spec = replace(BUILDING_SPEC, utc_offset_hours=config.SIMULATOR.utc_offset_hours)
        frame = build_features(df, spec)
        bundle, metadata = train_and_evaluate(frame, spec, info, random_state=args.random_state,
                                              artifact_root=args.artifacts)
    except DatasetError as exc:
        print(f"Dataset error: {exc}", file=sys.stderr)
        return 1

    report = render_report(bundle.metadata)
    report_path = args.artifacts / spec.name / bundle.version / "report.md"
    report_path.write_text(report, encoding="utf-8")
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report, encoding="utf-8")
    print(report)
    print(f"Model saved: {report_path.parent}")
    return 0


def train_project_datasets(args: argparse.Namespace) -> int:
    """Tabular datasets, or all three project datasets with a combined report."""
    from ml.tabular import BUILDING_ENERGY_SPEC, CUSTOMER_COST_SPEC

    jobs = {"building_energy": [("test_energy_data.csv", BUILDING_ENERGY_SPEC)],
            "customer_cost": [("energy_consumption.csv", CUSTOMER_COST_SPEC)]}
    jobs["all"] = [("KAG_energydata_complete.csv", None), *jobs["building_energy"], *jobs["customer_cost"]]
    entries = []
    results_dir = BASE_DIR / "docs" / "results"
    try:
        for label, spec in jobs[args.dataset]:
            if spec is None:
                df, info = load_kag(args.data)
                bundle, _ = train_and_evaluate(build_features(df, KAG_SPEC), KAG_SPEC, info,
                                               random_state=args.random_state, artifact_root=args.artifacts)
                report = render_report(bundle.metadata)
            else:
                bundle = train_tabular(spec, None, random_state=args.random_state,
                                       artifact_root=args.artifacts)
                report = render_tabular_report(bundle.metadata)
            (args.artifacts / bundle.spec.name / bundle.version / "report.md").write_text(report, encoding="utf-8")
            if args.dataset == "all":
                name = {"kag_appliances": "kag"}.get(bundle.spec.name, bundle.spec.name)
                (results_dir / f"energy_prediction_{name}.md").write_text(report, encoding="utf-8")
            entries.append((label, bundle.metadata))
            print(report)
    except DatasetError as exc:
        print(f"Dataset error: {exc}", file=sys.stderr)
        return 1
    if args.dataset == "all":
        combined = render_combined_report(entries)
        print(combined)
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(combined, encoding="utf-8")
    elif args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
