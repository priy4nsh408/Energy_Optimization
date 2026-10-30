"""Cross-sectional (non-time-series) energy datasets.

Two of the project datasets describe *buildings/customers*, not time series:

* ``test_energy_data.csv`` - building type, floor area (ft²), occupants,
  appliances, average temperature, weekday/weekend -> energy consumption.
* ``energy_consumption.csv`` - customer type, Brazilian region, floor area
  (m²), occupants -> energy cost in BRL.

They have different targets and units than each other and than the KAG time
series, so they are modelled separately with the same model families. Rows
have no time order, so evaluation uses repeated K-fold cross-validation;
encoding and scaling are fitted inside each fold (no leakage). Identifier
columns are dropped.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import RepeatedKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from xgboost import XGBRegressor

from config import BASE_DIR
from ml.dataset import DatasetError, DatasetInfo, file_sha256
from ml.evaluate import regression_metrics

logger = logging.getLogger(__name__)

RAW = BASE_DIR / "data" / "raw"


@dataclass(frozen=True)
class TabularSpec:
    name: str
    path: Path
    target: str
    target_unit: str
    numeric: tuple[str, ...]
    categorical: tuple[str, ...]
    drop: tuple[str, ...] = ()
    description: str = ""
    spec_type: str = field(default="tabular", init=False)

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["path"] = str(self.path)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TabularSpec":
        data = {k: tuple(v) if isinstance(v, list) else v for k, v in data.items() if k != "spec_type"}
        data["path"] = Path(data["path"])
        return cls(**data)


BUILDING_ENERGY_SPEC = TabularSpec(
    name="building_energy_tabular", path=RAW / "test_energy_data.csv",
    target="Energy Consumption", target_unit="energy units as given in the dataset",
    numeric=("Square Footage", "Number of Occupants", "Appliances Used", "Average Temperature"),
    categorical=("Building Type", "Day of Week"),
    description="Per-building energy consumption from building characteristics (100 buildings)")

CUSTOMER_COST_SPEC = TabularSpec(
    name="customer_energy_cost", path=RAW / "energy_consumption.csv",
    target="energy_cost_brl", target_unit="BRL",
    numeric=("building_size_m2", "occupants"), categorical=("customer_type", "regions"),
    drop=("customer_id",),
    description="Energy cost per customer in Brazil from customer characteristics (5,000 customers)")

TABULAR_SPECS = {s.name: s for s in (BUILDING_ENERGY_SPEC, CUSTOMER_COST_SPEC)}


def load_tabular(spec: TabularSpec, path: Path | None = None) -> tuple[pd.DataFrame, DatasetInfo]:
    path = Path(path or spec.path)
    if not path.is_file():
        raise DatasetError(f"Dataset not found: {path}")
    df = pd.read_csv(path, skipinitialspace=True)
    df.columns = [c.strip() for c in df.columns]
    required = [spec.target, *spec.numeric, *spec.categorical]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise DatasetError(f"{path.name} is missing columns: {missing}")
    dropped: dict[str, int] = {}
    df = df.drop(columns=[c for c in spec.drop if c in df.columns])
    for column in (*spec.numeric, spec.target):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    for column in spec.categorical:
        df[column] = df[column].astype(str).str.strip()
    before = len(df)
    df = df.dropna(subset=required)
    dropped["missing_values"] = before - len(df)
    before = len(df)
    df = df.drop_duplicates()
    dropped["duplicate_rows"] = before - len(df)
    before = len(df)
    df = df[df[spec.target] >= 0]
    dropped["negative_target"] = before - len(df)
    if len(df) < 30:
        raise DatasetError(f"Only {len(df)} usable rows in {path.name}")
    info = DatasetInfo(name=spec.name, source=str(path), rows=len(df), start="", end="",
                       sha256=file_sha256(path), dropped_rows=dropped,
                       notes=[f"dropped identifier columns {list(spec.drop)}" if spec.drop else "no identifier columns",
                              "cross-sectional data: no timestamps"])
    return df.reset_index(drop=True), info


def make_tabular_models(spec: TabularSpec, random_state: int = 42) -> dict[str, Pipeline]:
    """Same model families as the time-series models; preprocessing inside each pipeline."""
    def pipeline(model, scale: bool) -> Pipeline:
        numeric = StandardScaler() if scale else "passthrough"
        prep = ColumnTransformer([
            ("num", numeric, list(spec.numeric)),
            ("cat", OneHotEncoder(handle_unknown="ignore"), list(spec.categorical)),
        ])
        return Pipeline([("prep", prep), ("model", model)])

    return {
        "mean (naive)": pipeline(DummyRegressor(strategy="mean"), scale=False),
        "linear_regression": pipeline(LinearRegression(), scale=True),
        "random_forest": pipeline(RandomForestRegressor(n_estimators=300, min_samples_leaf=2,
                                                        n_jobs=-1, random_state=random_state), scale=False),
        "xgboost": pipeline(XGBRegressor(n_estimators=300, learning_rate=0.05, max_depth=4,
                                         subsample=0.8, colsample_bytree=0.8, n_jobs=-1,
                                         random_state=random_state), scale=False),
    }


def cross_validate(spec: TabularSpec, df: pd.DataFrame, *, folds: int = 5, repeats: int = 10,
                   random_state: int = 42) -> dict[str, dict[str, float]]:
    """Repeated K-fold CV; returns mean and std of each metric per model."""
    X, y = df[[*spec.numeric, *spec.categorical]], df[spec.target].to_numpy(dtype=float)
    splitter = RepeatedKFold(n_splits=folds, n_repeats=repeats, random_state=random_state)
    per_model: dict[str, list[dict[str, float]]] = {}
    for train_idx, test_idx in splitter.split(X):
        for name, model in make_tabular_models(spec, random_state).items():
            model.fit(X.iloc[train_idx], y[train_idx])
            per_model.setdefault(name, []).append(
                regression_metrics(y[test_idx], model.predict(X.iloc[test_idx])))
    summary = {}
    for name, runs in per_model.items():
        summary[name] = {"folds": len(runs), "n_per_fold": int(np.mean([r["n"] for r in runs]))}
        for metric in ("mae", "rmse", "r2"):
            values = np.array([r[metric] for r in runs])
            summary[name][f"{metric}_mean"] = float(values.mean())
            summary[name][f"{metric}_std"] = float(values.std(ddof=1))
    return summary


def fit_final(spec: TabularSpec, df: pd.DataFrame, model_name: str = "xgboost",
              random_state: int = 42) -> Pipeline:
    """Fit the chosen model on all rows (after cross-validation) for persistence."""
    model = make_tabular_models(spec, random_state)[model_name]
    model.fit(df[[*spec.numeric, *spec.categorical]], df[spec.target].to_numpy(dtype=float))
    return model


class TabularPredictor:
    """Predict from records such as {"Building Type": "Commercial", "Square Footage": 20000, ...}."""

    def __init__(self, model: Pipeline, spec: TabularSpec) -> None:
        self.model, self.spec = model, spec

    def predict(self, records: list[dict[str, Any]]) -> list[float]:
        frame = pd.DataFrame(records)
        missing = [c for c in (*self.spec.numeric, *self.spec.categorical) if c not in frame.columns]
        if missing:
            raise ValueError(f"Missing input fields: {missing}")
        return [float(v) for v in self.model.predict(frame[[*self.spec.numeric, *self.spec.categorical]])]


def linear_structure(spec: TabularSpec, df: pd.DataFrame) -> dict[str, Any]:
    """How well a plain linear model reproduces the target on *all* rows.

    A near-zero residual means the target is (almost) a deterministic linear
    formula of the inputs, typical of synthetic data. Cross-validation scores
    then describe the generator, not real-world predictability.
    """
    model = make_tabular_models(spec)["linear_regression"]
    X, y = df[[*spec.numeric, *spec.categorical]], df[spec.target].to_numpy(dtype=float)
    model.fit(X, y)
    residual = np.abs(y - model.predict(X))
    relative = float(residual.max() / (y.std() or 1.0))
    return {"in_sample_r2": float(1 - ((y - model.predict(X)) ** 2).sum() / ((y - y.mean()) ** 2).sum()),
            "max_abs_residual": float(residual.max()),
            "deterministic_linear": relative < 1e-3}
