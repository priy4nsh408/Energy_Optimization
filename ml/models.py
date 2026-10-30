"""Model definitions: Linear Regression baseline, Random Forest, XGBoost."""

from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor

PRIMARY_MODEL = "xgboost"
XGB_MAX_ESTIMATORS = 2000
XGB_EARLY_STOPPING_ROUNDS = 50


def make_models(random_state: int = 42) -> dict[str, Any]:
    """Fresh, unfitted estimators. Scaling lives inside the pipeline, so it is
    fitted on the training split only."""
    return {
        "linear_regression": Pipeline([("scale", StandardScaler()), ("model", LinearRegression())]),
        "random_forest": RandomForestRegressor(
            n_estimators=300, min_samples_leaf=3, max_features=0.5,
            n_jobs=-1, random_state=random_state),
        "xgboost": XGBRegressor(
            n_estimators=XGB_MAX_ESTIMATORS, learning_rate=0.03, max_depth=6,
            min_child_weight=5, subsample=0.8, colsample_bytree=0.8, reg_lambda=1.0,
            objective="reg:squarederror", tree_method="hist",
            early_stopping_rounds=XGB_EARLY_STOPPING_ROUNDS, eval_metric="rmse",
            n_jobs=-1, random_state=random_state),
    }


def fit_model(name: str, model: Any, X_train, y_train, X_val, y_val) -> Any:
    """Fit a model. XGBoost uses the *validation* split for early stopping;
    the test split is never used for any fitting decision."""
    if name == "xgboost":
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
    else:
        model.fit(X_train, y_train)
    return model


def predict(model: Any, X) -> np.ndarray:
    """Predictions clipped at zero (energy cannot be negative)."""
    return np.clip(np.asarray(model.predict(X), dtype=float), 0.0, None)
