"""Regression metrics and evaluation reports."""

from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


def regression_metrics(y_true: Any, y_pred: Any) -> dict[str, float]:
    """MAE, RMSE and R² (computed, never estimated)."""
    y_true, y_pred = np.asarray(y_true, dtype=float), np.asarray(y_pred, dtype=float)
    if len(y_true) == 0 or len(y_true) != len(y_pred):
        raise ValueError("y_true and y_pred must be non-empty and of equal length")
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "r2": float(r2_score(y_true, y_pred)),
        "n": int(len(y_true)),
    }


def metrics_table(results: dict[str, dict[str, dict[str, float]]], unit: str) -> str:
    """Markdown table: rows = models, columns = split metrics."""
    lines = [f"| Model | Split | MAE ({unit}) | RMSE ({unit}) | R² | n |", "|---|---|---|---|---|---|"]
    for model, splits in results.items():
        for split, m in splits.items():
            lines.append(f"| {model} | {split} | {m['mae']:.3f} | {m['rmse']:.3f} | "
                         f"{m['r2']:.3f} | {m['n']} |")
    return "\n".join(lines)
