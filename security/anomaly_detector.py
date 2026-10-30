"""Isolation Forest anomaly detection (individual readings).

One model per zone, trained on clean readings. The decision threshold is the
``threshold_quantile`` (99.9th percentile) of anomaly scores on a separate
*clean validation* period, which fixes the expected false-positive rate; attack data is never
used to set it. Robust per-feature statistics (median / MAD) explain which
feature group made a reading unusual.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from sklearn.ensemble import IsolationForest

from security.features import FEATURE_GROUPS, FEATURES


@dataclass
class ZoneAnomalyModel:
    forest: IsolationForest
    threshold: float
    median: dict[str, float]
    mad: dict[str, float]

    def score(self, values: dict[str, float]) -> float:
        """Anomaly score: higher = more anomalous."""
        x = np.array([[values[f] for f in FEATURES]])
        return float(-self.forest.score_samples(x)[0])

    def robust_z(self, values: dict[str, float]) -> dict[str, float]:
        return {f: abs(values[f] - self.median[f]) / self.mad[f] for f in FEATURES}

    def top_group(self, values: dict[str, float]) -> tuple[str, str, float]:
        """(group, feature, |z|) of the most unusual feature in any group."""
        z = self.robust_z(values)
        best = max(((g, f, z[f]) for g, fs in FEATURE_GROUPS.items() for f in fs),
                   key=lambda item: item[2])
        return best


@dataclass
class AnomalyDetector:
    models: dict[str, ZoneAnomalyModel] = field(default_factory=dict)
    threshold_quantile: float = 0.999

    @staticmethod
    def _matrix(rows: list[dict[str, float]]) -> np.ndarray:
        return np.array([[r[f] for f in FEATURES] for r in rows], dtype=float)

    def fit(self, train: dict[str, list[dict[str, float]]],
            validation: dict[str, list[dict[str, float]]], *, random_state: int = 0) -> "AnomalyDetector":
        for zone, rows in train.items():
            X = self._matrix(rows)
            forest = IsolationForest(n_estimators=200, max_samples=min(512, len(X)),
                                     contamination="auto", random_state=random_state).fit(X)
            val_scores = -forest.score_samples(self._matrix(validation[zone]))
            median = np.median(X, axis=0)
            mad = np.median(np.abs(X - median), axis=0) * 1.4826
            std = X.std(axis=0)
            # MAD is 0 for mostly-constant features (e.g. lights off at night); fall back to std.
            scale = np.where(mad > 1e-9, mad, np.where(std > 1e-9, std, 1.0))
            self.models[zone] = ZoneAnomalyModel(
                forest, float(np.quantile(val_scores, self.threshold_quantile)),
                dict(zip(FEATURES, median.tolist())), dict(zip(FEATURES, scale.tolist())))
        return self

    def evaluate(self, zone_id: str, values: dict[str, float]) -> tuple[bool, float, float] | None:
        """(is_anomaly, score, threshold); None for a zone without a model."""
        model = self.models.get(zone_id)
        if model is None:
            return None
        score = model.score(values)
        return score > model.threshold, score, model.threshold
