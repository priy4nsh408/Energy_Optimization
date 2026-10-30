"""Inference: batch predictions and an online predictor for Kafka events.

:class:`OnlinePredictor` keeps a short history per zone and computes features
with exactly the same code as training (:func:`ml.features.build_features`),
so online and offline features cannot diverge.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

import pandas as pd

from ml.features import build_features
from ml.models import predict
from ml.registry import ModelBundle

logger = logging.getLogger(__name__)


class IncompatibleModelError(ValueError):
    """The model's features cannot be computed from the available data."""


@dataclass(frozen=True)
class Prediction:
    group: str
    timestamp: datetime          # time of the latest reading used
    target_time: datetime        # time the prediction is for
    value: float                 # in the model's target unit
    model_name: str
    model_version: str


class EnergyPredictor:
    """Batch predictions for a standardised frame."""

    def __init__(self, bundle: ModelBundle) -> None:
        self.bundle = bundle
        self.spec = bundle.spec

    def predict_frame(self, df: pd.DataFrame) -> list[Prediction]:
        frame = build_features(df, self.spec, require_target=False)
        if frame.X.empty:
            return []
        if list(frame.X.columns) != self.bundle.feature_names:
            raise IncompatibleModelError(
                f"Feature mismatch for model {self.bundle.version}: expected "
                f"{self.bundle.feature_names}, got {list(frame.X.columns)}")
        values = predict(self.bundle.model, frame.X)
        return [Prediction(str(g), ts.to_pydatetime(), tt.to_pydatetime(), float(v),
                           self.bundle.model_name, self.bundle.version)
                for g, ts, tt, v in zip(frame.groups, frame.timestamps, frame.target_times, values)]


class OnlinePredictor:
    """Per-zone rolling history -> prediction for the next interval.

    ``prepare`` converts a list of raw rows (dicts) into the standardised
    frame expected by the feature spec (e.g. ``ml.dataset.prepare_building_frame``).
    """

    HISTORY_MARGIN = 2

    def __init__(self, bundle: ModelBundle,
                 prepare: Callable[[pd.DataFrame], pd.DataFrame]) -> None:
        self.predictor = EnergyPredictor(bundle)
        self.prepare = prepare
        self.maxlen = bundle.spec.history_required + self.HISTORY_MARGIN
        self._history: dict[str, deque[dict[str, Any]]] = {}
        self._seen: dict[str, set[str]] = {}

    def history_size(self, group: str) -> int:
        return len(self._history.get(group, ()))

    def update(self, row: dict[str, Any], *, group_key: str = "zone_id",
               time_key: str = "event_time", id_key: str = "event_id") -> Prediction | None:
        """Add one reading; return a prediction once enough evenly spaced history exists."""
        group = str(row[group_key])
        history = self._history.setdefault(group, deque(maxlen=self.maxlen))
        seen = self._seen.setdefault(group, set())
        event_id = str(row.get(id_key))
        if event_id in seen:
            return None                                  # duplicate delivery
        if history and pd.Timestamp(row[time_key]) <= pd.Timestamp(history[-1][time_key]):
            history.clear()                              # time went backwards: new session
            seen.clear()
        history.append(row)
        seen.add(event_id)
        if len(seen) > 4 * self.maxlen:                  # keep the de-dup set bounded
            self._seen[group] = {str(r.get(id_key)) for r in history}
        if len(history) < self.predictor.spec.history_required:
            return None
        frame = self.prepare(pd.DataFrame(list(history)))
        latest = pd.Timestamp(row[time_key])   # aware timestamps compare as instants
        predictions = [p for p in self.predictor.predict_frame(frame)
                       if pd.Timestamp(p.timestamp) == latest]
        return predictions[-1] if predictions else None   # None: gap in recent history
