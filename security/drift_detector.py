"""Distributional drift detection (KS test and PSI).

Drift is a property of a *window* of readings, not of a single reading: a
slow sensor manipulation or degradation can keep every reading plausible
while the distribution moves. Every ``step`` readings per zone, the last
``window`` readings of each monitored feature are compared with a reference
sample from clean training data.

To avoid flagging the normal daily cycle as drift, the reference is
*condition-matched*: only training readings from the same hours of the day
and the same day type (weekday/weekend) as the window are used.

Two kinds of drift are monitored with separate detectors:

* *security drift* on physical-consistency residuals (temperature control
  error while the HVAC is active; CO2 vs occupancy residual), which stay
  stationary in normal operation, so a shift suggests manipulation;
* *data drift* on raw readings (temperature, humidity, CO2, power), which
  legitimately change with seasons and weather; reported for model
  monitoring, not as an attack.

Missing values (e.g. no control error while the HVAC is idle) are skipped.

A feature drifts when both
    * the two-sample Kolmogorov-Smirnov test rejects equality (p < ``ks_alpha``), and
    * the Population Stability Index is >= ``psi_threshold`` (0.25 = significant shift),
for ``persistence`` consecutive evaluations. The PSI criterion guards
against the KS test's over-sensitivity on autocorrelated time series. For
security drift the window mean must also move by at least
``min_effect_size`` reference standard deviations: a short window of an
autocorrelated signal is naturally narrower than the pooled reference, which
alone changes KS/PSI, while manipulation shifts the *level*.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
from scipy.stats import ks_2samp

# Security drift: residuals that should be stationary regardless of weather and
# occupancy, so a shift indicates manipulated or faulty sensors.
SECURITY_DRIFT_FEATURES = ("control_error_active", "co2_residual")
# Data drift: raw input distributions (seasons, weather, usage) - relevant for
# ML model validity, not evidence of an attack.
DATA_DRIFT_FEATURES = ("temperature_c", "humidity_pct", "co2_ppm", "total_power_kw")
DRIFT_FEATURES = SECURITY_DRIFT_FEATURES + DATA_DRIFT_FEATURES
MIN_WINDOW_VALUES = 12
PSI_BINS = 10
PSI_EPSILON = 1e-4


def population_stability_index(reference: np.ndarray, current: np.ndarray, bins: int = PSI_BINS) -> float:
    """PSI with quantile bins of the reference distribution."""
    edges = np.unique(np.quantile(reference, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:                      # (almost) constant reference
        edges = np.array([reference.min() - 1e-9, reference.max() + 1e-9])
    edges[0], edges[-1] = -np.inf, np.inf
    ref_share = np.histogram(reference, edges)[0] / len(reference)
    cur_share = np.histogram(current, edges)[0] / len(current)
    ref_share = np.clip(ref_share, PSI_EPSILON, None)
    cur_share = np.clip(cur_share, PSI_EPSILON, None)
    return float(np.sum((cur_share - ref_share) * np.log(cur_share / ref_share)))


@dataclass(frozen=True)
class FeatureDrift:
    feature: str
    ks_statistic: float
    ks_p_value: float
    psi: float
    shifted: bool                # both criteria met in this evaluation
    mean_shift: float            # window mean - reference mean


@dataclass(frozen=True)
class DriftResult:
    zone_id: str
    window_start: datetime
    window_end: datetime
    features: tuple[FeatureDrift, ...]
    drifting: tuple[str, ...]    # features meeting the criteria for `persistence` evaluations

    @property
    def is_drift(self) -> bool:
        return bool(self.drifting)


@dataclass
class DriftReference:
    """Clean reference values per zone, feature, day type and hour."""

    values: dict = field(default_factory=lambda: defaultdict(list))

    def add(self, zone_id: str, when: datetime, row: dict[str, float],
            features: tuple[str, ...] = DRIFT_FEATURES) -> None:
        for feature in features:
            value = row.get(feature)
            if value is not None and not np.isnan(value):
                self.values[(zone_id, feature, when.weekday() >= 5, when.hour)].append(value)

    def sample(self, zone_id: str, feature: str, times: list[datetime]) -> np.ndarray:
        keys = {(zone_id, feature, t.weekday() >= 5, t.hour) for t in times}
        return np.array([v for key in keys for v in self.values.get(key, [])], dtype=float)

    def freeze(self) -> "DriftReference":
        self.values = {k: list(v) for k, v in self.values.items()}
        return self


@dataclass
class DriftDetector:
    reference: DriftReference
    features: tuple[str, ...] = SECURITY_DRIFT_FEATURES
    window: int = 72                    # 6 h at 5-minute readings
    step: int = 12                      # evaluate every hour
    ks_alpha: float = 0.01
    psi_threshold: float = 0.25
    persistence: int = 2
    min_effect_size: float = 0.0        # |mean shift| in reference standard deviations
    min_reference: int = 100
    disabled: frozenset = frozenset()   # (zone, feature) monitors that were unreliable on clean data
    _buffers: dict = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=72)))
    _counts: dict = field(default_factory=lambda: defaultdict(int))
    _streak: dict = field(default_factory=lambda: defaultdict(int))

    def __post_init__(self) -> None:
        self._buffers = defaultdict(lambda: deque(maxlen=self.window))

    def update(self, zone_id: str, when: datetime, row: dict[str, float]) -> DriftResult | None:
        """Add a reading; returns a result every ``step`` readings once the window is full."""
        buffer = self._buffers[zone_id]
        buffer.append((when, row))
        self._counts[zone_id] += 1
        if len(buffer) < self.window or self._counts[zone_id] % self.step:
            return None
        times = [t for t, _ in buffer]
        features, drifting = [], []
        for feature in self.features:
            if (zone_id, feature) in self.disabled:
                continue
            current = np.array([r.get(feature, np.nan) for _, r in buffer], dtype=float)
            current = current[~np.isnan(current)]
            reference = self.reference.sample(zone_id, feature, times)
            if len(reference) < self.min_reference or len(current) < MIN_WINDOW_VALUES:
                continue
            ks = ks_2samp(current, reference)
            psi = population_stability_index(reference, current)
            mean_shift = float(current.mean() - reference.mean())
            effect = abs(mean_shift) / max(float(reference.std()), 1e-9)
            shifted = (ks.pvalue < self.ks_alpha and psi >= self.psi_threshold
                       and effect >= self.min_effect_size)
            key = (zone_id, feature)
            self._streak[key] = self._streak[key] + 1 if shifted else 0
            if self._streak[key] >= self.persistence:
                drifting.append(feature)
            features.append(FeatureDrift(feature, float(ks.statistic), float(ks.pvalue), psi, shifted,
                                         mean_shift))
        return DriftResult(zone_id, times[0], times[-1], tuple(features), tuple(drifting))


@dataclass
class RollingDriftDetector:
    """Drift against a *recent* reference instead of fixed training data.

    The current ``window`` is compared with the *same time of day* on the
    previous ``reference_days`` days (``period`` readings per day), so the daily
    cycle is matched and slow seasonal change (weeks) is absorbed by the moving
    reference, while changes over hours (e.g. gradual sensor manipulation) are
    detected. Same KS + PSI + effect-size + persistence criteria as
    :class:`DriftDetector`. Useful for real data with strong seasonality.
    """

    features: tuple[str, ...]
    window: int = 36
    period: int = 144
    reference_days: int = 3
    step: int = 6
    ks_alpha: float = 0.01
    psi_threshold: float = 0.25
    min_effect_size: float = 0.5
    persistence: int = 2
    disabled: frozenset = frozenset()
    _history: dict = field(default_factory=dict)
    _times: deque = field(default_factory=deque)
    _count: int = 0
    _streak: dict = field(default_factory=lambda: defaultdict(int))

    def update(self, zone_id: str, when: datetime, row: dict[str, float]) -> DriftResult | None:
        size = self.window + self.period * self.reference_days
        for feature in self.features:
            self._history.setdefault(feature, deque(maxlen=size)).append(row[feature])
        self._times.append(when)
        if len(self._times) > size:
            self._times.popleft()
        self._count += 1
        if len(self._times) < size or self._count % self.step:
            return None
        results, drifting = [], []
        for feature in self.features:
            if (zone_id, feature) in self.disabled:
                continue
            values = np.array(self._history[feature], dtype=float)
            current = values[-self.window:]
            reference = np.concatenate([values[len(values) - self.window - k * self.period:
                                               len(values) - k * self.period]
                                        for k in range(1, self.reference_days + 1)])
            ks = ks_2samp(current, reference)
            psi = population_stability_index(reference, current)
            mean_shift = float(current.mean() - reference.mean())
            effect = abs(mean_shift) / max(float(reference.std()), 1e-9)
            shifted = ks.pvalue < self.ks_alpha and psi >= self.psi_threshold and effect >= self.min_effect_size
            self._streak[feature] = self._streak[feature] + 1 if shifted else 0
            if self._streak[feature] >= self.persistence:
                drifting.append(feature)
            results.append(FeatureDrift(feature, float(ks.statistic), float(ks.pvalue), psi, shifted, mean_shift))
        times = list(self._times)
        return DriftResult(zone_id, times[-self.window], times[-1], tuple(results), tuple(drifting))
