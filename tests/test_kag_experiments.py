"""Tests for the KAG drift analysis and anomaly experiment helpers (synthetic data, no CSV needed)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from experiments.kag_anomaly_experiment import (INDOOR_RH, INDOOR_T, SENSORS, KagAttack, KagMonitor,
                                                inject)
from experiments.kag_drift_analysis import feature_drift, psi_level
from security.drift_detector import RollingDriftDetector

START = datetime(2016, 1, 11, tzinfo=timezone.utc)


def kag_like(days: int, seed: int = 0, start: datetime = START) -> pd.DataFrame:
    """Nine correlated rooms with a daily cycle, 10-minute steps, meters in 10 Wh units."""
    rng = np.random.default_rng(seed)
    n = days * 144
    times = pd.date_range(start, periods=n, freq="10min")
    hour = times.hour + times.minute / 60
    base = 20 + 1.5 * np.sin(2 * np.pi * (hour - 9) / 24) + np.cumsum(rng.normal(0, 0.01, n))
    data = {"timestamp": times}
    for i, t in enumerate(INDOOR_T):
        data[t] = base + 0.3 * i + rng.normal(0, 0.1, n)
    for i, rh in enumerate(INDOOR_RH):
        data[rh] = 40 + rng.normal(0, 1, n)
    for column in SENSORS:
        data.setdefault(column, rng.normal(5, 1, n))
    data["Appliances"] = 10 * rng.integers(3, 30, n)
    data["lights"] = 10 * rng.integers(0, 4, n)
    return pd.DataFrame(data)


@pytest.fixture(scope="module")
def fitted() -> tuple[KagMonitor, pd.DataFrame]:
    train, validation = kag_like(14, 1), kag_like(7, 2, START + timedelta(days=14))
    return KagMonitor().fit(train, validation), kag_like(7, 3, START + timedelta(days=21))


class TestInject:
    def test_spoofing_offsets_only_the_window_and_labels_it(self):
        df = kag_like(2)
        attack = KagAttack("temperature_spoofing", START + timedelta(hours=10), 2, {"room": "T2", "offset_c": 3.0})
        out, labels, owner = inject(df, [attack])
        diff = (out["T2"] - df["T2"]).to_numpy()
        assert np.allclose(diff[60:72], 3.0) and np.allclose(np.delete(diff, range(60, 72)), 0.0)
        assert labels[60:72] == ["temperature_spoofing"] * 12 and labels[59] is None and owner[60] == 0
        assert (out["T1"] == df["T1"]).all()

    def test_replay_copies_yesterday_and_gradual_ramps(self):
        df = kag_like(3)
        replay = KagAttack("replay", START + timedelta(days=1, hours=5), 1, {"lag_steps": 144})
        ramp = KagAttack("gradual_manipulation", START + timedelta(days=2), 10, {"room": "T5", "rate_c_per_h": 0.5})
        out, labels, _ = inject(df, [replay, ramp])
        rows = range(144 + 30, 144 + 36)
        assert np.allclose(out.loc[rows, list(INDOOR_T)].to_numpy(), df.loc[[r - 144 for r in rows], list(INDOOR_T)].to_numpy())
        offset = (out["T5"] - df["T5"]).to_numpy()[288:288 + 60]
        assert offset[0] == 0 and offset[-1] == pytest.approx(0.5 * 59 / 6) and np.all(np.diff(offset) > 0)
        assert labels[288] == "gradual_manipulation"

    def test_abnormal_power_scales_meters(self):
        df = kag_like(1)
        out, _, _ = inject(df, [KagAttack("abnormal_power", START, 1, {"factor": 3.0})])
        assert (out.loc[:5, "Appliances"] == 3 * df.loc[:5, "Appliances"]).all()


class TestKagMonitor:
    def test_clean_data_has_few_alarms(self, fitted):
        monitor, clean = fitted
        detections, _, _ = monitor.run(clean)
        assert sum(kind is not None for kind, _ in detections) / len(detections) < 0.02

    def test_detects_spoofing_replay_and_power(self, fitted):
        monitor, clean = fitted
        day = clean["timestamp"].iloc[0].to_pydatetime()
        attacks = [KagAttack("temperature_spoofing", day + timedelta(days=4, hours=10), 2, {"room": "T2", "offset_c": 3.0}),
                   KagAttack("replay", day + timedelta(days=2, hours=14), 2, {"lag_steps": 144}),
                   KagAttack("abnormal_power", day + timedelta(days=5, hours=18), 1, {"factor": 3.5})]
        attacked, labels, _ = inject(clean, attacks)
        detections, _, _ = monitor.run(attacked)
        found = {label for (kind, _), label in zip(detections, labels) if label and kind == label}
        assert found == {"temperature_spoofing", "replay", "abnormal_power"}

    def test_cusum_detects_gradual_ramp(self, fitted):
        monitor, clean = fitted
        ramp = KagAttack("gradual_manipulation", clean["timestamp"].iloc[0].to_pydatetime() + timedelta(days=4),
                         20, {"room": "T5", "rate_c_per_h": 0.3})
        attacked, _, _ = inject(clean, [ramp])
        _, _, alarms = monitor.run(attacked)
        assert any(room == "T5" and ramp.start <= when < ramp.end for when, room in alarms)


class TestRollingDriftDetector:
    def feed(self, detector, values):
        results = []
        for i, v in enumerate(values):
            r = detector.update("z", START + timedelta(minutes=10 * i), {"x": float(v)})
            if r:
                results.append(r)
        return results

    def test_matches_same_hours_on_previous_days(self):
        rng = np.random.default_rng(0)
        hour = np.arange(144 * 6) % 144
        daily = 5 * np.sin(2 * np.pi * hour / 144) + rng.normal(0, 0.3, len(hour))
        results = self.feed(RollingDriftDetector(features=("x",), window=36, period=144, reference_days=3), daily)
        assert results and not any(r.is_drift for r in results)

    def test_flags_a_shift_and_respects_disabled(self):
        rng = np.random.default_rng(1)
        values = np.concatenate([rng.normal(0, 1, 144 * 4), rng.normal(4, 1, 72)])
        results = self.feed(RollingDriftDetector(features=("x",), window=36, period=144, reference_days=3), values)
        assert results[-1].is_drift and results[-1].drifting == ("x",)
        disabled = RollingDriftDetector(features=("x",), window=36, period=144, reference_days=3,
                                        disabled=frozenset({("z", "x")}))
        assert not any(r.is_drift for r in self.feed(disabled, values))


class TestDriftAnalysis:
    def test_feature_drift_ranks_shifted_features(self):
        rng = np.random.default_rng(0)
        train = pd.DataFrame({"a": rng.normal(0, 1, 2000), "b": rng.normal(0, 1, 2000)})
        other = pd.DataFrame({"a": rng.normal(3, 1, 500), "b": rng.normal(0, 1, 500)})
        result = feature_drift(train, other, ["a", "b"])
        assert list(result["feature"]) == ["a", "b"]
        assert result.loc[0, "ks_p_value"] < 1e-6 and result.loc[1, "psi"] < 0.1
        assert result.loc[0, "outside_train_range"] > result.loc[1, "outside_train_range"]
        assert psi_level(result.loc[0, "psi"]) == "significant" and psi_level(0.01) == "stable"
