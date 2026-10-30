"""Anomaly and drift detection on REAL data (KAG) with injected, labelled attacks.

    python -m experiments.kag_anomaly_experiment --period validation    # development
    python -m experiments.kag_anomaly_experiment --period test --report docs/results/kag_anomaly_detection.md

The Module 8 detectors were built for the simulated building's sensors. KAG
is a real house (nine rooms, 10-minute readings) without HVAC status, CO2 or
occupancy, so this experiment uses the same *methods* with the sensors KAG has:

* Detectors are trained on the clean training period (January - mid-April).
* The Isolation Forest threshold and the drift monitors are calibrated on the
  clean validation period. Attacks are injected into validation (development)
  or test (final evaluation, May) and labelled.
* Features avoid absolute temperatures, which drift strongly between winter
  and May (see ``experiments.kag_drift_analysis``). They use per-step changes
  and room-to-room consistency residuals instead (each indoor temperature
  predicted from the other rooms by a linear model). Because even these
  relationships change with the season and with daily sunshine, point
  detection uses the residual minus its median at the *same time on the
  previous 3 days* (seasonal-naive baseline), and drift compares each 6-hour
  window with the same hours on the previous 3 days.

Attacks: temperature spoofing (one room), replay (all sensors from one day
earlier), abnormal power (Appliances and lights scaled), gradual manipulation
(slow temperature ramp in one room). Occupancy manipulation is not possible:
KAG has no occupancy data.

Rules (calibrated on training data): replay (consecutive exact repetitions of
the 16 indoor sensor values), temperature rate of change, room-consistency
residual (seasonal-naive baseline), power above the training maximum, and the
meters' value domain (energy is reported in 10 Wh steps). Point detection and slow
manipulation are evaluated separately. Slow manipulation is tested with the
Module 8 KS + PSI window tests and with a per-room CUSUM (Page's test) on the
consistency residual; both are calibrated on the clean validation period.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LinearRegression

from config import get_config
from experiments.cyberattack_experiment import Counts
from ml.dataset import DEFAULT_KAG_PATH, load_kag
from ml.features import KAG_SPEC, build_features
from ml.train import chronological_split
from security.drift_detector import RollingDriftDetector
from utils.logging_config import configure_logging

INDOOR_T = ("T1", "T2", "T3", "T4", "T5", "T7", "T8", "T9")
INDOOR_RH = ("RH_1", "RH_2", "RH_3", "RH_4", "RH_5", "RH_7", "RH_8", "RH_9")
SENSORS = INDOOR_T + INDOOR_RH + ("T6", "RH_6", "T_out", "Press_mm_hg", "RH_out", "Windspeed",
                                  "Visibility", "Tdewpoint", "Appliances", "lights")
RESIDUALS = tuple(f"resid_{t}" for t in INDOOR_T)
DETRENDED = tuple(f"resid_dt_{t}" for t in INDOOR_T)
FEATURES = (*(f"d_{t}" for t in INDOOR_T), *DETRENDED, "Appliances", "lights", "d_Appliances",
            "hour_sin", "hour_cos")
BASELINE_STEPS = 144          # one day of 10-minute readings (seasonal-naive baseline)
METER_RESOLUTION_WH = 10.0
GROUPS = {"temperature": (*(f"d_{t}" for t in INDOOR_T), *DETRENDED),
          "power": ("Appliances", "lights", "d_Appliances")}
GROUP_TO_ATTACK = {"temperature": "temperature_spoofing", "power": "abnormal_power"}
CUSUM_K = 1.5                 # CUSUM allowance in residual standard deviations
STEP = timedelta(minutes=10)
ZONE = "house"


# --------------------------------------------------------------------------- attacks
@dataclass(frozen=True)
class KagAttack:
    attack_type: str
    start: datetime
    hours: float
    params: dict = field(default_factory=dict)

    @property
    def end(self) -> datetime:
        return self.start + timedelta(hours=self.hours)


def schedule(period: str, start: datetime) -> list[KagAttack]:
    d = lambda day, hour: start + timedelta(days=day, hours=hour)  # noqa: E731
    if period == "validation":
        return [KagAttack("temperature_spoofing", d(2, 10), 2, {"room": "T2", "offset_c": 3.0}),
                KagAttack("replay", d(4, 14), 3, {"lag_steps": 144}),
                KagAttack("abnormal_power", d(6, 18), 2, {"factor": 3.0}),
                KagAttack("gradual_manipulation", d(9, 0), 18, {"room": "T5", "rate_c_per_h": 0.25})]
    return [KagAttack("temperature_spoofing", d(1, 15), 2, {"room": "T8", "offset_c": -2.5}),
            KagAttack("temperature_spoofing", d(11, 3), 1, {"room": "T1", "offset_c": 4.0}),
            KagAttack("replay", d(3, 9), 4, {"lag_steps": 144}),
            KagAttack("replay", d(14, 20), 2, {"lag_steps": 144}),
            KagAttack("abnormal_power", d(5, 12), 2, {"factor": 2.5}),
            KagAttack("abnormal_power", d(16, 7), 1, {"factor": 4.0}),
            KagAttack("gradual_manipulation", d(7, 0), 20, {"room": "T3", "rate_c_per_h": 0.2}),
            KagAttack("gradual_manipulation", d(18, 6), 12, {"room": "T7", "rate_c_per_h": 0.4})]


def inject(df: pd.DataFrame, attacks: list[KagAttack]) -> tuple[pd.DataFrame, list[str | None], list[int | None]]:
    """Return manipulated data plus a label and attack index per row."""
    out = df.copy()
    labels: list[str | None] = [None] * len(df)
    index: list[int | None] = [None] * len(df)
    for i, attack in enumerate(attacks):
        mask = (df["timestamp"] >= attack.start) & (df["timestamp"] < attack.end)
        rows = np.flatnonzero(mask.to_numpy())
        p = attack.params
        if attack.attack_type == "temperature_spoofing":
            out.loc[mask, p["room"]] += p["offset_c"]
        elif attack.attack_type == "gradual_manipulation":
            hours = (df.loc[mask, "timestamp"] - attack.start).dt.total_seconds() / 3600
            out.loc[mask, p["room"]] += np.minimum(p.get("max_offset_c", 6.0), p["rate_c_per_h"] * hours)
        elif attack.attack_type == "abnormal_power":
            out.loc[mask, ["Appliances", "lights"]] *= p["factor"]
        elif attack.attack_type == "replay":
            source = rows - int(p["lag_steps"])
            rows, source = rows[source >= 0], source[source >= 0]
            out.iloc[rows, [out.columns.get_loc(c) for c in SENSORS]] = df.iloc[source][list(SENSORS)].to_numpy()
        for r in rows:
            labels[r], index[r] = attack.attack_type, i
    return out, labels, index


# --------------------------------------------------------------------------- detector
class KagMonitor:
    """Rules + Isolation Forest + security drift for the KAG sensors."""

    def __init__(self) -> None:
        self.consistency: dict[str, LinearRegression] = {}
        self.limits: dict[str, float] = {}
        self.forest: IsolationForest | None = None
        self.threshold = 0.0
        self.median: dict[str, float] = {}
        self.scale: dict[str, float] = {}
        self.disabled: frozenset = frozenset()
        self.resid_sd: dict[str, float] = {}
        self.cusum_h: dict[str, float] = {}

    # -- features ---------------------------------------------------------------------
    def features(self, df: pd.DataFrame) -> pd.DataFrame:
        f = pd.DataFrame(index=df.index)
        gap_ok = df["timestamp"].diff().eq(STEP)
        for t in INDOOR_T:
            f[f"d_{t}"] = df[t].diff().where(gap_ok, 0.0).fillna(0.0)
            others = [o for o in INDOOR_T if o != t]
            f[f"resid_{t}"] = df[t] - self.consistency[t].predict(df[others].to_numpy())
            same_time = pd.concat([f[f"resid_{t}"].shift(k * BASELINE_STEPS) for k in (1, 2, 3)], axis=1)
            f[f"resid_dt_{t}"] = (f[f"resid_{t}"] - same_time.median(axis=1)).fillna(0.0)
        f["Appliances"], f["lights"] = df["Appliances"], df["lights"]
        f["d_Appliances"] = df["Appliances"].diff().where(gap_ok, 0.0).fillna(0.0)
        hour = df["timestamp"].dt.hour + df["timestamp"].dt.minute / 60
        f["hour_sin"], f["hour_cos"] = np.sin(2 * np.pi * hour / 24), np.cos(2 * np.pi * hour / 24)
        return f

    def fit(self, train: pd.DataFrame, validation: pd.DataFrame) -> "KagMonitor":
        for t in INDOOR_T:
            others = [o for o in INDOOR_T if o != t]
            self.consistency[t] = LinearRegression().fit(train[others].to_numpy(), train[t].to_numpy())
        ft, fv = self.features(train), self.features(validation)
        for t in INDOOR_T:
            self.limits[f"rate_{t}"] = ft[f"d_{t}"].abs().max() * 1.5
            self.limits[f"resid_{t}"] = float(np.quantile(ft[f"resid_dt_{t}"].abs(), 0.999)) * 1.2
        self.limits["Appliances"] = train["Appliances"].max() * 1.2
        self.limits["lights"] = train["lights"].max() * 1.2
        self.forest = IsolationForest(n_estimators=200, max_samples=512, random_state=0).fit(ft[list(FEATURES)])
        self.threshold = float(np.quantile(-self.forest.score_samples(fv[list(FEATURES)]), 0.999))
        median = ft[list(FEATURES)].median()
        mad = (ft[list(FEATURES)] - median).abs().median() * 1.4826
        self.median, self.scale = median.to_dict(), mad.where(mad > 1e-9, ft[list(FEATURES)].std()).to_dict()
        # Disable residual monitors that false-alarm on the clean validation period (> 2 %).
        detector, evaluations, alarms = self._drift_detector(), defaultdict(int), defaultdict(int)
        for when, row in zip(validation["timestamp"], fv.to_dict("records")):
            result = detector.update(ZONE, when, row)
            if result:
                for feature in result.features:
                    evaluations[feature.feature] += 1
                for feature in result.drifting:
                    alarms[feature] += 1
        self.disabled = frozenset((ZONE, f) for f, n in evaluations.items() if alarms[f] / n > 0.02)
        self.clean_drift_alarm_rate = {f: alarms[f] / n for f, n in evaluations.items()}
        # CUSUM per room on the standardised residual; threshold 1.2 x the clean validation maximum.
        self.resid_sd = {t: float(ft[f"resid_dt_{t}"].std()) for t in INDOOR_T}
        self.cusum_h = {t: 1.2 * float(self._cusum(fv, t).max()) for t in INDOOR_T}
        return self

    def _cusum(self, f: pd.DataFrame, room: str) -> np.ndarray:
        """Two-sided CUSUM statistic (Page's test) of one room's residual, without resets."""
        z = (f[f"resid_dt_{room}"] / self.resid_sd[room]).to_numpy()
        upper = lower = 0.0
        out = np.empty(len(z))
        for i, value in enumerate(z):
            upper, lower = max(0.0, upper + value - CUSUM_K), max(0.0, lower - value - CUSUM_K)
            out[i] = max(upper, lower)
        return out

    def _drift_detector(self) -> RollingDriftDetector:
        return RollingDriftDetector(features=RESIDUALS, window=36, period=BASELINE_STEPS, reference_days=3,
                                    step=6, min_effect_size=0.5, disabled=self.disabled)

    # -- detection --------------------------------------------------------------------
    def run(self, df: pd.DataFrame) -> tuple[list[tuple[str | None, list[str]]], list]:
        """Point detections per row (classification, rules) and drift results."""
        f = self.features(df)
        scores = -self.forest.score_samples(f[list(FEATURES)])
        history: deque = deque(maxlen=2000)
        seen: dict[tuple, int] = {}
        previous_match = False
        previous_resid: dict[str, bool] = defaultdict(bool)
        detections, drift_results = [], []
        drift = self._drift_detector()
        cusum_alarms: list[tuple[datetime, str]] = []
        for t in INDOOR_T:                          # alarm episodes: first crossing, re-armed below h/2
            armed = True
            for when, value in zip(df["timestamp"], self._cusum(f, t)):
                if armed and value > self.cusum_h[t]:
                    cusum_alarms.append((when, t))
                    armed = False
                elif value < self.cusum_h[t] / 2:
                    armed = True
        values = df[list(INDOOR_T + INDOOR_RH)].round(6).to_numpy()
        for i, (when, row) in enumerate(zip(df["timestamp"], f.to_dict("records"))):
            rules = []
            vector = tuple(values[i])
            match = vector in seen and i - seen[vector] > 1
            if match and previous_match:
                rules.append(("replayed_values", "replay"))
            previous_match = match
            seen[vector] = i
            for t in INDOOR_T:
                if abs(row[f"d_{t}"]) > self.limits[f"rate_{t}"]:
                    rules.append((f"rate_{t}", "temperature_spoofing"))
                bad = abs(row[f"resid_dt_{t}"]) > self.limits[f"resid_{t}"]
                if bad and previous_resid[t]:
                    rules.append((f"consistency_{t}", "temperature_spoofing"))
                previous_resid[t] = bad
            for column in ("Appliances", "lights"):
                if row[column] > self.limits[column]:
                    rules.append((f"{column}_out_of_range", "abnormal_power"))
                if abs(row[column] / METER_RESOLUTION_WH - round(row[column] / METER_RESOLUTION_WH)) > 1e-6:
                    rules.append((f"{column}_value_domain", "abnormal_power"))
            anomaly = scores[i] > self.threshold
            if rules:
                kinds = [k for _, k in rules]
                kind = "replay" if "replay" in kinds else max(set(kinds), key=kinds.count)
            elif anomaly:
                z = {g: max(abs(row[c] - self.median[c]) / self.scale[c] for c in cols) for g, cols in GROUPS.items()}
                group = max(z, key=z.get)
                kind = GROUP_TO_ATTACK[group] if z[group] >= 4 else "unknown_anomaly"
            else:
                kind = None
            detections.append((kind, [r for r, _ in rules] + (["isolation_forest"] if anomaly else [])))
            result = drift.update(ZONE, when, row)
            if result is not None:
                drift_results.append(result)
        return detections, drift_results, sorted(cusum_alarms)


# --------------------------------------------------------------------------- evaluation
def evaluate(df: pd.DataFrame, period: str) -> dict:
    frame = build_features(df, KAG_SPEC)
    split = chronological_split(frame)
    train = df[df["timestamp"] < split.validation_start].reset_index(drop=True)
    validation = df[(df["timestamp"] >= split.validation_start) & (df["timestamp"] < split.test_start)].reset_index(drop=True)
    test = df[df["timestamp"] >= split.test_start].reset_index(drop=True)
    monitor = KagMonitor().fit(train, validation)
    target = validation if period == "validation" else test
    start = target["timestamp"].iloc[0].normalize() + timedelta(days=1)
    attacks = schedule(period, start.to_pydatetime())
    attacked, labels, owner = inject(target, attacks)
    detections, drift_results, cusum_alarms = monitor.run(attacked)

    counts, per_type = Counts(), defaultdict(lambda: [0, 0])
    first: dict[int, datetime] = {}
    fp_examples, gradual_flagged = [], [0, 0]
    ends = [a.end for a in attacks]
    for i, ((kind, rules), label) in enumerate(zip(detections, labels)):
        when = attacked["timestamp"].iloc[i]
        flagged = kind is not None
        if label == "gradual_manipulation":
            gradual_flagged[0] += flagged
            gradual_flagged[1] += 1
        elif label is not None:
            per_type[label][0] += flagged
            per_type[label][1] += 1
            counts.tp += flagged
            counts.fn += not flagged
            if flagged:
                first.setdefault(owner[i], when)
        elif any(end <= when < end + STEP for end in ends):
            continue                                   # attack-induced jump back, unlabelled
        elif flagged:
            counts.fp += 1
            if len(fp_examples) < 8:
                fp_examples.append(f"{when:%m-%d %H:%M} {kind} ({','.join(rules)})")
        else:
            counts.tn += 1

    drift_alarms, false_drift, clean_evals = {}, 0, 0
    for result in drift_results:
        active = [i for i, a in enumerate(attacks) if a.start < result.window_end and a.end > result.window_start]
        if not active:
            clean_evals += 1
            false_drift += result.is_drift
        elif result.is_drift:
            for i in active:
                if attacks[i].attack_type == "gradual_manipulation" and attacks[i].start <= result.window_end < attacks[i].end:
                    drift_alarms.setdefault(i, (result.window_end, result.drifting))
    cusum_hits, cusum_false = {}, []
    for when, room in cusum_alarms:
        active = [i for i, a in enumerate(attacks) if a.start <= when < a.end + timedelta(hours=6)]
        if not active:
            cusum_false.append(f"{when:%m-%d %H:%M} {room}")
        for i in active:
            if attacks[i].attack_type == "gradual_manipulation" and attacks[i].start <= when < attacks[i].end \
                    and attacks[i].params["room"] == room:
                cusum_hits.setdefault(i, when)
    segments = []
    for i, a in enumerate(attacks):
        if a.attack_type == "gradual_manipulation":
            candidates = [t for t in (drift_alarms.get(i, (None, ()))[0], cusum_hits.get(i)) if t is not None]
            hit = min(candidates) if candidates else None
        else:
            hit = first.get(i)
        offset = None
        if a.attack_type == "gradual_manipulation" and hit is not None:
            offset = a.params["rate_c_per_h"] * (hit - a.start).total_seconds() / 3600
        segments.append({"attack": a.attack_type, "params": a.params, "start": a.start, "hours": a.hours,
                         "detected": hit is not None,
                         "latency_min": (hit - a.start).total_seconds() / 60 if hit is not None else None,
                         "offset_at_detection_c": offset,
                         "drift_features": list(drift_alarms.get(i, (None, ()))[1]),
                         "cusum": cusum_hits.get(i) is not None})
    return {"period": period, "rows": len(target), "train_rows": len(train), "counts": counts,
            "per_type": dict(per_type), "segments": segments, "fp_examples": fp_examples,
            "gradual_flagged": gradual_flagged, "false_drift": false_drift, "clean_drift_evaluations": clean_evals,
            "disabled": sorted(f for _, f in monitor.disabled), "threshold": monitor.threshold,
            "clean_drift_alarm_rate": monitor.clean_drift_alarm_rate, "cusum_false": cusum_false,
            "cusum_h": monitor.cusum_h, "days": (target["timestamp"].iloc[-1] - target["timestamp"].iloc[0]).days,
            "start": target["timestamp"].iloc[0], "end": target["timestamp"].iloc[-1]}


def render(r: dict, sha: str) -> str:
    m = r["counts"].metrics()
    lines = [
        f"# Anomaly and drift detection on real data: KAG ({r['period']} period)", "",
        f"Generated by `python -m experiments.kag_anomaly_experiment --period {r['period']}` on "
        f"{datetime.now(timezone.utc).isoformat()}. All values are computed from this run.", "",
        f"* Data: `KAG_energydata_complete.csv` (SHA-256 `{sha}`), real 10-minute readings of one house.",
        f"* Detectors trained on {r['train_rows']} clean rows (January - mid-April); Isolation Forest threshold "
        f"and drift monitors calibrated on the clean validation period.",
        f"* Evaluated on {r['rows']} rows ({r['start']:%Y-%m-%d} to {r['end']:%Y-%m-%d}) with {len(r['segments'])} "
        "injected, labelled attacks. Occupancy manipulation is not tested (KAG has no occupancy data).",
        f"* Drift monitors disabled after calibration (> 2 % false alarms on clean validation): "
        f"{', '.join(r['disabled']) or 'none'}.", "",
        "* Development used only the validation period. Disclosure: one diagnostic script during CUSUM "
        "development also printed CUSUM values for the test period. The allowance k = 1.5 is the only "
        "tested value (0.5, 1.0, 1.5) that detects the validation attack, so it was chosen by the validation "
        "criterion; no setting was changed after this test run.", "",
        "## Individual-reading (point) detection", "",
        "| Precision | Recall | F1 | False-positive rate | TP | FP | FN | TN |", "|---|---|---|---|---|---|---|---|",
        f"| {m['precision']:.3f} | {m['recall']:.3f} | {m['f1']:.3f} | {m['false_positive_rate']:.4f} | "
        f"{m['tp']} | {m['fp']} | {m['fn']} | {m['tn']} |", "",
        "| Attack type | Readings | Detected | Recall |", "|---|---|---|---|",
        *[f"| {k} | {total} | {hit} | {hit / total:.3f} |" for k, (hit, total) in sorted(r["per_type"].items())], "",
        "Example false positives: " + ("; ".join(r["fp_examples"]) or "none"), "",
        "## Detection latency per attack", "",
        "| Attack | Parameters | Start | Hours | Detected | Latency (min) | Offset at detection (°C) |",
        "|---|---|---|---|---|---|---|",
        *[f"| {s['attack']} | {json.dumps(s['params'])} | {s['start']:%Y-%m-%d %H:%M} | {s['hours']} | "
          f"{'yes' if s['detected'] else 'no'} | {s['latency_min'] if s['latency_min'] is not None else '-'} | "
          f"{round(s['offset_at_detection_c'], 2) if s['offset_at_detection_c'] is not None else '-'} |"
          for s in r["segments"]], "",
        "## Slow manipulation (drift) detection", "",
        "### KS + PSI window tests (as in Module 8)", "",
        "Each 6-hour window of room-consistency residuals is compared with the same hours on the previous "
        "3 days. Share of clean validation windows that raised a drift alarm, per monitor (monitors above "
        "2 % are disabled):", "",
        "| Monitor | " + " | ".join(sorted(r["clean_drift_alarm_rate"])) + " |",
        "|---|" + "---|" * len(r["clean_drift_alarm_rate"]),
        "| False-alarm share | " + " | ".join(f"{r['clean_drift_alarm_rate'][f]:.1%}"
                                          for f in sorted(r["clean_drift_alarm_rate"])) + " |", "",
        f"* False drift alarms in this period (enabled monitors only): {r['false_drift']} of "
        f"{r['clean_drift_evaluations']} window evaluations without attack.", "",
        f"### CUSUM per room (Page's test, allowance k = {CUSUM_K} σ, threshold 1.2 × clean validation maximum)", "",
        "| Attack | Room | Start | Detected by CUSUM | Latency (min) | Offset at detection (°C) |",
        "|---|---|---|---|---|---|",
        *[f"| gradual_manipulation | {s['params']['room']} | {s['start']:%Y-%m-%d %H:%M} | "
          f"{'yes' if s['cusum'] else 'no'} | {s['latency_min'] if s['latency_min'] is not None else '-'} | "
          f"{round(s['offset_at_detection_c'], 2) if s['offset_at_detection_c'] is not None else '-'} |"
          for s in r["segments"] if s["attack"] == "gradual_manipulation"], "",
        f"* CUSUM alarms not explained by an attack (attack window + 6 h): {len(r['cusum_false'])} in "
        f"{r['days']} days" + (f" ({'; '.join(r['cusum_false'][:8])})" if r["cusum_false"] else "") + ".",
        f"* For comparison, point detectors flagged {r['gradual_flagged'][0]} of {r['gradual_flagged'][1]} "
        "gradual-manipulation readings.", "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m experiments.kag_anomaly_experiment", description=__doc__.splitlines()[0])
    parser.add_argument("--period", choices=["validation", "test"], default="test")
    parser.add_argument("--data", type=Path, default=DEFAULT_KAG_PATH)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args(argv)
    configure_logging(get_config().LOGGING, "kag_anomaly", stream=sys.stderr)
    df, info = load_kag(args.data)
    report = render(evaluate(df, args.period), info.sha256)
    print(report)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
