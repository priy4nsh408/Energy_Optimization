"""Evaluate attack and drift detection on labelled simulated attacks.

    python -m experiments.cyberattack_experiment                      # held-out test week
    python -m experiments.cyberattack_experiment --week development   # week used while developing
    python -m experiments.cyberattack_experiment --report docs/results/security_evaluation.md

A clean simulated week is streamed through the attack injector and the
security monitor (the same code as the Kafka service). Ground-truth labels
come from the injector and are never shown to the detectors.

Point detection (individual readings) and drift detection (windows) are
evaluated separately:

* Point metrics use every reading except gradual-manipulation readings
  (which target drift detection) and the first reading after each attack ends
  (the jump back to genuine values is attack-induced but unlabelled).
  Positive = attacked reading; predicted positive = any point detection
  (before alert rate-limiting).
* Detection latency = time from attack start to the first detection of that
  attack segment.
* Drift metrics: a gradual-manipulation segment is detected if a drift alarm
  fires in its zone while it is active; false drift alarms are alarms in
  zones/windows without any attack.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config import SimulatorSettings, get_config
from security.monitor import SecurityModels, SecurityMonitor
from security.train import load_models
from simulator.attack_simulator import AttackInjector, AttackScenario
from simulator.engine import BuildingSimulator
from utils.logging_config import configure_logging

H = timedelta(hours=1)


def week_scenarios(start: datetime, week: str) -> list[AttackScenario]:
    """Attack schedules. 'development' was used while building the detectors;
    'test' (different times, zones and parameters) is only used for the report."""
    d = lambda day, hour: start + timedelta(days=day, hours=hour)          # noqa: E731
    if week == "development":
        return [
            AttackScenario("temperature_spoofing", "Z1", d(1, 10), d(1, 12), {"offset_c": 4.0}),
            AttackScenario("occupancy_manipulation", "Z1", d(2, 11), d(2, 13), {"mode": "hide"}),
            AttackScenario("occupancy_manipulation", "Z3", d(2, 2), d(2, 4), {"mode": "inflate", "count": 10}),
            AttackScenario("replay", "Z4", d(1, 13), d(1, 16), {"lag": 288}),
            AttackScenario("abnormal_power", "Z5", d(0, 15), d(0, 16), {"factor": 3.0}),
            AttackScenario("gradual_manipulation", "Z2", d(2, 6), d(2, 18), {"rate_c_per_h": 0.4}),
        ]
    if week == "test2":
        # Second held-out week, created after test week 1 revealed detector gaps;
        # never used for development.
        return [
            AttackScenario("temperature_spoofing", "Z3", d(0, 11), d(0, 13), {"offset_c": 3.5}),
            AttackScenario("temperature_spoofing", "Z1", d(3, 22), d(3, 23), {"offset_c": -4.0}),
            AttackScenario("occupancy_manipulation", "Z4", d(1, 12), d(1, 14), {"mode": "hide"}),
            AttackScenario("occupancy_manipulation", "Z1", d(4, 2), d(4, 4), {"mode": "inflate", "count": 15}),
            AttackScenario("replay", "Z2", d(2, 10), d(2, 12), {"lag": 288}),
            AttackScenario("replay", "Z5", d(3, 14), d(3, 15), {"mode": "verbatim", "lag": 12}),
            AttackScenario("abnormal_power", "Z1", d(2, 15), d(2, 16), {"factor": 2.0}),
            AttackScenario("abnormal_power", "Z2", d(0, 21), d(0, 23), {"factor": 3.0}),
            AttackScenario("abnormal_power", "Z4", d(4, 13), d(4, 14), {"factor": 0.4}),
            AttackScenario("gradual_manipulation", "Z3", d(1, 4), d(1, 20), {"rate_c_per_h": 0.3}),
            AttackScenario("gradual_manipulation", "Z2", d(5, 2), d(5, 14), {"rate_c_per_h": 0.6}),
        ]
    return [
        AttackScenario("temperature_spoofing", "Z2", d(1, 15), d(1, 17), {"offset_c": -3.0}),
        AttackScenario("temperature_spoofing", "Z4", d(4, 3), d(4, 4), {"offset_c": 5.0}),
        AttackScenario("occupancy_manipulation", "Z1", d(3, 10), d(3, 12), {"mode": "hide"}),
        AttackScenario("occupancy_manipulation", "Z2", d(2, 1), d(2, 3), {"mode": "inflate", "count": 6}),
        AttackScenario("replay", "Z1", d(2, 14), d(2, 17), {"lag": 288}),
        AttackScenario("replay", "Z3", d(4, 9), d(4, 10), {"mode": "verbatim", "lag": 24}),
        AttackScenario("abnormal_power", "Z4", d(1, 9), d(1, 10), {"factor": 2.5}),
        AttackScenario("abnormal_power", "Z3", d(3, 20), d(3, 22), {"factor": 4.0}),
        AttackScenario("gradual_manipulation", "Z1", d(5, 0), d(5, 18), {"rate_c_per_h": 0.25}),
        AttackScenario("gradual_manipulation", "Z4", d(3, 6), d(3, 16), {"rate_c_per_h": 0.5}),
    ]


WEEKS = {"development": (datetime(2026, 2, 9, tzinfo=timezone.utc), 202),
         "test": (datetime(2026, 2, 16, tzinfo=timezone.utc), 303),
         "test2": (datetime(2026, 3, 2, tzinfo=timezone.utc), 404)}


@dataclass
class Counts:
    tp: int = 0
    fp: int = 0
    fn: int = 0
    tn: int = 0

    def metrics(self) -> dict[str, float]:
        precision = self.tp / (self.tp + self.fp) if self.tp + self.fp else 0.0
        recall = self.tp / (self.tp + self.fn) if self.tp + self.fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        fpr = self.fp / (self.fp + self.tn) if self.fp + self.tn else 0.0
        return {"precision": precision, "recall": recall, "f1": f1, "false_positive_rate": fpr,
                **vars(self)}


@dataclass
class Evaluation:
    week: str
    readings: int = 0
    point: Counts = field(default_factory=Counts)
    per_type_recall: dict[str, list[int]] = field(default_factory=lambda: defaultdict(lambda: [0, 0]))
    classification: dict[str, dict[str, int]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))
    segments: list[dict[str, Any]] = field(default_factory=list)
    false_positive_detectors: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    false_positive_examples: list[str] = field(default_factory=list)
    gradual_flagged_by_point: list[int] = field(default_factory=lambda: [0, 0])
    drift_evaluations_clean: int = 0
    drift_false_alarms: int = 0
    drift_alarms_during_point_attacks: int = 0
    data_drift_evaluations: int = 0
    data_drift_windows: int = 0
    alerts_emitted: int = 0
    alerts_suppressed: int = 0


def evaluate(models: SecurityModels, week: str, days: float = 7.0) -> Evaluation:
    start, seed = WEEKS[week]
    scenarios = week_scenarios(start, week)
    injector = AttackInjector(scenarios)
    monitor = SecurityMonitor(models)
    simulator = BuildingSimulator(SimulatorSettings(interval_seconds=1, speed_factor=300,
                                                    random_seed=seed, start_time=start))
    result = Evaluation(week)
    first_detection: dict[int, datetime] = {}
    drift_detected: dict[int, datetime] = {}
    interval = timedelta(seconds=300)

    def after_attack(zone: str, when: datetime) -> bool:
        return any(s.zone_id == zone and s.end <= when < s.end + interval for s in scenarios)

    for _ in range(int(days * 288)):
        for event in simulator.tick():
            attacked = injector.apply(event)
            label = injector.labels[-1]
            output = monitor.process(attacked)
            result.readings += 1
            detection = output.detection
            flagged = bool(detection and detection.flagged)
            kind = label.attack_type

            if kind == "gradual_manipulation":
                result.gradual_flagged_by_point[0] += flagged
                result.gradual_flagged_by_point[1] += 1
            elif kind is not None:
                result.per_type_recall[kind][0] += flagged
                result.per_type_recall[kind][1] += 1
                if flagged:
                    result.point.tp += 1
                    result.classification[kind][detection.attack_type] += 1
                    first_detection.setdefault(label.scenario_index, label.timestamp)
                else:
                    result.point.fn += 1
            elif not after_attack(label.zone_id, label.timestamp):
                if flagged:
                    result.point.fp += 1
                    result.false_positive_detectors[f"{detection.attack_type} via {detection.detector}"] += 1
                    if len(result.false_positive_examples) < 10:
                        rules = ",".join(h.rule for h in detection.hits) or "isolation_forest"
                        result.false_positive_examples.append(
                            f"{label.zone_id} {label.timestamp:%a %H:%M} {detection.attack_type} ({rules})")
                else:
                    result.point.tn += 1

            if output.data_drift is not None:
                result.data_drift_evaluations += 1
                result.data_drift_windows += output.data_drift.is_drift
            drift = output.drift
            if drift is not None:
                active = [i for i, s in enumerate(scenarios) if s.zone_id == drift.zone_id
                          and s.start < drift.window_end and s.end > drift.window_start]
                gradual = [i for i in active if scenarios[i].attack_type == "gradual_manipulation"]
                if drift.is_drift:
                    for i in gradual:
                        if scenarios[i].start <= drift.window_end < scenarios[i].end:
                            drift_detected.setdefault(i, drift.window_end)
                    if not active:
                        result.drift_false_alarms += 1
                    elif not gradual:
                        result.drift_alarms_during_point_attacks += 1
                if not active:
                    result.drift_evaluations_clean += 1

    for i, s in enumerate(scenarios):
        detected_at = (drift_detected if s.attack_type == "gradual_manipulation" else first_detection).get(i)
        offset = None
        if s.attack_type == "gradual_manipulation" and detected_at:
            hours = (detected_at - s.start).total_seconds() / 3600
            offset = min(s.params.get("max_offset_c", 6.0), s.params.get("rate_c_per_h", 0.5) * hours)
        result.segments.append({
            "attack_type": s.attack_type, "zone_id": s.zone_id, "start": s.start.isoformat(),
            "duration_min": (s.end - s.start).total_seconds() / 60, "params": s.params,
            "detected": detected_at is not None,
            "latency_min": (detected_at - s.start).total_seconds() / 60 if detected_at else None,
            "offset_at_detection_c": offset})
    result.alerts_emitted, result.alerts_suppressed = monitor.alerts.emitted, monitor.alerts.suppressed
    return result


def render(ev: Evaluation, models: SecurityModels) -> str:
    m = ev.point.metrics()
    lines = [
        f"# Security evaluation ({ev.week} week)", "",
        f"Generated by `python -m experiments.cyberattack_experiment --week {ev.week}` on "
        f"{datetime.now(timezone.utc).isoformat()}. All values are computed from this run.", "",
        f"* Detectors: version `{models.metadata.get('version')}`, trained on {models.metadata.get('readings')} "
        f"clean readings ({models.metadata.get('start')} to {models.metadata.get('end')}, source "
        f"{models.metadata.get('source', {}).get('type')}).",
        f"* Security-drift monitors disabled after calibration on clean validation data (> 2 % false "
        f"alarms): {', '.join(models.metadata.get('disabled_drift_monitors', [])) or 'none'}.",
        f"* Evaluation: {ev.readings} readings (7 simulated days, 5 zones, 5-minute interval), "
        f"{len(ev.segments)} labelled attack segments.", "",
        "## Individual-reading (point) detection", "",
        "Rules + Isolation Forest. Gradual-manipulation readings are excluded here (see drift).", "",
        "| Precision | Recall | F1 | False-positive rate | TP | FP | FN | TN |", "|---|---|---|---|---|---|---|---|",
        f"| {m['precision']:.3f} | {m['recall']:.3f} | {m['f1']:.3f} | {m['false_positive_rate']:.4f} | "
        f"{m['tp']} | {m['fp']} | {m['fn']} | {m['tn']} |", "",
        "| Attack type | Readings | Detected | Recall | Classified as |", "|---|---|---|---|---|"]
    for kind, (hit, total) in sorted(ev.per_type_recall.items()):
        classes = ", ".join(f"{k}: {v}" for k, v in sorted(ev.classification[kind].items(), key=lambda x: -x[1]))
        lines.append(f"| {kind} | {total} | {hit} | {hit / total:.3f} | {classes} |")
    lines += ["", "False positives by classification and detector: " +
              (", ".join(f"{k}: {v}" for k, v in sorted(ev.false_positive_detectors.items())) or "none"), "",
              "Example false positives: " + ("; ".join(ev.false_positive_examples) or "none"), "",
              "## Detection latency per attack segment", "",
              "| Attack | Zone | Start | Duration (min) | Detected | Latency (min) | Offset at detection (°C) |",
              "|---|---|---|---|---|---|---|"]
    for s in ev.segments:
        lines.append(f"| {s['attack_type']} {json.dumps(s['params'])} | {s['zone_id']} | {s['start'][:16]} | "
                     f"{s['duration_min']:.0f} | {'yes' if s['detected'] else 'no'} | "
                     f"{s['latency_min'] if s['latency_min'] is not None else '-'} | "
                     f"{round(s['offset_at_detection_c'], 2) if s['offset_at_detection_c'] is not None else '-'} |")
    g_hit, g_total = ev.gradual_flagged_by_point
    lines += ["", "## Distributional drift detection (KS test + PSI)", "",
              "Drift is evaluated on 6-hour windows every hour, not on single readings. *Security drift* "
              "monitors physical-consistency residuals (temperature control error while the HVAC is active, "
              "CO2-vs-occupancy residual); *data drift* monitors raw inputs and is not treated as an attack.", "",
              f"* Gradual-manipulation segments detected: "
              f"{sum(s['detected'] for s in ev.segments if s['attack_type'] == 'gradual_manipulation')} of "
              f"{sum(s['attack_type'] == 'gradual_manipulation' for s in ev.segments)} (latency above).",
              f"* False drift alarms: {ev.drift_false_alarms} of {ev.drift_evaluations_clean} window evaluations "
              "without any attack.",
              f"* Drift alarms during point attacks (not counted as false): {ev.drift_alarms_during_point_attacks}.",
              f"* Data drift (raw inputs vs. January training data): {ev.data_drift_windows} of "
              f"{ev.data_drift_evaluations} window evaluations. This is genuine seasonal/weather change "
              "between training and evaluation weeks, reported for ML model monitoring.",
              f"* For comparison, point detectors flagged {g_hit} of {g_total} gradual-manipulation readings "
              f"({g_hit / g_total:.1%})." if g_total else "", "",
              f"Alerts emitted: {ev.alerts_emitted}; detections suppressed by rate limiting: {ev.alerts_suppressed}.", ""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m experiments.cyberattack_experiment",
                                     description=__doc__.splitlines()[0])
    parser.add_argument("--week", choices=sorted(WEEKS), default="test")
    parser.add_argument("--days", type=float, default=7.0)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args(argv)
    configure_logging(get_config().LOGGING, "security_eval", stream=sys.stderr)
    models = load_models()
    evaluation = evaluate(models, args.week, args.days)
    report = render(evaluation, models)
    print(report)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
