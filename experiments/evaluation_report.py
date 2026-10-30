"""Module 11: comparison tables and graphs from the raw evaluation results.

    python -m experiments.evaluation_report --run docs/results/evaluation

Reads only ``config.json`` and ``raw/`` written by ``experiments.evaluation``
(and ``streaming_latency.json`` if present); every number in the report is
computed here from those files. Writes ``report.md``, ``summary.json`` and
``figures/*.png`` into the run directory.

Definitions
-----------
* Savings / reductions: ``(baseline - scenario) / baseline`` per seed, then
  mean ± sample standard deviation over seeds.
* Post-recovery window: from the moment every sensor isolated in
  ``attack_defended`` has been restored after the last attack, to the end of
  the run (per seed); all scenarios are compared on that same window.
  *Savings retention* = post-recovery saving of ``attack_defended`` divided by
  the saving of ``optimized`` in the same window.
* Point detection: readings after the warm-up; positives are readings of
  point attacks (gradual manipulation is evaluated by drift alerts); the first
  reading after an attack is excluded (jump back to true values).
* Detection latency: attack start -> first flagged reading (point) or first
  security-drift alert (gradual), simulated minutes.
* Containment: attack start -> first executed isolation in the attacked zone.
* Recovery time: attack end -> the attacked zone has no isolated sensor again
  (optimizer resumes).
* Processing latency: wall-clock per in-process step on the machine in
  ``config.json``.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, stdev
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

ORDER = ["baseline", "optimized", "attack_undefended", "attack_defended"]
LABELS = {
    "baseline": "1 Baseline",
    "optimized": "2 ML-optimized",
    "attack_undefended": "3 Attack, no defence",
    "attack_defended": "4/5 Attack + detection + agent",
}
COLORS = {"baseline": "#6c757d", "optimized": "#198754", "attack_undefended": "#dc3545", "attack_defended": "#0d6efd"}
POINT_TYPES = ("temperature_spoofing", "occupancy_manipulation", "replay", "abnormal_power")
CONTAIN_MARGIN = timedelta(minutes=60)


def _t(value: str) -> datetime:
    return datetime.fromisoformat(value)


def ms(values: list[float], fmt: str = "{:.1f}") -> str:
    """mean ± std (or the single value)."""
    values = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not values:
        return "–"
    if len(values) == 1:
        return fmt.format(values[0])
    return f"{fmt.format(mean(values))} ± {fmt.format(stdev(values))}"


def pct_change(base: float, value: float) -> float:
    return 100 * (base - value) / base if base else float("nan")


# --------------------------------------------------------------------------- loading
def load(run: Path) -> tuple[dict, dict[int, dict[str, dict]], dict[int, dict[str, pd.DataFrame]]]:
    config = json.loads((run / "config.json").read_text(encoding="utf-8"))
    results: dict[int, dict[str, dict]] = defaultdict(dict)
    series: dict[int, dict[str, pd.DataFrame]] = defaultdict(dict)
    for path in sorted((run / "raw").glob("seed*_*.json*")):
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as handle:
            data = json.load(handle)
        results[data["seed"]][data["scenario"]] = data
        frame = pd.read_csv(run / "raw" / f"seed{data['seed']}_{data['scenario']}_series.csv")
        frame["time"] = pd.to_datetime(frame["time"])
        series[data["seed"]][data["scenario"]] = frame
    if not results:
        raise SystemExit(f"No raw results in {run / 'raw'}")
    return config, dict(results), dict(series)


# --------------------------------------------------------------------------- analyses
def isolation_intervals(actions: list[dict]) -> dict[tuple[str, str], list[list[datetime | None]]]:
    """(zone, sensor) -> [[isolated_at, restored_at or None], ...] from executed actions."""
    intervals: dict[tuple[str, str], list[list]] = defaultdict(list)
    for a in sorted(
        (a for a in actions if a["status"] == "executed" and a["action_type"] in ("isolate_sensor", "restore_sensor")),
        key=lambda a: a["created_at"],
    ):
        key, at = (a["zone_id"], a["parameters"]["sensor"]), _t(a["created_at"])
        if a["action_type"] == "isolate_sensor":
            intervals[key].append([at, None])
        elif intervals[key] and intervals[key][-1][1] is None:
            intervals[key][-1][1] = at
    return intervals


def zone_clear_after(intervals, zone: str, after: datetime) -> datetime | None:
    """First time >= ``after`` at which no sensor of ``zone`` is isolated (None = never within the run)."""
    spans = [s for (z, _), lst in intervals.items() if z == zone for s in lst]
    open_end = [s for s in spans if s[0] <= after and (s[1] is None or s[1] > after)]
    if not open_end:
        return after
    if any(s[1] is None for s in open_end):
        return None
    candidate = max(s[1] for s in open_end)
    # isolations that started before the candidate and are still open then extend it
    while True:
        extending = [s for s in spans if s[0] <= candidate and (s[1] is None or s[1] > candidate)]
        if not extending:
            return candidate
        if any(s[1] is None for s in extending):
            return None
        candidate = max(s[1] for s in extending)


def full_recovery(result: dict) -> datetime | None:
    """When every isolation in the defended run has been restored after the last attack.

    None if any isolation is still open at the end of the run (not recovered).
    """
    last_end = max(_t(a["end"]) for a in result["attacks"])
    spans = [s for lst in isolation_intervals(result["actions"]).values() for s in lst]
    if any(s[1] is None for s in spans):
        return None
    return max([last_end, *(s[1] for s in spans if s[1] > last_end)])


def energy_table(results) -> list[dict[str, Any]]:
    rows = []
    for name in ORDER:
        per_seed = [r[name] for r in results.values() if name in r]
        if not per_seed:
            continue
        base = [results[s]["baseline"] for s in results if name in results[s]]
        rows.append(
            {
                "scenario": name,
                "seeds": len(per_seed),
                "energy_kwh": [r["energy_kwh"] for r in per_seed],
                "energy_saving_pct": [pct_change(b["energy_kwh"], r["energy_kwh"]) for b, r in zip(base, per_seed)],
                "carbon_kg": [r["carbon_kg"] for r in per_seed],
                "carbon_reduction_pct": [pct_change(b["carbon_kg"], r["carbon_kg"]) for b, r in zip(base, per_seed)],
                "peak_kw": [r["peak_power_kw"] for r in per_seed],
                "peak_reduction_pct": [
                    pct_change(b["peak_power_kw"], r["peak_power_kw"]) for b, r in zip(base, per_seed)
                ],
                "comfort_pct": [
                    100 * r["comfort"]["in_band"] / max(r["comfort"]["occupied_readings"], 1) for r in per_seed
                ],
                "discomfort_degree_hours": [r["comfort"]["discomfort_degree_hours"] for r in per_seed],
            }
        )
    return rows


def window_energy(frame: pd.DataFrame, start: datetime, end: datetime | None = None) -> tuple[float, float, float]:
    mask = frame["time"] > pd.Timestamp(start)
    if end is not None:
        mask &= frame["time"] <= pd.Timestamp(end)
    part = frame[mask]
    return float(part["energy_kwh"].sum()), float(part["carbon_kg"].sum()), float(part["power_kw"].max())


def phase_table(results, series) -> dict[str, Any]:
    phases = {"attack": defaultdict(list), "post": defaultdict(list)}
    meta = []
    for seed, scenarios in results.items():
        defended = scenarios.get("attack_defended")
        if defended is None:
            continue
        first_start = min(_t(a["start"]) for a in defended["attacks"])
        last_end = max(_t(a["end"]) for a in defended["attacks"])
        recovered = full_recovery(defended)
        meta.append(
            {
                "seed": seed,
                "attack_window": [first_start.isoformat(), last_end.isoformat()],
                "full_recovery": recovered.isoformat() if recovered else None,
                "post_window_hours": (
                    (series[seed]["baseline"]["time"].max() - pd.Timestamp(recovered)).total_seconds() / 3600
                    if recovered
                    else None
                ),
            }
        )
        for name in ORDER:
            if name not in series[seed]:
                continue
            phases["attack"][name].append(window_energy(series[seed][name], first_start, last_end))
            if recovered is not None:
                phases["post"][name].append(window_energy(series[seed][name], recovered))
    return {"phases": phases, "meta": meta}


def prediction_table(results) -> list[dict[str, Any]]:
    rows = []
    for name in ORDER:
        clean_err, attacked_err, actual = [], [], []
        for scenarios in results.values():
            if name not in scenarios:
                continue
            for _, _, predicted, real, attacked in scenarios[name]["predictions"]:
                (attacked_err if attacked else clean_err).append(predicted - real)
                actual.append(real)
        if not actual:
            continue

        def stats(errors):
            if not errors:
                return None, None, 0
            e = np.array(errors)
            return float(np.abs(e).mean()), float(np.sqrt((e**2).mean())), len(e)

        all_errors = clean_err + attacked_err
        mae, rmse, n = stats(all_errors)
        rows.append(
            {
                "scenario": name,
                "n": n,
                "mae_kw": mae,
                "rmse_kw": rmse,
                "mean_actual_kw": float(np.mean(actual)),
                "clean": stats(clean_err),
                "attacked_input": stats(attacked_err),
            }
        )
    return rows


def detection(result: dict, monitor: str, warmup_end: datetime) -> dict[str, Any]:
    attacks = result["attacks"]
    ends = {(a["zone_id"], _t(a["end"])) for a in attacks}
    counts = {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
    per_type: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    first: dict[int, datetime] = {}
    for when, zone, label, index, kind in result["detections"][monitor]:
        when = _t(when)
        if when < warmup_end:
            continue
        flagged = kind is not None
        if label == "gradual_manipulation":
            continue
        if label is not None:
            counts["tp" if flagged else "fn"] += 1
            per_type[label][0] += flagged
            per_type[label][1] += 1
            if flagged:
                first.setdefault(index, when)
        elif any(z == zone and end <= when < end + timedelta(minutes=5) for z, end in ends):
            continue
        else:
            counts["fp" if flagged else "tn"] += 1
    alerts = [a for a in result["alerts"] if a["monitor"] == monitor]
    latency, false_alerts = {}, 0
    for a in attacks:
        start, end = _t(a["start"]), _t(a["end"])
        if a["attack_type"] == "gradual_manipulation":
            hits = [
                _t(x["detected_at"])
                for x in alerts
                if x["zone_id"] == a["zone_id"]
                and x["classification"] == "gradual_manipulation"
                and start <= _t(x["detected_at"]) < end
            ]
            hit = min(hits) if hits else None
        else:
            hit = first.get(a["index"])
        latency[a["index"]] = (hit - start).total_seconds() / 60 if hit else None
    for x in alerts:
        when = _t(x["detected_at"])
        if when < warmup_end or x["classification"] == "data_drift":
            continue
        if not any(
            x["zone_id"] == a["zone_id"] and _t(a["start"]) <= when < _t(a["end"]) + timedelta(minutes=30)
            for a in attacks
        ):
            false_alerts += 1
    tp, fp, fn, tn = counts["tp"], counts["fp"], counts["fn"], counts["tn"]
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0.0,
        "fpr": fp / (fp + tn) if fp + tn else 0.0,
        **counts,
        "per_type_recall": {k: v[0] / v[1] for k, v in per_type.items() if v[1]},
        "latency_min": latency,
        "false_security_alerts": false_alerts,
        "hours_evaluated": (_t(result["start"]) + timedelta(days=result["days"]) - warmup_end).total_seconds() / 3600,
    }


def corrective(result: dict) -> list[dict[str, Any]]:
    intervals = isolation_intervals(result["actions"])
    rows = []
    for a in result["attacks"]:
        start, end, zone = _t(a["start"]), _t(a["end"]), a["zone_id"]
        isolations = sorted(
            s[0] for (z, _), lst in intervals.items() if z == zone for s in lst if start <= s[0] <= end + CONTAIN_MARGIN
        )
        contained = isolations[0] if isolations else None
        clear = zone_clear_after(intervals, zone, end) if contained else None
        rows.append(
            {
                "index": a["index"],
                "attack_type": a["attack_type"],
                "zone_id": zone,
                "containment_min": (contained - start).total_seconds() / 60 if contained else None,
                "recovery_min": (clear - end).total_seconds() / 60 if clear else None,
                "recovered": bool(clear) if contained else None,
                "sensors": sorted(
                    {
                        s
                        for (z, s), lst in intervals.items()
                        if z == zone
                        for i in lst
                        if start <= i[0] <= end + CONTAIN_MARGIN
                    }
                ),
            }
        )
    return rows


def false_isolations(result: dict) -> int:
    count = 0
    for (zone, _), spans in isolation_intervals(result["actions"]).items():
        for at, _ in spans:
            if not any(
                a["zone_id"] == zone and _t(a["start"]) <= at <= _t(a["end"]) + CONTAIN_MARGIN
                for a in result["attacks"]
            ):
                count += 1
    return count


def latency_table(results) -> list[dict[str, Any]]:
    pooled: dict[str, list[float]] = defaultdict(list)
    for scenarios in results.values():
        for r in scenarios.values():
            for key, values in r["latency_ms"].items():
                pooled[key] += values
    rows = []
    for key in sorted(pooled):
        v = np.array(pooled[key])
        rows.append(
            {
                "step": key,
                "n": len(v),
                "mean": float(v.mean()),
                "p50": float(np.percentile(v, 50)),
                "p95": float(np.percentile(v, 95)),
                "p99": float(np.percentile(v, 99)),
                "max": float(v.max()),
            }
        )
    return rows


# --------------------------------------------------------------------------- figures
def figures(run: Path, energy_rows, phase, preds, det, latency, results, series, config) -> list[str]:
    out = run / "figures"
    out.mkdir(exist_ok=True)
    names = []

    def save(fig, name):
        fig.tight_layout()
        fig.savefig(out / name, dpi=110)
        plt.close(fig)
        names.append(name)

    # 1. energy, carbon, peak with std error bars
    fig, axes = plt.subplots(1, 3, figsize=(13, 4))
    for ax, key, title in zip(
        axes,
        ("energy_kwh", "carbon_kg", "peak_kw"),
        ("Energy (kWh)", "Carbon (kg CO₂e, synthetic intensity)", "Peak demand (kW)"),
    ):
        values = [row[key] for row in energy_rows]
        ax.bar(
            [LABELS[r["scenario"]] for r in energy_rows],
            [np.mean(v) for v in values],
            yerr=[np.std(v, ddof=1) if len(v) > 1 else 0 for v in values],
            capsize=4,
            color=[COLORS[r["scenario"]] for r in energy_rows],
        )
        ax.set_title(title)
        ax.tick_params(axis="x", rotation=25, labelsize=8)
    save(fig, "energy_carbon_peak.png")

    # 2. savings vs baseline
    fig, ax = plt.subplots(figsize=(9, 4))
    others = [r for r in energy_rows if r["scenario"] != "baseline"]
    width, x = 0.25, np.arange(len(others))
    for i, (key, label) in enumerate(
        (
            ("energy_saving_pct", "energy saving"),
            ("carbon_reduction_pct", "carbon reduction"),
            ("peak_reduction_pct", "peak reduction"),
        )
    ):
        vals = [r[key] for r in others]
        ax.bar(
            x + (i - 1) * width,
            [np.mean(v) for v in vals],
            width,
            yerr=[np.std(v, ddof=1) if len(v) > 1 else 0 for v in vals],
            capsize=3,
            label=label,
        )
    ax.set_xticks(x, [LABELS[r["scenario"]] for r in others])
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_ylabel("% vs. baseline")
    ax.legend()
    save(fig, "savings_vs_baseline.png")

    # 3. power time series of the first seed with attack windows and recovery
    seed = sorted(series)[0]
    fig, ax = plt.subplots(figsize=(13, 4.5))
    for name in ORDER:
        if name in series[seed]:
            f = series[seed][name]
            ax.plot(
                f["time"],
                f["power_kw"].rolling(3, min_periods=1).mean(),
                label=LABELS[name],
                color=COLORS[name],
                linewidth=1,
            )
    key = []
    if "attack_defended" in results[seed]:
        top = ax.get_ylim()[1]
        for i, a in enumerate(results[seed]["attack_defended"]["attacks"], 1):
            ax.axvspan(_t(a["start"]), _t(a["end"]), color="#dc3545", alpha=0.08)
            ax.text(
                _t(a["start"]),
                top * (0.98 - 0.05 * ((i - 1) % 3)),
                f"A{i}",
                fontsize=8,
                va="top",
                color="#dc3545",
                fontweight="bold",
            )
            key.append(f"A{i} {a['attack_type'].replace('_', ' ')} {a['zone_id']}")
        recovered = full_recovery(results[seed]["attack_defended"])
        if recovered:
            ax.axvline(recovered, color="#0d6efd", linestyle="--", linewidth=1)
            ax.text(recovered, ax.get_ylim()[0], " full recovery", color="#0d6efd", fontsize=8, va="bottom")
    ax.set_ylabel("building power (kW, 15-min mean)")
    ax.set_title(f"Seed {seed}: building power (true) — red bands = attacks: " + "; ".join(key), fontsize=9)
    ax.legend(fontsize=8, ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.08), frameon=False)
    save(fig, "power_timeline.png")

    # 4. prediction accuracy
    fig, ax = plt.subplots(figsize=(9, 4))
    x = np.arange(len(preds))
    ax.bar(x - 0.2, [p["clean"][0] or 0 for p in preds], 0.4, label="clean input")
    ax.bar(x + 0.2, [p["attacked_input"][0] or 0 for p in preds], 0.4, label="manipulated input")
    ax.set_xticks(x, [LABELS[p["scenario"]] for p in preds], rotation=15)
    ax.set_ylabel("MAE (kW) vs. true power")
    ax.set_title("ML next-interval prediction error")
    ax.legend()
    save(fig, "prediction_accuracy.png")

    # 5. detection metrics
    if det:
        fig, axes = plt.subplots(1, 2, figsize=(13, 4.8))
        keys = list(det)
        x = np.arange(len(keys))
        for i, metric in enumerate(("precision", "recall", "f1")):
            vals = [[d[metric] for d in det[k]] for k in keys]
            axes[0].bar(
                x + (i - 1) * 0.25,
                [np.mean(v) for v in vals],
                0.25,
                yerr=[np.std(v, ddof=1) if len(v) > 1 else 0 for v in vals],
                capsize=3,
                label=metric,
            )
        axes[0].set_xticks(x, keys, fontsize=8)
        axes[0].set_ylim(0, 1.05)
        axes[0].set_title("Point detection (after warm-up)")
        for i, k in enumerate(keys):
            types = sorted({t for d in det[k] for t in d["per_type_recall"]})
            axes[1].bar(
                np.arange(len(types)) + (i - (len(keys) - 1) / 2) * 0.25,
                [np.mean([d["per_type_recall"].get(t, 0) for d in det[k]]) for t in types],
                0.25,
                label=k,
            )
            axes[1].set_xticks(np.arange(len(types)), [t.replace("_", "\n") for t in types], fontsize=8)
        axes[1].set_ylim(0, 1.05)
        axes[1].set_title("Recall per attack type")
        axes[0].legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3, frameon=False)
        axes[1].legend(fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=3, frameon=False)
        save(fig, "detection.png")

    # 6. latency
    fig, ax = plt.subplots(figsize=(10, 4))
    data = []
    for row in latency:
        pooled = []
        for scenarios in results.values():
            for r in scenarios.values():
                pooled += r["latency_ms"].get(row["step"], [])
        data.append(pooled)
    ax.boxplot(data, orientation="horizontal", showfliers=False)
    ax.set_yticks(range(1, len(latency) + 1), [r["step"] for r in latency], fontsize=8)
    ax.set_xscale("log")
    ax.set_xlabel("ms per call (log scale, whiskers 1.5 IQR)")
    ax.set_title("Processing latency per step (in-process, wall clock)")
    save(fig, "latency.png")
    return names


# --------------------------------------------------------------------------- report
def render(run: Path, config, results, series) -> tuple[str, dict]:
    ev = config["evaluation"]
    warmup_end = _t(ev["start"]) + timedelta(hours=ev["warmup_hours"])
    seeds = sorted(results)
    energy_rows = energy_table(results)
    phase = phase_table(results, series)
    preds = prediction_table(results)
    det: dict[str, list[dict]] = defaultdict(list)
    for seed in seeds:
        for name, monitor, label in (
            ("attack_undefended", "original", "Module 8 detectors (passive)"),
            ("attack_undefended", "retrained", "retrained detectors (passive)"),
            ("attack_defended", "retrained", "retrained detectors (live, with agent)"),
        ):
            if name in results[seed] and monitor in results[seed][name]["detections"]:
                det[label].append(detection(results[seed][name], monitor, warmup_end))
    corr = {seed: corrective(results[seed]["attack_defended"]) for seed in seeds if "attack_defended" in results[seed]}
    latency = latency_table(results)
    kafka_path = run / "streaming_latency.json"
    kafka = json.loads(kafka_path.read_text(encoding="utf-8")) if kafka_path.is_file() else None
    figs = figures(run, energy_rows, phase, preds, det, latency, results, series, config)

    L = [
        "# Module 11: experimental evaluation",
        "",
        f"Generated by `python -m experiments.evaluation_report` from the raw results in `{run.name}/raw/` "
        f"(produced by `{config['command']}`, {config['generated_at'][:19]} UTC, git "
        f"`{(config['environment']['git_commit'] or '?')[:9]}`). "
        "All values below are computed from those files; nothing is entered by hand.",
        "",
        "## Setup",
        "",
        f"* **Seeds:** {', '.join(map(str, seeds))} — each seed changes occupancy, weather and sensor noise; the four "
        "scenarios of a seed share them, so they differ only in control, attacks and defence.",
        f"* **Period:** {ev['days']:g} simulated days from {ev['start'][:10]}, 5-minute readings, 5 zones. "
        f"The first {ev['warmup_hours']:g} h are attack-free warm-up (alerts not investigated, excluded from "
        "detection metrics).",
        "* **Scenarios:** 1 baseline schedule control · 2 ML-based optimizer (SciPy SLSQP with the building model "
        f"`{config['versions']['ml_model']}` as forecast basis) · 3 optimizer + attacks, no defence · 4/5 optimizer + "
        "attacks + live detection, agent (offline rule-based policy — no LLM) with safety validator, sensor "
        "isolation and automatic recovery.",
        "* **Attacks** (minutes after start): " + "; ".join(f"`{a}`" for a in ev["attacks"]) + ".",
        f"* **Detectors:** Module 8 detectors `{config['versions']['original_detectors']}` (trained on "
        "baseline-controlled data) "
        f"and retrained detectors `{config['versions']['optimizer_detectors']}` (trained on "
        f"{ev['detector_training_days']:g} days of "
        f"clean optimizer-controlled simulation, seed {ev['detector_training_seed']}, from "
        f"{ev['detector_training_start'][:10]}).",
        "* **Carbon:** synthetic grid-intensity profile (`MockCarbonProvider`), identical in all scenarios.",
        "* Values are mean ± sample standard deviation over seeds. Full configuration: `config.json`."
        + "".join(
            f" Notes on how this run was produced: `{n}`."
            for n in ("PROVENANCE.md", "README.md")
            if (run / n).is_file()
        ),
        "",
        "## 1–2, 5. Energy, carbon, peak demand and comfort (whole period)",
        "",
        "| Scenario | Energy (kWh) | Saving vs. baseline | CO₂e (kg) | Reduction | Peak (kW) | Peak reduction | "
        "Comfort compliance | Discomfort (°C·h) |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in energy_rows:
        L.append(
            f"| {LABELS[r['scenario']]} | {ms(r['energy_kwh'])} | {ms(r['energy_saving_pct'], '{:.2f}')} % | "
            f"{ms(r['carbon_kg'])} | {ms(r['carbon_reduction_pct'], '{:.2f}')} % | {ms(r['peak_kw'])} | "
            f"{ms(r['peak_reduction_pct'], '{:.2f}')} % | {ms(r['comfort_pct'])} % | "
            f"{ms(r['discomfort_degree_hours'])} |"
        )
    L += [
        "",
        "Comfort compliance: occupied readings (server room excluded) with the true temperature in "
        f"{ev['comfort_band'][0]:g}–{ev['comfort_band'][1]:g} °C.",
        "",
    ]

    L += ["### Attack period and post-recovery period", ""]
    meta = phase["meta"]
    L += ["| Seed | Attack window | Full recovery (all sensors restored) | Post-recovery window |", "|---|---|---|---|"]
    for m in meta:
        recovered = m["full_recovery"][5:16] if m["full_recovery"] else "not recovered by end of run"
        window = f"{m['post_window_hours']:.1f} h" if m["post_window_hours"] else "–"
        L.append(
            f"| {m['seed']} | {m['attack_window'][0][5:16]} → {m['attack_window'][1][5:16]} | "
            f"{recovered} | {window} |"
        )
    for key, title in (("attack", "Attack window"), ("post", "Post-recovery window")):
        rows = phase["phases"][key]
        if not rows.get("baseline"):
            L += ["", f"{title}: no data (no seed recovered fully)."]
            continue
        L += [
            "",
            f"**{title}** (energy and saving vs. baseline in the same window):",
            "",
            "| Scenario | Energy (kWh) | Saving vs. baseline | CO₂e (kg) | Peak (kW) |",
            "|---|---|---|---|---|",
        ]
        for name in ORDER:
            if name not in rows:
                continue
            saving = [pct_change(b[0], v[0]) for b, v in zip(rows["baseline"], rows[name])]
            L.append(
                f"| {LABELS[name]} | {ms([v[0] for v in rows[name]])} | {ms(saving, '{:.2f}')} % | "
                f"{ms([v[1] for v in rows[name]])} | {ms([v[2] for v in rows[name]])} |"
            )
        if key == "post" and rows.get("optimized") and rows.get("attack_defended"):
            retention = [
                pct_change(b[0], d[0]) / pct_change(b[0], o[0]) * 100 if pct_change(b[0], o[0]) else float("nan")
                for b, o, d in zip(rows["baseline"], rows["optimized"], rows["attack_defended"])
            ]
            L += [
                "",
                f"Savings retention after recovery (defended saving ÷ optimized saving, same window): "
                f"**{ms(retention)} %**.",
            ]

    L += [
        "",
        "## Prediction accuracy (ML next-interval power, vs. the true power)",
        "",
        "| Scenario | Predictions | MAE (kW) | RMSE (kW) | Mean power (kW) | MAE, clean input | MAE, manipulated "
        "input (n) |",
        "|---|---|---|---|---|---|---|",
    ]
    for p in preds:
        attacked = f"{p['attacked_input'][0]:.3f} ({p['attacked_input'][2]})" if p["attacked_input"][2] else "–"
        L.append(
            f"| {LABELS[p['scenario']]} | {p['n']} | {p['mae_kw']:.3f} | {p['rmse_kw']:.3f} | "
            f"{p['mean_actual_kw']:.2f} | {p['clean'][0]:.3f} | {attacked} |"
        )
    L += [
        "",
        "Pooled over seeds and zones. 'Manipulated input' = the reading the model used was manipulated by an attack.",
        "",
    ]

    L += [
        "## 3–4. Attack detection",
        "",
        "| Detectors | Precision | Recall | F1 | FPR | TP | FP | FN | False security alerts per day |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for label, rows in det.items():
        per_day = [d["false_security_alerts"] / (d["hours_evaluated"] / 24) for d in rows]
        L.append(
            f"| {label} | {ms([d['precision'] for d in rows], '{:.3f}')} | "
            f"{ms([d['recall'] for d in rows], '{:.3f}')} | "
            f"{ms([d['f1'] for d in rows], '{:.3f}')} | {ms([100 * d['fpr'] for d in rows], '{:.2f}')} % | "
            f"{sum(d['tp'] for d in rows)} | {sum(d['fp'] for d in rows)} | {sum(d['fn'] for d in rows)} | "
            f"{ms(per_day)} |"
        )
    L += [
        "",
        "Per-reading metrics after the warm-up (gradual manipulation is scored by drift alerts below). "
        "False security alerts: attack/anomaly/security-drift alerts outside every attack window (+30 min); "
        "data-drift alerts are excluded.",
        "",
        "| Attack | Zone | " + " | ".join(f"Detection latency (min), {k}" for k in det) + " |",
        "|---|---|" + "---|" * len(det),
    ]
    attacks = (
        results[seeds[0]].get("attack_defended", results[seeds[0]].get("attack_undefended", {})).get("attacks", [])
    )
    for a in attacks:
        cells = []
        for label, rows in det.items():
            values = [d["latency_min"].get(a["index"], d["latency_min"].get(str(a["index"]))) for d in rows]
            detected = [v for v in values if v is not None]
            cells.append(f"{ms(detected, '{:.0f}')} ({len(detected)}/{len(values)} seeds)")
        L.append(f"| {a['attack_type']} | {a['zone_id']} | " + " | ".join(cells) + " |")

    if corr:
        L += [
            "",
            "## 4–5. Corrective action and recovery (attack_defended)",
            "",
            "| Attack | Zone | Contained (seeds) | Containment (min after start) | Recovery (min after attack end) | "
            "Sensors isolated |",
            "|---|---|---|---|---|---|",
        ]
        for i, a in enumerate(attacks):
            rows = [corr[s][i] for s in corr]
            contained = [r for r in rows if r["containment_min"] is not None]
            recovered = [r["recovery_min"] for r in rows if r["recovery_min"] is not None]
            sensors = sorted({s for r in rows for s in r["sensors"]})
            L.append(
                f"| {a['attack_type']} | {a['zone_id']} | {len(contained)}/{len(rows)} | "
                f"{ms([r['containment_min'] for r in contained], '{:.0f}')} | "
                f"{ms(recovered, '{:.0f}')} ({len(recovered)}/{len(contained)} recovered) | "
                f"{', '.join(sensors) or '–'} |"
            )
        defended = [results[s]["attack_defended"] for s in corr]
        L += [
            "",
            "| Seed | Agent incidents | Actions executed | Rejected by safety validator | Restores (recovery policy) | "
            "Isolations outside attack windows | Optimizer decisions skipped (zone isolated) |",
            "|---|---|---|---|---|---|---|",
        ]
        for seed, r in zip(corr, defended):
            executed = sum(a["status"] == "executed" for a in r["actions"])
            rejected = sum(a["status"] == "rejected" for a in r["actions"])
            restores = sum(a["action_type"] == "restore_sensor" and a["status"] == "executed" for a in r["actions"])
            L.append(
                f"| {seed} | {len(r['decisions'])} | {executed} | {rejected} | {restores} | {false_isolations(r)} | "
                f"{sum(r['optimizer']['paused_by_isolation'].values())} |"
            )

    L += [
        "",
        "## Processing latency",
        "",
        "In-process wall-clock time per step (all seeds and scenarios pooled):",
        "",
        "| Step | Calls | Mean (ms) | p50 | p95 | p99 | Max |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in latency:
        L.append(
            f"| {r['step']} | {r['n']} | {r['mean']:.2f} | {r['p50']:.2f} | {r['p95']:.2f} | {r['p99']:.2f} | "
            f"{r['max']:.1f} |"
        )
    env = config["environment"]
    L += [
        "",
        f"Machine: {env['platform']}, {env['cpu_count']} CPUs, Python {env['python']}; "
        f"{config.get('workers_note', 'scenarios ran in parallel processes, so timings include contention')}. "
        "`agent_incident` = one full LangGraph investigation (tools, safety gate, simulated execution).",
    ]
    if kafka:
        L += [
            "",
            "**Kafka transport** (`streaming_latency.json`, producer → consumer, real broker):",
            "",
            "| Rate (msg/s) | Messages | Mean (ms) | p50 | p95 | p99 | Max |",
            "|---|---|---|---|---|---|---|",
        ]
        for r in kafka["runs"]:
            s = r["latency_ms"]
            L.append(
                f"| {r['rate_per_s']} | {r['received']} | {s['mean']:.2f} | {s['p50']:.2f} | {s['p95']:.2f} | "
                f"{s['p99']:.2f} | {s['max']:.1f} |"
            )
    else:
        L += [
            "",
            "Kafka transport latency: not measured in this run (`python -m experiments.streaming_latency --out ...`).",
        ]

    L += ["", "## Figures", ""] + [f"![{f}](figures/{f})" for f in figs]
    L += [
        "",
        "## Limitations",
        "",
        "* Simulated building and simulated attacks; carbon intensity is synthetic.",
        "* The agent uses the deterministic offline policy (no LLM) so runs are reproducible; Claude is not "
        "evaluated here.",
        f"* {len(seeds)} seeds of {ev['days']:g} days each; standard deviations over so few seeds are rough.",
        "* Latencies are in-process on one machine; they exclude Kafka and database round trips except in the Kafka "
        "table.",
        "",
    ]
    summary = {
        "energy": energy_rows,
        "phases": {k: {n: v for n, v in d.items()} for k, d in phase["phases"].items()},
        "phase_meta": phase["meta"],
        "prediction": preds,
        "detection": det,
        "corrective": {str(k): v for k, v in corr.items()},
        "latency": latency,
        "kafka": kafka,
    }
    return "\n".join(L), summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m experiments.evaluation_report", description=__doc__.splitlines()[0]
    )
    parser.add_argument("--run", type=Path, required=True, help="directory written by experiments.evaluation")
    args = parser.parse_args(argv)
    config, results, series = load(args.run)
    report, summary = render(args.run, config, results, series)
    (args.run / "report.md").write_text(report, encoding="utf-8")
    (args.run / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
