"""Data-drift analysis of the KAG dataset (real data).

    python -m experiments.kag_drift_analysis --report docs/results/kag_drift_analysis.md

Question: do the KAG input distributions change between the training period
(January - April) and the validation/test periods (April - May), and does
this explain why the tree models lost accuracy on the test period (Module 6)?

Steps (same chronological split as the Module 6 model):
    1. KS test and PSI per input feature: train vs. validation, train vs. test.
    2. Share of test values *outside* the training range per feature: tree
       models cannot extrapolate beyond the values they were trained on.
    3. Daily drift timeline (mean PSI of the indoor temperatures vs. the
       training distribution) next to the daily MAE of the saved XGBoost model
       and of a Linear Regression baseline (refitted on the training period).
All numbers are computed from the data.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy.stats import ks_2samp, spearmanr  # noqa: E402

from config import BASE_DIR, get_config  # noqa: E402
from ml.dataset import DEFAULT_KAG_PATH, load_kag  # noqa: E402
from ml.features import KAG_SENSORS, KAG_SPEC, build_features  # noqa: E402
from ml.models import fit_model, make_models, predict  # noqa: E402
from ml.registry import ModelNotFoundError, load_bundle  # noqa: E402
from ml.train import chronological_split  # noqa: E402
from security.drift_detector import population_stability_index  # noqa: E402
from utils.logging_config import configure_logging  # noqa: E402

INDOOR_TEMPERATURES = ("T1", "T2", "T3", "T4", "T5", "T7", "T8", "T9")
FIGURE_DIR = BASE_DIR / "docs" / "results" / "figures"
PSI_MODERATE, PSI_SIGNIFICANT = 0.1, 0.25


def feature_drift(train: pd.DataFrame, other: pd.DataFrame, columns) -> pd.DataFrame:
    rows = []
    for column in columns:
        a, b = train[column].to_numpy(float), other[column].to_numpy(float)
        ks = ks_2samp(a, b)
        outside = np.mean((b < a.min()) | (b > a.max()))
        rows.append({"feature": column, "train_mean": a.mean(), "other_mean": b.mean(),
                     "ks_statistic": ks.statistic, "ks_p_value": ks.pvalue,
                     "psi": population_stability_index(a, b), "outside_train_range": outside})
    return pd.DataFrame(rows).sort_values("psi", ascending=False).reset_index(drop=True)


def psi_level(psi: float) -> str:
    return "significant" if psi >= PSI_SIGNIFICANT else "moderate" if psi >= PSI_MODERATE else "stable"


def analyse(data_path: Path) -> dict:
    df, info = load_kag(data_path)
    frame = build_features(df, KAG_SPEC)
    split = chronological_split(frame)
    times = frame.timestamps
    periods = {
        "train": df[df["timestamp"] < split.validation_start],
        "validation": df[(df["timestamp"] >= split.validation_start) & (df["timestamp"] < split.test_start)],
        "test": df[df["timestamp"] >= split.test_start],
    }
    columns = (*KAG_SENSORS, "Appliances")
    drift_val = feature_drift(periods["train"], periods["validation"], columns)
    drift_test = feature_drift(periods["train"], periods["test"], columns)

    # Model errors per day on validation + test.
    try:
        xgb = load_bundle(KAG_SPEC.name)
        xgb_model, xgb_version = xgb.model, xgb.version
    except ModelNotFoundError:
        xgb_model, xgb_version = None, None
    X, y = frame.X, frame.y
    linear = make_models()["linear_regression"]
    fit_model("linear_regression", linear, X[split.train], y[split.train], None, None)
    evaluation = (split.validation | split.test)
    days = times[evaluation].dt.floor("D")
    errors = pd.DataFrame({"day": days.to_numpy(), "y": y[evaluation].to_numpy(),
                           "linear": predict(linear, X[evaluation])})
    if xgb_model is not None:
        errors["xgboost"] = predict(xgb_model, X[evaluation])

    # Daily drift timeline: mean PSI of indoor temperatures vs. training distribution.
    train = periods["train"]
    daily = []
    for day, group in df[df["timestamp"] >= split.validation_start].groupby(df["timestamp"].dt.floor("D")):
        if len(group) < 100:
            continue
        psis = [population_stability_index(train[c].to_numpy(float), group[c].to_numpy(float))
                for c in INDOOR_TEMPERATURES]
        outside = np.mean([np.mean((group[c] > train[c].max()) | (group[c] < train[c].min()))
                           for c in INDOOR_TEMPERATURES])
        row = {"day": day, "mean_psi_indoor_temp": float(np.mean(psis)), "outside_range": float(outside),
               "mean_indoor_temp": float(group[list(INDOOR_TEMPERATURES)].to_numpy().mean())}
        e = errors[errors["day"] == day]
        if len(e):
            row["mae_linear"] = float(np.mean(np.abs(e["y"] - e["linear"])))
            if "xgboost" in e:
                row["mae_xgboost"] = float(np.mean(np.abs(e["y"] - e["xgboost"])))
        daily.append(row)
    daily = pd.DataFrame(daily)
    correlations = {}
    if "mae_xgboost" in daily:
        gap = daily["mae_xgboost"] - daily["mae_linear"]
        correlations["psi_vs_xgb_minus_linear_mae"] = spearmanr(daily["mean_psi_indoor_temp"], gap)
        correlations["outside_range_vs_xgb_minus_linear_mae"] = spearmanr(daily["outside_range"], gap)
    return {"info": info, "split": split, "periods": periods, "drift_val": drift_val,
            "drift_test": drift_test, "daily": daily, "correlations": correlations,
            "xgb_version": xgb_version}


def make_figures(result: dict) -> list[Path]:
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)
    periods, daily = result["periods"], result["daily"]
    paths = []

    fig, axes = plt.subplots(1, 3, figsize=(15, 4))
    for ax, column in zip(axes, ("T2", "T_out", "RH_1")):
        for name, color in (("train", "tab:blue"), ("test", "tab:red")):
            ax.hist(periods[name][column], bins=40, density=True, alpha=0.55, color=color,
                    label=f"{name} ({periods[name]['timestamp'].min():%b %d}-{periods[name]['timestamp'].max():%b %d})")
        ax.set_title(f"{column}: train vs test distribution")
        ax.legend(fontsize=8)
    fig.tight_layout()
    paths.append(FIGURE_DIR / "kag_drift_distributions.png")
    fig.savefig(paths[-1], dpi=110)
    plt.close(fig)

    fig, ax1 = plt.subplots(figsize=(12, 4.5))
    ax1.bar(daily["day"], daily["mean_psi_indoor_temp"], color="tab:orange", alpha=0.6,
            label="mean PSI of indoor temperatures (vs training)")
    ax1.axhline(PSI_SIGNIFICANT, color="tab:orange", linestyle="--", linewidth=1, label="PSI = 0.25 (significant)")
    ax1.axvline(result["split"].test_start, color="grey", linestyle=":", label="start of test period")
    ax1.set_ylabel("PSI")
    ax2 = ax1.twinx()
    if "mae_xgboost" in daily:
        ax2.plot(daily["day"], daily["mae_xgboost"], color="tab:red", marker="o", label="XGBoost daily MAE")
    ax2.plot(daily["day"], daily["mae_linear"], color="tab:blue", marker="s", label="Linear Regression daily MAE")
    ax2.set_ylabel("MAE (Wh per 10 min)")
    lines = ax1.get_legend_handles_labels()
    lines2 = ax2.get_legend_handles_labels()
    ax1.legend(lines[0] + lines2[0], lines[1] + lines2[1], fontsize=8, loc="upper left")
    ax1.set_title("KAG: daily input drift and model error (validation + test periods)")
    fig.autofmt_xdate()
    fig.tight_layout()
    paths.append(FIGURE_DIR / "kag_drift_timeline.png")
    fig.savefig(paths[-1], dpi=110)
    plt.close(fig)
    return paths


def render(result: dict, figures: list[Path]) -> str:
    info, split = result["info"], result["split"]

    def table(df: pd.DataFrame, top: int = 12) -> list[str]:
        lines = ["| Feature | Train mean | Other mean | KS D | KS p | PSI | Level | Outside train range |",
                 "|---|---|---|---|---|---|---|---|"]
        for _, r in df.head(top).iterrows():
            lines.append(f"| {r.feature} | {r.train_mean:.2f} | {r.other_mean:.2f} | {r.ks_statistic:.3f} | "
                         f"{r.ks_p_value:.1e} | {r.psi:.3f} | {psi_level(r.psi)} | {r.outside_train_range:.1%} |")
        return lines

    test = result["drift_test"]
    indoor = test[test.feature.isin(INDOOR_TEMPERATURES)]
    daily = result["daily"]
    lines = [
        "# KAG dataset: data-drift analysis", "",
        f"Generated by `python -m experiments.kag_drift_analysis` on {datetime.now(timezone.utc).isoformat()}. "
        "All values are computed from the data.", "",
        f"* Dataset: `{Path(info.source).name}` ({info.rows} rows, SHA-256 `{info.sha256}`).",
        f"* Periods (same chronological split as the Module 6 model): train until {split.validation_start:%Y-%m-%d %H:%M}, "
        f"validation until {split.test_start:%Y-%m-%d %H:%M}, test afterwards.",
        f"* PSI levels: < {PSI_MODERATE} stable, {PSI_MODERATE}-{PSI_SIGNIFICANT} moderate, >= {PSI_SIGNIFICANT} significant.", "",
        "## Summary", "",
        f"* Test vs. train: {int((test.psi >= PSI_SIGNIFICANT).sum())} of {len(test)} features drift significantly "
        f"(PSI >= 0.25); {int((test.ks_p_value < 0.01).sum())} differ by the KS test (p < 0.01).",
        f"* Indoor temperatures: mean PSI {indoor.psi.mean():.2f}; on average {indoor.outside_train_range.mean():.1%} of test "
        "values lie outside the range seen in training (tree models predict as if they were at the edge of the range).",
        f"* Target `Appliances`: PSI {test.loc[test.feature == 'Appliances', 'psi'].iloc[0]:.3f} "
        f"({psi_level(test.loc[test.feature == 'Appliances', 'psi'].iloc[0])}).",
    ]
    for name, corr in result["correlations"].items():
        lines.append(f"* Spearman correlation, {name.replace('_', ' ')} (daily, n={len(daily)}): "
                     f"rho = {corr.statistic:.2f}, p = {corr.pvalue:.3f}.")
    lines += ["", "## Train vs. test (top 12 by PSI)", "", *table(test), "",
              "## Train vs. validation (top 12 by PSI)", "", *table(result["drift_val"]), "",
              "## Daily timeline", "",
              "Note: one day of readings is much narrower than four months of training data, so daily PSI is "
              "high even without a seasonal change (it also measures the concentration of a single day). Read "
              "the *trend* and the share of values outside the training range, not the absolute PSI level.", "",
              "| Day | Mean PSI indoor temp. | Outside train range | Mean indoor temp. (°C) | MAE XGBoost | MAE Linear |",
              "|---|---|---|---|---|---|"]
    for _, r in daily.iterrows():
        lines.append(f"| {r.day:%Y-%m-%d} | {r.mean_psi_indoor_temp:.2f} | {r.outside_range:.1%} | {r.mean_indoor_temp:.2f} | "
                     f"{r.get('mae_xgboost', float('nan')):.1f} | {r.get('mae_linear', float('nan')):.1f} |")
    lines += ["", "## Figures", ""] + [f"![{p.stem}](figures/{p.name})" for p in figures] + [""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m experiments.kag_drift_analysis", description=__doc__.splitlines()[0])
    parser.add_argument("--data", type=Path, default=DEFAULT_KAG_PATH)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args(argv)
    configure_logging(get_config().LOGGING, "kag_drift", stream=sys.stderr)
    result = analyse(args.data)
    report = render(result, make_figures(result))
    print(report)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
