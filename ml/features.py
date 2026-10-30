"""Leakage-safe feature engineering for next-interval energy prediction.

Task: at time *t* predict the target ``horizon`` steps ahead (*t + h*), using
only information available at *t*:

* current and lagged target values (``target_lag0`` = value at *t*),
* rolling mean/std of the target over windows ending at *t*,
* sensor readings at *t*,
* calendar features of the *target* time (known in advance),
* one-hot group indicators (zones) for multi-zone models.

Lags and windows are only valid when the readings are evenly spaced; across
gaps (missing data, a new simulator session) they are set to NaN and such
rows are dropped. The same function computes features offline (training) and
online (prediction service), which guarantees identical features.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np
import pandas as pd

KAG_SENSORS = ("lights", *[f"T{i}" for i in range(1, 10)], *[f"RH_{i}" for i in range(1, 10)],
               "T_out", "Press_mm_hg", "RH_out", "Windspeed", "Visibility", "Tdewpoint")
BUILDING_SENSORS = ("temperature_c", "humidity_pct", "co2_ppm", "occupancy_count",
                    "lighting_level_pct", "hvac_setpoint_c", "hvac_power_kw", "lighting_power_kw",
                    "appliance_power_kw", "outdoor_temperature_c", "outdoor_humidity_pct",
                    "hvac_heating", "hvac_cooling", "hvac_off")


@dataclass(frozen=True)
class FeatureSpec:
    """Describes how features are built; stored inside every model bundle."""

    name: str
    target: str
    target_unit: str
    sensor_columns: tuple[str, ...]
    lags: tuple[int, ...] = (1, 2, 3, 6)
    rolling_windows: tuple[int, ...] = (3, 6)
    horizon: int = 1
    group_values: tuple[str, ...] = ()       # one-hot encoded groups (zones)
    utc_offset_hours: float = 0.0            # local time for calendar features (tz-aware data)

    @property
    def history_required(self) -> int:
        """Rows of history (including the current one) needed for one feature row."""
        return max([*self.lags, *(w - 1 for w in self.rolling_windows), 0]) + 1

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FeatureSpec":
        return cls(**{k: tuple(v) if isinstance(v, list) else v for k, v in data.items()})


KAG_SPEC = FeatureSpec(
    name="kag_appliances", target="Appliances", target_unit="Wh per 10 min",
    sensor_columns=KAG_SENSORS, lags=(1, 2, 3, 6, 144), rolling_windows=(3, 6, 36))

BUILDING_SPEC = FeatureSpec(
    name="building_zones", target="avg_power_kw", target_unit="kW (average over interval)",
    sensor_columns=BUILDING_SENSORS, lags=(1, 2, 3, 6, 12), rolling_windows=(3, 6, 12),
    group_values=("Z1", "Z2", "Z3", "Z4", "Z5"))


@dataclass
class FeatureFrame:
    X: pd.DataFrame
    y: pd.Series | None
    timestamps: pd.Series        # time t of each row
    target_times: pd.Series      # time t + h being predicted
    groups: pd.Series
    current: pd.Series           # target value at t (for the persistence baseline)
    dropped_rows: int = 0
    feature_names: list[str] = field(default_factory=list)


def _expected_steps(group: pd.DataFrame) -> pd.Series:
    """Time between consecutive readings: ``interval_seconds`` if present, else the median gap."""
    if "interval_seconds" in group.columns:
        return pd.to_timedelta(group["interval_seconds"], unit="s")
    median = group["timestamp"].diff().median()
    if pd.isna(median):
        median = pd.Timedelta(0)
    return pd.Series(median, index=group.index)


def _evenly_spaced(timestamps: pd.Series, steps: pd.Series, k: int) -> pd.Series:
    """True where the reading ``k`` rows away is exactly ``k`` steps away in time."""
    if k == 0:
        return pd.Series(True, index=timestamps.index)
    # Compare in float seconds; NaN (no neighbour) compares as False.
    if k > 0:
        actual_s = (timestamps - timestamps.shift(k)).dt.total_seconds()
    else:
        actual_s = (timestamps.shift(k) - timestamps).dt.total_seconds()
    expected_s = steps.dt.total_seconds() * abs(k)
    return (actual_s - expected_s).abs() <= expected_s * 0.01 + 1.0


def _group_features(group: pd.DataFrame, spec: FeatureSpec) -> pd.DataFrame:
    ts, target = group["timestamp"], group[spec.target].astype(float)
    steps = _expected_steps(group)
    out = pd.DataFrame(index=group.index)
    out["target_lag0"] = target
    for k in spec.lags:
        out[f"target_lag{k}"] = target.shift(k).where(_evenly_spaced(ts, steps, k))
    out["target_diff1"] = out["target_lag0"] - target.shift(1).where(_evenly_spaced(ts, steps, 1))
    for w in spec.rolling_windows:
        valid = _evenly_spaced(ts, steps, w - 1)
        rolling = target.rolling(w, min_periods=w)
        out[f"target_roll_mean{w}"] = rolling.mean().where(valid)
        out[f"target_roll_std{w}"] = rolling.std(ddof=0).where(valid)
    for column in spec.sensor_columns:
        out[column] = group[column].astype(float)

    target_time = ts + pd.to_timedelta(steps.dt.total_seconds() * spec.horizon, unit="s")
    local = target_time
    if getattr(target_time.dt, "tz", None) is not None:
        local = (target_time.dt.tz_convert("UTC").dt.tz_localize(None)
                 + pd.Timedelta(seconds=round(spec.utc_offset_hours * 3600)))
    minutes = local.dt.hour * 60 + local.dt.minute
    out["tgt_hour"] = local.dt.hour.astype(float)
    out["tgt_dayofweek"] = local.dt.day_of_week.astype(float)
    out["tgt_weekend"] = (local.dt.day_of_week >= 5).astype(float)
    out["tgt_tod_sin"] = np.sin(2 * np.pi * minutes / 1440)
    out["tgt_tod_cos"] = np.cos(2 * np.pi * minutes / 1440)
    for value in spec.group_values:
        out[f"group_{value}"] = float(group["group"].iloc[0] == value) if len(group) else 0.0

    out["_y"] = target.shift(-spec.horizon).where(_evenly_spaced(ts, steps, -spec.horizon))
    out["_timestamp"], out["_target_time"], out["_group"] = ts, target_time, group["group"]
    return out


def build_features(df: pd.DataFrame, spec: FeatureSpec, *, require_target: bool = True) -> FeatureFrame:
    """Build the feature matrix for every row with complete features.

    Args:
        df: standardised frame (columns ``timestamp``, ``group``, target, sensors).
        require_target: drop rows whose future target is unknown (training);
            ``False`` keeps them (online prediction of the latest row).
    """
    missing = [c for c in ("timestamp", "group", spec.target, *spec.sensor_columns) if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for feature spec '{spec.name}': {missing}")
    parts = [_group_features(g.sort_values("timestamp"), spec)
             for _, g in df.groupby("group", sort=True)]
    full = pd.concat(parts) if parts else pd.DataFrame()
    feature_names = [c for c in full.columns if not c.startswith("_")]
    mask = full[feature_names].notna().all(axis=1)
    if require_target:
        mask &= full["_y"].notna()
    kept = full[mask].sort_values(["_timestamp", "_group"])
    return FeatureFrame(
        X=kept[feature_names].reset_index(drop=True),
        y=kept["_y"].reset_index(drop=True) if require_target else None,
        timestamps=kept["_timestamp"].reset_index(drop=True),
        target_times=kept["_target_time"].reset_index(drop=True),
        groups=kept["_group"].reset_index(drop=True),
        current=kept["target_lag0"].reset_index(drop=True),
        dropped_rows=int(len(full) - len(kept)),
        feature_names=feature_names,
    )
