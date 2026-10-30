"""Rule-based validation and attack classification for individual readings.

Rules (thresholds calibrated per zone on *clean* training data: the largest
observed value times a safety factor, so normal operation never triggers them):

* ``timestamp_regression``   time or sequence goes backwards  -> replay
* ``replayed_values``        two consecutive readings repeat an earlier
                             consecutive pair exactly (sensor noise makes
                             this practically impossible)   -> replay
* ``temperature_rate``       temperature changes faster than physically observed -> temperature spoofing
* ``occupancy_jump``         occupancy changes more than observed in one interval -> occupancy manipulation
* ``co2_without_occupants``  CO2 rises while the zone reports no people          -> occupancy manipulation
* ``hvac_contradiction``     temperature impossible for the HVAC state, e.g. heating
                             while 4 °C above setpoint (HVAC status is reported
                             by a separate controller)     -> temperature spoofing
* ``co2_occupancy_mismatch`` CO2 inconsistent with reported occupancy for two
                             readings (people hidden or invented) -> occupancy manipulation
* ``lighting_power_mismatch`` lighting power inconsistent with the reported dimming level
* ``hvac_power_mismatch``    HVAC power while idle/off differs from the fan-only power seen in training
* ``appliance_power_mismatch`` appliance power inconsistent with occupancy (two readings)
* ``power_out_of_range``     power above anything observed for the zone           -> abnormal power
* ``power_rate``             power changes faster than observed                  -> abnormal power

Classification combines rule hits with the Isolation Forest result: rules
decide when they fire; otherwise the most unusual feature group of an
Isolation Forest anomaly decides (temperature / occupancy / power).
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, field

import numpy as np

from database.models import SensorReadingRecord
from security.features import FeatureRow

SAFETY_FACTOR = 1.5
REPLAY_FIELDS = ("temperature_c", "humidity_pct", "co2_ppm", "occupancy_count", "lighting_lux",
                 "hvac_power_kw", "lighting_power_kw", "appliance_power_kw", "total_power_kw")
REPLAY_HISTORY = 600
GROUP_TO_ATTACK = {"temperature": "temperature_spoofing", "occupancy": "occupancy_manipulation",
                   "power": "abnormal_power"}
MIN_GROUP_Z = 4.0     # an IF anomaly is only attributed when one feature is clearly extreme


@dataclass(frozen=True)
class RuleHit:
    rule: str
    attack_type: str
    detail: str


CONTROL_ERROR_MARGIN_C = 0.5   # >= 3 sigma sensor noise (0.3 °C) plus thermostat offset
MIN_BUCKET_SAMPLES = 60
POWER_RATIO_TOLERANCE = 0.05     # power meters are accurate to about 1 %
POWER_MARGIN_KW = 0.05
APPLIANCE_RESIDUAL_QUANTILE = 0.999
CO2_RESIDUAL_MARGIN_PPM = 30.0
CO2_RESIDUAL_QUANTILE = 0.999


def _co2_bucket(values: dict) -> tuple[bool, int]:
    """Ventilation follows the building schedule: model CO2 per (weekend, hour)."""
    return bool(values["weekend"]), int(round(math.atan2(values["hour_sin"], values["hour_cos"])
                                              / (2 * math.pi) * 24)) % 24


@dataclass
class Co2OccupancyModel:
    """CO2 ~ a + b * occupancy_ema, fitted per (day type, hour) on clean data."""

    coefficients: dict[tuple[bool, int], tuple[float, float]]
    fallback: tuple[float, float]
    max_abs_residual: dict[bool, float]      # per day type (weekend?): weekends differ strongly

    @classmethod
    def fit(cls, rows: list[FeatureRow]) -> "Co2OccupancyModel":
        buckets: dict[tuple[bool, int], list[tuple[float, float]]] = defaultdict(list)
        for r in rows:
            buckets[_co2_bucket(r.values)].append((r.values["occupancy_ema"], r.values["co2_ppm"]))

        def solve(points: list[tuple[float, float]], default_slope: float) -> tuple[float, float]:
            x = np.array([p[0] for p in points])
            y = np.array([p[1] for p in points])
            if np.ptp(x) < 0.5:
                # Nobody was ever present in these hours: keep the zone's overall
                # slope so that reported (phantom) occupants still imply more CO2.
                return float(y.mean() - default_slope * x.mean()), default_slope
            slope, intercept = np.polyfit(x, y, 1)
            return float(intercept), float(slope)

        everything = [p for points in buckets.values() for p in points]
        overall = solve(everything, 0.0)
        model = cls({k: solve(v, overall[1]) for k, v in buckets.items() if len(v) >= 20}, overall, {})
        # 99.9th percentile rather than the maximum: rare legitimate events (e.g. a
        # weekend meeting with ventilation off) must not disable the rule.
        for weekend in (False, True):
            residuals = [abs(model.residual(r.values)) for r in rows if bool(r.values["weekend"]) == weekend]
            if residuals:
                model.max_abs_residual[weekend] = (float(np.quantile(residuals, CO2_RESIDUAL_QUANTILE)) * 1.2
                                                   + CO2_RESIDUAL_MARGIN_PPM)
        return model

    def threshold(self, values: dict) -> float:
        return self.max_abs_residual.get(bool(values["weekend"]), max(self.max_abs_residual.values()))

    def residual(self, values: dict) -> float:
        a, b = self.coefficients.get(_co2_bucket(values), self.fallback)
        return values["co2_ppm"] - (a + b * values["occupancy_ema"])


@dataclass
class ZoneRuleLimits:
    max_temperature_rate: float          # °C per minute
    max_occupancy_jump: float            # people per interval
    max_co2_rise_empty: float            # ppm per minute while empty
    max_total_power: float               # kW
    max_power_rate: float                # kW per minute
    control_error_range: dict[str, tuple[float, float]] = field(default_factory=dict)  # per HVAC status
    co2_model: Co2OccupancyModel | None = None
    # Control-error range per (HVAC status, weekend, hour): night setback and
    # daytime comfort control allow very different errors.
    control_error_bucket_range: dict[tuple[str, bool, int], tuple[float, float]] = field(default_factory=dict)
    lighting_kw_per_level: tuple[float, float] | None = None   # lighting power / dimming level (0-1)
    max_lighting_kw_when_off: float = 0.0
    max_idle_hvac_kw: float = 0.0                             # fan only
    appliance_model: tuple[float, float] = (0.0, 0.0)        # appliance kW ~ a + b * occupancy
    max_appliance_residual_kw: float = 0.0

    def control_error_limits(self, status: str, values: dict) -> tuple[float, float] | None:
        bucket = self.control_error_bucket_range.get((status, *_co2_bucket(values)))
        return bucket or self.control_error_range.get(status)

    @classmethod
    def calibrate(cls, rows: list[FeatureRow]) -> "ZoneRuleLimits":
        def biggest(values: list[float], floor: float) -> float:
            return max([abs(v) for v in values] + [floor]) * SAFETY_FACTOR

        with_prev = [r for r in rows if r.previous is not None]
        empty = [r.values["d_co2_per_min"] for r in with_prev
                 if r.values["occupancy_count"] == 0 and r.previous.occupancy_count == 0]
        return cls(
            max_temperature_rate=biggest([r.values["d_temperature_per_min"] for r in with_prev], 0.05),
            max_occupancy_jump=biggest([r.values["d_occupancy"] for r in with_prev], 2.0),
            max_co2_rise_empty=biggest([v for v in empty if v > 0], 1.0),
            max_total_power=biggest([r.values["total_power_kw"] for r in rows], 0.5),
            max_power_rate=biggest([r.values["d_total_power_per_min"] for r in with_prev], 0.05),
            control_error_range={
                status: (min(errs) - CONTROL_ERROR_MARGIN_C, max(errs) + CONTROL_ERROR_MARGIN_C)
                for status in ("heating", "cooling", "idle")
                if (errs := [r.values["temp_minus_setpoint"] for r in rows
                             if r.values["hvac_status"] == status])},
            co2_model=Co2OccupancyModel.fit(rows),
            **_power_and_bucket_limits(rows),
        )


def _power_and_bucket_limits(rows: list[FeatureRow]) -> dict:
    margin = CONTROL_ERROR_MARGIN_C
    buckets: dict[tuple[str, bool, int], list[float]] = defaultdict(list)
    for r in rows:
        status, (weekend, hour) = r.values["hvac_status"], _co2_bucket(r.values)
        for h in (hour - 1, hour, hour + 1):      # pool neighbouring hours for robustness
            buckets[(status, weekend, h % 24)].append(r.values["temp_minus_setpoint"])
    bucket_range = {k: (min(v) - margin, max(v) + margin) for k, v in buckets.items()
                    if len(v) >= MIN_BUCKET_SAMPLES}

    lit = [r.values["lighting_power_kw"] / (r.values["lighting_level_pct"] / 100) for r in rows
           if r.values["lighting_level_pct"] >= 5]
    lighting_ratio = ((min(lit) * (1 - POWER_RATIO_TOLERANCE), max(lit) * (1 + POWER_RATIO_TOLERANCE))
                      if lit else None)
    off = [r.values["lighting_power_kw"] for r in rows if r.values["lighting_level_pct"] == 0]
    idle = [r.values["hvac_power_kw"] for r in rows if r.values["hvac_status"] in ("idle", "off")]

    occupancy = np.array([r.values["occupancy_count"] for r in rows])
    appliance = np.array([r.values["appliance_power_kw"] for r in rows])
    if np.ptp(occupancy) > 0:
        slope, intercept = np.polyfit(occupancy, appliance, 1)
    else:
        slope, intercept = 0.0, float(appliance.mean())
    residuals = np.abs(appliance - (intercept + slope * occupancy))
    return {
        "control_error_bucket_range": bucket_range,
        "lighting_kw_per_level": lighting_ratio,
        "max_lighting_kw_when_off": (max(off) if off else 0.0) + POWER_MARGIN_KW,
        "max_idle_hvac_kw": (max(idle) if idle else 0.0) * (1 + POWER_RATIO_TOLERANCE) + POWER_MARGIN_KW,
        "appliance_model": (float(intercept), float(slope)),
        "max_appliance_residual_kw": float(np.quantile(residuals, APPLIANCE_RESIDUAL_QUANTILE)) * 1.2 + POWER_MARGIN_KW,
    }


@dataclass
class RuleEngine:
    limits: dict[str, ZoneRuleLimits]
    _last_time: dict = field(default_factory=dict)
    _last_sequence: dict = field(default_factory=dict)
    _history: dict = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=REPLAY_HISTORY)))
    _previous_match: dict = field(default_factory=dict)
    _empty_rise: dict = field(default_factory=dict)
    _co2_mismatch: dict = field(default_factory=dict)
    _appliance_mismatch: dict = field(default_factory=dict)

    def check(self, reading: SensorReadingRecord, row: FeatureRow) -> list[RuleHit]:
        zone = reading.zone_id
        hits: list[RuleHit] = []

        # Replay: time/sequence going backwards.
        last_time = self._last_time.get(zone)
        if last_time is not None and (reading.event_time <= last_time
                                      or reading.sequence <= self._last_sequence[zone]):
            hits.append(RuleHit("timestamp_regression", "replay",
                                f"time {reading.event_time.isoformat()} / sequence {reading.sequence} "
                                f"not after {last_time.isoformat()} / {self._last_sequence[zone]}"))
        else:
            self._last_time[zone], self._last_sequence[zone] = reading.event_time, reading.sequence

        # Replay: exact repetition of an earlier *sequence* of readings.
        vector = tuple(getattr(reading, f) for f in REPLAY_FIELDS)
        history = self._history[zone]
        matches = {i for i, v in enumerate(history) if v == vector}
        previous = self._previous_match.get(zone, set())
        if any(i - 1 in previous for i in matches):
            hits.append(RuleHit("replayed_values", "replay",
                                "two consecutive readings repeat earlier readings exactly"))
        # Appending to a full deque drops the oldest entry: indices shift by one.
        shift = 1 if len(history) == history.maxlen else 0
        self._previous_match[zone] = {i - shift for i in matches}
        history.append(vector)

        limits = self.limits.get(zone)
        if limits is None:
            return hits
        v = row.values
        allowed = limits.control_error_limits(reading.hvac_status, v)
        if allowed and not allowed[0] <= v["temp_minus_setpoint"] <= allowed[1]:
            hits.append(RuleHit("hvac_contradiction", "temperature_spoofing",
                                f"temperature {v['temp_minus_setpoint']:+.1f} °C from setpoint while HVAC is "
                                f"'{reading.hvac_status}' (possible range {allowed[0]:+.1f}..{allowed[1]:+.1f})"))
        if limits.co2_model is not None:
            residual = limits.co2_model.residual(v)
            mismatch = abs(residual) > limits.co2_model.threshold(v)
            if mismatch and self._co2_mismatch.get(zone):
                hits.append(RuleHit("co2_occupancy_mismatch", "occupancy_manipulation",
                                    f"CO2 {residual:+.0f} ppm from the level expected for "
                                    f"{v['occupancy_ema']:.1f} recent occupants"))
            self._co2_mismatch[zone] = mismatch
        hits.extend(self._power_consistency(zone, reading, limits))
        if row.previous is None:
            return hits
        if abs(v["d_temperature_per_min"]) > limits.max_temperature_rate:
            hits.append(RuleHit("temperature_rate", "temperature_spoofing",
                                f"{v['d_temperature_per_min']:+.2f} °C/min (limit {limits.max_temperature_rate:.2f})"))
        if abs(v["d_occupancy"]) > limits.max_occupancy_jump:
            hits.append(RuleHit("occupancy_jump", "occupancy_manipulation",
                                f"{v['d_occupancy']:+.0f} people in one interval (limit {limits.max_occupancy_jump:.0f})"))
        empty_rising = (reading.occupancy_count == 0 and row.previous.occupancy_count == 0
                        and v["d_co2_per_min"] > limits.max_co2_rise_empty)
        if empty_rising and self._empty_rise.get(zone):
            hits.append(RuleHit("co2_without_occupants", "occupancy_manipulation",
                                f"CO2 rising {v['d_co2_per_min']:.1f} ppm/min with 0 occupants reported"))
        self._empty_rise[zone] = empty_rising
        if v["total_power_kw"] > limits.max_total_power:
            hits.append(RuleHit("power_out_of_range", "abnormal_power",
                                f"{v['total_power_kw']:.2f} kW (max plausible {limits.max_total_power:.2f})"))
        if abs(v["d_total_power_per_min"]) > limits.max_power_rate:
            hits.append(RuleHit("power_rate", "abnormal_power",
                                f"{v['d_total_power_per_min']:+.2f} kW/min (limit {limits.max_power_rate:.2f})"))
        return hits


    def _power_consistency(self, zone: str, reading: SensorReadingRecord,
                           limits: ZoneRuleLimits) -> list[RuleHit]:
        hits = []
        level = reading.lighting_level_pct / 100
        if limits.lighting_kw_per_level and level >= 0.05:
            low, high = limits.lighting_kw_per_level
            ratio = reading.lighting_power_kw / level
            if not low <= ratio <= high:
                hits.append(RuleHit("lighting_power_mismatch", "abnormal_power",
                                    f"{reading.lighting_power_kw:.2f} kW at {level:.0%} dimming "
                                    f"(expected {low * level:.2f}-{high * level:.2f} kW)"))
        elif level == 0 and reading.lighting_power_kw > limits.max_lighting_kw_when_off:
            hits.append(RuleHit("lighting_power_mismatch", "abnormal_power",
                                f"{reading.lighting_power_kw:.2f} kW with lights off"))
        if reading.hvac_status in ("idle", "off") and reading.hvac_power_kw > limits.max_idle_hvac_kw:
            hits.append(RuleHit("hvac_power_mismatch", "abnormal_power",
                                f"{reading.hvac_power_kw:.2f} kW while HVAC is {reading.hvac_status} "
                                f"(fan-only maximum {limits.max_idle_hvac_kw:.2f} kW)"))
        a, b = limits.appliance_model
        residual = reading.appliance_power_kw - (a + b * reading.occupancy_count)
        mismatch = abs(residual) > limits.max_appliance_residual_kw
        if mismatch and self._appliance_mismatch.get(zone):
            hits.append(RuleHit("appliance_power_mismatch", "abnormal_power",
                                f"appliance power {residual:+.2f} kW from the level expected for "
                                f"{reading.occupancy_count} occupants"))
        self._appliance_mismatch[zone] = mismatch
        return hits


def classify(hits: list[RuleHit], anomaly: tuple[bool, float, float] | None,
             top_group: tuple[str, str, float] | None) -> tuple[str | None, str]:
    """Return (attack_type or None, detector) for one reading."""
    is_anomaly = bool(anomaly and anomaly[0])
    if hits:
        votes: dict[str, int] = defaultdict(int)
        for hit in hits:
            votes[hit.attack_type] += 1
        # Replay evidence is the most specific; otherwise the most frequent type.
        attack = "replay" if "replay" in votes else max(votes, key=lambda k: (votes[k], k))
        return attack, "rules+isolation_forest" if is_anomaly else "rules"
    if is_anomaly:
        if top_group and top_group[2] >= MIN_GROUP_Z:
            return GROUP_TO_ATTACK[top_group[0]], "isolation_forest"
        return "unknown_anomaly", "isolation_forest"
    return None, ""
