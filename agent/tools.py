"""Tools the incident agent can call.

Read tools (no side effects):
    get_sensor_data, get_energy_data, get_weather_and_carbon,
    run_anomaly_detection, run_drift_analysis, run_energy_optimization
Action tools (proposals only - nothing is executed here):
    adjust_hvac, adjust_lighting, isolate_sensor
Final answer:
    submit_assessment

Action tools build a :class:`ProposedAction`, run the deterministic safety
validator as a pre-check so the agent can see a rejection and its reason, and
queue it. The graph's safety gate validates queued actions again against the
latest state before anything is executed.
"""

from __future__ import annotations

import dataclasses
import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable

from agent.actions import SENSORS, ProposedAction
from agent.data import BuildingDataSource
from agent.isolation import SensorIsolationRegistry
from agent.safety import SafetyValidator
from optimization.hvac_optimizer import ZoneConditions
from optimization.optimizer import ZoneOptimizer
from processors.data_validator import ValidationError, validate_sensor_event
from security.monitor import SecurityModels, SecurityMonitor

logger = logging.getLogger(__name__)

ZONE_ID = {"type": "string", "description": "Zone id, e.g. Z1"}
REASON = {"type": "string", "description": "Why this action helps, citing the evidence"}


def _schema(properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": list(properties),
            "additionalProperties": False}


TOOL_SPECS: list[dict[str, Any]] = [
    {"name": "get_sensor_data",
     "description": "Recent sensor readings of one zone: the latest reading (isolated sensors shown as null), "
                    "min/mean/max over the window, temperature trend, and the latest temperatures of the other "
                    "zones for comparison.",
     "input_schema": _schema({"zone_id": ZONE_ID,
                              "minutes": {"type": "integer", "description": "Window length, 5-1440"}})},
    {"name": "get_energy_data",
     "description": "Energy use of one zone over the last hours (total, HVAC, lighting, appliances, kWh), "
                    "mean and peak power, the same hours one day earlier if available, the building total, "
                    "and the estimated emissions at the current carbon intensity.",
     "input_schema": _schema({"zone_id": ZONE_ID,
                              "hours": {"type": "number", "description": "Window length in hours, 0.5-24"}})},
    {"name": "get_weather_and_carbon",
     "description": "Latest outdoor weather observation and grid carbon intensity, with their data source "
                    "('mock' means synthetic data).",
     "input_schema": _schema({})},
    {"name": "run_anomaly_detection",
     "description": "Re-run the trained point detectors (rules + Isolation Forest, Module 8) on the zone's recent "
                    "readings. Returns how many readings were flagged, by attack type and rule.",
     "input_schema": _schema({"zone_id": ZONE_ID,
                              "minutes": {"type": "integer", "description": "Window length, 30-360"}})},
    {"name": "run_drift_analysis",
     "description": "Re-run the KS + PSI drift detectors on the zone's last 24 hours: security drift (physical-"
                    "consistency residuals, possible gradual manipulation) and data drift (input distribution).",
     "input_schema": _schema({"zone_id": ZONE_ID})},
    {"name": "run_energy_optimization",
     "description": "Run the deterministic SciPy optimizer (Module 7) for the zone's current conditions. Returns "
                    "the recommended setpoint and lighting level and the predicted saving. Read-only.",
     "input_schema": _schema({"zone_id": ZONE_ID})},
    {"name": "adjust_hvac",
     "description": "PROPOSE an HVAC change for the simulated building: 'set_setpoint' (needs setpoint_c), "
                    "'set_hvac_mode' (needs hvac_mode) or 'clear_overrides' (return to the baseline schedule, the "
                    "safe fallback). Nothing is executed by this tool: the proposal goes to the deterministic "
                    "safety validator, and only approved proposals are executed in the simulator.",
     "input_schema": _schema({
         "zone_id": ZONE_ID,
         "action": {"type": "string", "enum": ["set_setpoint", "set_hvac_mode", "clear_overrides"]},
         "setpoint_c": {"type": ["number", "null"], "description": "For set_setpoint, else null"},
         "hvac_mode": {"type": ["string", "null"], "enum": ["auto", "heat", "cool", "off", None],
                       "description": "For set_hvac_mode, else null"},
         "reason": REASON})},
    {"name": "adjust_lighting",
     "description": "PROPOSE a lighting level (0-100 %) for a zone of the simulated building. Goes to the safety "
                    "validator; only approved proposals are executed.",
     "input_schema": _schema({"zone_id": ZONE_ID,
                              "lighting_level_pct": {"type": "number", "description": "0-100"},
                              "reason": REASON})},
    {"name": "isolate_sensor",
     "description": "PROPOSE isolating (quarantining) a compromised sensor: its readings are no longer trusted by "
                    "the analytics and the safety layer assumes the worst case instead. Requires the id of an "
                    "alert for this zone as evidence. Goes to the safety validator.",
     "input_schema": _schema({"zone_id": ZONE_ID,
                              "sensor": {"type": "string", "enum": list(SENSORS)},
                              "alert_id": {"type": "string", "description": "Alert that is the evidence"},
                              "reason": REASON})},
    {"name": "submit_assessment",
     "description": "Submit the final incident assessment. Call exactly once, after investigating and after "
                    "proposing any actions.",
     "input_schema": _schema({
         "diagnosis": {"type": "string", "description": "What is happening, based on the evidence"},
         "likely_cause": {"type": "string", "description": "e.g. sensor spoofing, replay, sensor fault, "
                                                           "weather, normal operation (false alarm)"},
         "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
         "energy_impact": {"type": "string", "description": "Effect on energy use, cost and emissions, "
                                                            "quantified where possible"},
         "operational_impact": {"type": "string", "description": "Effect on comfort, equipment and operations"},
         "recommendations": {"type": "array", "items": {"type": "string"},
                             "description": "Corrective actions and follow-ups for the operator"}})},
]
REPLAY_WINDOW_MIN = 50 * 60          # 24 h analysed + 26 h detector warm-up
MAX_ANOMALY_WINDOW_MIN = 360
READ_TOOLS = {"get_sensor_data", "get_energy_data", "get_weather_and_carbon", "run_anomaly_detection",
              "run_drift_analysis", "run_energy_optimization"}
ACTION_TOOLS = {"adjust_hvac", "adjust_lighting", "isolate_sensor"}


class _WindowedAnomaly:
    """Isolation Forest wrapper that only scores while ``active`` (used for replays)."""

    def __init__(self, detector) -> None:
        self.detector, self.active = detector, True
        self.models = detector.models

    def evaluate(self, zone_id: str, values: dict[str, float]):
        return self.detector.evaluate(zone_id, values) if self.active else None


@dataclass
class IncidentContext:
    """Per-incident state shared by the tools."""

    decision_id: str
    alert: dict[str, Any] | None
    proposals: list[ProposedAction] = field(default_factory=list)
    assessment: dict[str, Any] | None = None


def _stats(values: list[float]) -> dict[str, float] | None:
    values = [v for v in values if v is not None]
    if not values:
        return None
    return {"min": round(min(values), 2), "mean": round(statistics.fmean(values), 2), "max": round(max(values), 2)}


def _time(event: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(str(event["timestamp"]))


def _history_hours(events: list[dict[str, Any]]) -> float:
    """Hours of readings available to the replayed detectors (less than ~24 h makes them unreliable)."""
    return round((_time(events[-1]) - _time(events[0])).total_seconds() / 3600, 1) if events else 0.0


class AgentToolbox:
    def __init__(self, data: BuildingDataSource, isolation: SensorIsolationRegistry, validator: SafetyValidator,
                 optimizer: ZoneOptimizer, security_models: SecurityModels | None = None) -> None:
        self.data, self.isolation, self.validator = data, isolation, validator
        self.optimizer, self.security_models = optimizer, security_models
        self._replays: dict[str, tuple[Any, list, list]] = {}
        self._handlers: dict[str, Callable[[dict[str, Any], IncidentContext], dict[str, Any]]] = {
            "get_sensor_data": self.get_sensor_data, "get_energy_data": self.get_energy_data,
            "get_weather_and_carbon": self.get_weather_and_carbon,
            "run_anomaly_detection": self.run_anomaly_detection, "run_drift_analysis": self.run_drift_analysis,
            "run_energy_optimization": self.run_energy_optimization, "adjust_hvac": self.adjust_hvac,
            "adjust_lighting": self.adjust_lighting, "isolate_sensor": self.isolate_sensor,
            "submit_assessment": self.submit_assessment,
        }

    specs = TOOL_SPECS

    def call(self, name: str, args: dict[str, Any], ctx: IncidentContext) -> dict[str, Any]:
        handler = self._handlers.get(name)
        if handler is None:
            return {"error": f"unknown tool '{name}'"}
        try:
            return handler(args, ctx)
        except (KeyError, TypeError, ValueError) as exc:
            return {"error": f"invalid arguments for {name}: {exc}"}
        except Exception as exc:  # noqa: BLE001 - a failing tool must not end the incident
            logger.exception("Tool failed", extra={"tool": name})
            return {"error": f"{name} failed: {type(exc).__name__}: {exc}"}

    # -- helpers ----------------------------------------------------------------------
    def _zone(self, args: dict[str, Any]) -> str:
        zone = str(args["zone_id"])
        if zone not in self.validator.models:
            raise ValueError(f"unknown zone '{zone}' (zones: {', '.join(self.validator.models)})")
        return zone

    def _now(self) -> datetime:
        now = self.data.now()
        if now is None:
            raise ValueError("no sensor data available yet")
        return now

    # -- read tools -------------------------------------------------------------------
    def get_sensor_data(self, args: dict[str, Any], ctx: IncidentContext) -> dict[str, Any]:
        zone = self._zone(args)
        minutes = min(max(int(args["minutes"]), 5), 1440)
        events = [self.isolation.mask(e) for e in self.data.recent_events(zone, minutes)]
        if not events:
            return {"zone_id": zone, "readings": 0}
        latest = events[-1]
        keys = ("timestamp", "temperature_c", "humidity_pct", "co2_ppm", "occupancy_count", "lighting_lux",
                "lighting_level_pct", "hvac_mode", "hvac_status", "hvac_setpoint_c", "total_power_kw",
                "outdoor_temperature_c")
        temps = [(e["temperature_c"], _time(e)) for e in events if e.get("temperature_c") is not None]
        trend = None
        if len(temps) >= 2 and temps[-1][1] > temps[0][1]:
            hours = (temps[-1][1] - temps[0][1]).total_seconds() / 3600
            trend = round((temps[-1][0] - temps[0][0]) / hours, 2)
        others = {}
        for other in self.validator.models:
            if other != zone and (event := self.data.latest_event(other)):
                others[other] = self.isolation.mask(event).get("temperature_c")
        return {
            "zone_id": zone, "readings": len(events), "window_minutes": minutes,
            "latest": {k: latest.get(k) for k in keys},
            "isolated_sensors": latest.get("isolated_sensors", []),
            "window_stats": {k: _stats([e.get(k) for e in events])
                             for k in ("temperature_c", "co2_ppm", "occupancy_count", "total_power_kw")},
            "temperature_trend_c_per_h": trend,
            "other_zones_latest_temperature_c": others,
        }

    def get_energy_data(self, args: dict[str, Any], ctx: IncidentContext) -> dict[str, Any]:
        zone = self._zone(args)
        hours = min(max(float(args["hours"]), 0.5), 24.0)
        events = self.data.recent_events(zone, minutes=(24 + hours) * 60)
        if not events:
            return {"zone_id": zone, "readings": 0}
        end = _time(events[-1])
        current = [e for e in events if _time(e) > end - timedelta(hours=hours)]
        previous = [e for e in events if end - timedelta(hours=24 + hours) < _time(e) <= end - timedelta(hours=24)]

        def summary(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
            if not rows:
                return None
            seconds = [float(e["interval_seconds"]) for e in rows]
            share = {k: sum(float(e[k]) * s / 3600 for e, s in zip(rows, seconds))
                     for k in ("hvac_power_kw", "lighting_power_kw", "appliance_power_kw")}
            return {"readings": len(rows), "energy_kwh": round(sum(float(e["energy_kwh"]) for e in rows), 3),
                    "hvac_kwh": round(share["hvac_power_kw"], 3),
                    "lighting_kwh": round(share["lighting_power_kw"], 3),
                    "appliance_kwh": round(share["appliance_power_kw"], 3),
                    "mean_power_kw": round(statistics.fmean(float(e["total_power_kw"]) for e in rows), 3),
                    "peak_power_kw": round(max(float(e["total_power_kw"]) for e in rows), 3)}

        now_summary = summary(current)
        building = 0.0
        for other in self.validator.models:
            building += sum(float(e["energy_kwh"]) for e in self.data.recent_events(other, hours * 60))
        carbon = self.data.carbon()
        result = {"zone_id": zone, "window_hours": hours, "current": now_summary,
                  "same_hours_yesterday": summary(previous), "building_energy_kwh": round(building, 3),
                  "power_meter_isolated": self.isolation.is_isolated(zone, "power_meter")}
        if carbon and now_summary:
            result["estimated_kg_co2e"] = round(now_summary["energy_kwh"] * carbon["intensity_g_per_kwh"] / 1000, 3)
            result["carbon_intensity_g_per_kwh"] = carbon["intensity_g_per_kwh"]
            result["carbon_source"] = carbon["source"]
        return result

    def get_weather_and_carbon(self, args: dict[str, Any], ctx: IncidentContext) -> dict[str, Any]:
        return {"weather": self.data.weather(), "carbon_intensity": self.data.carbon(),
                "note": "source 'mock' = synthetic development data, not a real measurement"}

    def _replay(self, zone: str):
        """Replay the zone's last REPLAY_WINDOW_MIN through fresh Module 8 detectors.

        The result is cached per zone until a newer reading arrives, so the
        anomaly and drift tools share one replay per incident.
        """
        if self.security_models is None:
            raise ValueError("no trained security detectors loaded (run python -m security.train)")
        latest = self.data.latest_event(zone)
        key = latest["event_id"] if latest else None
        cached = self._replays.get(zone)
        if cached is None or cached[0] != key:
            events = self.data.recent_events(zone, REPLAY_WINDOW_MIN)
            # Isolation Forest scores do not depend on history and are slow (~50 ms per reading), so
            # they are computed for the analysed window only; rules and drift see the full history.
            forest = _WindowedAnomaly(self.security_models.anomaly)
            monitor = SecurityMonitor(dataclasses.replace(self.security_models, anomaly=forest), cooldown_s=0.0)
            scored_from = _time(events[-1]) - timedelta(minutes=MAX_ANOMALY_WINDOW_MIN) if events else None
            outputs = []
            for event in events:
                forest.active = _time(event) > scored_from
                outputs.append(monitor.process(event))
            cached = (key, events, outputs)
            self._replays[zone] = cached
        return cached[1], cached[2]

    def run_anomaly_detection(self, args: dict[str, Any], ctx: IncidentContext) -> dict[str, Any]:
        zone = self._zone(args)
        minutes = min(max(int(args["minutes"]), 30), MAX_ANOMALY_WINDOW_MIN)
        # The replay includes >= 26 h of history before the window, as in streaming (rate features,
        # day-old data for replay checks).
        events, outputs = self._replay(zone)
        if not events:
            return {"zone_id": zone, "readings": 0}
        start = _time(events[-1]) - timedelta(minutes=minutes)
        by_type: dict[str, int] = {}
        rules: dict[str, int] = {}
        flagged_times, scores, evaluated = [], [], 0
        for event, output in zip(events, outputs):
            if _time(event) <= start:
                continue
            evaluated += 1
            detection = output.detection
            if detection is None:
                by_type["invalid_reading"] = by_type.get("invalid_reading", 0) + 1
                continue
            if detection.anomaly:
                scores.append(detection.anomaly[1])
            if detection.flagged:
                by_type[detection.attack_type] = by_type.get(detection.attack_type, 0) + 1
                flagged_times.append(event["timestamp"])
                for hit in detection.hits:
                    rules[hit.rule] = rules.get(hit.rule, 0) + 1
        return {"zone_id": zone, "history_hours": _history_hours(events),
                "readings_evaluated": evaluated, "flagged": len(flagged_times),
                "flagged_by_type": by_type, "rules_triggered": dict(sorted(rules.items(), key=lambda kv: -kv[1])[:8]),
                "first_flagged": flagged_times[0] if flagged_times else None,
                "last_flagged": flagged_times[-1] if flagged_times else None,
                "max_isolation_forest_score": round(max(scores), 4) if scores else None,
                "isolation_forest_threshold": (round(outputs[-1].detection.anomaly[2], 4)
                                               if outputs[-1].detection and outputs[-1].detection.anomaly else None)}

    def run_drift_analysis(self, args: dict[str, Any], ctx: IncidentContext) -> dict[str, Any]:
        zone = self._zone(args)
        events, outputs = self._replay(zone)
        cutoff = _time(events[-1]) - timedelta(hours=24) if events else None
        outputs = [o for e, o in zip(events, outputs) if _time(e) > cutoff]

        def describe(results) -> dict[str, Any]:
            results = [r for r in results if r is not None]
            if not results:
                return {"windows_evaluated": 0, "note": "not enough history for a 6-hour window"}
            latest = results[-1]
            return {"windows_evaluated": len(results), "windows_with_drift": sum(r.is_drift for r in results),
                    "latest_window": f"{latest.window_start.isoformat()} - {latest.window_end.isoformat()}",
                    "latest_drifting_features": list(latest.drifting),
                    "latest_features": [{"feature": f.feature, "ks_p_value": float(f"{f.ks_p_value:.3g}"),
                                         "psi": round(f.psi, 3), "mean_shift": round(f.mean_shift, 3)}
                                        for f in latest.features]}

        return {"zone_id": zone, "readings": len(outputs), "history_hours": _history_hours(events),
                "security_drift": describe(o.drift for o in outputs),
                "data_drift": describe(o.data_drift for o in outputs),
                "interpretation": "security drift = physical-consistency residuals shifted (possible gradual "
                                  "manipulation or sensor fault); data drift = inputs differ from the training "
                                  "period (e.g. season), which degrades the ML models but is not an attack"}

    def run_energy_optimization(self, args: dict[str, Any], ctx: IncidentContext) -> dict[str, Any]:
        zone = self._zone(args)
        event = self.data.latest_event(zone)
        if event is None:
            return {"zone_id": zone, "error": "no reading available"}
        try:
            conditions = ZoneConditions.from_reading(validate_sensor_event(event))
        except ValidationError as exc:
            return {"zone_id": zone, "error": f"latest reading invalid: {exc.errors}"}
        carbon = self.data.carbon()
        decision = self.optimizer.optimize(conditions, carbon_intensity=carbon["intensity_g_per_kwh"] if carbon
                                           else None)
        untrusted = [i.sensor for i in self.isolation.isolated(zone)]
        return {"zone_id": zone, "current_setpoint_c": decision.baseline_setpoint_c,
                "recommended_setpoint_c": round(decision.setpoint_c, 2),
                "current_lighting_pct": round(decision.baseline_lighting_level * 100, 1),
                "recommended_lighting_pct": round(decision.lighting_level * 100, 1),
                "predicted_saving_kw": round(decision.predicted_saving_kw, 3),
                "constraints_satisfied": decision.approved, "violations": list(decision.violations),
                "warning": (f"based on readings from isolated sensors {untrusted}; prefer clear_overrides"
                            if untrusted else None)}

    # -- action tools (proposals) ----------------------------------------------------
    def _propose(self, ctx: IncidentContext, zone: str, action_type: str, parameters: dict[str, Any],
                 reason: str, alert_id: str | None) -> dict[str, Any]:
        for existing in ctx.proposals:
            if (existing.zone_id, existing.action_type, existing.parameters) == (zone, action_type, parameters):
                return {"action_id": existing.action_id, "status": existing.status, "note": "duplicate proposal"}
        now = self._now()
        action = ProposedAction(zone_id=zone, action_type=action_type, parameters=parameters, reason=reason,
                                created_at=now, decision_id=ctx.decision_id, related_alert_id=alert_id,
                                sequence=len(ctx.proposals))
        verdict = self.validator.validate(action, latest_event=self.data.latest_event(zone), now=now,
                                          zone_alerts=self.data.alerts(zone, hours=24))
        if not verdict.approved:
            action.status, action.safety_reason = "rejected", verdict.text
        ctx.proposals.append(action)
        return {"action_id": action.action_id, "action_type": action_type, "zone_id": zone,
                "pre_check": "passed" if verdict.approved else "rejected", "safety_reasons": list(verdict.reasons),
                "next_step": ("queued: final safety check and simulated execution happen after the assessment"
                              if verdict.approved else "will not be executed")}

    def adjust_hvac(self, args: dict[str, Any], ctx: IncidentContext) -> dict[str, Any]:
        zone, kind = self._zone(args), str(args["action"])
        if kind == "set_setpoint":
            if args.get("setpoint_c") is None:
                raise ValueError("setpoint_c is required for set_setpoint")
            parameters = {"setpoint_c": float(args["setpoint_c"])}
        elif kind == "set_hvac_mode":
            if not args.get("hvac_mode"):
                raise ValueError("hvac_mode is required for set_hvac_mode")
            parameters = {"hvac_mode": str(args["hvac_mode"])}
        elif kind == "clear_overrides":
            parameters = {}
        else:
            raise ValueError(f"unknown HVAC action '{kind}'")
        return self._propose(ctx, zone, kind, parameters, str(args["reason"]), self._alert_id(ctx, zone))

    def adjust_lighting(self, args: dict[str, Any], ctx: IncidentContext) -> dict[str, Any]:
        zone = self._zone(args)
        level = float(args["lighting_level_pct"]) / 100
        return self._propose(ctx, zone, "set_lighting", {"lighting_level": round(level, 4)},
                             str(args["reason"]), self._alert_id(ctx, zone))

    def isolate_sensor(self, args: dict[str, Any], ctx: IncidentContext) -> dict[str, Any]:
        zone = self._zone(args)
        return self._propose(ctx, zone, "isolate_sensor", {"sensor": str(args["sensor"])}, str(args["reason"]),
                             str(args["alert_id"]))

    @staticmethod
    def _alert_id(ctx: IncidentContext, zone: str) -> str | None:
        alert = ctx.alert or {}
        return str(alert["alert_id"]) if alert.get("alert_id") and alert.get("zone_id") == zone else None

    def submit_assessment(self, args: dict[str, Any], ctx: IncidentContext) -> dict[str, Any]:
        confidence = str(args["confidence"])
        if confidence not in ("low", "medium", "high"):
            raise ValueError("confidence must be low, medium or high")
        ctx.assessment = {
            "diagnosis": str(args["diagnosis"]), "likely_cause": str(args["likely_cause"]),
            "confidence": confidence, "energy_impact": str(args["energy_impact"]),
            "operational_impact": str(args["operational_impact"]),
            "recommendations": [str(r) for r in args["recommendations"]],
        }
        return {"status": "assessment recorded"}
