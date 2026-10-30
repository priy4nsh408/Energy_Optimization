"""Read-only queries for the dashboard (PostgreSQL only).

Flask never consumes Kafka. Everything the dashboard shows was persisted by
the workers: readings (``sensor_persistence``), energy and carbon
(``energy_processor``), predictions, alerts, commands and agent actions
(``record_persistence``), agent decisions (``agent_service``) and the
simulation state (``simulation_manager``).

Time windows are in *simulated* time, which usually runs faster than real
time, and are anchored on the most recently **stored** data (not the largest
timestamp), so the dashboard follows the current run even if the database
also holds older runs with later simulated dates. Windows are bounded on both
sides. Runs that reuse the same simulated dates in one database would overlap;
use a different ``--start`` or a fresh database for separate experiments.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row

MAX_HOURS = 24 * 14
MAX_ROWS = 500
# Simulated "now" of the current run, per table.
SENSOR_NOW = "(SELECT event_time FROM sensor_readings ORDER BY ingested_at DESC, event_time DESC LIMIT 1)"
ENERGY_NOW = "(SELECT event_time FROM energy_records ORDER BY processed_at DESC, event_time DESC LIMIT 1)"
PREDICTION_NOW = "(SELECT predicted_for FROM energy_predictions ORDER BY ingested_at DESC, predicted_for DESC LIMIT 1)"
ALERT_NOW = "(SELECT detected_at FROM security_alerts ORDER BY updated_at DESC, detected_at DESC LIMIT 1)"


def jsonable(value: Any) -> Any:
    """Convert database values (datetime, UUID, Decimal, NaN) to JSON-safe values."""
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _hours(value: Any, default: float) -> float:
    try:
        hours = float(value)
    except (TypeError, ValueError):
        return default
    return min(max(hours, 0.25), MAX_HOURS) if math.isfinite(hours) else default


def _limit(value: Any, default: int) -> int:
    try:
        return min(max(int(value), 1), MAX_ROWS)
    except (TypeError, ValueError):
        return default


class DashboardData:
    def __init__(self, database, *, comparison_path: Path | None = None) -> None:
        self.db = database
        self.comparison_path = comparison_path

    def _rows(self, query: str, params: tuple | dict = ()) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                return [jsonable(r) for r in cur.execute(query, params)]

    def _one(self, query: str, params: tuple | dict = ()) -> dict[str, Any] | None:
        rows = self._rows(query, params)
        return rows[0] if rows else None

    # -- status / live ------------------------------------------------------------------
    def status(self) -> dict[str, Any]:
        sim = self._rows("SELECT *, extract(epoch FROM now() - updated_at) AS age_s FROM simulation_status "
                         "ORDER BY updated_at DESC")
        latest = self._one(f"SELECT {SENSOR_NOW} AS latest_reading, (SELECT count(*) FROM sensor_readings) AS readings")
        carbon = self._one("SELECT observed_at, source, region, intensity_g_per_kwh FROM carbon_intensity_records "
                           "WHERE NOT is_forecast ORDER BY observed_at DESC LIMIT 1")
        weather = self._one("SELECT observed_at, source, temperature_c, humidity_pct, wind_speed_ms, "
                            "solar_irradiance_w_m2, cloud_cover_pct FROM weather_records WHERE NOT is_forecast "
                            "ORDER BY observed_at DESC LIMIT 1")
        counts = self._one("""
            SELECT (SELECT count(*) FROM security_alerts WHERE status <> 'resolved') AS open_alerts,
                   (SELECT count(*) FROM agent_decisions) AS agent_decisions,
                   (SELECT count(*) FROM optimization_actions) AS actions,
                   (SELECT count(*) FROM simulated_attacks) AS simulated_attacks""")
        for row in sim:                                   # a manager that stopped updating is not running
            if row["state"] != "stopped" and row["age_s"] is not None and row["age_s"] > 30:
                row["state"], row["message"] = "stopped", "no heartbeat from workers.simulation_manager"
        return {"simulation": sim[0] if sim else None, **(latest or {}), "carbon": carbon, "weather": weather,
                "counts": counts}

    def live_readings(self) -> list[dict[str, Any]]:
        """Latest reading of every zone plus its isolated sensors."""
        readings = self._rows("""
            SELECT DISTINCT ON (zone_id) zone_id, zone_type, event_time, temperature_c, humidity_pct, co2_ppm,
                   occupancy_count, lighting_lux, lighting_level_pct, hvac_mode, hvac_status, hvac_setpoint_c,
                   hvac_power_kw, lighting_power_kw, appliance_power_kw, total_power_kw, outdoor_temperature_c
            FROM sensor_readings ORDER BY zone_id, ingested_at DESC, event_time DESC""")
        isolated = self.isolated_sensors()
        for row in readings:
            row["isolated_sensors"] = sorted(i["sensor"] for i in isolated if i["zone_id"] == row["zone_id"])
        return readings

    def isolated_sensors(self) -> list[dict[str, Any]]:
        """Sensors whose latest executed isolate/restore action is an isolation (since = simulated time)."""
        return self._rows("""
            SELECT zone_id, sensor, created_at AS since, reason FROM (
                SELECT DISTINCT ON (zone_id, parameters->>'sensor') zone_id, parameters->>'sensor' AS sensor,
                       action_type, created_at, updated_at, reason
                FROM optimization_actions
                WHERE action_type IN ('isolate_sensor', 'restore_sensor') AND status = 'executed'
                ORDER BY zone_id, parameters->>'sensor', updated_at DESC) latest
            WHERE action_type = 'isolate_sensor' ORDER BY zone_id, sensor""")

    def readings_since(self, ingested_after: str | None, limit: int = 200) -> list[dict[str, Any]]:
        """Readings stored after a wall-clock watermark (for live push)."""
        return self._rows("""
            SELECT zone_id, event_time, ingested_at, temperature_c, co2_ppm, occupancy_count, total_power_kw,
                   hvac_setpoint_c, lighting_level_pct
            FROM sensor_readings WHERE ingested_at > COALESCE(%s::timestamptz, now() - interval '10 seconds')
            ORDER BY ingested_at LIMIT %s""", (ingested_after, limit))

    # -- energy ------------------------------------------------------------------------
    def power_timeseries(self, hours: Any = 6) -> dict[str, Any]:
        """Total power per zone per reading over the last hours of simulated time."""
        rows = self._rows(f"""
            WITH now AS (SELECT {SENSOR_NOW} AS t)
            SELECT zone_id, event_time, total_power_kw, temperature_c FROM sensor_readings, now
            WHERE event_time > now.t - %s * interval '1 hour' AND event_time <= now.t
            ORDER BY event_time""", (_hours(hours, 6),))
        series: dict[str, dict[str, list]] = {}
        for r in rows:
            s = series.setdefault(r["zone_id"], {"t": [], "power_kw": [], "temperature_c": []})
            s["t"].append(r["event_time"])
            s["power_kw"].append(r["total_power_kw"])
            s["temperature_c"].append(r["temperature_c"])
        return {"zones": series}

    def hourly_energy(self, hours: Any = 48) -> list[dict[str, Any]]:
        """Hourly energy, breakdown, carbon and comfort per zone (view hourly_zone_energy)."""
        return self._rows(f"""
            WITH now AS (SELECT {ENERGY_NOW} AS t)
            SELECT zone_id, hour, readings, energy_kwh, hvac_energy_kwh, lighting_energy_kwh, appliance_energy_kwh,
                   avg_power_kw, peak_power_kw, carbon_kg_co2e, avg_temperature_c
            FROM hourly_zone_energy, now
            WHERE hour > now.t - %s * interval '1 hour' AND hour <= now.t
            ORDER BY hour, zone_id""", (_hours(hours, 48),))

    def energy_summary(self, hours: Any = 24) -> dict[str, Any]:
        row = self._one(f"""
            WITH now AS (SELECT {ENERGY_NOW} AS t)
            SELECT sum(energy_kwh) AS energy_kwh, sum(hvac_energy_kwh) AS hvac_kwh,
                   sum(lighting_energy_kwh) AS lighting_kwh, sum(appliance_energy_kwh) AS appliance_kwh,
                   sum(carbon_kg_co2e) AS carbon_kg_co2e,
                   count(*) FILTER (WHERE carbon_kg_co2e IS NULL) AS readings_without_carbon,
                   count(*) AS readings, min(event_time) AS since, max(event_time) AS until
            FROM energy_records, now
            WHERE event_time > now.t - %s * interval '1 hour' AND event_time <= now.t""",
                        (_hours(hours, 24),))
        return row or {}

    def predictions(self, hours: Any = 12, zone_id: str | None = None) -> dict[str, Any]:
        """ML predictions next to the measured power (view prediction_accuracy)."""
        rows = self._rows(f"""
            WITH now AS (SELECT {PREDICTION_NOW} AS t)
            SELECT zone_id, model_version, predicted_for, predicted_power_kw, actual_power_kw, abs_error_kw
            FROM prediction_accuracy, now
            WHERE predicted_for > now.t - %s * interval '1 hour' AND predicted_for <= now.t
              AND (%s::text IS NULL OR zone_id = %s)
            ORDER BY predicted_for""", (_hours(hours, 12), zone_id, zone_id))
        accuracy = self._rows("""
            SELECT zone_id, model_version, count(*) AS n, avg(abs_error_kw) AS mae_kw,
                   sqrt(avg(abs_error_kw ^ 2)) AS rmse_kw, avg(actual_power_kw) AS mean_actual_kw
            FROM prediction_accuracy GROUP BY zone_id, model_version ORDER BY zone_id""")
        return {"rows": rows, "accuracy": accuracy}

    def optimizer_savings(self, hours: Any = 24) -> list[dict[str, Any]]:
        """Optimizer's predicted baseline vs. optimized power per hour (approved decisions, executed or not)."""
        return self._rows(f"""
            WITH now AS (SELECT {SENSOR_NOW} AS t)
            SELECT date_trunc('hour', created_at) AS hour, zone_id, count(*) AS decisions,
                   avg((parameters->>'predicted_power_baseline_kw')::float) AS baseline_kw,
                   avg((parameters->>'predicted_power_optimized_kw')::float) AS optimized_kw
            FROM optimization_actions, now
            WHERE source = 'optimizer' AND status IN ('approved', 'executed')
              AND parameters ? 'predicted_power_baseline_kw'
              AND created_at > now.t - %s * interval '1 hour' AND created_at <= now.t
            GROUP BY 1, 2 ORDER BY 1, 2""", (_hours(hours, 24),))

    def comparison(self) -> dict[str, Any] | None:
        """Measured closed-loop experiment results (experiments.comparison --json), if generated."""
        if self.comparison_path is None or not Path(self.comparison_path).is_file():
            return None
        data = json.loads(Path(self.comparison_path).read_text(encoding="utf-8"))
        return {"source": Path(self.comparison_path).name, "cases": jsonable(data)}

    # -- security ------------------------------------------------------------------------
    def alerts(self, limit: Any = 100, alert_type: str | None = None, zone_id: str | None = None
               ) -> list[dict[str, Any]]:
        return self._rows("""
            SELECT alert_id, zone_id, detected_at, alert_type, severity, detector, score, description, details,
                   status, details->>'classification' AS classification
            FROM security_alerts
            WHERE (%(type)s::text IS NULL OR alert_type = %(type)s) AND (%(zone)s::text IS NULL OR zone_id = %(zone)s)
            ORDER BY updated_at DESC, detected_at DESC LIMIT %(limit)s""",
                          {"type": alert_type, "zone": zone_id, "limit": _limit(limit, 100)})

    def alert_timeline(self, hours: Any = 48) -> list[dict[str, Any]]:
        return self._rows(f"""
            WITH now AS (SELECT {ALERT_NOW} AS t)
            SELECT date_trunc('hour', detected_at) AS hour,
                   COALESCE(details->>'classification', alert_type) AS classification, count(*) AS alerts
            FROM security_alerts, now
            WHERE detected_at > now.t - %s * interval '1 hour' AND detected_at <= now.t
            GROUP BY 1, 2 ORDER BY 1, 2""", (_hours(hours, 48),))

    def drift(self, limit: Any = 50) -> list[dict[str, Any]]:
        """Drift alerts with per-feature KS/PSI; security drift vs. data drift."""
        rows = self.alerts(limit=limit, alert_type="drift")
        for row in rows:
            details = row.get("details") or {}
            row["kind"] = ("data drift (model validity)" if row["classification"] == "data_drift"
                           else "security drift (possible manipulation)")
            row["features"] = details.get("features", [])
            row["window"] = [details.get("window_start"), details.get("window_end")]
        return rows

    def attacks(self, limit: Any = 50) -> list[dict[str, Any]]:
        return self._rows("""
            SELECT a.*, (SELECT count(*) FROM security_alerts s WHERE s.zone_id = a.zone_id
                         AND s.detected_at >= a.starts_at AND s.detected_at < a.ends_at + interval '30 minutes')
                        AS alerts_during_attack,
                   (SELECT min(s.detected_at) FROM security_alerts s WHERE s.zone_id = a.zone_id
                         AND s.detected_at >= a.starts_at AND s.detected_at < a.ends_at + interval '30 minutes')
                        AS first_alert_at
            FROM simulated_attacks a ORDER BY requested_at DESC LIMIT %s""", (_limit(limit, 50),))

    # -- agent -------------------------------------------------------------------------
    def decisions(self, limit: Any = 30) -> list[dict[str, Any]]:
        return self._rows("""
            SELECT d.*, a.alert_type, a.severity, a.description AS alert_description,
                   a.details->>'classification' AS classification
            FROM agent_decisions d LEFT JOIN security_alerts a ON a.alert_id = d.alert_id
            ORDER BY d.finished_at DESC LIMIT %s""", (_limit(limit, 30),))

    def actions(self, limit: Any = 100, source: str | None = None) -> list[dict[str, Any]]:
        return self._rows("""
            SELECT action_id, zone_id, created_at, updated_at, source, action_type, parameters, status, reason,
                   safety_reason, related_alert_id
            FROM optimization_actions WHERE (%s::text IS NULL OR source = %s)
            ORDER BY updated_at DESC LIMIT %s""", (source, source, _limit(limit, 100)))

    def action_summary(self) -> list[dict[str, Any]]:
        return self._rows("SELECT source, action_type, status, count(*) AS n FROM optimization_actions "
                          "GROUP BY 1, 2, 3 ORDER BY 1, 2, 3")
