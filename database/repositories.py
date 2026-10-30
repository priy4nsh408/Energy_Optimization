"""Data access: idempotent inserts and queries.

Every insert is keyed by the record's own id, so a message that Kafka
delivers twice is stored once (``ON CONFLICT DO NOTHING``). Alerts and
actions can legitimately be published several times with a new status; those
upserts only move the status *forward* in its lifecycle, so an old or
duplicated message never overwrites a newer state.
"""

from __future__ import annotations

from dataclasses import asdict, fields
from datetime import datetime
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from database.models import (
    AgentDecisionRecord,
    CarbonIntensityRecord,
    EnergyPredictionRecord,
    EnergyRecord,
    InvalidEventRecord,
    OptimizationActionRecord,
    SecurityAlertRecord,
    SensorReadingRecord,
    WeatherRecord,
    ZoneRecord,
)

TABLES: dict[type, str] = {
    SensorReadingRecord: "sensor_readings",
    EnergyRecord: "energy_records",
    WeatherRecord: "weather_records",
    CarbonIntensityRecord: "carbon_intensity_records",
    InvalidEventRecord: "invalid_events",
    EnergyPredictionRecord: "energy_predictions",
    AgentDecisionRecord: "agent_decisions",
}

ALERT_STATUS_RANK = {"open": 0, "acknowledged": 1, "resolved": 2}
ACTION_STATUS_RANK = {"proposed": 0, "approved": 1, "rejected": 1, "executed": 2, "failed": 2}


def _values(record: Any) -> dict[str, Any]:
    values = asdict(record)
    for key, value in values.items():
        if isinstance(value, (dict, list)):
            values[key] = Jsonb(value)
    return values


def _insert_sql(table: str, columns: list[str], conflict: sql.Composable) -> sql.Composed:
    return sql.SQL("INSERT INTO {table} ({columns}) VALUES ({values}) {conflict}").format(
        table=sql.Identifier(table),
        columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
        values=sql.SQL(", ").join(map(sql.Placeholder, columns)),
        conflict=conflict,
    )


def insert_record(conn: psycopg.Connection, record: Any) -> bool:
    """Insert a record; returns ``False`` if it already existed (duplicate)."""
    table = TABLES[type(record)]
    columns = [f.name for f in fields(record)]
    cursor = conn.execute(_insert_sql(table, columns, sql.SQL("ON CONFLICT DO NOTHING")),
                          _values(record))
    return cursor.rowcount == 1


def _rank_case(column: sql.Composable, ranks: dict[str, int]) -> sql.Composed:
    whens = sql.SQL(" ").join(
        sql.SQL("WHEN {} THEN {}").format(sql.Literal(status), sql.Literal(rank))
        for status, rank in ranks.items())
    return sql.SQL("(CASE {} {} END)").format(column, whens)


def upsert_security_alert(conn: psycopg.Connection, record: SecurityAlertRecord) -> str:
    """Insert an alert or advance its status. Returns 'inserted', 'updated' or 'unchanged'."""
    columns = [f.name for f in fields(record)]
    existing = sql.SQL("security_alerts.status")
    incoming = sql.SQL("EXCLUDED.status")
    conflict = sql.SQL(
        "ON CONFLICT (alert_id) DO UPDATE SET status = EXCLUDED.status, "
        "resolved_at = COALESCE(EXCLUDED.resolved_at, security_alerts.resolved_at), "
        "details = security_alerts.details || EXCLUDED.details, updated_at = now() "
        "WHERE {incoming} > {existing} RETURNING (xmax = 0) AS inserted"
    ).format(incoming=_rank_case(incoming, ALERT_STATUS_RANK),
             existing=_rank_case(existing, ALERT_STATUS_RANK))
    row = conn.execute(_insert_sql("security_alerts", columns, conflict), _values(record)).fetchone()
    return "unchanged" if row is None else ("inserted" if row[0] else "updated")


def upsert_optimization_action(conn: psycopg.Connection, record: OptimizationActionRecord) -> str:
    """Insert an action or advance its status. Returns 'inserted', 'updated' or 'unchanged'."""
    columns = [f.name for f in fields(record)]
    existing = sql.SQL("optimization_actions.status")
    incoming = sql.SQL("EXCLUDED.status")
    conflict = sql.SQL(
        "ON CONFLICT (action_id) DO UPDATE SET status = EXCLUDED.status, "
        "updated_at = EXCLUDED.updated_at, "
        "safety_reason = COALESCE(EXCLUDED.safety_reason, optimization_actions.safety_reason), "
        "reason = COALESCE(EXCLUDED.reason, optimization_actions.reason), "
        "kafka_topic = EXCLUDED.kafka_topic "
        "WHERE {incoming} > {existing} RETURNING (xmax = 0) AS inserted"
    ).format(incoming=_rank_case(incoming, ACTION_STATUS_RANK),
             existing=_rank_case(existing, ACTION_STATUS_RANK))
    row = conn.execute(_insert_sql("optimization_actions", columns, conflict),
                       _values(record)).fetchone()
    return "unchanged" if row is None else ("inserted" if row[0] else "updated")


def upsert_zone(conn: psycopg.Connection, record: ZoneRecord) -> None:
    columns = [f.name for f in fields(record)]
    updates = sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in columns if c != "zone_id")
    conflict = sql.SQL("ON CONFLICT (zone_id) DO UPDATE SET {}, updated_at = now()").format(updates)
    conn.execute(_insert_sql("zones", columns, conflict), _values(record))


# -- queries -------------------------------------------------------------------------
def fetch_zones(conn: psycopg.Connection) -> dict[str, ZoneRecord]:
    names = [f.name for f in fields(ZoneRecord)]
    query = sql.SQL("SELECT {} FROM zones").format(sql.SQL(", ").join(map(sql.Identifier, names)))
    with conn.cursor(row_factory=dict_row) as cur:
        return {row["zone_id"]: ZoneRecord(**row) for row in cur.execute(query)}


def fetch_recent_energy(conn: psycopg.Connection, limit_per_zone: int) -> list[dict[str, Any]]:
    """Latest records per zone (oldest first) to warm-start rolling features."""
    query = """
        SELECT event_id, zone_id, event_time, total_power_kw, temperature_c FROM (
            SELECT event_id, zone_id, event_time, total_power_kw, temperature_c,
                   row_number() OVER (PARTITION BY zone_id ORDER BY event_time DESC) AS rn
            FROM energy_records) recent
        WHERE rn <= %s ORDER BY zone_id, event_time
    """
    with conn.cursor(row_factory=dict_row) as cur:
        return list(cur.execute(query, (limit_per_zone,)))


def latest_carbon_observation(conn: psycopg.Connection,
                              at: datetime | None = None) -> tuple[float, datetime] | None:
    """(intensity, observed_at) of the most recent observed (non-forecast) value at or before ``at``."""
    row = conn.execute(
        "SELECT intensity_g_per_kwh, observed_at FROM carbon_intensity_records "
        "WHERE NOT is_forecast AND observed_at <= COALESCE(%s, now()) "
        "ORDER BY observed_at DESC LIMIT 1", (at,)).fetchone()
    return (row[0], row[1]) if row else None


def latest_carbon_intensity(conn: psycopg.Connection, at: datetime | None = None) -> float | None:
    """Most recent observed (non-forecast) carbon intensity at or before ``at``."""
    observation = latest_carbon_observation(conn, at)
    return observation[0] if observation else None


def table_counts(conn: psycopg.Connection) -> dict[str, int]:
    tables = ["zones", "sensor_readings", "energy_records", "weather_records",
              "carbon_intensity_records", "security_alerts", "optimization_actions",
              "energy_predictions", "agent_decisions", "invalid_events"]
    return {t: conn.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(t))).fetchone()[0]
            for t in tables}
