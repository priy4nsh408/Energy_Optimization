"""Database administration.

    python -m database init      # create database (if missing), tables, zone registry
    python -m database status    # schema version and row counts
"""

from __future__ import annotations

import argparse
import sys

from config import ConfigurationError, get_config
from database.db import Database, DatabaseUnavailableError, create_database_if_missing
from database.models import ZoneRecord
from database.repositories import table_counts, upsert_zone
from simulator.building_model import default_zones
from utils.logging_config import configure_logging


def zone_registry(building_id: str) -> list[ZoneRecord]:
    """Zone metadata of the reference building (as defined by the simulator)."""
    return [ZoneRecord(zone_id=z.zone_id, building_id=building_id, name=z.name,
                       zone_type=z.zone_type, floor_area_m2=z.floor_area_m2,
                       max_occupancy=z.max_occupancy, occupied_setpoint_c=z.occupied_setpoint_c)
            for z in default_zones()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m database", description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["init", "status"])
    parser.add_argument("--wait", type=float, default=30.0, help="seconds to wait for PostgreSQL")
    args = parser.parse_args(argv)

    try:
        config = get_config()
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    configure_logging(config.LOGGING, "database", stream=sys.stderr)
    settings = config.DATABASE

    try:
        if args.command == "init":
            created = create_database_if_missing(settings)
            print(f"database {settings.name}: {'created' if created else 'exists'}")
        with Database(settings, name="admin") as db:
            if args.command == "init":
                db.apply_schema()
                zones = zone_registry(config.SIMULATOR.building_id)
                with db.transaction() as conn:
                    for zone in zones:
                        upsert_zone(conn, zone)
                print(f"schema version {db.schema_version()} applied; {len(zones)} zones registered")
            else:
                print(f"database {settings.safe_dsn}, schema version {db.schema_version()}")
                db.require_schema()
                with db.transaction() as conn:
                    for table, count in table_counts(conn).items():
                        print(f"  {table:<26} {count:>10}")
    except DatabaseUnavailableError as exc:
        print(f"Database unavailable: {exc}", file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
