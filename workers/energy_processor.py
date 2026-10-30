"""Service: validate sensor events, engineer features and store ``energy_records``.

Also listens to ``carbon_intensity`` to attach CO2e emissions to each record.

    python -m workers.energy_processor
"""

from __future__ import annotations

import sys
import threading

from database.db import Database
from database.repositories import (
    fetch_recent_energy,
    fetch_zones,
    insert_record,
    latest_carbon_observation,
)
from processors.data_validator import validate_carbon_message, validate_sensor_event
from processors.energy_processor import EnergyProcessor
from processors.feature_engineering import DEFAULT_WINDOW, FeatureEngineer
from processors.sensor_processor import validate_or_reject
from streaming.consumer import ConsumedMessage
from streaming.topics import BUILDING_SENSOR_DATA, CARBON_INTENSITY
from workers.base import PersistenceHandler, parse_worker_args, run_worker

SERVICE = "energy_processor"


class EnergyProcessorHandler(PersistenceHandler):
    worker_name = SERVICE

    def __init__(self, db: Database, logger, processor: EnergyProcessor) -> None:
        super().__init__(db, logger)
        self.processor = processor
        self._unknown_zones: set[str] = set()

    @classmethod
    def create(cls, db: Database, logger) -> "EnergyProcessorHandler":
        """Load zone metadata, recent history and the latest carbon intensity."""
        with db.transaction() as conn:
            zones = fetch_zones(conn)
            recent = fetch_recent_energy(conn, DEFAULT_WINDOW)
            carbon = latest_carbon_observation(conn)
        features = FeatureEngineer(zones)
        loaded = features.warm_start(recent)
        intensity, observed_at = carbon if carbon else (None, None)
        logger.info("Energy processor state loaded", extra={
            "zones": len(zones), "history_points": loaded, "carbon_intensity_g_per_kwh": intensity,
            "carbon_observed_at": observed_at.isoformat() if observed_at else None})
        return cls(db, logger, EnergyProcessor(features, intensity, observed_at))

    def handle(self, message: ConsumedMessage) -> None:
        if message.topic == CARBON_INTENSITY:
            record, invalid = validate_or_reject(SERVICE, message, validate_carbon_message)
            if invalid:
                self.reject(invalid)
            elif self.processor.update_carbon_intensity(record):
                self.stats["carbon_updates"] += 1
            return

        reading, invalid = validate_or_reject(
            SERVICE, message, validate_sensor_event,
            kafka_partition=message.partition, kafka_offset=message.offset)
        if invalid:
            self.reject(invalid)
            return
        self._refresh_zone_metadata(reading.zone_id)
        record = self.processor.process(reading)
        inserted = self.write(message, lambda conn: insert_record(conn, record))
        if inserted:
            self.stats["stored"] += 1
        elif inserted is False:
            self.stats["duplicates"] += 1

    def _refresh_zone_metadata(self, zone_id: str) -> None:
        """Reload zones once when an unknown zone appears (e.g. registry updated)."""
        features = self.processor.features
        if zone_id in features.zones or zone_id in self._unknown_zones:
            return
        with self.db.transaction() as conn:
            features.zones.update(fetch_zones(conn))
        if zone_id not in features.zones:
            self._unknown_zones.add(zone_id)
            self.logger.warning("Zone not in registry; area/capacity features will be NULL "
                                "(run: python -m database init)", extra={"zone_id": zone_id})


def main(argv: list[str] | None = None, stop_event: threading.Event | None = None) -> int:
    args = parse_worker_args(f"python -m workers.{SERVICE}", __doc__.splitlines()[0], argv)
    return run_worker(service=SERVICE, group=SERVICE,
                      topics=[BUILDING_SENSOR_DATA, CARBON_INTENSITY], args=args,
                      build_handler=lambda db, config, logger: EnergyProcessorHandler.create(db, logger),
                      stop_event=stop_event)


if __name__ == "__main__":
    sys.exit(main())
