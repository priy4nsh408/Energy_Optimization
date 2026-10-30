"""Service: validate sensor events and store them in ``sensor_readings``.

    python -m workers.sensor_persistence
"""

from __future__ import annotations

import sys
import threading

from database.repositories import insert_record
from processors.data_validator import validate_sensor_event
from processors.sensor_processor import validate_or_reject
from streaming.consumer import ConsumedMessage
from streaming.topics import BUILDING_SENSOR_DATA
from workers.base import PersistenceHandler, parse_worker_args, run_worker

SERVICE = "sensor_persistence"


class SensorPersistenceHandler(PersistenceHandler):
    worker_name = SERVICE

    def handle(self, message: ConsumedMessage) -> None:
        record, invalid = validate_or_reject(
            SERVICE, message, validate_sensor_event,
            kafka_partition=message.partition, kafka_offset=message.offset)
        if invalid:
            self.reject(invalid)
            return
        inserted = self.write(message, lambda conn: insert_record(conn, record))
        if inserted:
            self.stats["stored"] += 1
        elif inserted is False:
            self.stats["duplicates"] += 1
            self.logger.debug("Duplicate sensor event ignored", extra={"event_id": str(record.event_id)})


def main(argv: list[str] | None = None, stop_event: threading.Event | None = None) -> int:
    args = parse_worker_args(f"python -m workers.{SERVICE}", __doc__.splitlines()[0], argv)
    return run_worker(service=SERVICE, group=SERVICE, topics=[BUILDING_SENSOR_DATA], args=args,
                      build_handler=lambda db, config, logger: SensorPersistenceHandler(db, logger),
                      stop_event=stop_event)


if __name__ == "__main__":
    sys.exit(main())
