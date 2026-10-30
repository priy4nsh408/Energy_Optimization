"""Service: store weather, carbon, alerts, optimization/agent actions and energy predictions.

    python -m workers.record_persistence
"""

from __future__ import annotations

import sys
import threading

from database.repositories import (
    insert_record,
    upsert_optimization_action,
    upsert_security_alert,
)
from processors.data_validator import (
    validate_action_message,
    validate_alert_message,
    validate_carbon_message,
    validate_prediction_message,
    validate_weather_message,
)
from processors.sensor_processor import validate_or_reject
from streaming.consumer import ConsumedMessage
from streaming.topics import (
    AGENT_ACTIONS,
    CARBON_INTENSITY,
    ENERGY_PREDICTIONS,
    OPTIMIZATION_COMMANDS,
    SECURITY_ALERTS,
    WEATHER_DATA,
)
from workers.base import PersistenceHandler, parse_worker_args, run_worker

SERVICE = "record_persistence"
TOPICS = [WEATHER_DATA, CARBON_INTENSITY, SECURITY_ALERTS, OPTIMIZATION_COMMANDS, AGENT_ACTIONS,
          ENERGY_PREDICTIONS]


class RecordPersistenceHandler(PersistenceHandler):
    worker_name = SERVICE

    def handle(self, message: ConsumedMessage) -> None:
        topic = message.topic
        if topic == WEATHER_DATA:
            validator, writer = validate_weather_message, insert_record
        elif topic == CARBON_INTENSITY:
            validator, writer = validate_carbon_message, insert_record
        elif topic == ENERGY_PREDICTIONS:
            validator, writer = validate_prediction_message, insert_record
        elif topic == SECURITY_ALERTS:
            validator, writer = validate_alert_message, upsert_security_alert
        elif topic in (OPTIMIZATION_COMMANDS, AGENT_ACTIONS):
            record, invalid = validate_or_reject(SERVICE, message, validate_action_message, topic=topic)
            self._store(message, record, invalid, upsert_optimization_action)
            return
        else:
            self.logger.warning("Message from unexpected topic ignored", extra={"topic": topic})
            return
        record, invalid = validate_or_reject(SERVICE, message, validator)
        self._store(message, record, invalid, writer)

    def _store(self, message, record, invalid, writer) -> None:
        if invalid:
            self.reject(invalid)
            return
        result = self.write(message, lambda conn: writer(conn, record))
        if result is None:
            return
        # insert_record -> bool; upserts -> 'inserted' | 'updated' | 'unchanged'
        outcome = {True: "inserted", False: "duplicate"}.get(result, result)
        self.stats[f"{message.topic}.{outcome}"] += 1


def main(argv: list[str] | None = None, stop_event: threading.Event | None = None) -> int:
    args = parse_worker_args(f"python -m workers.{SERVICE}", __doc__.splitlines()[0], argv)
    return run_worker(service=SERVICE, group=SERVICE, topics=TOPICS, args=args,
                      build_handler=lambda db, config, logger: RecordPersistenceHandler(db, logger),
                      stop_event=stop_event)


if __name__ == "__main__":
    sys.exit(main())
