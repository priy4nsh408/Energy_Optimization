"""Service: poll weather and carbon-intensity providers and publish to Kafka.

Publishes to ``weather_data`` and ``carbon_intensity`` (key: building id).
``workers.record_persistence`` stores the values in PostgreSQL and
``workers.energy_processor`` uses the latest carbon intensity for CO2e.

    python -m workers.external_data_publisher                   # poll forever
    python -m workers.external_data_publisher --once            # single fetch
    python -m workers.external_data_publisher --once --backfill-hours 24

Providers are chosen with WEATHER_PROVIDER / CARBON_PROVIDER (``mock`` for
offline development).
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from confluent_kafka import KafkaException

from config import ConfigurationError, get_config
from data_sources.factory import create_carbon_provider, create_weather_provider
from data_sources.http_client import ApiAuthError, ApiRequestError, ApiUnavailableError
from streaming.kafka_config import KafkaUnavailableError, create_admin_client, wait_for_broker
from streaming.producer import JsonProducer
from streaming.topics import CARBON_INTENSITY, WEATHER_DATA, ensure_topics
from utils.logging_config import configure_logging
from utils.shutdown import install_shutdown_handlers

RETRY_AFTER_FAILURE_S = 60.0
RETRY_AFTER_AUTH_FAILURE_S = 3600.0


@dataclass
class PollTask:
    """One provider polled on its own schedule."""

    name: str
    topic: str
    fetch: Callable[[int], list[Any]]
    interval_s: float
    next_run: float = 0.0
    backfill_hours: int = 0
    stats: dict[str, int] = field(default_factory=lambda: {
        "runs": 0, "published": 0, "failures": 0})


class ExternalDataPublisher:
    def __init__(self, producer: JsonProducer, building_id: str, tasks: list[PollTask], logger) -> None:
        self.producer = producer
        self.building_id = building_id
        self.tasks = tasks
        self.logger = logger

    def run_task(self, task: PollTask) -> float:
        """Fetch and publish once; returns seconds until the next run."""
        task.stats["runs"] += 1
        try:
            observations = task.fetch(task.backfill_hours)
            task.backfill_hours = 0  # back-fill only on the first run
        except ApiAuthError as exc:
            task.stats["failures"] += 1
            self.logger.error("API authentication failed", extra={"task": task.name, "error": str(exc)})
            return RETRY_AFTER_AUTH_FAILURE_S
        except ApiUnavailableError as exc:
            task.stats["failures"] += 1
            delay = max(RETRY_AFTER_FAILURE_S, exc.retry_after_s or 0)
            self.logger.warning("Provider unavailable; will retry", extra={
                "task": task.name, "error": str(exc), "retry_in_s": round(delay)})
            return delay
        except ApiRequestError as exc:
            task.stats["failures"] += 1
            self.logger.error("Provider returned an unusable response",
                              extra={"task": task.name, "error": str(exc)})
            return task.interval_s

        for observation in observations:
            self.producer.send(task.topic, observation.to_message(self.building_id),
                               key=self.building_id)
        self.producer.flush(10)
        task.stats["published"] += len(observations)
        latest = [o for o in observations if not o.is_forecast]
        self.logger.info("External data published", extra={
            "task": task.name, "topic": task.topic, "observations": len(observations),
            "forecasts": len(observations) - len(latest),
            "latest_observed_at": max((o.observed_at for o in latest), default=None),
            "source": observations[0].source if observations else None})
        return task.interval_s

    def run(self, stop_event: threading.Event, *, once: bool = False) -> None:
        while not stop_event.is_set():
            now = time.monotonic()
            for task in self.tasks:
                if now >= task.next_run:
                    task.next_run = time.monotonic() + self.run_task(task)
            if once:
                return
            next_due = min(task.next_run for task in self.tasks)
            stop_event.wait(max(1.0, next_due - time.monotonic()))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m workers.external_data_publisher",
                                     description="Publish weather and carbon intensity to Kafka.")
    parser.add_argument("--once", action="store_true", help="fetch once and exit")
    parser.add_argument("--backfill-hours", type=int, default=0,
                        help="also publish the last N hours on the first run")
    parser.add_argument("--only", choices=["weather", "carbon"], help="run a single provider")
    parser.add_argument("--broker-wait", type=float, default=60.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None, stop_event: threading.Event | None = None) -> int:
    args = parse_args(argv)
    try:
        config = get_config()
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    logger = configure_logging(config.LOGGING, "external_data_publisher")
    if stop_event is None:
        stop_event = threading.Event()
        install_shutdown_handlers(stop_event)

    settings = config.EXTERNAL_DATA
    tasks: list[PollTask] = []
    try:
        if args.only in (None, "weather"):
            weather = create_weather_provider(config)
            tasks.append(PollTask(f"weather:{weather.name}", WEATHER_DATA,
                                  lambda hours, p=weather: p.fetch(past_hours=hours),
                                  settings.weather_poll_interval_s, backfill_hours=args.backfill_hours))
        if args.only in (None, "carbon"):
            carbon = create_carbon_provider(config)
            tasks.append(PollTask(f"carbon:{carbon.name}", CARBON_INTENSITY,
                                  lambda hours, p=carbon: p.fetch(past_hours=hours),
                                  settings.carbon_poll_interval_s, backfill_hours=args.backfill_hours))
    except ConfigurationError as exc:
        logger.error("Provider configuration error", extra={"error": str(exc)})
        return 2

    try:
        wait_for_broker(config.KAFKA, timeout_s=args.broker_wait)
        ensure_topics(create_admin_client(config.KAFKA), config.KAFKA)
    except (KafkaUnavailableError, KafkaException) as exc:
        logger.error("Kafka not available; publisher not started", extra={"error": str(exc)})
        return 1

    producer = JsonProducer(config.KAFKA, name="external_data_publisher")
    publisher = ExternalDataPublisher(producer, config.SIMULATOR.building_id, tasks, logger)
    logger.info("External data publisher started", extra={
        "tasks": [t.name for t in tasks], "latitude": settings.latitude,
        "longitude": settings.longitude})
    try:
        publisher.run(stop_event, once=args.once)
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        producer.close()
    failures = sum(t.stats["failures"] for t in tasks)
    logger.info("External data publisher stopped", extra={t.name: t.stats for t in tasks})
    return 0 if not (args.once and failures) else 1


if __name__ == "__main__":
    sys.exit(main())
