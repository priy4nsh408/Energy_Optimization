"""Service: run the building simulator and publish readings to Kafka.

Each sensor event is published to ``building_sensor_data`` with the zone id as
the message key, so all readings of a zone go to the same partition and stay
in order.

    python -m workers.sensor_publisher                        # real time
    python -m workers.sensor_publisher --interval 1 --speed 60
    python -m workers.sensor_publisher --fast --ticks 100 --seed 1
"""

from __future__ import annotations

import argparse
import sys
import threading

from confluent_kafka import KafkaException

from config import ConfigurationError, get_config
from simulator.__main__ import add_simulation_arguments, build_settings
from simulator.engine import BuildingSimulator
from streaming.kafka_config import KafkaUnavailableError, create_admin_client, wait_for_broker
from streaming.producer import JsonProducer
from streaming.topics import BUILDING_SENSOR_DATA, ensure_topics
from utils.logging_config import configure_logging
from utils.shutdown import install_shutdown_handlers


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m workers.sensor_publisher",
                                     description="Publish simulated sensor readings to Kafka.")
    add_simulation_arguments(parser)
    parser.add_argument("--broker-wait", type=float, default=60.0,
                        help="seconds to wait for Kafka at start-up (default 60)")
    parser.add_argument("--no-create-topics", action="store_true",
                        help="do not create missing topics at start-up")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None, stop_event: threading.Event | None = None) -> int:
    args = parse_args(argv)
    try:
        config = get_config()
        settings = build_settings(config.SIMULATOR, args)
    except (ConfigurationError, ValueError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    logger = configure_logging(config.LOGGING, "sensor_publisher")
    if stop_event is None:
        stop_event = threading.Event()
        install_shutdown_handlers(stop_event)

    try:
        wait_for_broker(config.KAFKA, timeout_s=args.broker_wait)
        if not args.no_create_topics:
            ensure_topics(create_admin_client(config.KAFKA), config.KAFKA)
    except (KafkaUnavailableError, KafkaException) as exc:
        logger.error("Kafka not available; publisher not started", extra={"error": str(exc)})
        return 1

    producer = JsonProducer(config.KAFKA, name="sensor_publisher")
    simulator = BuildingSimulator(settings)

    def publish(event: dict) -> None:
        producer.send(BUILDING_SENSOR_DATA, event, key=event["zone_id"],
                      headers={"event_type": event["event_type"]})

    try:
        simulator.run(publish, max_ticks=args.ticks, realtime=not args.fast, stop_event=stop_event)
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        producer.close(timeout_s=30)
    stats = producer.stats
    logger.info("Sensor publisher finished", extra={
        "sent": stats.sent, "delivered": stats.delivered, "failed": stats.failed,
        "rejected_by_sink": simulator.sink_failures})
    return 0 if stats.failed == 0 and simulator.sink_failures == 0 and stats.pending == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
