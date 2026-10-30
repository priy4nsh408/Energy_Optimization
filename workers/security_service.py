"""Service: real-time security monitoring of sensor data.

Consumes ``building_sensor_data`` (own consumer group, newest events first),
runs schema validation, rule checks, Isolation Forest and KS/PSI drift
detection (``security.monitor``) and publishes alerts to ``security_alerts``.
``workers.record_persistence`` stores them in PostgreSQL.

    python -m security.train                 # once: train detectors on clean data
    python -m workers.security_service
"""

from __future__ import annotations

import argparse
import sys
import threading
from collections import Counter

from confluent_kafka import KafkaException

from config import ConfigurationError, get_config
from security.monitor import SecurityMonitor
from security.train import SecurityModelNotFoundError, load_models
from streaming.consumer import ConsumedMessage, JsonConsumer, KafkaConsumerFatalError
from streaming.kafka_config import KafkaUnavailableError, create_admin_client, wait_for_broker
from streaming.producer import JsonProducer
from streaming.topics import BUILDING_SENSOR_DATA, SECURITY_ALERTS, ensure_topics
from utils.logging_config import configure_logging
from utils.shutdown import install_shutdown_handlers

SERVICE = "security_service"


class SecurityHandler:
    def __init__(self, monitor: SecurityMonitor, producer: JsonProducer, logger) -> None:
        self.monitor = monitor
        self.producer = producer
        self.logger = logger
        self.stats: Counter[str] = Counter()

    def __call__(self, message: ConsumedMessage) -> None:
        output = self.monitor.process(message.value)
        self.stats["readings"] += 1
        if not output.valid:
            self.stats["invalid"] += 1
        elif output.detection and output.detection.flagged:
            self.stats[f"detected_{output.detection.attack_type}"] += 1
        if output.drift is not None and output.drift.is_drift:
            self.stats["drift_windows"] += 1
        for alert in output.alerts:
            self.producer.send(SECURITY_ALERTS, alert, key=alert["zone_id"] or "building")
            self.stats[f"alert_{alert['alert_type']}"] += 1
            self.logger.warning("Security alert", extra={
                "zone_id": alert["zone_id"], "alert_type": alert["alert_type"],
                "classification": alert["details"].get("classification"), "severity": alert["severity"],
                "detector": alert["detector"]})


def main(argv: list[str] | None = None, stop_event: threading.Event | None = None) -> int:
    parser = argparse.ArgumentParser(prog=f"python -m workers.{SERVICE}",
                                     description="Detect attacks, anomalies and drift in sensor data.")
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--idle-timeout", type=float)
    parser.add_argument("--broker-wait", type=float, default=60.0)
    parser.add_argument("--from-beginning", action="store_true",
                        help="a new consumer group starts at the oldest events (default: newest)")
    args = parser.parse_args(argv)
    try:
        config = get_config()
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    logger = configure_logging(config.LOGGING, SERVICE)
    if stop_event is None:
        stop_event = threading.Event()
        install_shutdown_handlers(stop_event)
    try:
        models = load_models()
        wait_for_broker(config.KAFKA, timeout_s=args.broker_wait)
        ensure_topics(create_admin_client(config.KAFKA), config.KAFKA)
    except SecurityModelNotFoundError as exc:
        logger.error("No trained detectors", extra={"error": str(exc)})
        return 1
    except (KafkaUnavailableError, KafkaException) as exc:
        logger.error("Kafka not available", extra={"error": str(exc)})
        return 1

    producer = JsonProducer(config.KAFKA, name=SERVICE)
    handler = SecurityHandler(SecurityMonitor(models), producer, logger)
    consumer = JsonConsumer(config.KAFKA, SERVICE, [BUILDING_SENSOR_DATA],
                            auto_offset_reset="earliest" if args.from_beginning else "latest")
    logger.info("Security service started", extra={"detectors": models.metadata.get("version")})
    try:
        consumer.run(handler, stop_event=stop_event, max_messages=args.max_messages,
                     idle_timeout_s=args.idle_timeout)
    except KafkaConsumerFatalError:
        return 1
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        consumer.close()
        producer.close()
        logger.info("Security service stopped", extra=dict(handler.stats))
    return 0


if __name__ == "__main__":
    sys.exit(main())
