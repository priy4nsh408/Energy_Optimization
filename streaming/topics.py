"""Kafka topic registry and topic creation.

This module is the single source of truth for topic names, partition counts,
retention and message-key conventions. Code refers to topics by their
*logical* name (e.g. ``building_sensor_data``); the physical name adds the
configured ``KAFKA_TOPIC_PREFIX`` (``test.`` in the testing environment).

Create or inspect topics from the command line::

    python -m streaming.topics            # create missing topics
    python -m streaming.topics --list     # show the registry
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import NewTopic

from config import ConfigurationError, KafkaSettings

logger = logging.getLogger(__name__)

DAY_MS = 24 * 60 * 60 * 1000

# Logical topic names.
BUILDING_SENSOR_DATA = "building_sensor_data"
WEATHER_DATA = "weather_data"
CARBON_INTENSITY = "carbon_intensity"
SECURITY_ALERTS = "security_alerts"
OPTIMIZATION_COMMANDS = "optimization_commands"
AGENT_ACTIONS = "agent_actions"


@dataclass(frozen=True)
class TopicSpec:
    """Definition of one topic."""

    name: str
    partitions: int
    retention_days: int
    key: str
    description: str
    extra_config: dict[str, str] = field(default_factory=dict)

    def kafka_config(self) -> dict[str, str]:
        return {"retention.ms": str(self.retention_days * DAY_MS),
                "cleanup.policy": "delete", **self.extra_config}


TOPICS: dict[str, TopicSpec] = {spec.name: spec for spec in (
    TopicSpec(BUILDING_SENSOR_DATA, partitions=5, retention_days=7, key="zone_id",
              description="Sensor readings from every zone (one event per zone per interval)"),
    TopicSpec(WEATHER_DATA, partitions=1, retention_days=30, key="building_id",
              description="Outdoor weather observations and forecasts"),
    TopicSpec(CARBON_INTENSITY, partitions=1, retention_days=30, key="building_id",
              description="Grid carbon intensity (gCO2e/kWh)"),
    TopicSpec(SECURITY_ALERTS, partitions=3, retention_days=30, key="zone_id",
              description="Anomaly, attack and drift alerts"),
    TopicSpec(OPTIMIZATION_COMMANDS, partitions=3, retention_days=7, key="zone_id",
              description="Validated control commands for HVAC and lighting"),
    TopicSpec(AGENT_ACTIONS, partitions=3, retention_days=30, key="zone_id",
              description="Agent proposals, safety decisions and executed actions (audit trail)"),
)}


def topic_name(settings: KafkaSettings, logical_name: str) -> str:
    """Physical topic name for a registered logical name."""
    if logical_name not in TOPICS:
        raise ConfigurationError(
            f"Unknown Kafka topic '{logical_name}'. Known topics: {sorted(TOPICS)}")
    return settings.topic(logical_name)


def logical_name(settings: KafkaSettings, physical_name: str) -> str:
    """Strip the configured prefix from a physical topic name."""
    prefix = settings.topic_prefix
    return physical_name[len(prefix):] if prefix and physical_name.startswith(prefix) else physical_name


def ensure_topics(admin: Any, settings: KafkaSettings, *, timeout_s: float = 15.0,
                  specs: list[TopicSpec] | None = None) -> dict[str, str]:
    """Create any missing topics; existing topics are left unchanged.

    Returns a mapping of physical topic name -> ``"created"`` or ``"exists"``.

    Raises:
        KafkaException: if the broker rejects a topic for another reason.
    """
    specs = specs or list(TOPICS.values())
    existing = admin.list_topics(timeout=timeout_s).topics
    result: dict[str, str] = {}
    to_create: list[NewTopic] = []
    for spec in specs:
        physical = topic_name(settings, spec.name)
        if physical in existing:
            result[physical] = "exists"
            partitions = len(existing[physical].partitions)
            if partitions < spec.partitions:
                logger.warning("Topic has fewer partitions than configured", extra={
                    "topic": physical, "partitions": partitions, "expected": spec.partitions})
        else:
            to_create.append(NewTopic(physical, num_partitions=spec.partitions,
                                      replication_factor=settings.replication_factor,
                                      config=spec.kafka_config()))
    if to_create:
        futures = admin.create_topics(to_create, operation_timeout=timeout_s)
        for physical, future in futures.items():
            try:
                future.result()
                result[physical] = "created"
                logger.info("Kafka topic created", extra={"topic": physical})
            except KafkaException as exc:
                if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    result[physical] = "exists"   # created concurrently by another service
                else:
                    logger.error("Failed to create Kafka topic",
                                 extra={"topic": physical, "error": str(exc)})
                    raise
    return result


def main(argv: list[str] | None = None) -> int:
    from config import get_config
    from streaming.kafka_config import KafkaUnavailableError, create_admin_client, wait_for_broker
    from utils.logging_config import configure_logging

    parser = argparse.ArgumentParser(prog="python -m streaming.topics",
                                     description="Create the project's Kafka topics.")
    parser.add_argument("--list", action="store_true", help="print the topic registry and exit")
    parser.add_argument("--wait", type=float, default=30.0, help="seconds to wait for the broker")
    args = parser.parse_args(argv)

    config = get_config()
    if args.list:
        for spec in TOPICS.values():
            print(f"{config.KAFKA.topic(spec.name):<32} partitions={spec.partitions} "
                  f"retention={spec.retention_days}d key={spec.key}  {spec.description}")
        return 0

    configure_logging(config.LOGGING, "topics", stream=sys.stderr)
    try:
        wait_for_broker(config.KAFKA, timeout_s=args.wait)
        result = ensure_topics(create_admin_client(config.KAFKA), config.KAFKA)
    except (KafkaUnavailableError, KafkaException) as exc:
        print(f"Topic creation failed: {exc}", file=sys.stderr)
        return 1
    for name, status in sorted(result.items()):
        print(f"{name:<32} {status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
