"""Service: run the building simulator and publish readings to Kafka.

Each sensor event is published to ``building_sensor_data`` with the zone id as
the message key, so all readings of a zone go to the same partition and stay
in order.

    python -m workers.sensor_publisher                        # real time
    python -m workers.sensor_publisher --interval 1 --speed 60
    python -m workers.sensor_publisher --fast --ticks 100 --seed 1
    python -m workers.sensor_publisher --apply-commands   # also execute approved agent commands

With ``--apply-commands`` the publisher also consumes ``optimization_commands``
and applies *approved* commands from the selected sources (default: the agent,
whose commands passed the safety validator) to the simulated building between
readings, reporting ``executed``/``failed`` on ``agent_actions``.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
from pathlib import Path

from confluent_kafka import KafkaException

from config import ConfigurationError, get_config
from simulator.__main__ import add_simulation_arguments, build_settings
from simulator.attack_simulator import AttackInjector, AttackScenario
from simulator.engine import BuildingSimulator
from streaming.consumer import JsonConsumer
from streaming.kafka_config import KafkaUnavailableError, create_admin_client, wait_for_broker
from streaming.producer import JsonProducer
from streaming.topics import AGENT_ACTIONS, BUILDING_SENSOR_DATA, OPTIMIZATION_COMMANDS, ensure_topics
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
    parser.add_argument("--attack", action="append", default=[], metavar="SPEC",
                        help="inject a simulated attack (research/testing): "
                             "TYPE:ZONE:START_MIN:DURATION_MIN[:key=value,...]; repeatable")
    parser.add_argument("--labels", type=Path,
                        help="write ground-truth attack labels (JSON Lines) to this file")
    parser.add_argument("--apply-commands", action="store_true",
                        help="execute approved commands from optimization_commands in the simulator")
    parser.add_argument("--command-sources", default="agent",
                        help="comma-separated sources whose approved commands are executed (default: agent)")
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
    injector = None
    if args.attack:
        try:
            scenarios = [AttackScenario.parse(spec, simulator.sim_time) for spec in args.attack]
        except ValueError as exc:
            logger.error("Invalid attack specification", extra={"error": str(exc)})
            producer.close()
            return 2
        injector = AttackInjector(scenarios)
        logger.warning("ATTACK SIMULATION ENABLED: published readings will be manipulated",
                       extra={"attacks": args.attack})

    actuator = consumer = consumer_thread = None
    if args.apply_commands:
        from agent.executor import SimulatorActuator

        sources = {s.strip() for s in args.command_sources.split(",") if s.strip()}
        actuator = SimulatorActuator(simulator, producer, AGENT_ACTIONS, sources)
        consumer = JsonConsumer(config.KAFKA, "sensor_publisher_actuator", [OPTIMIZATION_COMMANDS],
                                auto_offset_reset="latest")
        consumer_thread = threading.Thread(target=consumer.run, args=(actuator.handle,),
                                           kwargs={"stop_event": stop_event}, name="actuator", daemon=True)
        consumer_thread.start()
        logger.info("Executing approved commands in the simulator", extra={"sources": sorted(sources)})

    def publish(event: dict) -> None:
        if actuator is not None:
            actuator.drain()
        if injector is not None:
            event = injector.apply(event)
        producer.send(BUILDING_SENSOR_DATA, event, key=event["zone_id"],
                      headers={"event_type": event["event_type"]})

    try:
        simulator.run(publish, max_ticks=args.ticks, realtime=not args.fast, stop_event=stop_event)
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        if consumer is not None:
            stop_event.set()
            consumer_thread.join(timeout=10)
            consumer.close()
            logger.info("Actuator stopped", extra=actuator.stats)
        producer.close(timeout_s=30)
        if injector is not None and args.labels:
            args.labels.parent.mkdir(parents=True, exist_ok=True)
            with args.labels.open("w", encoding="utf-8") as handle:
                for label in injector.labels:
                    handle.write(json.dumps({**vars(label), "timestamp": label.timestamp.isoformat()}) + "\n")
    stats = producer.stats
    logger.info("Sensor publisher finished", extra={
        "sent": stats.sent, "delivered": stats.delivered, "failed": stats.failed,
        "rejected_by_sink": simulator.sink_failures})
    return 0 if stats.failed == 0 and simulator.sink_failures == 0 and stats.pending == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
