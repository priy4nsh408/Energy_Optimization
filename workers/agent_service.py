"""Service: investigate security alerts with the LangGraph incident agent.

Consumes ``security_alerts`` (own consumer group, newest first). For each alert
that passes the dispatcher (severity, cooldown, containment) the agent
investigates with data from PostgreSQL, and every proposal goes through the
deterministic safety validator.

* Approved control commands are published to ``optimization_commands``
  (source ``agent``, status ``approved``). They are executed only in the
  simulator: run ``python -m workers.sensor_publisher --apply-commands``,
  which applies them and reports ``executed``/``failed`` on ``agent_actions``.
* Sensor isolation is a software quarantine held by this service.
* Decisions are stored in ``agent_decisions`` and the JSON Lines audit log;
  action outcomes in ``optimization_actions`` and on ``agent_actions``.
* The quarantine (isolated sensors) is restored from PostgreSQL at start-up.
  A background thread runs :class:`agent.recovery.RecoveryPolicy` every
  ``AGENT_RECOVERY_INTERVAL_S``: sensors are restored once their zone has been
  quiet for an hour; restores are recorded like every other action, so the
  optimizer resumes the zone. Investigations and recovery share one lock, so
  they never act on the same state concurrently.
* Detectors for the investigation tools follow ``CONTROL_MODE`` /
  ``SECURITY_DETECTORS`` (the same set as ``workers.security_service``).

The LLM provider is chosen with ``AGENT_LLM_PROVIDER`` (see config.AgentSettings).

    python -m workers.agent_service
"""

from __future__ import annotations

import argparse
import sys
import threading
from collections import Counter

from confluent_kafka import KafkaException

from agent.data import PostgresDataSource
from agent.dispatch import IncidentDispatcher
from agent.executor import KafkaCommandGateway
from agent.graph import IncidentAgent
from agent.isolation import SensorIsolationRegistry
from agent.llm import create_llm
from agent.recorder import CompositeRecorder, JsonlRecorder, KafkaActionRecorder, PostgresRecorder
from agent.recovery import RecoveryPolicy
from agent.safety import SafetyValidator
from agent.tools import AgentToolbox
from config import ConfigurationError, get_config
from database.db import Database, DatabaseUnavailableError
from optimization.hvac_optimizer import building_models
from optimization.optimizer import ZoneOptimizer
from processors.data_validator import ValidationError, validate_alert_message
from security.train import SecurityModelNotFoundError, load_configured_models
from streaming.consumer import ConsumedMessage, JsonConsumer, KafkaConsumerFatalError
from streaming.kafka_config import KafkaUnavailableError, create_admin_client, wait_for_broker
from streaming.producer import JsonProducer
from streaming.topics import AGENT_ACTIONS, OPTIMIZATION_COMMANDS, SECURITY_ALERTS, ensure_topics
from utils.logging_config import configure_logging
from utils.shutdown import install_shutdown_handlers

SERVICE = "agent_service"


class AgentHandler:
    def __init__(self, agent: IncidentAgent, dispatcher: IncidentDispatcher, logger,
                 lock: threading.Lock | None = None) -> None:
        self.agent, self.dispatcher, self.logger = agent, dispatcher, logger
        self.lock = lock or threading.Lock()
        self.stats: Counter[str] = Counter()

    def __call__(self, message: ConsumedMessage) -> None:
        alert = message.value
        try:
            validate_alert_message(alert)
        except ValidationError as exc:
            self.stats["invalid"] += 1
            self.logger.debug("Invalid alert skipped", extra={"errors": exc.errors})
            return
        investigate, why = self.dispatcher.should_investigate(alert)
        if not investigate:
            self.stats["skipped"] += 1
            self.logger.debug("Alert not investigated", extra={"alert_id": alert["alert_id"], "reason": why})
            return
        with self.lock:
            decision = self.agent.run(alert)
            self.dispatcher.remember(alert, decision)
        self.stats["investigated"] += 1
        self.logger.info("Incident handled", extra={
            "alert_id": alert["alert_id"], "zone_id": alert.get("zone_id"), "provider": decision.llm_provider,
            "likely_cause": decision.likely_cause, "confidence": decision.confidence,
            "outcome": decision.outcome, "errors": decision.errors})


class RecoveryLoop:
    """Periodically restore isolated sensors whose zone is quiet (simulated time from PostgreSQL)."""

    def __init__(self, recovery: RecoveryPolicy, data: PostgresDataSource, recorder, lock: threading.Lock,
                 logger, interval_s: float) -> None:
        self.recovery, self.data, self.recorder, self.lock = recovery, data, recorder, lock
        self.logger, self.interval_s = logger, interval_s
        self.restored = 0

    def run_once(self) -> int:
        now = self.data.now()
        if now is None:
            return 0
        with self.lock:
            actions = self.recovery.check(now)
        for action in actions:
            self.recorder.record_actions([action])
            self.logger.info("Recovery action", extra={"zone_id": action.zone_id, "status": action.status,
                                                       "sensor": action.parameters.get("sensor")})
        self.restored += sum(a.status == "executed" for a in actions)
        return len(actions)

    def run(self, stop_event: threading.Event) -> None:
        while not stop_event.wait(self.interval_s):
            try:
                self.run_once()
            except Exception:  # noqa: BLE001 - keep the loop alive (e.g. database briefly down)
                self.logger.exception("Recovery check failed")


def main(argv: list[str] | None = None, stop_event: threading.Event | None = None) -> int:
    parser = argparse.ArgumentParser(prog=f"python -m workers.{SERVICE}",
                                     description="Investigate security alerts with the incident agent.")
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--idle-timeout", type=float)
    parser.add_argument("--broker-wait", type=float, default=60.0)
    parser.add_argument("--from-beginning", action="store_true",
                        help="a new consumer group starts at the oldest alerts (default: newest)")
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
        security_models = load_configured_models(config)
    except SecurityModelNotFoundError as exc:
        logger.warning("No trained detectors: anomaly/drift tools unavailable", extra={"error": str(exc)})
        security_models = None
    database = Database(config.DATABASE, name=SERVICE)
    try:
        database.open()
        database.require_schema()
        wait_for_broker(config.KAFKA, timeout_s=args.broker_wait)
        ensure_topics(create_admin_client(config.KAFKA), config.KAFKA)
    except (DatabaseUnavailableError, RuntimeError, KafkaUnavailableError, KafkaException) as exc:
        logger.error("Dependencies not available", extra={"error": str(exc)})
        database.close()
        return 1

    settings = config.AGENT
    producer = JsonProducer(config.KAFKA, name=SERVICE)
    isolation = SensorIsolationRegistry()
    restored = isolation.load_from_database(database)
    models = building_models()
    validator = SafetyValidator(settings, config.OPTIMIZATION, models, isolation)
    data = PostgresDataSource(database)
    toolbox = AgentToolbox(data, isolation, validator, ZoneOptimizer(config.OPTIMIZATION, models), security_models)
    recorder = CompositeRecorder(JsonlRecorder(settings.audit_log), PostgresRecorder(database),
                                 KafkaActionRecorder(producer, AGENT_ACTIONS))
    llm = create_llm(settings)
    agent = IncidentAgent(llm=llm, toolbox=toolbox, data=data, isolation=isolation, validator=validator,
                          gateway=KafkaCommandGateway(producer, isolation, OPTIMIZATION_COMMANDS),
                          recorder=recorder, max_iterations=settings.max_iterations)
    lock = threading.Lock()
    handler = AgentHandler(agent, IncidentDispatcher(settings, isolation), logger, lock)
    action_recorder = CompositeRecorder(PostgresRecorder(database), KafkaActionRecorder(producer, AGENT_ACTIONS))
    recovery = RecoveryLoop(RecoveryPolicy(data, isolation, validator,
                                           KafkaCommandGateway(producer, isolation, OPTIMIZATION_COMMANDS)),
                            data, action_recorder, lock, logger, config.SYSTEM.recovery_interval_s)
    recovery_thread = threading.Thread(target=recovery.run, args=(stop_event,), name="recovery", daemon=True)
    recovery_thread.start()
    consumer = JsonConsumer(config.KAFKA, SERVICE, [SECURITY_ALERTS],
                            auto_offset_reset="earliest" if args.from_beginning else "latest")
    logger.info("Agent service started", extra={
        "provider": llm.provider, "model": llm.model, "min_severity": settings.min_alert_severity,
        "detectors": security_models.metadata.get("version") if security_models else None,
        "detector_set": config.SYSTEM.detector_set, "isolated_sensors_restored": restored})
    try:
        consumer.run(handler, stop_event=stop_event, max_messages=args.max_messages,
                     idle_timeout_s=args.idle_timeout)
    except KafkaConsumerFatalError:
        return 1
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        stop_event.set()
        recovery_thread.join(timeout=10)
        consumer.close()
        producer.close()
        database.close()
        logger.info("Agent service stopped", extra={**handler.stats, "restored": recovery.restored})
    return 0


if __name__ == "__main__":
    sys.exit(main())
