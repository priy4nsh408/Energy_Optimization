"""Service: deterministic HVAC/lighting optimization -> ``optimization_commands``.

Consumes (own consumer group, starting at the newest events):
    building_sensor_data   current zone conditions (trigger decisions)
    energy_predictions     ML forecast for the same event (forecast basis)
    carbon_intensity       latest observed grid intensity (carbon weight)

For each decision the optimizer's output is re-validated against the hard
constraints and published with status ``approved`` or ``rejected`` (audit
trail). ``record_persistence`` stores the commands in ``optimization_actions``.
Executing approved commands in the building is a separate step (safety layer
and actuator module); this service never changes the building itself.

No LLM is involved: decisions are reproducible for the same inputs.

    python -m workers.optimization_service
"""

from __future__ import annotations

import argparse
import sys
import threading
import uuid
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import KafkaException

from config import ConfigurationError, OptimizationSettings, get_config
from database.models import SensorReadingRecord
from optimization.constraints import setpoint_band
from optimization.hvac_optimizer import ZoneConditions, building_models
from optimization.lighting_optimizer import is_lighting_short
from optimization.optimizer import Decision, DecisionScheduler, ZoneOptimizer
from processors.data_validator import (
    ValidationError,
    validate_carbon_message,
    validate_prediction_message,
    validate_sensor_event,
)
from streaming.consumer import ConsumedMessage, JsonConsumer, KafkaConsumerFatalError
from streaming.kafka_config import KafkaUnavailableError, create_admin_client, wait_for_broker
from streaming.producer import JsonProducer
from streaming.topics import (
    BUILDING_SENSOR_DATA,
    CARBON_INTENSITY,
    ENERGY_PREDICTIONS,
    OPTIMIZATION_COMMANDS,
    ensure_topics,
)
from utils.logging_config import configure_logging
from utils.shutdown import install_shutdown_handlers

SERVICE = "optimization_service"
_NAMESPACE = uuid.UUID("7d2f6f0e-31a9-4f0b-9f57-3e0c1b6a9a11")


class OptimizationHandler:
    def __init__(self, settings: OptimizationSettings, producer: JsonProducer, logger) -> None:
        self.settings = settings
        self.producer = producer
        self.logger = logger
        self.optimizer = ZoneOptimizer(settings, building_models())
        self.scheduler = DecisionScheduler(settings.decision_interval_s)
        self.carbon_intensity: float | None = None
        self._carbon_observed_at: datetime | None = None
        self._predictions: dict[str, tuple[str, float]] = {}       # zone -> (source event id, kW)
        self._pending: dict[str, SensorReadingRecord] = {}          # waiting for its prediction
        self._predictions_seen = False
        self.stats: Counter[str] = Counter()

    # -- message dispatch -------------------------------------------------------------
    def __call__(self, message: ConsumedMessage) -> None:
        try:
            if message.topic == CARBON_INTENSITY:
                self._on_carbon(message.value)
            elif message.topic == ENERGY_PREDICTIONS:
                self._on_prediction(message.value)
            else:
                self._on_reading(validate_sensor_event(message.value))
        except ValidationError as exc:
            self.stats["invalid"] += 1
            self.logger.debug("Invalid message skipped", extra={"topic": message.topic,
                                                                "errors": exc.errors})

    def _on_carbon(self, value: dict) -> None:
        record = validate_carbon_message(value)
        if record.is_forecast or (self._carbon_observed_at and record.observed_at <= self._carbon_observed_at):
            return
        self.carbon_intensity, self._carbon_observed_at = record.intensity_g_per_kwh, record.observed_at

    def _on_prediction(self, value: dict) -> None:
        record = validate_prediction_message(value)
        self._predictions_seen = True
        self._predictions[record.zone_id] = (str(record.source_event_id), record.predicted_power_kw)
        pending = self._pending.get(record.zone_id)
        if pending is not None and str(pending.event_id) == str(record.source_event_id):
            del self._pending[record.zone_id]
            self._decide(pending, record.predicted_power_kw)

    def _on_reading(self, reading: SensorReadingRecord) -> None:
        zone = reading.zone_id
        if zone not in self.optimizer.models:
            self.stats["unknown_zone"] += 1
            return
        stale = self._pending.pop(zone, None)
        if stale is not None:                       # its prediction never arrived
            self._decide(stale, None)
        conditions = ZoneConditions.from_reading(reading)
        short = is_lighting_short(self.optimizer.models[zone], conditions)
        if not self.scheduler.due(zone, reading.event_time, reading.occupancy_count, short):
            return
        prediction = self._predictions.get(zone)
        if prediction and prediction[0] == str(reading.event_id):
            self._decide(reading, prediction[1])
        elif self._predictions_seen:
            self._pending[zone] = reading           # wait briefly for the ML forecast
        else:
            self._decide(reading, None)

    # -- decisions --------------------------------------------------------------------
    def _decide(self, reading: SensorReadingRecord, predicted_power_kw: float | None) -> None:
        conditions = ZoneConditions.from_reading(reading)
        decision = self.optimizer.optimize(conditions, predicted_power_kw=predicted_power_kw,
                                           carbon_intensity=self.carbon_intensity)
        self.stats["decisions"] += 1
        self.stats[f"basis_{decision.basis}"] += 1
        band = setpoint_band(self.settings, self.optimizer.models[reading.zone_id], conditions)
        changes = []
        if decision.approved:
            if abs(decision.setpoint_c - decision.baseline_setpoint_c) >= self.settings.min_setpoint_change_c:
                changes.append(("set_setpoint", {"setpoint_c": decision.setpoint_c,
                                                 "previous_setpoint_c": decision.baseline_setpoint_c}))
            if abs(decision.lighting_level - decision.baseline_lighting_level) >= self.settings.min_lighting_change:
                changes.append(("set_lighting", {"lighting_level": decision.lighting_level,
                                                 "previous_lighting_level": decision.baseline_lighting_level}))
        else:
            self.stats["rejected"] += 1
            changes.append(("set_setpoint", {"setpoint_c": decision.setpoint_c,
                                             "lighting_level": decision.lighting_level}))
            self.logger.warning("Optimizer decision rejected by constraint check", extra={
                "zone_id": reading.zone_id, "violations": list(decision.violations)})
        for action_type, parameters in changes:
            self._publish(reading, decision, action_type, parameters, band.reason)

    def _publish(self, reading: SensorReadingRecord, decision: Decision, action_type: str,
                 parameters: dict, band_reason: str) -> None:
        status = "approved" if decision.approved else "rejected"
        self.producer.send(OPTIMIZATION_COMMANDS, {
            "action_id": str(uuid.uuid5(_NAMESPACE, f"{reading.event_id}|{action_type}")),
            "zone_id": reading.zone_id,
            "created_at": reading.event_time.isoformat(),
            "source": "optimizer",
            "action_type": action_type,
            "status": status,
            "parameters": {
                **parameters,
                "source_event_id": str(reading.event_id),
                "predicted_power_baseline_kw": decision.predicted_power_baseline_kw,
                "predicted_power_optimized_kw": decision.predicted_power_optimized_kw,
                "forecast_basis": decision.basis,
                "carbon_intensity_g_per_kwh": decision.carbon_intensity_g_per_kwh,
                "objective_baseline": decision.objective_baseline,
                "objective_optimized": decision.objective_optimized,
                "occupancy": reading.occupancy_count,
            },
            "reason": (f"{band_reason}; predicted saving {decision.predicted_saving_kw:.3f} kW "
                       f"({decision.message})"),
            "safety_reason": ("all optimizer constraints satisfied" if decision.approved
                              else "; ".join(decision.violations)),
            "emitted_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }, key=reading.zone_id)
        self.stats[f"published_{status}"] += 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=f"python -m workers.{SERVICE}",
                                     description="Publish optimized HVAC/lighting commands.")
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--idle-timeout", type=float)
    parser.add_argument("--broker-wait", type=float, default=60.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None, stop_event: threading.Event | None = None) -> int:
    args = parse_args(argv)
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
        wait_for_broker(config.KAFKA, timeout_s=args.broker_wait)
        ensure_topics(create_admin_client(config.KAFKA), config.KAFKA)
    except (KafkaUnavailableError, KafkaException) as exc:
        logger.error("Kafka not available", extra={"error": str(exc)})
        return 1

    producer = JsonProducer(config.KAFKA, name=SERVICE)
    handler = OptimizationHandler(config.OPTIMIZATION, producer, logger)
    consumer = JsonConsumer(config.KAFKA, SERVICE,
                            [BUILDING_SENSOR_DATA, ENERGY_PREDICTIONS, CARBON_INTENSITY],
                            auto_offset_reset="latest")
    logger.info("Optimization service started", extra={"settings": vars(config.OPTIMIZATION)})
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
        logger.info("Optimization service stopped", extra=dict(handler.stats))
    return 0


if __name__ == "__main__":
    sys.exit(main())
