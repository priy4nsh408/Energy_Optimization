"""Service: real-time energy prediction from Kafka sensor events.

Consumes ``building_sensor_data`` (own consumer group), keeps a short history
per zone, computes the same features as in training and publishes the
predicted average power of each zone's *next* interval to
``energy_predictions``. ``workers.record_persistence`` stores the predictions,
and the ``prediction_accuracy`` view compares them with the actual readings.

    python -m ml.train --dataset building ...      # train the deployable model first
    python -m workers.prediction_service
    python -m workers.prediction_service --model-version 20260105T120000Z

The KAG benchmark model cannot be deployed here: its features (nine rooms of a
house, Wh) do not exist in the building's sensor events.
"""

from __future__ import annotations

import argparse
import sys
import threading
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

from confluent_kafka import KafkaException

from config import ConfigurationError, get_config
from ml.dataset import BUILDING_COLUMNS, prepare_building_frame
from ml.features import BUILDING_SPEC
from ml.predict import IncompatibleModelError, OnlinePredictor
from ml.registry import ModelBundle, ModelNotFoundError, load_bundle
from processors.data_validator import ValidationError, validate_sensor_event
from streaming.consumer import ConsumedMessage, JsonConsumer, KafkaConsumerFatalError
from streaming.kafka_config import KafkaUnavailableError, create_admin_client, wait_for_broker
from streaming.producer import JsonProducer
from streaming.topics import BUILDING_SENSOR_DATA, ENERGY_PREDICTIONS, ensure_topics
from utils.logging_config import configure_logging
from utils.shutdown import install_shutdown_handlers

SERVICE = "prediction_service"
_NAMESPACE = uuid.UUID("0c5a7a52-9b1e-4f34-8a63-2d4f7b6e1a01")
INTERVAL_MISMATCH_TOLERANCE = 0.10


class PredictionHandler:
    """Turns validated sensor events into prediction messages."""

    def __init__(self, bundle: ModelBundle, producer: JsonProducer, logger) -> None:
        if bundle.spec.name != BUILDING_SPEC.name:
            raise IncompatibleModelError(
                f"Model '{bundle.spec.name}' cannot be served from building sensor events; "
                f"train one with: python -m ml.train --dataset building")
        self.bundle = bundle
        self.producer = producer
        self.logger = logger
        self.online = OnlinePredictor(bundle, prepare_building_frame)
        self.training_interval_s = bundle.metadata.get("data_interval_seconds")
        self.stats: Counter[str] = Counter()
        self._warned_interval = False

    def __call__(self, message: ConsumedMessage) -> None:
        try:
            reading = validate_sensor_event(message.value)
        except ValidationError as exc:
            self.stats["invalid"] += 1
            self.logger.debug("Invalid event skipped", extra={"errors": exc.errors})
            return
        row = {column: getattr(reading, column) for column in BUILDING_COLUMNS}
        row["event_id"] = str(reading.event_id)
        self._check_interval(reading.interval_seconds)

        prediction = self.online.update(row)
        if prediction is None:
            self.stats["no_prediction"] += 1   # warming up, duplicate or gap
            return
        horizon_s = reading.interval_seconds * self.bundle.spec.horizon
        predicted_for = reading.event_time + timedelta(seconds=horizon_s)
        self.producer.send(ENERGY_PREDICTIONS, {
            "prediction_id": str(uuid.uuid5(_NAMESPACE, f"{reading.event_id}|{self.bundle.version}")),
            "source_event_id": str(reading.event_id),
            "building_id": reading.building_id,
            "zone_id": reading.zone_id,
            "event_time": reading.event_time.isoformat(),
            "predicted_for": predicted_for.isoformat(),
            "horizon_seconds": horizon_s,
            "predicted_power_kw": round(prediction.value, 4),
            "predicted_energy_kwh": round(prediction.value * horizon_s / 3600, 6),
            "model_name": self.bundle.model_name,
            "model_version": self.bundle.version,
            "created_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }, key=reading.zone_id)
        self.stats["predictions"] += 1

    def _check_interval(self, interval_s: float) -> None:
        trained = self.training_interval_s
        if self._warned_interval or not trained:
            return
        if abs(interval_s - trained) > INTERVAL_MISMATCH_TOLERANCE * trained:
            self._warned_interval = True
            self.logger.warning(
                "Live reading interval differs from the training data; lag features cover a "
                "different time span and accuracy may drop",
                extra={"live_interval_s": interval_s, "training_interval_s": trained})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=f"python -m workers.{SERVICE}",
                                     description="Publish real-time energy predictions.")
    parser.add_argument("--model-version", help="model version (default: latest)")
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
        bundle = load_bundle(BUILDING_SPEC.name, args.model_version)
        wait_for_broker(config.KAFKA, timeout_s=args.broker_wait)
        ensure_topics(create_admin_client(config.KAFKA), config.KAFKA)
    except ModelNotFoundError as exc:
        logger.error("No model available", extra={"error": str(exc)})
        return 1
    except (KafkaUnavailableError, KafkaException) as exc:
        logger.error("Kafka not available", extra={"error": str(exc)})
        return 1

    producer = JsonProducer(config.KAFKA, name=SERVICE)
    try:
        handler = PredictionHandler(bundle, producer, logger)
    except IncompatibleModelError as exc:
        logger.error("Incompatible model", extra={"error": str(exc)})
        producer.close()
        return 1
    test_metrics = bundle.metadata.get("metrics", {}).get(bundle.model_name, {}).get("test", {})
    logger.info("Prediction service started", extra={
        "model": bundle.model_name, "version": bundle.version,
        "history_required": bundle.spec.history_required,
        "training_interval_s": handler.training_interval_s, "offline_test_metrics": test_metrics})

    # Real-time service: a new consumer group starts at the newest events
    # instead of predicting thousands of historical readings.
    consumer = JsonConsumer(config.KAFKA, SERVICE, [BUILDING_SENSOR_DATA], auto_offset_reset="latest")
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
        logger.info("Prediction service stopped", extra=dict(handler.stats))
    return 0


if __name__ == "__main__":
    sys.exit(main())
