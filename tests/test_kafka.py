"""Unit tests for the streaming layer using fake Kafka clients (no broker needed)."""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

from config import ConfigurationError, KafkaSettings
from streaming import topics
from streaming.consumer import (
    ConsumedMessage,
    JsonConsumer,
    KafkaConsumerFatalError,
    TransientProcessingError,
)
from streaming.kafka_config import KafkaUnavailableError, make_error_callback, wait_for_broker
from streaming.producer import JsonProducer, ProducerError
from streaming.serialization import (
    DeserializationError,
    SerializationError,
    deserialize_value,
    serialize_value,
)


@pytest.fixture
def settings() -> KafkaSettings:
    return KafkaSettings(bootstrap_servers="localhost:9092", client_id="test",
                         consumer_group_prefix="sb", topic_prefix="t.")


# --------------------------------------------------------------------------- fakes
class FakeMessage:
    def __init__(self, value=None, *, key=None, topic="t.building_sensor_data", partition=0,
                 offset=0, error=None, headers=None):
        self._value, self._key, self._topic = value, key, topic
        self._partition, self._offset, self._error = partition, offset, error
        self._headers = headers

    def value(self): return self._value
    def key(self): return self._key
    def topic(self): return self._topic
    def partition(self): return self._partition
    def offset(self): return self._offset
    def error(self): return self._error
    def headers(self): return self._headers
    def timestamp(self): return (1, 1_700_000_000_000)


class FakeProducer:
    def __init__(self, config):
        self.config = config
        self.produced = []
        self.pending = []
        self.buffer_errors = 0          # number of produce() calls that raise BufferError
        self.delivery_error = None      # KafkaError to report on delivery

    def produce(self, topic, value, key, headers, on_delivery):
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        self.produced.append({"topic": topic, "value": value, "key": key, "headers": headers})
        self.pending.append((on_delivery, FakeMessage(value, key=key, topic=topic,
                                                      offset=len(self.produced) - 1)))

    def poll(self, timeout):
        served = len(self.pending)
        for callback, message in self.pending:
            callback(self.delivery_error, message)
        self.pending.clear()
        return served

    def flush(self, timeout):
        self.poll(timeout)
        return 0


class FakeConsumer:
    def __init__(self, config, messages=()):
        self.config = config
        self.messages = list(messages)
        self.commits = []
        self.stored = []
        self.subscribed = None
        self.closed = False

    def subscribe(self, topics, on_assign=None, on_revoke=None):
        self.subscribed = topics

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None

    def store_offsets(self, message):
        self.stored.append(message.offset())

    def commit(self, message=None, asynchronous=True):
        if not self.stored:
            raise KafkaException(KafkaError(KafkaError._NO_OFFSET))
        self.commits.append(("sync", asynchronous))

    def close(self):
        self.closed = True


def make_consumer(settings, messages, **kwargs) -> tuple[JsonConsumer, FakeConsumer]:
    holder = {}

    def factory(config):
        holder["fake"] = FakeConsumer(config, messages)
        return holder["fake"]

    consumer = JsonConsumer(settings, "unit", [topics.BUILDING_SENSOR_DATA],
                            consumer_factory=factory, retry_backoff_s=0.001, **kwargs)
    return consumer, holder["fake"]


def event_bytes(**fields) -> bytes:
    return json.dumps({"event_id": "e1", "zone_id": "Z1", **fields}).encode()


# --------------------------------------------------------------------------- serialization
def test_serialization_round_trip_with_datetime():
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    payload = serialize_value({"zone_id": "Z1", "at": when, "name": "Büro"})
    assert deserialize_value(payload) == {"zone_id": "Z1", "at": when.isoformat(), "name": "Büro"}


@pytest.mark.parametrize("value", [["not", "a", "dict"], {"x": object()}, {"x": float("nan")}])
def test_serialization_rejects_invalid_values(value):
    with pytest.raises(SerializationError):
        serialize_value(value)


@pytest.mark.parametrize("payload", [None, b"{broken", b"[1, 2]", b"\xff\xfe"])
def test_deserialization_rejects_invalid_payloads(payload):
    with pytest.raises(DeserializationError):
        deserialize_value(payload)


# --------------------------------------------------------------------------- topics
def test_required_topics_are_registered():
    assert set(topics.TOPICS) == {
        "building_sensor_data", "weather_data", "carbon_intensity",
        "security_alerts", "optimization_commands", "agent_actions", "energy_predictions",
        "simulation_control"}
    assert topics.TOPICS["building_sensor_data"].key == "zone_id"
    assert all(spec.partitions >= 1 for spec in topics.TOPICS.values())
    assert topics.TOPICS["weather_data"].kafka_config()["retention.ms"] == str(30 * topics.DAY_MS)


def test_topic_name_mapping(settings):
    assert topics.topic_name(settings, "weather_data") == "t.weather_data"
    assert topics.logical_name(settings, "t.weather_data") == "weather_data"
    with pytest.raises(ConfigurationError, match="Unknown Kafka topic"):
        topics.topic_name(settings, "sensor.readings")


class FakeFuture:
    def __init__(self, error=None):
        self.error = error

    def result(self):
        if self.error:
            raise KafkaException(KafkaError(self.error))


class FakeAdmin:
    def __init__(self, existing, create_errors=None):
        self.existing = existing
        self.create_errors = create_errors or {}
        self.created = []

    def list_topics(self, timeout):
        return SimpleNamespace(topics={name: SimpleNamespace(partitions={i: None for i in range(n)})
                                       for name, n in self.existing.items()})

    def create_topics(self, new_topics, operation_timeout):
        self.created.extend(new_topics)
        return {t.topic: FakeFuture(self.create_errors.get(t.topic)) for t in new_topics}


def test_ensure_topics_creates_only_missing(settings):
    admin = FakeAdmin({"t.weather_data": 1},
                      create_errors={"t.agent_actions": KafkaError.TOPIC_ALREADY_EXISTS})
    result = topics.ensure_topics(admin, settings)
    assert result["t.weather_data"] == "exists"
    assert result["t.agent_actions"] == "exists"     # created concurrently elsewhere
    assert result["t.building_sensor_data"] == "created"
    created = {t.topic: t for t in admin.created}
    assert "t.weather_data" not in created
    assert created["t.building_sensor_data"].num_partitions == 5


def test_ensure_topics_raises_on_other_errors(settings):
    admin = FakeAdmin({}, create_errors={"t.weather_data": KafkaError.POLICY_VIOLATION})
    with pytest.raises(KafkaException):
        topics.ensure_topics(admin, settings)


# --------------------------------------------------------------------------- producer
def make_producer(settings, **kwargs) -> tuple[JsonProducer, FakeProducer]:
    holder = {}

    def factory(config):
        holder["fake"] = FakeProducer(config)
        return holder["fake"]

    return JsonProducer(settings, producer_factory=factory, **kwargs), holder["fake"]


def test_producer_sends_json_with_zone_key(settings):
    producer, fake = make_producer(settings, name="sim")
    producer.send("building_sensor_data", {"zone_id": "Z3", "value": 1}, key="Z3",
                  headers={"event_type": "sensor_reading"})
    sent = fake.produced[0]
    assert sent["topic"] == "t.building_sensor_data" and sent["key"] == b"Z3"
    assert json.loads(sent["value"]) == {"zone_id": "Z3", "value": 1}
    assert ("content-type", b"application/json") in sent["headers"]
    assert ("event_type", b"sensor_reading") in sent["headers"]
    assert fake.config["client.id"] == "test.sim" and callable(fake.config["error_cb"])
    assert producer.stats.sent == 1 and producer.stats.delivered == 1 and producer.stats.pending == 0


def test_producer_delivery_failure_is_counted_and_reported(settings, caplog):
    failures = []
    producer, fake = make_producer(settings, on_delivery_failure=lambda e, t, k: failures.append((t, k)))
    fake.delivery_error = KafkaError(KafkaError._MSG_TIMED_OUT)
    with caplog.at_level(logging.ERROR):
        producer.send("security_alerts", {"a": 1}, key="Z1")
    assert producer.stats.failed == 1 and producer.stats.delivered == 0
    assert failures == [("t.security_alerts", "Z1")]
    assert "Kafka delivery failed" in caplog.text


def test_producer_retries_when_local_queue_is_full(settings):
    producer, fake = make_producer(settings)
    fake.buffer_errors = 2
    producer.send("weather_data", {"t": 1})
    assert producer.stats.queue_full_retries == 2 and len(fake.produced) == 1


def test_producer_gives_up_when_queue_stays_full(settings):
    producer, fake = make_producer(settings)
    fake.buffer_errors = 100
    with pytest.raises(ProducerError, match="queue full"):
        producer.send("weather_data", {"t": 1})
    assert producer.stats.sent == 0


def test_producer_rejects_unknown_topic_and_bad_values(settings):
    producer, _ = make_producer(settings)
    with pytest.raises(ConfigurationError):
        producer.send("not_a_topic", {"a": 1})
    with pytest.raises(SerializationError):
        producer.send("weather_data", {"a": object()})


def test_producer_context_manager_flushes(settings):
    producer, fake = make_producer(settings)
    with producer:
        fake.produce("x", b"{}", None, [], lambda e, m: None)
    assert fake.pending == []


# --------------------------------------------------------------------------- consumer
def test_consumer_config_and_subscription(settings):
    consumer, fake = make_consumer(settings, [])
    consumer.run(lambda m: None, max_messages=0)
    assert fake.subscribed == ["t.building_sensor_data"]
    assert fake.config["group.id"] == "sb.unit" and fake.config["enable.auto.offset.store"] is False


def test_consumer_decodes_and_commits_messages(settings):
    msgs = [FakeMessage(event_bytes(n=i), key=b"Z1", offset=i,
                        headers=[("content-type", b"application/json")]) for i in range(3)]
    consumer, fake = make_consumer(settings, msgs)
    received: list[ConsumedMessage] = []
    stats = consumer.run(received.append, max_messages=3)
    assert stats.processed == 3 and [m.value["n"] for m in received] == [0, 1, 2]
    first = received[0]
    assert first.topic == "building_sensor_data" and first.key == "Z1"
    assert first.headers == {"content-type": "application/json"} and first.timestamp_ms
    assert fake.stored == [0, 1, 2]


def test_consumer_skips_poison_messages(settings):
    msgs = [FakeMessage(b"{not json", offset=0), FakeMessage(event_bytes(), offset=1)]
    consumer, fake = make_consumer(settings, msgs)
    received = []
    stats = consumer.run(received.append, max_messages=2)
    assert stats.invalid == 1 and stats.processed == 1 and len(received) == 1
    assert fake.stored == [0, 1]  # poison message not re-read


def test_consumer_retries_handler_then_succeeds(settings):
    consumer, _ = make_consumer(settings, [FakeMessage(event_bytes())])
    calls = []

    def flaky(message):
        calls.append(message)
        if len(calls) < 3:
            raise RuntimeError("database busy")

    stats = consumer.run(flaky, max_messages=1)
    assert len(calls) == 3 and stats.processed == 1 and stats.handler_failures == 0


def test_consumer_skips_message_after_handler_retries(settings):
    consumer, fake = make_consumer(settings, [FakeMessage(event_bytes(), offset=7)],
                                   max_handler_retries=2)
    calls = []

    def broken(message):
        calls.append(1)
        raise ValueError("bad data")

    stats = consumer.run(broken, max_messages=1)
    assert len(calls) == 3 and stats.handler_failures == 1 and stats.processed == 0
    assert fake.stored == [7]


def test_consumer_retries_transient_errors_without_skipping(settings):
    consumer, fake = make_consumer(settings, [FakeMessage(event_bytes(), offset=4)],
                                   max_handler_retries=1)
    calls = []

    def database_down_then_up(message):
        calls.append(1)
        if len(calls) < 6:  # more failures than max_handler_retries
            raise TransientProcessingError("database unavailable")

    stats = consumer.run(database_down_then_up, max_messages=1)
    assert len(calls) == 6 and stats.processed == 1 and stats.handler_failures == 0
    assert stats.transient_retries == 5 and fake.stored == [4]


def test_consumer_does_not_store_offset_when_stopped_during_failure(settings):
    consumer, fake = make_consumer(settings, [FakeMessage(event_bytes(), offset=9),
                                              FakeMessage(event_bytes(), offset=10)])
    stop = threading.Event()

    def failing(message):
        stop.set()  # shutdown requested while the database is down
        raise TransientProcessingError("database unavailable")

    stats = consumer.run(failing, stop_event=stop)
    assert stats.processed == 0 and fake.stored == []  # message 9 will be redelivered
    assert len(fake.messages) == 1                      # message 10 was never polled


def test_consumer_survives_transient_errors(settings, caplog):
    msgs = [FakeMessage(error=KafkaError(KafkaError._PARTITION_EOF)),
            FakeMessage(error=KafkaError(KafkaError._TRANSPORT, "broker gone")),
            FakeMessage(error=KafkaError(KafkaError._ALL_BROKERS_DOWN)),
            FakeMessage(event_bytes())]
    consumer, _ = make_consumer(settings, msgs)
    with caplog.at_level(logging.WARNING):
        stats = consumer.run(lambda m: None, max_messages=1)
    assert stats.processed == 1 and stats.errors == 2  # EOF is not an error
    assert "consumer will reconnect" in caplog.text


def test_consumer_warns_about_missing_topic(settings, caplog):
    consumer, _ = make_consumer(settings, [FakeMessage(error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)),
                                           FakeMessage(event_bytes())])
    with caplog.at_level(logging.WARNING):
        consumer.run(lambda m: None, max_messages=1)
    assert "python -m streaming.topics" in caplog.text


def test_consumer_raises_on_fatal_error(settings):
    consumer, _ = make_consumer(settings, [FakeMessage(error=KafkaError(KafkaError._FATAL, fatal=True))])
    with pytest.raises(KafkaConsumerFatalError):
        consumer.run(lambda m: None, max_messages=1)


def test_consumer_stops_on_event_and_idle_timeout(settings):
    consumer, _ = make_consumer(settings, [])
    stop = threading.Event()
    stop.set()
    assert consumer.run(lambda m: None, stop_event=stop).received == 0
    stats = consumer.run(lambda m: None, idle_timeout_s=0.05, poll_timeout_s=0.01)
    assert stats.received == 0


def test_consumer_close_commits_and_is_idempotent(settings):
    consumer, fake = make_consumer(settings, [FakeMessage(event_bytes())])
    consumer.run(lambda m: None, max_messages=1)
    consumer.close()
    consumer.close()
    assert fake.closed and ("sync", False) in fake.commits


def test_consumer_requires_topics(settings):
    with pytest.raises(ValueError):
        JsonConsumer(settings, "x", [], consumer_factory=FakeConsumer)


# --------------------------------------------------------------------------- connection helpers
@pytest.mark.parametrize(("code", "level", "text"), [
    (KafkaError._ALL_BROKERS_DOWN, logging.WARNING, "All Kafka brokers are down"),
    (KafkaError._TRANSPORT, logging.WARNING, "reconnecting automatically"),
    (KafkaError._SSL, logging.ERROR, "Kafka client error"),
])
def test_error_callback_levels(caplog, code, level, text):
    with caplog.at_level(logging.DEBUG):
        make_error_callback("unit")(KafkaError(code))
    record = caplog.records[-1]
    assert record.levelno == level and text in record.getMessage()


def test_error_callback_fatal(caplog):
    with caplog.at_level(logging.DEBUG):
        make_error_callback("unit")(KafkaError(KafkaError._FATAL, fatal=True))
    assert caplog.records[-1].levelno == logging.CRITICAL


class FlakyAdmin:
    def __init__(self, failures):
        self.failures = failures

    def list_topics(self, timeout):
        if self.failures:
            self.failures -= 1
            raise KafkaException(KafkaError(KafkaError._TRANSPORT))
        return SimpleNamespace(brokers={1: None}, topics={})


def test_wait_for_broker_retries_until_available(settings):
    admin = FlakyAdmin(failures=2)
    wait_for_broker(settings, timeout_s=10, admin_factory=lambda config: admin)
    assert admin.failures == 0


def test_wait_for_broker_times_out(settings):
    with pytest.raises(KafkaUnavailableError, match="not reachable"):
        wait_for_broker(settings, timeout_s=0.3, admin_factory=lambda config: FlakyAdmin(10**6))
