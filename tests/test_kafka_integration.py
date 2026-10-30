"""Integration tests against a real Kafka broker.

Run ``docker compose -f docker/docker-compose.yml up -d`` first. The tests are
skipped automatically when no broker is reachable. Each test session uses
its own topic prefix and deletes its topics afterwards.
"""

from __future__ import annotations

import dataclasses
import threading
import time
import uuid
from collections import defaultdict

import pytest
from confluent_kafka.admin import AdminClient

from config import get_config
from simulator.engine import BuildingSimulator
from streaming.consumer import JsonConsumer
from streaming.kafka_config import KafkaUnavailableError, create_admin_client, wait_for_broker
from streaming.producer import JsonProducer
from streaming.topics import BUILDING_SENSOR_DATA, TOPICS, ensure_topics, topic_name
from tests.test_simulator import make_settings

pytestmark = pytest.mark.kafka


@pytest.fixture(scope="session")
def kafka():
    base = get_config("testing", load_dotenv_file=False).KAFKA
    settings = dataclasses.replace(base, topic_prefix=f"it_{uuid.uuid4().hex[:8]}.",
                                   consumer_group_prefix=f"it_{uuid.uuid4().hex[:8]}")
    try:
        wait_for_broker(settings, timeout_s=3)
    except KafkaUnavailableError:
        pytest.skip("Kafka broker not reachable (start docker/docker-compose.yml)")
    admin = create_admin_client(settings)
    ensure_topics(admin, settings)
    yield settings
    futures = admin.delete_topics([topic_name(settings, name) for name in TOPICS])
    for future in futures.values():
        future.result()


def publish_simulated_readings(settings, ticks: int, seed: int) -> list[dict]:
    simulator = BuildingSimulator(make_settings(random_seed=seed))
    sent = []
    with JsonProducer(settings, name="it") as producer:
        for _ in range(ticks):
            for event in simulator.tick():
                producer.send(BUILDING_SENSOR_DATA, event, key=event["zone_id"])
                sent.append(event)
        assert producer.flush(15) == 0
    assert producer.stats.delivered == len(sent) and producer.stats.failed == 0
    return sent


def consume_all(settings, group: str, expected: int, timeout_s: float = 20) -> list:
    received = []
    consumer = JsonConsumer(settings, group, [BUILDING_SENSOR_DATA])
    try:
        consumer.run(received.append, max_messages=expected, idle_timeout_s=timeout_s)
    finally:
        consumer.close()
    return received


def test_topics_exist_with_configured_partitions(kafka):
    metadata = AdminClient(kafka.admin_config()).list_topics(timeout=10)
    for spec in TOPICS.values():
        physical = topic_name(kafka, spec.name)
        assert physical in metadata.topics
        assert len(metadata.topics[physical].partitions) == spec.partitions
    assert set(ensure_topics(create_admin_client(kafka), kafka).values()) == {"exists"}


def test_simulated_readings_flow_through_kafka(kafka):
    sent = publish_simulated_readings(kafka, ticks=4, seed=1)
    received = consume_all(kafka, "flow", expected=len(sent))

    assert sorted(m.value["event_id"] for m in received) == sorted(e["event_id"] for e in sent)
    by_id = {e["event_id"]: e for e in sent}
    partitions = defaultdict(set)
    for message in received:
        assert message.value == by_id[message.value["event_id"]]   # JSON round trip intact
        assert message.key == message.value["zone_id"]             # zone_id is the key
        assert message.headers["content-type"] == "application/json"
        partitions[message.key].add(message.partition)
    assert all(len(p) == 1 for p in partitions.values())           # one zone -> one partition
    # Readings of each zone arrive in order.
    for zone in partitions:
        sequences = [m.value["sequence"] for m in received if m.key == zone]
        assert sequences == sorted(sequences)


def test_independent_consumer_groups_each_receive_everything(kafka):
    sent = publish_simulated_readings(kafka, ticks=2, seed=2)
    ids = {e["event_id"] for e in sent}
    # Groups start from the earliest offset, so filter to this test's events.
    for group in ("database_writer", "security_monitor"):
        received = consume_all(kafka, group, expected=10**6, timeout_s=5)
        assert ids <= {m.value["event_id"] for m in received}


def test_consumers_in_one_group_share_partitions(kafka):
    group, results, stop = "shared", [[], []], threading.Event()
    consumers = [JsonConsumer(kafka, group, [BUILDING_SENSOR_DATA]) for _ in range(2)]
    threads = [threading.Thread(target=c.run, args=(results[i].append,),
                                kwargs={"stop_event": stop}) for i, c in enumerate(consumers)]
    for thread in threads:
        thread.start()
    time.sleep(6)  # let both members join and receive their partitions
    for result in results:
        result.clear()  # ignore messages from earlier tests

    sent = publish_simulated_readings(kafka, ticks=3, seed=3)
    ids = {e["event_id"] for e in sent}
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        got = {m.value["event_id"] for r in results for m in r} & ids
        if got == ids:
            break
        time.sleep(0.5)
    stop.set()
    for thread in threads:
        thread.join(10)
    for consumer in consumers:
        consumer.close()

    first = {m.value["event_id"] for m in results[0]} & ids
    second = {m.value["event_id"] for m in results[1]} & ids
    assert first | second == ids          # every message processed ...
    assert not first & second             # ... by exactly one group member
    assert first and second               # and the work was actually shared


def test_consumer_handles_missing_topic(kafka):
    missing = dataclasses.replace(kafka, topic_prefix=f"missing_{uuid.uuid4().hex[:6]}.")
    consumer = JsonConsumer(missing, "missing", ["weather_data"], retry_backoff_s=0.1)
    try:
        stats = consumer.run(lambda m: None, idle_timeout_s=3)
    finally:
        consumer.close()
    assert stats.received == 0 and stats.errors >= 1


def test_publisher_and_monitor_services(kafka, clean_env):
    clean_env.setenv("APP_ENV", "testing")
    clean_env.setenv("KAFKA_TOPIC_PREFIX", kafka.topic_prefix)
    clean_env.setenv("KAFKA_CONSUMER_GROUP_PREFIX", kafka.consumer_group_prefix)
    from workers import sensor_publisher, topic_monitor

    assert sensor_publisher.main(["--fast", "--ticks", "2", "--seed", "4", "--broker-wait", "5"],
                                 stop_event=threading.Event()) == 0
    assert topic_monitor.main(["--group", "cli_monitor", "--idle-timeout", "5", "--quiet"],
                              stop_event=threading.Event()) == 0
