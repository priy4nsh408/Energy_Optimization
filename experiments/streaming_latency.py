"""Kafka transport latency: producer -> consumer on the real broker.

    python -m experiments.streaming_latency --out docs/results/evaluation

Publishes simulated sensor events to a dedicated, temporary topic set
(prefix ``latency_<random>_``) at fixed rates and measures, for every message,
the wall-clock time from just before ``produce`` to its arrival in a
consumer (JsonConsumer, same process, separate thread). Topics are deleted
afterwards. Raw samples and settings are written to ``streaming_latency.json``.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from config import SimulatorSettings, get_config
from simulator.engine import BuildingSimulator
from streaming.consumer import JsonConsumer
from streaming.kafka_config import create_admin_client, wait_for_broker
from streaming.producer import JsonProducer
from streaming.topics import BUILDING_SENSOR_DATA, TOPICS, ensure_topics


def measure(kafka, rate: float, count: int) -> dict:
    simulator = BuildingSimulator(
        SimulatorSettings(
            interval_seconds=1, speed_factor=300, random_seed=1, start_time=datetime(2026, 2, 9, tzinfo=timezone.utc)
        )
    )
    events = [e for _ in range(count // 5 + 1) for e in simulator.tick()][:count]
    sent_at: dict[str, float] = {}
    latencies: list[float] = []
    stop = threading.Event()

    def handle(message) -> None:
        run_id = message.value.get("latency_probe")
        if run_id in sent_at:
            latencies.append((time.perf_counter() - sent_at.pop(run_id)) * 1000)
            if len(latencies) >= count:
                stop.set()

    group = f"latency_{uuid.uuid4().hex[:8]}"
    consumer = JsonConsumer(kafka, group, [BUILDING_SENSOR_DATA], auto_offset_reset="latest")
    thread = threading.Thread(target=consumer.run, args=(handle,), kwargs={"stop_event": stop}, daemon=True)
    thread.start()
    time.sleep(5)  # partition assignment before producing
    producer = JsonProducer(kafka, name="latency")
    started = time.perf_counter()
    for i, event in enumerate(events):
        target = started + i / rate
        while time.perf_counter() < target:
            time.sleep(max(0.0, min(0.002, target - time.perf_counter())))
        probe = uuid.uuid4().hex
        sent_at[probe] = time.perf_counter()
        producer.send(BUILDING_SENSOR_DATA, {**event, "latency_probe": probe}, key=event["zone_id"])
    producer.flush(30)
    stop.wait(timeout=30)
    stop.set()
    thread.join(timeout=10)
    consumer.close()
    producer.close()
    v = np.array(latencies) if latencies else np.array([float("nan")])
    return {
        "rate_per_s": rate,
        "sent": len(events),
        "received": len(latencies),
        "latency_ms": {
            "mean": float(v.mean()),
            "p50": float(np.percentile(v, 50)),
            "p95": float(np.percentile(v, 95)),
            "p99": float(np.percentile(v, 99)),
            "max": float(v.max()),
        },
        "samples_ms": [round(x, 3) for x in latencies],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m experiments.streaming_latency", description=__doc__.splitlines()[0]
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--rates", type=float, nargs="+", default=[5.0, 50.0, 500.0], help="messages per second")
    parser.add_argument("--count", type=int, default=1000, help="messages per rate")
    args = parser.parse_args(argv)
    config = get_config()
    kafka = dataclasses.replace(config.KAFKA, topic_prefix=f"latency_{uuid.uuid4().hex[:6]}_")
    wait_for_broker(kafka, timeout_s=30)
    admin = create_admin_client(kafka)
    ensure_topics(admin, kafka, specs=[TOPICS[BUILDING_SENSOR_DATA]])
    try:
        runs = [measure(kafka, rate, args.count) for rate in args.rates]
    finally:
        for future in admin.delete_topics([kafka.topic(BUILDING_SENSOR_DATA)]).values():
            future.result()
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "broker": kafka.bootstrap_servers,
        "producer_config": {k: v for k, v in kafka.producer_config().items() if "password" not in k.lower()},
        "count_per_rate": args.count,
        "runs": runs,
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "streaming_latency.json").write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    for r in runs:
        print(
            f"{r['rate_per_s']:>6} msg/s: {r['received']}/{r['sent']} received, "
            f"p50 {r['latency_ms']['p50']:.2f} ms, p95 {r['latency_ms']['p95']:.2f} ms"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
