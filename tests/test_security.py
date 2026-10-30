"""Tests for attack simulation, detection (rules, Isolation Forest, drift),
classification, alerting and the Kafka security service."""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from config import SimulatorSettings
from processors.data_validator import validate_alert_message, validate_sensor_event
from security.alert_manager import AlertManager
from security.attack_detector import RuleHit, classify
from security.drift_detector import DriftDetector, DriftReference, population_stability_index
from security.monitor import SecurityMonitor
from security.train import load_models, save_models, simulate_clean_readings, train_models
from simulator.attack_simulator import AttackInjector, AttackScenario
from simulator.engine import BuildingSimulator
from streaming.consumer import ConsumedMessage

START = datetime(2026, 1, 5, tzinfo=timezone.utc)          # Monday
EVAL_START = datetime(2026, 1, 12, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def models():
    return train_models(simulate_clean_readings(days=6, seed=1, start=START))


def events(days: float, seed: int = 9, start: datetime = EVAL_START) -> list[dict]:
    sim = BuildingSimulator(SimulatorSettings(interval_seconds=1, speed_factor=300,
                                              random_seed=seed, start_time=start))
    return [e for _ in range(int(days * 288)) for e in sim.tick()]


def run(monitor: SecurityMonitor, stream, injector: AttackInjector | None = None):
    out = []
    for event in stream:
        event = injector.apply(event) if injector else event
        out.append((event, injector.labels[-1] if injector else None, monitor.process(event)))
    return out


# --------------------------------------------------------------------------- attack simulator
def test_attack_scenario_parsing_and_validation():
    s = AttackScenario.parse("temperature_spoofing:Z1:60:30:offset_c=3", START)
    assert s.start == START + timedelta(hours=1) and s.end == START + timedelta(minutes=90)
    assert s.params == {"offset_c": 3.0}
    with pytest.raises(ValueError):
        AttackScenario("teleport", "Z1", START, START + timedelta(hours=1))
    with pytest.raises(ValueError):
        AttackScenario("replay", "Z1", START, START)
    with pytest.raises(ValueError):
        AttackScenario.parse("replay:Z1", START)


def test_injector_manipulates_only_target_zone_and_window():
    t0 = EVAL_START + timedelta(hours=10)
    injector = AttackInjector([
        AttackScenario("temperature_spoofing", "Z1", t0, t0 + timedelta(hours=1), {"offset_c": 4}),
        AttackScenario("abnormal_power", "Z5", t0, t0 + timedelta(hours=1), {"factor": 3}),
        AttackScenario("occupancy_manipulation", "Z2", t0, t0 + timedelta(hours=1), {"mode": "inflate", "count": 7}),
    ])
    stream = events(0.5)
    for original in stream:
        attacked = injector.apply(dict(original))
        label = injector.labels[-1]
        when = datetime.fromisoformat(original["timestamp"])
        inside = t0 <= when < t0 + timedelta(hours=1)
        assert "attack" not in str(attacked.keys())                      # labels never in events
        if original["zone_id"] == "Z1" and inside:
            assert attacked["temperature_c"] == pytest.approx(original["temperature_c"] + 4, abs=0.06)
            assert label.attack_type == "temperature_spoofing"
        elif original["zone_id"] == "Z5" and inside:
            assert attacked["total_power_kw"] == pytest.approx(3 * original["total_power_kw"], rel=1e-3)
            validate_sensor_event(attacked)                               # still schema-valid
        elif original["zone_id"] == "Z2" and inside:
            assert attacked["occupancy_count"] == 7
        else:
            assert attacked == original and label.attack_type is None


def test_replay_and_gradual_attacks():
    t0 = EVAL_START + timedelta(hours=2)
    injector = AttackInjector([
        AttackScenario("replay", "Z1", t0, t0 + timedelta(hours=1), {"lag": 12}),
        AttackScenario("gradual_manipulation", "Z2", t0, t0 + timedelta(hours=4), {"rate_c_per_h": 0.5}),
        AttackScenario("replay", "Z3", t0, t0 + timedelta(minutes=10), {"mode": "verbatim", "lag": 6}),
    ])
    stream = events(0.3)
    sent = [injector.apply(dict(e)) for e in stream]
    by_zone = lambda zone: [(o, s) for o, s in zip(stream, sent) if o["zone_id"] == zone]  # noqa: E731
    z1 = by_zone("Z1")
    replayed = [(i, s) for i, (o, s) in enumerate(z1) if s != o]
    first_index, first = replayed[0]
    assert first["temperature_c"] == z1[first_index - 12][0]["temperature_c"]
    assert first["timestamp"] == z1[first_index][0]["timestamp"]           # fresh metadata
    z3_verbatim = [s for o, s in by_zone("Z3") if s != o]
    assert z3_verbatim and z3_verbatim[0]["timestamp"] < by_zone("Z3")[24][0]["timestamp"]
    offsets = [s["temperature_c"] - o["temperature_c"] for o, s in by_zone("Z2") if s != o]
    assert offsets[0] < 0.2 and 1.0 < offsets[-1] <= 2.1                  # slow ramp


# --------------------------------------------------------------------------- drift statistics
def test_psi_and_drift_detector():
    rng = np.random.default_rng(0)
    reference = rng.normal(0, 1, 5000)
    assert population_stability_index(reference, rng.normal(0, 1, 500)) < 0.1
    assert population_stability_index(reference, rng.normal(1.5, 1, 500)) > 0.25

    ref = DriftReference()
    t = EVAL_START
    names = ("control_error_active", "co2_residual")
    for i in range(4000):
        ref.add("Z9", t + timedelta(minutes=5 * i), {f: rng.normal(0, 1) for f in names})
    ref.freeze()
    clean = DriftDetector(ref, window=72, step=12)
    shifted = DriftDetector(ref, window=72, step=12)
    clean_results, shifted_results = [], []
    for i in range(240):
        when = t + timedelta(days=30, minutes=5 * i)
        base = {f: rng.normal(0, 1) for f in names}
        if i % 3 == 0:
            base["control_error_active"] = float("nan")          # HVAC idle: value missing
        clean_results.append(clean.update("Z9", when, base))
        shifted_results.append(shifted.update("Z9", when, {**base, "co2_residual": base["co2_residual"] + 2}))
    assert not any(r.is_drift for r in clean_results if r)
    flagged = [r for r in shifted_results if r and r.is_drift]
    assert flagged and all(r.drifting == ("co2_residual",) for r in flagged)
    first_eval = next(r for r in shifted_results if r)
    assert not first_eval.is_drift                                         # persistence: needs 2 evaluations


# --------------------------------------------------------------------------- rules / classification
def test_rule_limits_calibrated_on_clean_data(models):
    for zone, limits in models.limits.items():
        assert limits.max_temperature_rate > 0 and limits.max_total_power > 0
    monitor = SecurityMonitor(models)
    results = run(monitor, events(1))
    rule_hits = [h.rule for _, _, o in results for h in o.detection.hits]
    assert rule_hits.count("replayed_values") == 0 and rule_hits.count("timestamp_regression") == 0
    flagged = sum(o.detection.flagged for _, _, o in results)
    assert flagged / len(results) < 0.02                                   # low false-positive rate on clean data


@pytest.mark.parametrize(("attack", "zone", "hour", "params", "expected"), [
    ("temperature_spoofing", "Z1", 10, {"offset_c": 4.0}, "temperature_spoofing"),
    ("abnormal_power", "Z1", 10, {"factor": 3.0}, "abnormal_power"),
    ("occupancy_manipulation", "Z1", 10, {"mode": "hide"}, "occupancy_manipulation"),
    ("occupancy_manipulation", "Z3", 2, {"mode": "inflate", "count": 10}, "occupancy_manipulation"),
    ("replay", "Z1", 10, {"lag": 36}, "replay"),
])
def test_point_attacks_are_detected_and_classified(models, attack, zone, hour, params, expected):
    t0 = EVAL_START + timedelta(hours=hour)
    injector = AttackInjector([AttackScenario(attack, zone, t0, t0 + timedelta(hours=1), params)])
    results = run(SecurityMonitor(models), events(0.6), injector)
    attacked = [o for _, label, o in results if label.attack_type]
    detected = [o.detection.attack_type for o in attacked if o.detection.flagged]
    assert detected, f"{attack} not detected"
    assert detected[0] == expected or expected in detected


def test_verbatim_replay_hits_timestamp_rule(models):
    t0 = EVAL_START + timedelta(hours=3)
    injector = AttackInjector([AttackScenario("replay", "Z2", t0, t0 + timedelta(minutes=30),
                                              {"mode": "verbatim", "lag": 6})])
    results = run(SecurityMonitor(models), events(0.2), injector)
    hits = {h.rule for _, label, o in results if label.attack_type for h in o.detection.hits}
    assert "timestamp_regression" in hits


def test_classification_priorities():
    replay = RuleHit("replayed_values", "replay", "")
    temp = RuleHit("temperature_rate", "temperature_spoofing", "")
    assert classify([temp, replay], None, None) == ("replay", "rules")
    assert classify([temp], (True, 0.7, 0.6), None) == ("temperature_spoofing", "rules+isolation_forest")
    assert classify([], (True, 0.7, 0.6), ("power", "total_power_kw", 9.0)) == ("abnormal_power", "isolation_forest")
    assert classify([], (True, 0.7, 0.6), ("power", "total_power_kw", 1.0)) == ("unknown_anomaly", "isolation_forest")
    assert classify([], (False, 0.5, 0.6), None) == (None, "")


# --------------------------------------------------------------------------- alerts
def test_alert_rate_limiting_and_contract():
    manager = AlertManager(cooldown_s=900)
    t = EVAL_START
    kwargs = dict(zone_id="Z1", attack_type="replay", detector="rules", anomaly=(True, 0.8, 0.6),
                  top_feature=("temperature", "temperature_c", 5.0),
                  hits=[RuleHit("replayed_values", "replay", "x")])
    first = manager.point_alert(event_id=str(uuid.uuid4()), when=t, **kwargs)
    assert manager.point_alert(event_id=str(uuid.uuid4()), when=t + timedelta(minutes=5), **kwargs) is None
    later = manager.point_alert(event_id=str(uuid.uuid4()), when=t + timedelta(minutes=20), **kwargs)
    assert later["details"]["suppressed_since_last_alert"] == 1
    for alert in (first, later):
        record = validate_alert_message(alert)
        assert record.alert_type == "attack" and record.severity == "high"
    validation = manager.validation_alert(zone_id=None, event_id="x", when=t, errors=["bad"])
    assert validate_alert_message(validation).alert_type == "validation"


def test_monitor_alerts_distinguish_points_and_drift(models):
    t0 = EVAL_START + timedelta(hours=6)
    injector = AttackInjector([
        AttackScenario("gradual_manipulation", "Z2", t0, t0 + timedelta(hours=14), {"rate_c_per_h": 0.6}),
        AttackScenario("temperature_spoofing", "Z1", t0, t0 + timedelta(hours=1), {"offset_c": 5}),
    ])
    results = run(SecurityMonitor(models), events(1), injector)
    alerts = [a for _, _, o in results for a in o.alerts]
    for alert in alerts:
        validate_alert_message(alert)
    drift = [a for a in alerts if a["alert_type"] == "drift"]
    points = [a for a in alerts if a["alert_type"] == "attack" and a["zone_id"] == "Z1"]
    assert drift and all(a["details"]["scope"] == "distribution_window" for a in drift)
    assert any(a["zone_id"] == "Z2" for a in drift)
    assert points and all(a["details"]["scope"] == "individual_reading" for a in points)


def test_invalid_message_produces_validation_alert(models):
    output = SecurityMonitor(models).process({"zone_id": "Z1", "event_id": "x", "temperature_c": "hot"})
    assert not output.valid and output.alerts[0]["alert_type"] == "validation"


def test_models_round_trip(models, tmp_path):
    save_models(models, tmp_path)
    loaded = load_models(root=tmp_path)
    assert loaded.metadata["version"] == models.metadata["version"]
    assert set(loaded.anomaly.models) == {"Z1", "Z2", "Z3", "Z4", "Z5"}


# --------------------------------------------------------------------------- Kafka service
def test_security_service_handler_publishes_alerts(models):
    from workers.security_service import SecurityHandler

    class Producer:
        def __init__(self):
            self.sent = []

        def send(self, topic, value, key=None):
            self.sent.append((topic, key, value))

    producer = Producer()
    handler = SecurityHandler(SecurityMonitor(models), producer, logging.getLogger("test"))
    t0 = EVAL_START + timedelta(hours=10)
    injector = AttackInjector([AttackScenario("abnormal_power", "Z4", t0, t0 + timedelta(hours=1), {"factor": 4})])
    for i, event in enumerate(events(0.5)):
        handler(ConsumedMessage("building_sensor_data", event["zone_id"], injector.apply(event), 0, i, None))
    assert producer.sent and all(topic == "security_alerts" for topic, _, _ in producer.sent)
    assert handler.stats["detected_abnormal_power"] > 0
    assert any(v["details"]["classification"] == "abnormal_power" and k == "Z4" for _, k, v in producer.sent)


@pytest.mark.kafka
@pytest.mark.postgres
def test_attack_to_alert_flow_through_kafka(db, fresh_kafka, clean_env, models, tmp_path, monkeypatch):
    clean_env.setenv("APP_ENV", "testing")
    clean_env.setenv("KAFKA_TOPIC_PREFIX", fresh_kafka.topic_prefix)
    clean_env.setenv("KAFKA_CONSUMER_GROUP_PREFIX", f"{fresh_kafka.consumer_group_prefix}_sec")
    clean_env.setenv("POSTGRES_PASSWORD", db.settings.password)
    import workers.security_service as service
    from workers import record_persistence, sensor_publisher

    monkeypatch.setattr(service, "load_configured_models", lambda config: models)
    labels = tmp_path / "labels.jsonl"
    assert sensor_publisher.main(["--fast", "--interval", "1", "--speed", "300", "--ticks", "48",
                                  "--seed", "5", "--start", "2026-01-12T09:00",
                                  "--attack", "temperature_spoofing:Z1:60:60:offset_c=5",
                                  "--labels", str(labels), "--broker-wait", "5"],
                                 stop_event=threading.Event()) == 0
    assert sum('"temperature_spoofing"' in line for line in labels.read_text().splitlines()) == 12
    assert service.main(["--from-beginning", "--idle-timeout", "6", "--broker-wait", "5"],
                        stop_event=threading.Event()) == 0
    assert record_persistence.main(["--idle-timeout", "6", "--broker-wait", "5", "--db-wait", "5"],
                                   stop_event=threading.Event()) == 0
    with db.transaction() as conn:
        rows = conn.execute("SELECT zone_id, alert_type, details->>'classification' FROM security_alerts").fetchall()
    assert ("Z1", "attack", "temperature_spoofing") in rows
