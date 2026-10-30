"""Tests for Module 11: recovery policy and the evaluation report's calculations."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from agent.executor import SimulatorGateway
from agent.recovery import RecoveryPolicy
from experiments import evaluation_report as rep
from tests.test_agent import Building

T0 = datetime(2026, 2, 10, 10, 0, tzinfo=timezone.utc)


def at(minutes: float) -> str:
    return (T0 + timedelta(minutes=minutes)).isoformat()


def action(kind: str, zone: str, sensor: str, minutes: float, status: str = "executed") -> dict:
    return {
        "action_type": kind,
        "zone_id": zone,
        "parameters": {"sensor": sensor},
        "status": status,
        "created_at": at(minutes),
    }


# --------------------------------------------------------------------------- recovery policy
class TestRecoveryPolicy:
    def setup(self, alert_minutes_ago: float | None):
        b = Building(hours=2)
        now = b.data.now()
        b.isolation.isolate("Z1", "temperature", since=now - timedelta(hours=2), reason="t")
        if alert_minutes_ago is not None:
            alert = b.alert()
            alert["detected_at"] = (now - timedelta(minutes=alert_minutes_ago)).isoformat()
        policy = RecoveryPolicy(b.data, b.isolation, b.validator, SimulatorGateway(b.simulator, b.isolation))
        return b, policy, now

    def test_restores_after_a_quiet_hour(self):
        b, policy, now = self.setup(alert_minutes_ago=90)
        done = policy.check(now)
        assert [(a.action_type, a.status) for a in done] == [("restore_sensor", "executed")]
        assert not b.isolation.is_isolated("Z1", "temperature")

    def test_waits_while_alerts_continue(self):
        b, policy, now = self.setup(alert_minutes_ago=10)
        assert policy.check(now) == [] and b.isolation.is_isolated("Z1", "temperature")

    def test_data_drift_does_not_block_recovery(self):
        b, policy, now = self.setup(alert_minutes_ago=None)
        drift = b.alert(classification="data_drift", severity="low")
        drift["detected_at"] = now.isoformat()
        assert policy.check(now)[0].status == "executed"

    def test_does_not_restore_a_fresh_isolation(self):
        b = Building(hours=1)
        now = b.data.now()
        b.isolation.isolate("Z1", "temperature", since=now - timedelta(minutes=20), reason="t")
        policy = RecoveryPolicy(b.data, b.isolation, b.validator, SimulatorGateway(b.simulator, b.isolation))
        assert policy.check(now) == []


# --------------------------------------------------------------------------- report calculations
def test_isolation_intervals_and_zone_clear():
    actions = [
        action("isolate_sensor", "Z1", "temperature", 10),
        action("isolate_sensor", "Z1", "occupancy", 20),
        action("restore_sensor", "Z1", "temperature", 200),
        action("restore_sensor", "Z1", "occupancy", 230),
        action("isolate_sensor", "Z2", "temperature", 30),
        action("isolate_sensor", "Z3", "co2", 40, "rejected"),
    ]
    intervals = rep.isolation_intervals(actions)
    assert intervals[("Z1", "temperature")] == [[T0 + timedelta(minutes=10), T0 + timedelta(minutes=200)]]
    assert ("Z3", "co2") not in intervals  # only executed actions count
    assert rep.zone_clear_after(intervals, "Z1", T0 + timedelta(minutes=120)) == T0 + timedelta(minutes=230)
    assert rep.zone_clear_after(intervals, "Z1", T0 + timedelta(minutes=300)) == T0 + timedelta(minutes=300)
    assert rep.zone_clear_after(intervals, "Z2", T0 + timedelta(minutes=120)) is None  # never restored


def raw_result() -> dict:
    """A small attack_defended-like result: one spoofing attack in Z1 from 0 to 60 min."""
    detections = []
    for minute in range(-60, 120, 5):
        label = "temperature_spoofing" if 0 <= minute < 60 else None
        flagged = "temperature_spoofing" if 10 <= minute < 60 else ("unknown_anomaly" if minute == 100 else None)
        detections.append((at(minute), "Z1", label, 0 if label else None, flagged))
    return {
        "start": at(-24 * 60),
        "days": 2,
        "attacks": [
            {
                "index": 0,
                "attack_type": "temperature_spoofing",
                "zone_id": "Z1",
                "start": at(0),
                "end": at(60),
                "params": {},
            }
        ],
        "detections": {"retrained": detections},
        "alerts": [
            {"monitor": "retrained", "zone_id": "Z1", "detected_at": at(10), "classification": "temperature_spoofing"},
            {"monitor": "retrained", "zone_id": "Z4", "detected_at": at(300), "classification": "abnormal_power"},
            {"monitor": "retrained", "zone_id": "Z4", "detected_at": at(310), "classification": "data_drift"},
        ],
        "actions": [
            action("isolate_sensor", "Z1", "temperature", 15),
            action("restore_sensor", "Z1", "temperature", 150),
        ],
    }


def test_detection_metrics():
    result = raw_result()
    d = rep.detection(result, "retrained", T0 - timedelta(hours=2))
    # 12 attacked readings, 10 flagged; reading at 60 min (first after the attack) excluded; one false flag at 100
    assert (d["tp"], d["fn"], d["fp"]) == (10, 2, 1)
    assert d["recall"] == pytest.approx(10 / 12) and d["precision"] == pytest.approx(10 / 11)
    assert d["latency_min"][0] == 10 and d["false_security_alerts"] == 1  # data drift is not counted


def test_corrective_and_recovery():
    result = raw_result()
    row = rep.corrective(result)[0]
    assert row["containment_min"] == 15 and row["recovery_min"] == 90 and row["sensors"] == ["temperature"]
    assert rep.full_recovery(result) == T0 + timedelta(minutes=150)
    assert rep.false_isolations(result) == 0
    result["actions"].append(action("isolate_sensor", "Z4", "power_meter", 600))  # far from any attack
    assert rep.false_isolations(result) == 1 and rep.full_recovery(result) is None


def test_mean_std_format():
    assert rep.ms([1.0, 3.0]) == "2.0 ± 1.4" and rep.ms([2.0]) == "2.0" and rep.ms([]) == "–"
    assert rep.pct_change(100, 90) == pytest.approx(10.0)


def test_report_from_raw_files(tmp_path):
    """End-to-end report generation from minimal raw files (two seeds, two scenarios)."""
    raw = tmp_path / "raw"
    raw.mkdir()
    for seed, scale in ((1, 1.0), (2, 1.1)):
        for name, energy in (("baseline", 100.0), ("optimized", 90.0)):
            result = {
                "scenario": name,
                "seed": seed,
                "energy_kwh": energy * scale,
                "carbon_kg": energy * scale / 4,
                "peak_power_kw": 10.0 * scale,
                "comfort": {"in_band": 9, "occupied_readings": 10, "discomfort_degree_hours": 1.0},
                "predictions": [["Z1", at(0), 1.0, 1.5, False]],
                "attacks": [],
                "detections": {},
                "alerts": [],
                "actions": [],
                "decisions": [],
                "latency_ms": {"simulator_tick": [1.0, 2.0]},
                "optimizer": {"paused_by_isolation": {}},
            }
            (raw / f"seed{seed}_{name}.json").write_text(json.dumps(result))
            (raw / f"seed{seed}_{name}_series.csv").write_text(
                f"time,power_kw,energy_kwh,carbon_kg\n{at(0)},1,1,0.25\n"
            )
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "evaluation": {
                    "start": at(-24 * 60),
                    "days": 1,
                    "warmup_hours": 24,
                    "attacks": [],
                    "comfort_band": [20, 25],
                    "detector_training_days": 28,
                    "detector_training_seed": 1,
                    "detector_training_start": at(0),
                },
                "versions": {"ml_model": "m", "original_detectors": "a", "optimizer_detectors": "b"},
                "environment": {"git_commit": "abc", "platform": "test", "cpu_count": 1, "python": "3"},
                "command": "test",
                "generated_at": at(0),
            }
        )
    )
    config, results, series = rep.load(tmp_path)
    report, summary = rep.render(tmp_path, config, results, series)
    assert "| 2 ML-optimized | 94.5 ± 6.4 | 10.00 ± 0.00 % |" in report  # std of 90 and 99
    assert summary["prediction"][0]["mae_kw"] == pytest.approx(0.5)
    assert (tmp_path / "figures" / "energy_carbon_peak.png").is_file()
