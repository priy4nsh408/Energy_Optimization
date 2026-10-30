"""Module 11: reproducible end-to-end evaluation (raw results only).

    python -m experiments.evaluation --out docs/results/evaluation            # 3 seeds, 4 workers
    python -m experiments.evaluation --seeds 101 --days 4 --out /tmp/eval      # quick single seed
    python -m experiments.evaluation_report --run docs/results/evaluation      # tables + graphs

Every seed runs the same simulated period in four closed-loop scenarios:

==================== ========== ============= ===========================================
scenario              optimizer  attacks       detection + agent + recovery
==================== ========== ============= ===========================================
baseline              no         no            no      (1. baseline building operation)
optimized             ML basis   no            no      (2. ML-based energy optimization)
attack_undefended     ML basis   yes           passive (3. cyberattack scenarios)
attack_defended       ML basis   yes           yes     (4. detection + corrective action,
                                                         5. post-recovery optimization)
==================== ========== ============= ===========================================

* Weather, occupancy and sensor noise come from separate random streams of the
  same seed, so the scenarios of a seed differ only in control and attacks.
* Energy, carbon, peak and comfort are computed from the *true* simulator
  readings; detectors, the ML model, the optimizer and the agent only see the
  published (possibly manipulated) readings.
* Carbon uses the synthetic ``MockCarbonProvider`` profile (reproducible offline).
* ``attack_undefended`` runs two detector sets passively: the Module 8
  detectors (trained on baseline-controlled data) and detectors retrained on
  clean *optimizer-controlled* data (trained here, different seed and weeks).
  ``attack_defended`` uses the retrained detectors live: alerts go to the
  LangGraph agent (offline rule-based policy, deterministic), its actions pass
  the safety validator and run on the simulator; isolated zones run on their
  baseline schedule until :class:`agent.recovery.RecoveryPolicy` restores the
  sensors and the optimizer resumes.
* Latencies are wall-clock times of each in-process step (no Kafka); Kafka
  transport latency is measured by ``experiments.streaming_latency``.

Raw per-seed/per-scenario results (JSON + CSV time series) and the full
configuration are written to ``--out``; the report is generated from them only.
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import json
import logging
import multiprocessing
import platform
import subprocess
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from importlib import metadata as pkg_metadata
from pathlib import Path
from typing import Any

from config import BASE_DIR, SimulatorSettings, get_config
from security.train import OPTIMIZER_CONTROL_ROOT as DETECTOR_ROOT

logger = logging.getLogger(__name__)

SCENARIOS: dict[str, dict[str, bool]] = {
    "baseline": {"optimizer": False, "attacks": False, "defense": False},
    "optimized": {"optimizer": True, "attacks": False, "defense": False},
    "attack_undefended": {"optimizer": True, "attacks": True, "defense": False},
    "attack_defended": {"optimizer": True, "attacks": True, "defense": True},
}
# TYPE:ZONE:START_MIN:DURATION_MIN[:params]; minutes after the start. Day 0 is attack-free (warm-up).
DEFAULT_ATTACKS = [
    "temperature_spoofing:Z1:2040:120:offset_c=4",            # day 1 10:00-12:00
    "occupancy_manipulation:Z3:2100:120:mode=hide",           # day 1 11:00-13:00
    "abnormal_power:Z4:2340:60:factor=3",                     # day 1 15:00-16:00
    "replay:Z2:2460:120:lag=288",                             # day 1 17:00-19:00
    "gradual_manipulation:Z4:3000:720:rate_c_per_h=0.4",      # day 2 02:00-14:00
]
# Each attacked zone is free of attacks for >= 10 h before its next attack, so recovery from one attack
# and detection of the next are not confounded (run 1 had the gradual attack in Z2 right after the replay).
DEFAULT_SEEDS = (101, 202, 303)
DEFAULT_START = datetime(2026, 2, 9, tzinfo=timezone.utc)     # Monday; detectors' season (trained on January)
UNTRUSTED_FOR_CONTROL = ("temperature", "occupancy")


@dataclass(frozen=True)
class EvalConfig:
    start: datetime = DEFAULT_START
    days: float = 4.0
    seeds: tuple[int, ...] = DEFAULT_SEEDS
    attacks: tuple[str, ...] = tuple(DEFAULT_ATTACKS)
    warmup_hours: float = 24.0                 # no attacks, alerts not investigated, excluded from detection metrics
    recovery_quiet_s: float = 3600.0
    comfort_band: tuple[float, float] = (20.0, 25.0)
    detector_training_days: float = 28.0
    detector_training_seed: int = 2026
    detector_training_start: datetime = datetime(2026, 1, 5, tzinfo=timezone.utc)
    original_detectors: str | None = None      # Module 8 detectors (None = latest)
    optimizer_detectors: str | None = None     # retrained detectors (None = train now)
    ml_model_version: str | None = None        # building model (None = latest)

    def as_dict(self) -> dict[str, Any]:
        data = dataclasses.asdict(self)
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        return data


# --------------------------------------------------------------------------- helpers
class Timer:
    """Wall-clock samples (milliseconds) per processing step."""

    def __init__(self) -> None:
        self.samples: dict[str, list[float]] = defaultdict(list)

    def measure(self, name: str, started: float) -> None:
        self.samples[name].append((time.perf_counter() - started) * 1000)


class ListRecorder:
    def __init__(self) -> None:
        self.decisions: list[Any] = []

    def record(self, decision, actions) -> None:
        self.decisions.append(decision)


def _sim_settings(cfg: EvalConfig, seed: int) -> SimulatorSettings:
    return SimulatorSettings(interval_seconds=1, speed_factor=300, random_seed=seed, start_time=cfg.start,
                             building_id=get_config().SIMULATOR.building_id)


def train_optimizer_detectors(cfg: EvalConfig):
    """Detectors trained on clean, optimizer-controlled simulation (measured basis, no attacks)."""
    from security.train import save_models, simulate_clean_readings, train_models

    readings = simulate_clean_readings(cfg.detector_training_days, cfg.detector_training_seed,
                                       cfg.detector_training_start, control="optimizer")
    models = train_models(readings, source={
        "type": "simulator, optimizer-controlled (measured basis), no attacks",
        "days": cfg.detector_training_days, "seed": cfg.detector_training_seed,
        "start": cfg.detector_training_start.isoformat()})
    DETECTOR_ROOT.mkdir(parents=True, exist_ok=True)
    save_models(models, root=DETECTOR_ROOT)
    return models.metadata["version"]


# --------------------------------------------------------------------------- one scenario
def run_scenario(name: str, seed: int, cfg: EvalConfig, versions: dict[str, str | None]) -> dict[str, Any]:
    """Run one scenario for one seed; returns raw, JSON-serialisable results."""
    from agent.data import InMemoryDataSource
    from agent.dispatch import IncidentDispatcher
    from agent.executor import SimulatorGateway
    from agent.graph import IncidentAgent
    from agent.isolation import SensorIsolationRegistry
    from agent.llm import OfflinePolicy
    from agent.recovery import RecoveryPolicy
    from agent.safety import SafetyValidator
    from agent.tools import AgentToolbox
    from data_sources.carbon import MockCarbonProvider
    from data_sources.weather import MockWeatherProvider
    from experiments.scenarios import make_optimizer
    from ml.dataset import BUILDING_COLUMNS, prepare_building_frame
    from ml.features import BUILDING_SPEC
    from ml.predict import OnlinePredictor
    from ml.registry import load_bundle
    from optimization.hvac_optimizer import REQUIRED_LUX, ZoneConditions, building_models
    from optimization.lighting_optimizer import LUX_SENSOR_TOLERANCE, is_lighting_short
    from optimization.optimizer import DecisionScheduler, ZoneOptimizer
    from processors.data_validator import ValidationError, validate_sensor_event
    from security.monitor import SecurityMonitor
    from security.train import ARTIFACT_ROOT, load_models
    from simulator.attack_simulator import AttackInjector, AttackScenario
    from simulator.engine import BuildingSimulator

    spec = SCENARIOS[name]
    config = get_config()
    sim_settings = _sim_settings(cfg, seed)
    simulator = BuildingSimulator(sim_settings)
    interval_s = sim_settings.simulated_seconds_per_tick
    ticks = int(cfg.days * 86400 / interval_s)
    warmup_end = cfg.start + timedelta(hours=cfg.warmup_hours)
    carbon = MockCarbonProvider().value_at
    timer = Timer()

    scenarios = [AttackScenario.parse(s, cfg.start) for s in cfg.attacks] if spec["attacks"] else []
    injector = AttackInjector(scenarios) if spec["attacks"] else None
    bundle = load_bundle(BUILDING_SPEC.name, versions["ml_model"])
    predictor = OnlinePredictor(bundle, prepare_building_frame)
    optimizer = make_optimizer(config.OPTIMIZATION) if spec["optimizer"] else None
    scheduler = DecisionScheduler(config.OPTIMIZATION.decision_interval_s) if optimizer else None

    monitors: dict[str, SecurityMonitor] = {}
    if spec["attacks"]:
        retrained = load_models(versions["optimizer_detectors"], root=DETECTOR_ROOT)
        monitors["retrained"] = SecurityMonitor(retrained)
        if not spec["defense"]:
            monitors["original"] = SecurityMonitor(load_models(versions["original_detectors"], root=ARTIFACT_ROOT))

    agent = recovery = dispatcher = isolation = data = recorder = None
    if spec["defense"]:
        agent_settings = dataclasses.replace(config.AGENT, llm_provider="offline")
        external = dataclasses.replace(config.EXTERNAL_DATA, weather_provider="mock", carbon_provider="mock")
        data = InMemoryDataSource(weather=MockWeatherProvider(external, sim_settings, seed=seed),
                                  carbon=MockCarbonProvider())
        isolation = SensorIsolationRegistry()
        models = building_models()
        validator = SafetyValidator(agent_settings, config.OPTIMIZATION, models, isolation)
        gateway = SimulatorGateway(simulator, isolation)
        recorder = ListRecorder()
        agent = IncidentAgent(llm=OfflinePolicy(), data=data, isolation=isolation, validator=validator,
                              gateway=gateway, recorder=recorder, max_iterations=agent_settings.max_iterations,
                              toolbox=AgentToolbox(data, isolation, validator,
                                                   ZoneOptimizer(config.OPTIMIZATION, models),
                                                   monitors["retrained"].models))
        dispatcher = IncidentDispatcher(agent_settings, isolation)
        recovery = RecoveryPolicy(data, isolation, validator, gateway, quiet_s=cfg.recovery_quiet_s)

    # per-tick series (true values) and accumulators
    series: dict[str, list] = {"time": [], "power_kw": [], "energy_kwh": [], "carbon_kg": []}
    energy_by_zone: dict[str, float] = defaultdict(float)
    comfort = {"occupied_readings": 0, "in_band": 0, "discomfort_degree_hours": 0.0, "lighting_shortfall": 0,
               "server_max_temperature_c": float("-inf")}
    pending: dict[str, tuple[datetime, float, bool]] = {}          # zone -> (target time, predicted kW, attacked input)
    predictions: list[tuple[str, str, float, float, bool]] = []   # zone, time, predicted, actual, attacked input
    detections: dict[str, list] = {k: [] for k in monitors}       # monitor -> [(time, zone, label, scenario, kind)]
    alerts: list[dict[str, Any]] = []
    skipped_alerts: dict[str, int] = defaultdict(int)
    optimizer_paused: dict[str, int] = defaultdict(int)
    decisions = applied = rejected = invalid_seen = 0
    low, high = cfg.comfort_band

    for _ in range(ticks):
        started = time.perf_counter()
        events = simulator.tick()
        timer.measure("simulator_tick", started)
        tick_power = tick_energy = tick_carbon = 0.0
        tick_time = None
        for event in events:
            true = validate_sensor_event(event)
            zone, tick_time = true.zone_id, true.event_time
            energy_by_zone[zone] += true.energy_kwh
            tick_energy += true.energy_kwh
            tick_carbon += true.energy_kwh * carbon(true.event_time) / 1000
            tick_power += true.total_power_kw
            if zone == "Z5":
                comfort["server_max_temperature_c"] = max(comfort["server_max_temperature_c"], true.temperature_c)
            elif true.occupancy_count > 0:
                comfort["occupied_readings"] += 1
                excess = max(0.0, low - true.temperature_c, true.temperature_c - high)
                comfort["in_band"] += excess == 0
                comfort["discomfort_degree_hours"] += excess * interval_s / 3600
                comfort["lighting_shortfall"] += true.lighting_lux < LUX_SENSOR_TOLERANCE * REQUIRED_LUX.get(
                    true.zone_type or "", 300.0)
            if zone in pending and pending[zone][0] == true.event_time:     # score the previous prediction
                target, predicted, attacked_input = pending.pop(zone)
                actual = true.energy_kwh * 3600 / true.interval_seconds
                predictions.append((zone, target.isoformat(), predicted, actual, attacked_input))

            # ---- what the building systems see ------------------------------------
            seen, label = event, None
            if injector is not None:
                seen = injector.apply(event)
                label = injector.labels[-1]
            attacked = bool(label and label.attack_type)
            try:
                seen_reading = validate_sensor_event(seen)
            except ValidationError:
                seen_reading, invalid_seen = None, invalid_seen + 1
            if seen_reading is not None:
                started = time.perf_counter()
                row = {c: getattr(seen_reading, c) for c in BUILDING_COLUMNS}
                row["event_id"] = str(seen_reading.event_id)
                prediction = predictor.update(row)
                timer.measure("ml_prediction", started)
                if prediction is not None:
                    pending[zone] = (prediction.target_time, prediction.value, attacked)
            if data is not None:
                data.add_event(seen)
            for monitor_name, monitor in monitors.items():
                started = time.perf_counter()
                output = monitor.process(seen)
                timer.measure(f"security_monitor_{monitor_name}", started)
                kind = output.detection.attack_type if output.detection else "invalid"
                detections[monitor_name].append((true.event_time.isoformat(), zone,
                                                 label.attack_type if label else None,
                                                 label.scenario_index if label else None, kind))
                for alert in output.alerts:
                    alert = {**alert, "monitor": monitor_name}
                    alerts.append({k: alert.get(k) for k in ("alert_id", "zone_id", "detected_at", "alert_type",
                                                             "severity", "detector", "monitor")}
                                  | {"classification": (alert.get("details") or {}).get("classification")})
                    if agent is None or monitor_name != "retrained":
                        continue
                    data.add_alert(alert)
                    if datetime.fromisoformat(alert["detected_at"]) < warmup_end:
                        skipped_alerts["warm-up"] += 1
                        continue
                    investigate, why = dispatcher.should_investigate(alert)
                    if not investigate:
                        skipped_alerts[why.split(" at ")[0]] += 1
                        continue
                    started = time.perf_counter()
                    decision = agent.run(alert)
                    timer.measure("agent_incident", started)
                    dispatcher.remember(alert, decision)

            # ---- optimizer --------------------------------------------------------
            if optimizer is None or seen_reading is None:
                continue
            conditions = ZoneConditions.from_reading(seen_reading)
            short = is_lighting_short(optimizer.models[zone], conditions)
            if not scheduler.due(zone, seen_reading.event_time, seen_reading.occupancy_count, short):
                continue
            if isolation is not None and any(isolation.is_isolated(zone, s) for s in UNTRUSTED_FOR_CONTROL):
                optimizer_paused[zone] += 1                  # zone stays on its baseline schedule
                continue
            pred = pending.get(zone)
            started = time.perf_counter()
            decision = optimizer.optimize(conditions, predicted_power_kw=pred[1] if pred else None,
                                          carbon_intensity=carbon(seen_reading.event_time))
            timer.measure("optimizer_decision", started)
            decisions += 1
            if decision.approved:
                simulator.apply_command(zone, setpoint_c=decision.setpoint_c, lighting_level=decision.lighting_level)
                applied += 1
            else:
                rejected += 1
        if recovery is not None and tick_time is not None:
            started = time.perf_counter()
            recovery.check(tick_time)
            timer.measure("recovery_check", started)
        if injector is not None:
            injector.labels.clear()
        series["time"].append(tick_time.isoformat())
        series["power_kw"].append(round(tick_power, 4))
        series["energy_kwh"].append(round(tick_energy, 5))
        series["carbon_kg"].append(round(tick_carbon, 6))

    actions = []
    if recorder is not None:
        for d in recorder.decisions:
            actions += [dict(a, origin="agent") for a in d.actions]
        actions += [dict(a.as_dict(), origin="recovery") for a in recovery.actions]

    return {
        "scenario": name, "seed": seed, "flags": spec,
        "start": cfg.start.isoformat(), "days": cfg.days, "interval_seconds": interval_s,
        "series": series,
        "energy_kwh": sum(energy_by_zone.values()), "energy_by_zone_kwh": dict(sorted(energy_by_zone.items())),
        "carbon_kg": sum(series["carbon_kg"]), "peak_power_kw": max(series["power_kw"]),
        "comfort": comfort,
        "optimizer": {"decisions": decisions, "applied": applied, "rejected": rejected,
                      "paused_by_isolation": dict(optimizer_paused)},
        "invalid_published_readings": invalid_seen,
        "predictions": predictions,
        "attacks": [{"index": i, "attack_type": s.attack_type, "zone_id": s.zone_id, "start": s.start.isoformat(),
                     "end": s.end.isoformat(), "params": s.params} for i, s in enumerate(scenarios)],
        "detections": detections,
        "alerts": alerts, "alerts_not_investigated": dict(skipped_alerts),
        "decisions": [{"decision_id": str(d.decision_id), "alert_id": str(d.alert_id) if d.alert_id else None,
                       "zone_id": d.zone_id, "likely_cause": d.likely_cause, "confidence": d.confidence,
                       "outcome": d.outcome, "tools": [t["tool"] for t in d.tool_calls], "errors": d.errors,
                       "wall_seconds": (d.finished_at - d.started_at).total_seconds()}
                      for d in (recorder.decisions if recorder else [])],
        "actions": actions,
        "latency_ms": {k: [round(v, 3) for v in vals] for k, vals in timer.samples.items()},
        "versions": versions,
    }


def _job(args: tuple[str, int, EvalConfig, dict[str, str | None], Path]) -> str:
    name, seed, cfg, versions, out = args
    logging.basicConfig(level=logging.WARNING)
    started = time.perf_counter()
    result = run_scenario(name, seed, cfg, versions)
    result["wall_seconds"] = time.perf_counter() - started
    raw = out / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    series = result.pop("series")
    with (raw / f"seed{seed}_{name}_series.csv").open("w", encoding="utf-8") as handle:
        handle.write("time,power_kw,energy_kwh,carbon_kg\n")
        for row in zip(series["time"], series["power_kw"], series["energy_kwh"], series["carbon_kg"]):
            handle.write(",".join(map(str, row)) + "\n")
    with gzip.open(raw / f"seed{seed}_{name}.json.gz", "wt", encoding="utf-8") as handle:
        json.dump(result, handle, default=str)
    return f"seed {seed} {name}: {result['wall_seconds']:.0f} s"


# --------------------------------------------------------------------------- orchestration
def _git_commit() -> str | None:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=BASE_DIR, capture_output=True, text=True,
                              check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _packages() -> dict[str, str]:
    names = ["numpy", "pandas", "scikit-learn", "xgboost", "scipy", "langgraph", "confluent-kafka", "matplotlib"]
    found = {}
    for name in names:
        try:
            found[name] = pkg_metadata.version(name)
        except pkg_metadata.PackageNotFoundError:
            pass
    return found


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m experiments.evaluation", description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=BASE_DIR / "docs" / "results" / "evaluation")
    parser.add_argument("--seeds", type=int, nargs="+", default=list(DEFAULT_SEEDS))
    parser.add_argument("--days", type=float, default=4.0)
    parser.add_argument("--scenarios", nargs="+", choices=list(SCENARIOS), default=list(SCENARIOS))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--optimizer-detectors", help="version in security/artifacts/optimizer_control "
                                                      "(default: train new ones)")
    parser.add_argument("--original-detectors", help="Module 8 detector version (default: latest)")
    parser.add_argument("--detector-training-days", type=float, default=28.0)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
    cfg = EvalConfig(days=args.days, seeds=tuple(args.seeds), optimizer_detectors=args.optimizer_detectors,
                     original_detectors=args.original_detectors,
                     detector_training_days=args.detector_training_days)

    from ml.features import BUILDING_SPEC
    from ml.registry import load_bundle
    from security.train import ARTIFACT_ROOT, load_models

    started = time.perf_counter()
    trained_now = False
    if cfg.optimizer_detectors is None:
        print("training detectors on optimizer-controlled data ...", flush=True)
        cfg = dataclasses.replace(cfg, optimizer_detectors=train_optimizer_detectors(cfg))
        trained_now = True
    bundle = load_bundle(BUILDING_SPEC.name, cfg.ml_model_version)
    original = load_models(cfg.original_detectors, root=ARTIFACT_ROOT)
    retrained = load_models(cfg.optimizer_detectors, root=DETECTOR_ROOT)
    versions = {"ml_model": bundle.version, "original_detectors": original.metadata["version"],
                "optimizer_detectors": retrained.metadata["version"]}
    app_config = get_config()
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "config.json").write_text(json.dumps({
        "evaluation": cfg.as_dict(), "scenarios": SCENARIOS, "versions": versions,
        "detectors_trained_in_this_run": trained_now,
        "ml_model": {"version": bundle.version, "model": bundle.model_name, "metadata": bundle.metadata},
        "original_detectors": original.metadata, "optimizer_detectors": retrained.metadata,
        "optimization": vars(app_config.OPTIMIZATION),
        "agent": {**{k: v for k, v in vars(app_config.AGENT).items() if k != "audit_log"},
                  "llm_provider": "offline (deterministic rule-based policy)"},
        "carbon_profile": "MockCarbonProvider (synthetic)",
        "simulation": {"interval_seconds": 1, "speed_factor": 300, "simulated_seconds_per_reading": 300},
        "environment": {"python": platform.python_version(), "platform": platform.platform(),
                        "cpu_count": __import__("os").cpu_count(), "packages": _packages(),
                        "git_commit": _git_commit()},
        "command": " ".join(["python -m experiments.evaluation", *(argv or sys.argv[1:])]),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }, indent=2, default=str), encoding="utf-8")

    jobs = [(name, seed, cfg, versions, args.out) for seed in cfg.seeds for name in args.scenarios]
    # Longest jobs first so the pool stays busy.
    jobs.sort(key=lambda j: ["attack_undefended", "attack_defended", "optimized", "baseline"].index(j[0]))
    # "spawn": forking after XGBoost/OpenMP are loaded can deadlock the workers (and spawn is the Windows default).
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        for message in pool.map(_job, jobs):
            print(message, flush=True)
    print(f"done in {time.perf_counter() - started:.0f} s; raw results in {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
