"""Closed-loop demonstration: simulated attacks -> detection -> agent -> safety -> simulated execution.

Everything runs in one process, at simulation speed, without Kafka:

    simulator --(attack injector)--> security monitor --alerts--> incident agent
        ^                                                               |
        +---- approved actions only (safety validator) <----------------+

With ``--optimizer`` the deterministic optimizer (Module 7) also controls the
zones, pausing zones whose temperature or occupancy sensor has been isolated
(those zones run on their baseline schedule). It is off by default because the
Module 8 detectors were trained on baseline-controlled data: optimizer setbacks
trigger the HVAC-contradiction rule (false spoofing alerts in every zone).

    python -m agent.demo                                   # default scenario, offline policy unless a key is set
    python -m agent.demo --provider offline --report docs/results/agent_demo.md
    python -m agent.demo --provider anthropic              # Claude (needs ANTHROPIC_API_KEY)
    python -m agent.demo --attack temperature_spoofing:Z1:1980:120:offset_c=4

Attack specs: TYPE:ZONE:START_MIN:DURATION_MIN[:key=value,...] (minutes after the start).
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from agent.data import InMemoryDataSource
from agent.dispatch import IncidentDispatcher
from agent.executor import SimulatorGateway, apply_to_simulator
from agent.graph import IncidentAgent
from agent.isolation import SensorIsolationRegistry
from agent.llm import create_llm
from agent.recorder import CompositeRecorder, JsonlRecorder
from agent.safety import SafetyValidator
from agent.tools import AgentToolbox
from config import AgentSettings, ExternalDataSettings, SimulatorSettings, get_config
from data_sources.carbon import MockCarbonProvider
from data_sources.weather import MockWeatherProvider
from database.models import AgentDecisionRecord
from optimization.hvac_optimizer import ZoneConditions, building_models
from optimization.optimizer import DecisionScheduler, ZoneOptimizer
from processors.data_validator import ValidationError, validate_sensor_event
from security.monitor import SecurityMonitor
from security.train import SecurityModelNotFoundError, load_models
from simulator.attack_simulator import AttackInjector, AttackScenario
from simulator.engine import BuildingSimulator
from utils.logging_config import configure_logging

DEFAULT_START = datetime(2026, 2, 9, tzinfo=timezone.utc)     # inside the detectors' season (trained on January)
# Day 0 is clean (history for replay and drift windows); attacks on day 1 (minutes after the start).
DEFAULT_ATTACKS = [
    "temperature_spoofing:Z1:2040:120:offset_c=4",          # day 1, 10:00-12:00
    "occupancy_manipulation:Z3:2100:120:mode=hide",           # day 1, 11:00-13:00
    "abnormal_power:Z4:2340:60:factor=3",                     # day 1, 15:00-16:00
    "replay:Z2:2460:120:lag=288",                             # day 1, 17:00-19:00
]
UNTRUSTED_FOR_CONTROL = ("temperature", "occupancy")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m agent.demo", description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=datetime.fromisoformat, default=DEFAULT_START)
    parser.add_argument("--hours", type=float, default=48.0, help="simulated hours (default 48)")
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--warmup-hours", type=float, default=24.0,
                        help="detectors run but alerts are not investigated (default 24: history for "
                             "rate features, replay checks and drift windows)")
    parser.add_argument("--attack", action="append", metavar="SPEC",
                        help="attack spec (repeatable); default: four attacks on day 1")
    parser.add_argument("--provider", choices=AgentSettings.PROVIDERS, help="override AGENT_LLM_PROVIDER")
    parser.add_argument("--optimizer", action="store_true",
                        help="also run the Module 7 optimizer in the loop (see the module docstring)")
    parser.add_argument("--report", type=Path, help="write a Markdown report")
    parser.add_argument("--audit-log", type=Path, help="JSON Lines decision log (default AGENT_AUDIT_LOG)")
    return parser.parse_args(argv)


class ClosedLoop:
    def __init__(self, args: argparse.Namespace) -> None:
        config = get_config()
        self.config = config
        self.settings = dataclasses.replace(config.AGENT, **({"llm_provider": args.provider} if args.provider else {}))
        sim_settings = SimulatorSettings(interval_seconds=1, speed_factor=300, random_seed=args.seed,
                                         start_time=args.start)
        self.simulator = BuildingSimulator(sim_settings)
        self.injector = AttackInjector([AttackScenario.parse(s, args.start)
                                        for s in (args.attack or DEFAULT_ATTACKS)])
        self.monitor = SecurityMonitor(load_models())
        external = dataclasses.replace(config.EXTERNAL_DATA, weather_provider="mock", carbon_provider="mock")
        self.data = InMemoryDataSource(weather=MockWeatherProvider(external, sim_settings, seed=args.seed),
                                       carbon=MockCarbonProvider())
        self.isolation = SensorIsolationRegistry()
        models = building_models()
        self.validator = SafetyValidator(self.settings, config.OPTIMIZATION, models, self.isolation)
        self.optimizer = ZoneOptimizer(config.OPTIMIZATION, models) if args.optimizer else None
        self.scheduler = DecisionScheduler(config.OPTIMIZATION.decision_interval_s)
        toolbox = AgentToolbox(self.data, self.isolation, self.validator,
                               ZoneOptimizer(config.OPTIMIZATION, models), self.monitor.models)
        self.llm = create_llm(self.settings)
        self.audit_log = args.audit_log or self.settings.audit_log
        self.agent = IncidentAgent(llm=self.llm, toolbox=toolbox, data=self.data, isolation=self.isolation,
                                   validator=self.validator, gateway=SimulatorGateway(self.simulator, self.isolation),
                                   recorder=CompositeRecorder(JsonlRecorder(self.audit_log)),
                                   max_iterations=self.settings.max_iterations)
        self.dispatcher = IncidentDispatcher(self.settings, self.isolation)
        self.warmup_end = args.start + timedelta(hours=args.warmup_hours)
        self.ticks = int(args.hours * 3600 / sim_settings.simulated_seconds_per_tick)
        self.stats: Counter[str] = Counter()
        self.decisions: list[AgentDecisionRecord] = []
        self.skipped: list[dict[str, Any]] = []
        self.optimizer_paused: Counter[str] = Counter()

    def _optimize(self, event: dict[str, Any]) -> None:
        zone = event["zone_id"]
        try:
            conditions = ZoneConditions.from_reading(validate_sensor_event(event))
        except ValidationError:
            return
        if not self.scheduler.due(zone, conditions.timestamp, conditions.occupancy):
            return
        if any(self.isolation.is_isolated(zone, s) for s in UNTRUSTED_FOR_CONTROL):
            self.optimizer_paused[zone] += 1           # baseline schedule while the sensor is untrusted
            return
        carbon = self.data.carbon()
        decision = self.optimizer.optimize(conditions, carbon_intensity=carbon["intensity_g_per_kwh"]
                                           if carbon else None)
        if decision.approved:
            apply_to_simulator(self.simulator, zone, "set_setpoint", {"setpoint_c": decision.setpoint_c})
            apply_to_simulator(self.simulator, zone, "set_lighting", {"lighting_level": decision.lighting_level})
            self.stats["optimizer_decisions"] += 1

    def run(self) -> None:
        for _ in range(self.ticks):
            for event in self.simulator.tick():
                event = self.injector.apply(event)
                self.data.add_event(event)
                output = self.monitor.process(event)
                for alert in output.alerts:
                    self.stats[f"alerts_{alert['alert_type']}"] += 1
                    self.data.add_alert(alert)
                    if datetime.fromisoformat(alert["detected_at"]) < self.warmup_end:
                        investigate, why = False, "monitoring warm-up"
                    else:
                        investigate, why = self.dispatcher.should_investigate(alert)
                    if investigate:
                        decision = self.agent.run(alert)
                        self.dispatcher.remember(alert, decision)
                        self.decisions.append(decision)
                    else:
                        self.skipped.append({"alert": alert, "why": why})
                if self.optimizer is not None and output.valid:
                    self._optimize(event)


def _alert_label(alert_id: Any, alerts: dict[str, dict[str, Any]]) -> str:
    alert = alerts.get(str(alert_id), {})
    return (f"{(alert.get('details') or {}).get('classification', alert.get('alert_type'))} in "
            f"{alert.get('zone_id')} at {alert.get('detected_at', '')[:16]}")


def render(loop: ClosedLoop, args: argparse.Namespace) -> str:
    attacks = args.attack or DEFAULT_ATTACKS
    alerts = {a["alert_id"]: a for a in loop.data.alerts(None, hours=10_000)}
    provider = loop.llm.provider
    lines = [
        "# Agentic AI demonstration: closed loop on the simulated building", "",
        f"Generated by `python -m agent.demo{' --provider ' + args.provider if args.provider else ''}` on "
        f"{datetime.now(timezone.utc).isoformat(timespec='seconds')}. All values come from this run.", "",
        f"* Agent decision-maker: **{provider}** (`{loop.llm.model}`)" + (
            " - deterministic rule-based playbook, **no LLM was called**. Run with "
            "`--provider anthropic` and `ANTHROPIC_API_KEY` for Claude." if provider == "offline" else ""),
        f"* Simulation: {args.hours:g} simulated hours from {args.start.isoformat()}, seed {args.seed}, "
        f"5-minute readings; Module 7 optimizer {'on' if args.optimizer else 'off'}; the first "
        f"{args.warmup_hours:g} h are a monitoring warm-up (alerts recorded, not investigated).",
        f"* Detectors: Module 8 models `{loop.monitor.models.metadata.get('version')}`.",
        f"* Injected attacks: {', '.join(f'`{a}`' for a in attacks)}.",
        f"* Alerts raised: {sum(v for k, v in loop.stats.items() if k.startswith('alerts_'))} "
        f"({', '.join(f'{k[7:]} {v}' for k, v in sorted(loop.stats.items()) if k.startswith('alerts_'))}); "
        f"investigated by the agent: {len(loop.decisions)}; not investigated: {len(loop.skipped)} "
        f"({', '.join(f'{v}x {k}' for k, v in Counter(s['why'].split(' at ')[0] for s in loop.skipped).items())}).",
        f"* Optimizer decisions applied: {loop.stats['optimizer_decisions']}; decisions skipped because a zone's "
        f"sensor was isolated: {dict(loop.optimizer_paused) or 'none'}.",
        f"* Sensors isolated at the end: "
        f"{', '.join(f'{i.zone_id}/{i.sensor}' for i in loop.isolation.isolated()) or 'none'}.", "",
        "## Incidents", "",
    ]
    for n, d in enumerate(loop.decisions, 1):
        lines += [f"### {n}. {_alert_label(d.alert_id, alerts)}", "",
                  f"*Provider:* {d.llm_provider} · *tools called:* "
                  f"{', '.join(t['tool'] for t in d.tool_calls) or 'none'}", "",
                  f"* **Diagnosis:** {d.diagnosis}",
                  f"* **Likely cause:** {d.likely_cause} (confidence {d.confidence})",
                  f"* **Energy impact:** {d.energy_impact}",
                  f"* **Operational impact:** {d.operational_impact}",
                  "* **Recommendations:** " + "; ".join(d.recommendations), ""]
        if d.actions:
            lines += ["| Proposed action | Parameters | Safety validator | Status | Result |",
                      "|---|---|---|---|---|"]
            for a in d.actions:
                lines.append(f"| {a['action_type']} {a['zone_id']} | `{json.dumps(a['parameters'])}` | "
                             f"{a['safety_reason'] or ''} | **{a['status']}** | {a['result'] or ''} |")
            lines.append("")
        else:
            lines += ["No actions proposed.", ""]
        if d.errors:
            lines += ["Errors: " + "; ".join(d.errors), ""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.start.tzinfo is None:
        args.start = args.start.replace(tzinfo=timezone.utc)
    config = get_config()
    configure_logging(config.LOGGING, "agent_demo", stream=sys.stderr)
    try:
        loop = ClosedLoop(args)
    except SecurityModelNotFoundError as exc:
        print(f"{exc}\nTrain the detectors first: python -m security.train", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"Invalid argument: {exc}", file=sys.stderr)
        return 2
    loop.run()
    report = render(loop, args)
    print(report)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
