"""Preflight: is everything in place to start the whole system?

    python -m scripts.preflight            # human-readable, exit code 1 if anything blocks start-up
    python -m scripts.preflight --json

Checks configuration, packages, Kafka, PostgreSQL schema, trained models and
detectors for the configured control mode, the agent's LLM settings, the
dashboard port and whether a simulation is already running. Every problem
comes with the command that fixes it. ``scripts.run_stack`` runs this first.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import socket
import sys
from dataclasses import asdict, dataclass
from typing import Callable

from config import BASE_DIR, ConfigurationError, get_config

OK, WARN, FAIL = "ok", "warn", "fail"


@dataclass
class Check:
    name: str
    status: str
    detail: str
    fix: str = ""


def _packages(config) -> list[Check]:
    from scripts.check_environment import REQUIRED_PACKAGES

    missing = [dist for module, dist in REQUIRED_PACKAGES.items() if importlib.util.find_spec(module) is None]
    if missing:
        return [Check("python packages", FAIL, f"missing: {', '.join(missing)}", "pip install -r requirements.txt")]
    return [Check("python packages", OK, f"{len(REQUIRED_PACKAGES)} required packages importable")]


def _env_file(config) -> list[Check]:
    if (BASE_DIR / ".env").is_file():
        return [Check(".env", OK, "found")]
    return [Check(".env", WARN, "no .env file: only defaults and process environment are used",
                  "copy .env.example .env   (then set POSTGRES_PASSWORD etc.)")]


def _kafka(config) -> list[Check]:
    from streaming.kafka_config import KafkaUnavailableError, create_admin_client, wait_for_broker
    from streaming.topics import TOPICS

    try:
        wait_for_broker(config.KAFKA, timeout_s=5)
    except KafkaUnavailableError as exc:
        return [Check("kafka", FAIL, str(exc), "docker compose -f docker/docker-compose.yml --env-file .env up -d")]
    existing = set(create_admin_client(config.KAFKA).list_topics(timeout=10).topics)
    missing = [t for t in TOPICS if config.KAFKA.topic(t) not in existing]
    checks = [Check("kafka", OK, f"broker {config.KAFKA.bootstrap_servers} reachable")]
    checks.append(Check("kafka topics", WARN if missing else OK,
                        f"missing (services create them): {', '.join(missing)}" if missing
                        else f"all {len(TOPICS)} topics exist", "python -m streaming.topics" if missing else ""))
    return checks


def _database(config) -> list[Check]:
    from database.db import SCHEMA_VERSION, Database, DatabaseUnavailableError

    db = Database(config.DATABASE, name="preflight")
    try:
        db.open(timeout_s=5)
    except DatabaseUnavailableError as exc:
        db.close()
        return [Check("postgresql", FAIL, str(exc),
                      "docker compose -f docker/docker-compose.yml --env-file .env up -d; check POSTGRES_PASSWORD")]
    try:
        version = db.schema_version()
        checks = [Check("postgresql", OK, config.DATABASE.safe_dsn)]
        if version is None or version < SCHEMA_VERSION:
            checks.append(Check("database schema", FAIL, f"version {version}, need {SCHEMA_VERSION}",
                                "python -m database init"))
            return checks
        with db.transaction() as conn:
            zones = conn.execute("SELECT count(*) FROM zones").fetchone()[0]
            readings = conn.execute("SELECT count(*) FROM sensor_readings").fetchone()[0]
            alive = conn.execute(
                "SELECT extract(epoch FROM now() - updated_at) FROM simulation_status "
                "WHERE building_id = %s AND state <> 'stopped'", (config.SIMULATOR.building_id,)).fetchone()
        checks.append(Check("database schema", OK, f"version {version}, {zones} zones, {readings} readings"))
        if alive and alive[0] is not None and alive[0] < 15:
            checks.append(Check("simulation manager", WARN, "a simulation manager is already running for "
                                f"{config.SIMULATOR.building_id} (heartbeat {alive[0]:.0f} s ago)",
                                "stop it first, or a second one will refuse to start"))
        return checks
    finally:
        db.close()


def _models(config) -> list[Check]:
    from ml.features import BUILDING_SPEC
    from ml.registry import ModelNotFoundError, load_bundle
    from security.train import DETECTOR_ROOTS, SecurityModelNotFoundError, load_configured_models

    checks = []
    try:
        bundle = load_bundle(BUILDING_SPEC.name)
        checks.append(Check("ML building model", OK, f"{bundle.model_name} {bundle.version}"))
    except ModelNotFoundError:
        checks.append(Check("ML building model", FAIL, "no trained model (prediction_service cannot start)",
                            "python -m scripts.bootstrap"))
    detector_set = config.SYSTEM.detector_set
    try:
        models = load_configured_models(config)
        checks.append(Check("security detectors", OK, f"{detector_set} set, version {models.metadata['version']}"))
    except SecurityModelNotFoundError:
        fix = ("python -m security.train --control optimizer" if detector_set == "optimizer_control"
               else "python -m security.train")
        checks.append(Check("security detectors", FAIL,
                            f"no '{detector_set}' detectors in {DETECTOR_ROOTS[detector_set]}", fix))
    return checks


def _consistency(config) -> list[Check]:
    system, agent = config.SYSTEM, config.AGENT
    checks = [Check("control mode", OK, f"{system.control_mode}: simulator executes "
                    f"{' + '.join(system.executed_command_sources)} commands; detectors '{system.detector_set}'")]
    expected = "optimizer_control" if system.control_mode == "closed_loop" else "baseline"
    if system.detector_set != expected:
        checks.append(Check("detector/control mismatch", WARN,
                            f"{system.control_mode} with '{system.detector_set}' detectors: expect many false "
                            "alarms (detectors are specific to the control regime, Module 11)",
                            "set SECURITY_DETECTORS=auto"))
    provider = agent.resolved_provider
    if agent.llm_provider == "anthropic" and not (os.environ.get("ANTHROPIC_API_KEY")
                                                  or os.environ.get("ANTHROPIC_AUTH_TOKEN")):
        checks.append(Check("agent LLM", WARN, "AGENT_LLM_PROVIDER=anthropic but no ANTHROPIC_API_KEY: every "
                            "incident will fall back to the offline policy", "set ANTHROPIC_API_KEY or use auto"))
    else:
        checks.append(Check("agent LLM", OK, f"{provider}" + (f" ({agent.llm_model})" if provider == "anthropic"
                                                               else " (rule-based playbook, no LLM)")))
    return checks


def _external_data(config) -> list[Check]:
    """Providers that need a key make external_data_publisher exit at once (and the stack give up)."""
    from data_sources.factory import create_carbon_provider, create_weather_provider

    checks = []
    for name, create in (("weather provider", create_weather_provider), ("carbon provider", create_carbon_provider)):
        try:
            provider = create(config)
            checks.append(Check(name, OK, provider.name + (" (synthetic data)" if provider.name == "mock" else "")))
        except ConfigurationError as exc:
            checks.append(Check(name, FAIL, str(exc), "set the API key in .env, set CARBON_PROVIDER=mock, or "
                                                      "start with --mock-external (start.ps1 -MockExternal)"))
    return checks


def _dashboard(config) -> list[Check]:
    checks = []
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((config.FLASK_HOST, config.FLASK_PORT))
            checks.append(Check("dashboard port", OK, f"{config.FLASK_HOST}:{config.FLASK_PORT} free"))
        except OSError:
            checks.append(Check("dashboard port", WARN, f"{config.FLASK_HOST}:{config.FLASK_PORT} in use "
                                "(dashboard already running?)", "set FLASK_PORT or stop the other process"))
    if not config.DASHBOARD.comparison_results.is_file():
        checks.append(Check("dashboard comparison results", WARN, "baseline-vs-optimized experiment file missing",
                            "python -m experiments.comparison --json docs/results/optimization_comparison.json"))
    return checks


CHECKS: list[Callable] = [_env_file, _packages, _consistency, _external_data, _kafka, _database, _models, _dashboard]


def run_checks() -> list[Check]:
    try:
        config = get_config()
    except ConfigurationError as exc:
        return [Check("configuration", FAIL, str(exc), "fix the variable in .env (see .env.example)")]
    results = [Check("configuration", OK, f"APP_ENV={config.ENV_NAME}")]
    for check in CHECKS:
        try:
            results += check(config)
        except Exception as exc:  # noqa: BLE001 - a broken check must not hide the others
            results.append(Check(check.__name__.strip("_"), FAIL, f"{type(exc).__name__}: {exc}"))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m scripts.preflight", description=__doc__.splitlines()[0])
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    results = run_checks()
    if args.json:
        print(json.dumps([asdict(r) for r in results], indent=2))
    else:
        for r in results:
            print(f"[{r.status.upper():<4}] {r.name}: {r.detail}" + (f"\n         fix: {r.fix}" if r.fix else ""))
        failed = sum(r.status == FAIL for r in results)
        print("\nPreflight " + ("FAILED" if failed else "passed") + f" ({failed} blocking problem(s), "
              f"{sum(r.status == WARN for r in results)} warning(s))")
    return 1 if any(r.status == FAIL for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
