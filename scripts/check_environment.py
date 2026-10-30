"""Verify the local Python environment and configuration.

Usage (from the project root)::

    python -m scripts.check_environment
    python -m scripts.check_environment --check-services   # also probe Kafka/PostgreSQL ports

Exit code is 0 when all required checks pass, 1 otherwise.
"""

from __future__ import annotations

import argparse
import importlib
from importlib import metadata
import platform
import socket
import sys

MIN_PYTHON = (3, 10)
REQUIRED_PACKAGES = {
    "flask": "Flask",
    "flask_socketio": "Flask-SocketIO",
    "confluent_kafka": "confluent-kafka",
    "psycopg": "psycopg",
    "psycopg_pool": "psycopg-pool",
    "dotenv": "python-dotenv",
    "requests": "requests",
}


def _report(ok: bool, label: str, detail: str = "") -> bool:
    status = "OK  " if ok else "FAIL"
    print(f"[{status}] {label}" + (f" - {detail}" if detail else ""))
    return ok


def check_python() -> bool:
    version = sys.version_info
    return _report(
        version >= MIN_PYTHON,
        f"Python {platform.python_version()} on {platform.system()} {platform.release()}",
        "" if version >= MIN_PYTHON else f"requires >= {'.'.join(map(str, MIN_PYTHON))}",
    )


def check_packages() -> bool:
    all_ok = True
    for module, dist in REQUIRED_PACKAGES.items():
        try:
            importlib.import_module(module)
            all_ok &= _report(True, f"import {module}", f"{dist} {metadata.version(dist)}")
        except (ImportError, metadata.PackageNotFoundError) as exc:
            all_ok &= _report(False, f"import {module}", f"pip install {dist} ({exc})")
    return all_ok


def check_config():
    try:
        from config import get_config

        config = get_config()
    except Exception as exc:  # noqa: BLE001 - report any configuration failure
        _report(False, "load configuration", str(exc))
        return None
    _report(True, "load configuration", f"APP_ENV={config.ENV_NAME}")
    for key, value in config.summary().items():
        print(f"         {key}: {value}")
    return config


def _tcp_reachable(host: str, port: int, timeout: float = 3.0) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, f"{host}:{port} reachable"
    except OSError as exc:
        return False, f"{host}:{port} unreachable ({exc})"


def check_services(config) -> bool:
    first_broker = config.KAFKA.bootstrap_servers.split(",")[0].strip()
    kafka_host, sep, kafka_port = first_broker.rpartition(":")
    if not sep or not kafka_port.isdigit():
        kafka_host, kafka_port = first_broker, "9092"
    ok, detail = _tcp_reachable(kafka_host or "localhost", int(kafka_port))
    ok_kafka = _report(ok, "Kafka broker", detail)
    ok, detail = _tcp_reachable(config.DATABASE.host, config.DATABASE.port)
    ok_db = _report(ok, "PostgreSQL server", detail)
    return ok_kafka and ok_db


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check-services", action="store_true",
                        help="also test TCP connectivity to Kafka and PostgreSQL")
    args = parser.parse_args(argv)

    ok = check_python()
    ok &= check_packages()
    config = check_config()
    ok &= config is not None
    if args.check_services and config is not None:
        ok &= check_services(config)

    print("\nEnvironment check " + ("PASSED" if ok else "FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
