"""Core JSON API: status, live readings and simulation control.

All reads go through :class:`services.dashboard_data.DashboardData`
(PostgreSQL). Control requests are published to Kafka by
:class:`services.simulation_control.SimulationControl`; Flask never consumes Kafka.
"""

from __future__ import annotations

import logging
from functools import wraps

from flask import Blueprint, current_app, jsonify, request

from processors.data_validator import ValidationError
from services.registry import DashboardServices, ServiceUnavailableError
from services.simulation_control import ControlUnavailableError

bp = Blueprint("api", __name__, url_prefix="/api")
logger = logging.getLogger(__name__)


def services() -> DashboardServices:
    return current_app.extensions["dashboard_services"]


def data():
    return services().data()


def json_endpoint(view):
    """Return 503 JSON when PostgreSQL is unavailable instead of an HTML error page."""
    @wraps(view)
    def wrapper(*args, **kwargs):
        from database.db import DatabaseUnavailableError

        try:
            return view(*args, **kwargs)
        except (ServiceUnavailableError, DatabaseUnavailableError) as exc:
            logger.warning("Dashboard data unavailable", extra={"error": str(exc)})
            return jsonify(error="database unavailable", detail=str(exc)), 503
    return wrapper


@bp.get("/status")
@json_endpoint
def status():
    return jsonify(data().status())


@bp.get("/live")
@json_endpoint
def live():
    return jsonify(readings=data().live_readings())


# -- simulation control (POST, JSON only) ---------------------------------------------
def _control(command: str, attack: dict | None = None):
    config = current_app.extensions["app_config"]
    if not config.DASHBOARD.controls_enabled:
        return jsonify(error="simulation controls are disabled (DASHBOARD_CONTROLS_ENABLED=false)"), 403
    # JSON bodies cannot be sent cross-site without a CORS preflight, which this app does not allow.
    if not request.is_json:
        return jsonify(error="send a JSON body (Content-Type: application/json)"), 415
    try:
        message = services().control().send(command, attack)
    except ValidationError as exc:
        return jsonify(error="invalid command", details=exc.errors), 400
    except ControlUnavailableError as exc:
        return jsonify(error=str(exc)), 503
    return jsonify(sent=message, note="executed by workers.simulation_manager; watch the status panel"), 202


@bp.post("/simulation/start")
def simulation_start():
    return _control("start")


@bp.post("/simulation/stop")
def simulation_stop():
    return _control("stop")


@bp.post("/simulation/attack")
def simulation_attack():
    body = request.get_json(silent=True) or {}
    attack = {k: body.get(k) for k in ("attack_type", "zone_id", "duration_min")}
    attack["params"] = body.get("params") or {}
    return _control("attack", attack)
