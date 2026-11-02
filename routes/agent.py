"""Agentic AI API: decisions (explanations) and corrective-action history."""

from __future__ import annotations

import uuid

from flask import Blueprint, current_app, jsonify, request

from processors.data_validator import ValidationError
from routes.api import data, json_endpoint, services
from services.simulation_control import ControlUnavailableError

bp = Blueprint("agent_api", __name__, url_prefix="/api/agent")


@bp.get("/decisions")
@json_endpoint
def decisions():
    return jsonify(rows=data().decisions(request.args.get("limit", 30)))


@bp.get("/actions")
@json_endpoint
def actions():
    return jsonify(rows=data().actions(request.args.get("limit", 100), request.args.get("source") or None),
                   summary=data().action_summary())


@bp.get("/approvals")
@json_endpoint
def approvals():
    config = current_app.extensions["app_config"]
    return jsonify(rows=data().approvals(request.args.get("limit", 50)), mode=config.AGENT.approval_mode,
                   timeout_s=config.AGENT.approval_timeout_s)


@bp.post("/actions/<action_id>/decision")
def decide(action_id: str):
    """Approve or decline an action waiting for approval (published to ``operator_decisions``)."""
    config = current_app.extensions["app_config"]
    if not config.DASHBOARD.controls_enabled:
        return jsonify(error="dashboard controls are disabled (DASHBOARD_CONTROLS_ENABLED=false)"), 403
    if not request.is_json:
        return jsonify(error="send a JSON body (Content-Type: application/json)"), 415
    try:
        uuid.UUID(action_id)
    except ValueError:
        return jsonify(error="unknown action id"), 404
    body = request.get_json(silent=True) or {}
    try:
        message = services().control().send_operator_decision(action_id, str(body.get("decision", "")),
                                                              note=body.get("note") or None)
    except ValidationError as exc:
        return jsonify(error="invalid decision", details=exc.errors), 400
    except ControlUnavailableError as exc:
        return jsonify(error=str(exc)), 503
    return jsonify(sent=message, note="applied by workers.agent_service; the action list updates when it is done"), 202
