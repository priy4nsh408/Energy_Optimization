"""Agentic AI API: decisions (explanations) and corrective-action history."""

from __future__ import annotations

from flask import Blueprint, jsonify, request

from routes.api import data, json_endpoint

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
