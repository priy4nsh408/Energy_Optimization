"""Security API: alerts, drift, simulated attacks, isolated sensors."""

from __future__ import annotations

from flask import Blueprint, jsonify, request

from routes.api import data, json_endpoint

bp = Blueprint("security_api", __name__, url_prefix="/api/security")


@bp.get("/alerts")
@json_endpoint
def alerts():
    return jsonify(rows=data().alerts(request.args.get("limit", 100), request.args.get("type") or None,
                                      request.args.get("zone_id") or None))


@bp.get("/timeline")
@json_endpoint
def timeline():
    return jsonify(rows=data().alert_timeline(request.args.get("hours", 48)))


@bp.get("/drift")
@json_endpoint
def drift():
    return jsonify(rows=data().drift(request.args.get("limit", 50)))


@bp.get("/attacks")
@json_endpoint
def attacks():
    return jsonify(rows=data().attacks(request.args.get("limit", 50)))


@bp.get("/isolated")
@json_endpoint
def isolated():
    return jsonify(rows=data().isolated_sensors())
