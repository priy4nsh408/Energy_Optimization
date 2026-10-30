"""Energy analytics API: power, hourly energy and carbon, predictions, baseline vs. optimized."""

from __future__ import annotations

from flask import Blueprint, jsonify, request

from routes.api import data, json_endpoint

bp = Blueprint("energy_api", __name__, url_prefix="/api/energy")


@bp.get("/power")
@json_endpoint
def power():
    return jsonify(data().power_timeseries(request.args.get("hours", 6)))


@bp.get("/hourly")
@json_endpoint
def hourly():
    return jsonify(rows=data().hourly_energy(request.args.get("hours", 48)))


@bp.get("/summary")
@json_endpoint
def summary():
    return jsonify(data().energy_summary(request.args.get("hours", 24)))


@bp.get("/predictions")
@json_endpoint
def predictions():
    return jsonify(data().predictions(request.args.get("hours", 12), request.args.get("zone_id") or None))


@bp.get("/optimization")
@json_endpoint
def optimization():
    return jsonify(live=data().optimizer_savings(request.args.get("hours", 24)), experiment=data().comparison())
