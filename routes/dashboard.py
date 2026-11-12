"""HTML pages. Data is loaded by JavaScript from the JSON API and Socket.IO."""

from __future__ import annotations

from flask import Blueprint, current_app, render_template

from processors.data_validator import ATTACK_PARAMETERS, MAX_ATTACK_MINUTES
from simulator.building_model import default_zones

bp = Blueprint("pages", __name__)

PAGES = [("pages.index", "Dashboard", "speedometer2"), ("pages.energy", "Energy Analytics", "lightning-charge"),
         ("pages.security", "Security Monitoring", "shield-lock"), ("pages.agent", "Agentic AI Activity", "robot"),
         ("pages.pipeline", "Data Pipeline", "diagram-3")]


def _render(template: str, **context):
    config = current_app.extensions["app_config"]
    attack_parameters = {k: {n: (sorted(r) if isinstance(r, set) else list(r)) for n, r in v.items()}
                         for k, v in ATTACK_PARAMETERS.items()}
    return render_template(template, pages=PAGES, controls_enabled=config.DASHBOARD.controls_enabled,
                           building_id=config.SIMULATOR.building_id, attack_parameters=attack_parameters,
                           max_attack_minutes=MAX_ATTACK_MINUTES, control_mode=config.SYSTEM.control_mode,
                           approval_mode=config.AGENT.approval_mode, approval_timeout_s=config.AGENT.approval_timeout_s,
                           slm_name=f"{config.SLM.model} (Ollama)" if config.SLM.enabled else None,
                           zones=[(z.zone_id, z.name, z.zone_type, z.floor_area_m2, z.max_occupancy)
                                  for z in default_zones()], **context)


@bp.get("/")
def index():
    return _render("dashboard.html", active="pages.index")


@bp.get("/energy")
def energy():
    return _render("energy.html", active="pages.energy")


@bp.get("/security")
def security():
    return _render("security.html", active="pages.security")


@bp.get("/agent")
def agent():
    return _render("agent.html", active="pages.agent")


@bp.get("/pipeline")
def pipeline():
    return _render("pipeline.html", active="pages.pipeline")
