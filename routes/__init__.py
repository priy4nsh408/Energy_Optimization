"""Flask blueprints: HTML pages and the JSON API used by the dashboard's JavaScript."""

from __future__ import annotations

from flask import Flask


def register_blueprints(app: Flask) -> None:
    from routes.agent import bp as agent_api
    from routes.api import bp as core_api
    from routes.dashboard import bp as pages
    from routes.energy import bp as energy_api
    from routes.security import bp as security_api

    for blueprint in (pages, core_api, energy_api, security_api, agent_api):
        app.register_blueprint(blueprint)
