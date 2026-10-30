"""Flask application entry point.

Only wiring lives here: configuration, logging, Socket.IO and blueprints.
Business logic belongs in the dedicated packages (simulator, ml, security,
agent, ...). Flask never consumes Kafka directly.

Run with::

    python app.py
"""

from __future__ import annotations

import logging

from flask import Flask, current_app, jsonify
from flask_socketio import SocketIO

from config import BaseConfig, get_config
from routes import register_blueprints
from services.live_updates import LiveUpdater
from services.registry import DashboardServices
from utils.logging_config import configure_logging

socketio = SocketIO()
logger = logging.getLogger(__name__)


def create_app(config: BaseConfig | None = None, *, services: DashboardServices | None = None) -> Flask:
    """Application factory.

    Args:
        config: Configuration object; defaults to :func:`config.get_config`.
        services: Data/control services (tests inject their own); created lazily otherwise.
    """
    config = config or get_config()
    configure_logging(config.LOGGING, service_name="web")

    app = Flask(__name__)
    app.config.from_object(config)
    app.extensions["app_config"] = config

    socketio.init_app(
        app,
        async_mode=config.SOCKETIO_ASYNC_MODE,
        message_queue=config.SOCKETIO_MESSAGE_QUEUE or None,
        cors_allowed_origins=config.SOCKETIO_CORS_ALLOWED_ORIGINS,
    )

    services = services or DashboardServices(config)
    app.extensions["dashboard_services"] = services
    app.extensions["live_updater"] = LiveUpdater(socketio, services.data, config.DASHBOARD.poll_interval_s)
    register_blueprints(app)

    @app.get("/health")
    def health():
        """Liveness check: confirms the app started with a valid configuration."""
        return jsonify(status="ok", env=config.ENV_NAME)

    logger.info("Flask application created", extra={"config": config.summary()})
    return app


@socketio.on("connect")
def _on_connect(auth=None):
    """Start the (single) live-update task when the first browser connects."""
    if not current_app.testing:
        current_app.extensions["live_updater"].ensure_started()


if __name__ == "__main__":
    application = create_app()
    cfg = application.extensions["app_config"]
    socketio.run(
        application,
        host=cfg.FLASK_HOST,
        port=cfg.FLASK_PORT,
        debug=cfg.DEBUG,
        # No auto-reloader: it forks a second process (two live-update pollers, and a child that
        # survives when a supervisor stops the parent).
        use_reloader=False,
        # The built-in server is fine for local use; production needs a real WSGI server.
        allow_unsafe_werkzeug=cfg.ENV_NAME != "production",
    )
