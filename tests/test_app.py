"""Tests for the Flask application factory and environment check script."""

from __future__ import annotations

from app import create_app
from config import get_config
from scripts import check_environment


def test_create_app_with_testing_config(clean_env):
    app = create_app(get_config("testing", load_dotenv_file=False))
    assert app.config["TESTING"] is True
    response = app.test_client().get("/health")
    assert response.status_code == 200
    assert response.get_json() == {"status": "ok", "env": "testing"}


def test_check_environment_passes(clean_env, capsys):
    clean_env.setenv("APP_ENV", "testing")
    assert check_environment.main([]) == 0
    assert "Environment check PASSED" in capsys.readouterr().out


def test_check_environment_reports_bad_config(clean_env, capsys):
    clean_env.setenv("APP_ENV", "testing")
    clean_env.setenv("FLASK_PORT", "not-a-port")
    assert check_environment.main([]) == 1
    assert "FAIL" in capsys.readouterr().out
