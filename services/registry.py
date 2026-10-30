"""Lazily created services shared by the Flask app (one instance per app)."""

from __future__ import annotations

import logging
import threading

from config import BaseConfig
from services.dashboard_data import DashboardData
from services.simulation_control import SimulationControl

logger = logging.getLogger(__name__)


class ServiceUnavailableError(RuntimeError):
    """A backing service (PostgreSQL) is not available; the API answers 503."""


class DashboardServices:
    def __init__(self, config: BaseConfig, *, database=None, control: SimulationControl | None = None) -> None:
        self.config = config
        self._database = database
        self._data: DashboardData | None = None
        self._control = control
        self._lock = threading.Lock()

    def data(self) -> DashboardData:
        with self._lock:
            if self._data is None:
                from database.db import Database, DatabaseUnavailableError

                database = self._database or Database(self.config.DATABASE, name="web")
                try:
                    if self._database is None:
                        database.open(timeout_s=3)
                    database.require_schema()
                except (DatabaseUnavailableError, RuntimeError) as exc:
                    if self._database is None:
                        database.close()
                    raise ServiceUnavailableError(str(exc)) from exc
                self._database = database
                self._data = DashboardData(database, comparison_path=self.config.DASHBOARD.comparison_results)
            return self._data

    def control(self) -> SimulationControl:
        with self._lock:
            if self._control is None:
                from streaming.producer import JsonProducer

                config = self.config
                self._control = SimulationControl(lambda: JsonProducer(config.KAFKA, name="web"),
                                                  config.SIMULATOR.building_id)
            return self._control
