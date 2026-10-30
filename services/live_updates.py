"""Push new data to browsers over Socket.IO.

A single background task (started with the first browser connection) polls
PostgreSQL every ``DASHBOARD_POLL_INTERVAL_S`` and emits only what is new:

* ``status``     simulation state, latest reading time, carbon, counters
* ``readings``   readings stored since the last poll
* ``alerts``     new alerts or status changes
* ``decisions``  new agent decisions
* ``actions``    new actions or status changes (proposed -> approved -> executed ...)

The task never touches Kafka, and short queries keep it non-blocking for the
web server (Socket.IO's ``sleep`` yields between polls).
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable

logger = logging.getLogger(__name__)


class LiveUpdater:
    def __init__(self, socketio, data_factory: Callable[[], Any], interval_s: float) -> None:
        self.socketio, self.data_factory, self.interval_s = socketio, data_factory, interval_s
        self._started = False
        self._lock = threading.Lock()
        self._watermark: str | None = None
        self._seen: dict[str, set] = {"alerts": set(), "decisions": set(), "actions": set()}
        self._primed = False
        self._db_ok = True
        self.polls = 0

    def ensure_started(self) -> bool:
        with self._lock:
            if self._started:
                return False
            self._started = True
        self.socketio.start_background_task(self._run)
        logger.info("Live updates started", extra={"interval_s": self.interval_s})
        return True

    def _run(self) -> None:
        while True:
            self.poll_once()
            self.socketio.sleep(self.interval_s)

    @staticmethod
    def _new(rows: list[dict[str, Any]], seen: set, key: Callable[[dict], Any]) -> list[dict[str, Any]]:
        fresh = [r for r in rows if key(r) not in seen]
        seen.update(key(r) for r in rows)
        return fresh

    def poll_once(self) -> dict[str, int]:
        """One poll; returns how many items of each kind were emitted."""
        self.polls += 1
        emitted: dict[str, int] = {}
        try:
            data = self.data_factory()
            self.socketio.emit("status", data.status())
            readings = data.readings_since(self._watermark)
            if readings:
                self._watermark = readings[-1]["ingested_at"]
                self.socketio.emit("readings", readings)
                emitted["readings"] = len(readings)
            batches = {
                "alerts": (data.alerts(limit=25), lambda r: (r["alert_id"], r["status"])),
                "decisions": (data.decisions(limit=10), lambda r: r["decision_id"]),
                "actions": (data.actions(limit=40), lambda r: (r["action_id"], r["status"])),
            }
            for name, (rows, key) in batches.items():
                fresh = self._new(rows, self._seen[name], key)
                if fresh and self._primed:               # first poll only records what already exists
                    self.socketio.emit(name, list(reversed(fresh)))
                    emitted[name] = len(fresh)
            self._primed = True
            if not self._db_ok:
                logger.info("Database reachable again")
            self._db_ok = True
        except Exception as exc:  # noqa: BLE001 - keep pushing once the database is back
            if self._db_ok:
                logger.warning("Live update poll failed", extra={"error": str(exc)})
            self._db_ok = False
            self.socketio.emit("status", {"error": "database unavailable"})
        return emitted
