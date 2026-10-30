"""Graceful shutdown for long-running worker processes (Windows and POSIX)."""

from __future__ import annotations

import logging
import signal
import threading

logger = logging.getLogger(__name__)


def install_shutdown_handlers(stop_event: threading.Event) -> None:
    """Set ``stop_event`` on Ctrl+C / SIGTERM (and Ctrl+Break on Windows).

    Must be called from the main thread.
    """

    def handle(signum: int, _frame: object) -> None:
        if stop_event.is_set():
            raise KeyboardInterrupt  # second Ctrl+C forces exit
        logger.info("Shutdown requested", extra={"signal": signal.Signals(signum).name})
        stop_event.set()

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):  # SIGBREAK exists only on Windows
        sig = getattr(signal, name, None)
        if sig is not None:
            signal.signal(sig, handle)
