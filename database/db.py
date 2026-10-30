"""PostgreSQL connection management (psycopg 3 + connection pool).

Usage::

    db = Database(config.DATABASE)
    db.open()                       # waits for the server, raises if unreachable
    with db.transaction() as conn:  # commit on success, rollback on error
        conn.execute("SELECT 1")
    db.close()

Connection problems (server down, network loss, pool exhausted) are raised as
:class:`DatabaseUnavailableError`, so callers can retry later instead of
treating them like bad data. The pool validates connections before handing
them out and reconnects automatically once the server is back.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg_pool import ConnectionPool, PoolTimeout

from config import DatabaseSettings

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
SCHEMA_VERSION = 4


class DatabaseUnavailableError(RuntimeError):
    """The database cannot be reached right now (transient)."""


# Errors that mean "try again later", not "this data is wrong".
TRANSIENT_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError, PoolTimeout)


class Database:
    """Connection pool wrapper with transactional helpers."""

    def __init__(self, settings: DatabaseSettings, *, name: str = "app") -> None:
        self.settings = settings
        self.pool = ConnectionPool(
            kwargs={**settings.connection_kwargs(), "application_name": f"smart-building-{name}"},
            min_size=settings.pool_min_size,
            max_size=settings.pool_max_size,
            open=False,
            check=ConnectionPool.check_connection,   # drop dead connections on checkout
            reconnect_timeout=0,                      # keep trying to refill the pool forever
            name=name,
        )

    def open(self, *, timeout_s: float = 30.0) -> None:
        """Open the pool and wait until the server accepts connections.

        Raises:
            DatabaseUnavailableError: no connection within ``timeout_s``.
        """
        self.pool.open(wait=False)
        deadline = time.monotonic() + timeout_s
        delay, attempt = 0.5, 0
        while True:
            attempt += 1
            try:
                with self.pool.connection(timeout=max(1.0, min(5.0, deadline - time.monotonic()))) as conn:
                    conn.execute("SELECT 1")
                logger.info("Database reachable", extra={"database": self.settings.safe_dsn,
                                                         "attempts": attempt})
                return
            except TRANSIENT_ERRORS as exc:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise DatabaseUnavailableError(
                        f"Database {self.settings.safe_dsn} not reachable after "
                        f"{timeout_s:.0f}s: {exc}") from exc
                logger.warning("Waiting for database", extra={
                    "database": self.settings.safe_dsn, "attempt": attempt,
                    "error": str(exc).strip()})
                time.sleep(min(delay, remaining))
                delay = min(delay * 2, 5.0)

    def close(self) -> None:
        self.pool.close()

    def __enter__(self) -> "Database":
        self.open()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    @contextmanager
    def transaction(self, *, timeout_s: float = 10.0) -> Iterator[psycopg.Connection]:
        """Yield a connection inside a transaction (commit on success, rollback on error).

        Raises:
            DatabaseUnavailableError: connection problems (retry later).
            psycopg.Error: data/constraint errors (do not retry the same data).
        """
        try:
            with self.pool.connection(timeout=timeout_s) as conn:
                with conn.transaction():
                    yield conn
        except TRANSIENT_ERRORS as exc:
            raise DatabaseUnavailableError(str(exc).strip() or type(exc).__name__) from exc

    def ping(self) -> bool:
        try:
            with self.transaction(timeout_s=3) as conn:
                conn.execute("SELECT 1")
            return True
        except DatabaseUnavailableError:
            return False

    # -- schema ---------------------------------------------------------------------
    def apply_schema(self, path: Path = SCHEMA_PATH) -> None:
        """Create/upgrade tables (idempotent)."""
        statements = path.read_text(encoding="utf-8")
        with self.transaction(timeout_s=30) as conn:
            # Serialise concurrent schema setup from several workers.
            conn.execute("SELECT pg_advisory_xact_lock(4242001)")
            conn.execute(statements)
        logger.info("Database schema applied", extra={"version": SCHEMA_VERSION})

    def schema_version(self) -> int | None:
        with self.transaction() as conn:
            exists = conn.execute("SELECT to_regclass('schema_version')").fetchone()[0]
            if exists is None:
                return None
            return conn.execute("SELECT max(version) FROM schema_version").fetchone()[0]

    def require_schema(self) -> None:
        """Raise if the schema has not been initialised (python -m database init)."""
        version = self.schema_version()
        if version is None or version < SCHEMA_VERSION:
            raise RuntimeError(
                f"Database schema missing or outdated (found {version}, need {SCHEMA_VERSION}). "
                "Run: python -m database init")


def create_database_if_missing(settings: DatabaseSettings) -> bool:
    """Create the configured database (connects to the ``postgres`` maintenance DB).

    Returns ``True`` if the database was created.
    """
    kwargs = {**settings.connection_kwargs(), "dbname": "postgres", "autocommit": True}
    try:
        with psycopg.connect(**kwargs) as conn:
            exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s",
                                  (settings.name,)).fetchone()
            if exists:
                return False
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(settings.name)))
    except TRANSIENT_ERRORS as exc:
        raise DatabaseUnavailableError(str(exc).strip()) from exc
    logger.info("Database created", extra={"database": settings.name})
    return True
