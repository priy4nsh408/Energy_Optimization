"""HTTP client for external APIs with caching, retries and rate-limit handling.

Behaviour of :meth:`CachedHttpClient.get_json`:

* **Fresh cache hit** (younger than ``ttl_s``): returned without a request.
* **Minimum interval**: never calls the same API more often than
  ``min_interval_s`` (protects free-tier quotas); serves the cache instead.
* **Retries**: connection errors, timeouts and HTTP 5xx are retried with
  exponential back-off (``max_retries``).
* **HTTP 429**: honours ``Retry-After`` and pauses the client until then.
* **HTTP 401/403**: raised as :class:`ApiAuthError` (bad/missing key); the
  client pauses for an hour instead of hammering the API.
* **Circuit breaker**: after repeated failures the client pauses with growing
  back-off (up to 1 h).
* **Stale-if-error**: while the API is unavailable, a cached response up to
  ``max_stale_s`` old is returned and flagged ``stale=True``.

Responses are cached in memory and as JSON files in ``cache_dir`` so restarts
do not consume quota. Cache keys and files never contain request headers, so
API keys are not written to disk.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable

import requests

logger = logging.getLogger(__name__)

AUTH_FAILURE_PAUSE_S = 3600.0
MAX_CIRCUIT_PAUSE_S = 3600.0


class ApiError(RuntimeError):
    """Base class for external API failures."""


class ApiUnavailableError(ApiError):
    """The API cannot be used right now (network, 5xx, rate limit, circuit open)."""

    def __init__(self, message: str, retry_after_s: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


class ApiAuthError(ApiError):
    """The API rejected the credentials (HTTP 401/403)."""


class ApiRequestError(ApiError):
    """The API rejected the request itself (other HTTP 4xx) or returned invalid JSON."""


@dataclass(frozen=True)
class ApiResponse:
    data: Any
    fetched_at: float          # epoch seconds of the original fetch
    from_cache: bool
    stale: bool = False

    @property
    def age_s(self) -> float:
        return time.time() - self.fetched_at


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
        except (TypeError, ValueError):
            return None


class CachedHttpClient:
    """One client per external API (rate limits and circuit state are per API)."""

    def __init__(self, name: str, *, cache_dir: Path | None = None, timeout_s: float = 10.0,
                 max_retries: int = 3, backoff_s: float = 1.0, min_interval_s: float = 0.0,
                 session: requests.Session | None = None,
                 sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.time) -> None:
        self.name = name
        self.cache_dir = cache_dir
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.backoff_s = backoff_s
        self.min_interval_s = min_interval_s
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "smart-building-energy-optimization/1.0")
        self._sleep = sleep
        self._clock = clock
        self._memory: dict[str, tuple[float, Any]] = {}
        self._paused_until = 0.0
        self._consecutive_failures = 0
        self._last_request_at: dict[str, float] = {}
        self.requests_made = 0

    # -- cache ----------------------------------------------------------------------
    @staticmethod
    def cache_key(url: str, params: dict[str, Any] | None) -> str:
        canonical = json.dumps({"url": url, "params": params or {}}, sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]

    def _cache_file(self, key: str) -> Path | None:
        return self.cache_dir / f"{self.name}-{key}.json" if self.cache_dir else None

    def _cache_get(self, key: str) -> tuple[float, Any] | None:
        if key in self._memory:
            return self._memory[key]
        path = self._cache_file(key)
        if path and path.is_file():
            try:
                entry = json.loads(path.read_text(encoding="utf-8"))
                self._memory[key] = (float(entry["fetched_at"]), entry["data"])
                return self._memory[key]
            except (OSError, ValueError, KeyError):
                logger.warning("Ignoring unreadable cache file", extra={"path": str(path)})
        return None

    def _cache_put(self, key: str, data: Any) -> None:
        fetched_at = self._clock()
        self._memory[key] = (fetched_at, data)
        path = self._cache_file(key)
        if not path:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            # Atomic replace: a crash never leaves a half-written cache file.
            fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump({"fetched_at": fetched_at, "data": data}, handle)
            os.replace(tmp, path)
        except OSError:
            logger.warning("Could not write cache file", extra={"path": str(path)}, exc_info=True)

    # -- requests -------------------------------------------------------------------
    def get_json(self, url: str, *, params: dict[str, Any] | None = None,
                 headers: dict[str, str] | None = None,
                 secret_params: dict[str, str] | None = None, ttl_s: float = 900.0,
                 max_stale_s: float = 6 * 3600.0) -> ApiResponse:
        """GET ``url`` and return parsed JSON, using the cache as described above.

        ``headers`` and ``secret_params`` (e.g. API keys) are sent with the
        request but excluded from the cache key and cache files.
        """
        key = self.cache_key(url, params)
        now = self._clock()
        cached = self._cache_get(key)
        if cached and now - cached[0] < ttl_s:
            return ApiResponse(cached[1], cached[0], from_cache=True)

        def stale_or_raise(error: ApiError) -> ApiResponse:
            if cached and now - cached[0] < max_stale_s:
                logger.warning("Serving stale cached response", extra={
                    "api": self.name, "age_s": round(now - cached[0]), "reason": str(error)})
                return ApiResponse(cached[1], cached[0], from_cache=True, stale=True)
            raise error

        if now < self._paused_until:
            return stale_or_raise(ApiUnavailableError(
                f"{self.name}: paused after failures/rate limit",
                retry_after_s=self._paused_until - now))
        last = self._last_request_at.get(key)
        if last is not None and now - last < self.min_interval_s:
            return stale_or_raise(ApiUnavailableError(
                f"{self.name}: minimum request interval not reached",
                retry_after_s=self.min_interval_s - (now - last)))

        try:
            data = self._request_with_retries(url, {**(params or {}), **(secret_params or {})},
                                              headers)
        except ApiAuthError:
            self._paused_until = self._clock() + AUTH_FAILURE_PAUSE_S
            raise
        except ApiUnavailableError as error:
            self._consecutive_failures += 1
            pause = error.retry_after_s or min(
                MAX_CIRCUIT_PAUSE_S, 30.0 * 2 ** (self._consecutive_failures - 1))
            self._paused_until = self._clock() + pause
            logger.error("External API unavailable", extra={
                "api": self.name, "error": str(error), "pause_s": round(pause),
                "consecutive_failures": self._consecutive_failures})
            return stale_or_raise(error)
        finally:
            self._last_request_at[key] = self._clock()

        self._consecutive_failures = 0
        self._cache_put(key, data)
        return ApiResponse(data, self._memory[key][0], from_cache=False)

    def _request_with_retries(self, url: str, params: dict[str, Any] | None,
                              headers: dict[str, str] | None) -> Any:
        last_error: ApiUnavailableError | None = None
        for attempt in range(self.max_retries + 1):
            if attempt:
                self._sleep(self.backoff_s * 2 ** (attempt - 1))
            self.requests_made += 1
            try:
                response = self.session.get(url, params=params, headers=headers,
                                            timeout=self.timeout_s)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = ApiUnavailableError(f"{self.name}: {type(exc).__name__}: {exc}")
                logger.warning("External API request failed", extra={
                    "api": self.name, "attempt": attempt + 1, "error": type(exc).__name__})
                continue

            status = response.status_code
            if status == 429:
                retry_after = _retry_after_seconds(response.headers.get("Retry-After")) or 60.0
                logger.warning("External API rate limit reached", extra={
                    "api": self.name, "retry_after_s": retry_after})
                raise ApiUnavailableError(f"{self.name}: rate limited (HTTP 429)", retry_after)
            if status in (401, 403):
                raise ApiAuthError(f"{self.name}: authentication failed (HTTP {status}); "
                                   "check the API key in .env")
            if status >= 500:
                last_error = ApiUnavailableError(f"{self.name}: server error HTTP {status}")
                logger.warning("External API server error", extra={
                    "api": self.name, "attempt": attempt + 1, "status": status})
                continue
            if status >= 400:
                raise ApiRequestError(f"{self.name}: HTTP {status}: {response.text[:300]}")
            try:
                return response.json()
            except ValueError as exc:
                raise ApiRequestError(f"{self.name}: response is not valid JSON") from exc
        assert last_error is not None
        raise last_error
