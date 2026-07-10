"""Per-connector circuit breaker.

A classic 3-state breaker (CLOSED → OPEN → HALF_OPEN) that lets a connector
fast-fail while an upstream is down instead of paying the full retry+backoff
budget on every call. ``allow()`` is synchronous (no ``await``) so its state
transitions are atomic under asyncio; the HALF_OPEN state itself is the
single-trial reservation.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from enum import Enum

from loguru import logger


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Track consecutive failures and gate calls to a flaky upstream.

    Contract: for every ``allow()`` that returns ``True``, the caller must call
    exactly one of ``record_success()`` / ``record_failure()``.
    """

    def __init__(
        self,
        name: str,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        enabled: bool = True,
        time_func: Callable[[], float] = time.monotonic,
    ) -> None:
        self._name = name
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._enabled = enabled
        self._time_func = time_func
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at = 0.0

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def recovery_timeout(self) -> float:
        return self._recovery_timeout

    def allow(self) -> bool:
        """Return True if a call may proceed to the upstream. Synchronous/atomic."""
        if not self._enabled:
            return True
        if self._state == CircuitState.CLOSED:
            return True
        # OPEN, or a HALF_OPEN whose trial never reported back (e.g. its coroutine
        # was cancelled): gate on the cooldown clock so a lost trial self-heals.
        if self._time_func() - self._opened_at >= self._recovery_timeout:
            was = self._state
            self._state = CircuitState.HALF_OPEN
            self._opened_at = self._time_func()  # anchor the trial watchdog
            if was == CircuitState.OPEN:
                logger.info(f"Circuit breaker '{self._name}': half-open — trialing one request.")
            else:
                logger.warning(
                    f"Circuit breaker '{self._name}': half-open trial did not report back "
                    f"within {self._recovery_timeout:.0f}s — retrying one request."
                )
            return True
        return False

    def record_success(self) -> None:
        if not self._enabled:
            return
        if self._state != CircuitState.CLOSED:
            logger.info(f"Circuit breaker '{self._name}': closing — upstream recovered.")
        self._state = CircuitState.CLOSED
        self._failures = 0

    def record_failure(self) -> None:
        if not self._enabled:
            return
        if self._state == CircuitState.HALF_OPEN:
            self._trip()
            return
        if self._state == CircuitState.OPEN:
            # Already open; ignore late in-flight failures so they neither extend
            # the cooldown nor inflate the failure count.
            return
        self._failures += 1
        if self._failures >= self._failure_threshold:
            self._trip()

    def _trip(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._time_func()
        logger.warning(
            f"Circuit breaker '{self._name}': OPEN after {self._failures} failures — "
            f"fast-failing for {self._recovery_timeout:.0f}s."
        )
