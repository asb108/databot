"""Unit tests for the connector circuit breaker."""

from __future__ import annotations

from databot.connectors.circuit_breaker import CircuitBreaker, CircuitState


def _breaker(**kw):
    """Build a breaker with an injectable clock; returns (breaker, clock_dict)."""
    clock = {"t": 0.0}
    kw.setdefault("failure_threshold", 3)
    kw.setdefault("recovery_timeout", 30.0)
    b = CircuitBreaker("test", time_func=lambda: clock["t"], **kw)
    return b, clock


class TestCircuitBreaker:
    def test_starts_closed_and_allows(self):
        b, _ = _breaker()
        assert b.state == CircuitState.CLOSED
        assert b.allow() is True

    def test_opens_after_threshold(self):
        b, _ = _breaker(failure_threshold=3)
        for _ in range(3):
            assert b.allow() is True
            b.record_failure()
        assert b.state == CircuitState.OPEN

    def test_open_fast_fails_before_cooldown(self):
        b, clock = _breaker(failure_threshold=1, recovery_timeout=30.0)
        b.record_failure()
        assert b.state == CircuitState.OPEN
        clock["t"] = 29.0
        assert b.allow() is False

    def test_open_to_half_open_after_cooldown(self):
        b, clock = _breaker(failure_threshold=1, recovery_timeout=30.0)
        b.record_failure()
        clock["t"] = 30.0
        assert b.allow() is True
        assert b.state == CircuitState.HALF_OPEN

    def test_half_open_success_closes(self):
        b, clock = _breaker(failure_threshold=1, recovery_timeout=30.0)
        b.record_failure()
        clock["t"] = 30.0
        b.allow()  # -> HALF_OPEN (this caller is the trial)
        b.record_success()
        assert b.state == CircuitState.CLOSED
        assert b.allow() is True

    def test_half_open_failure_reopens_and_resets_timer(self):
        b, clock = _breaker(failure_threshold=1, recovery_timeout=30.0)
        b.record_failure()
        clock["t"] = 30.0
        b.allow()  # -> HALF_OPEN
        b.record_failure()  # trial fails
        assert b.state == CircuitState.OPEN
        assert b.allow() is False  # timer reset — immediately fast-fails again
        clock["t"] = 60.0  # 30s after reopen
        assert b.allow() is True

    def test_concurrent_half_open_second_allow_false(self):
        b, clock = _breaker(failure_threshold=1, recovery_timeout=30.0)
        b.record_failure()
        clock["t"] = 30.0
        assert b.allow() is True   # reserves the trial (flips to HALF_OPEN)
        assert b.allow() is False  # second caller fast-fails
        assert b.state == CircuitState.HALF_OPEN

    def test_disabled_never_opens(self):
        b, _ = _breaker(failure_threshold=1, enabled=False)
        for _ in range(5):
            assert b.allow() is True
            b.record_failure()
        assert b.state == CircuitState.CLOSED
        assert b.allow() is True

    def test_success_resets_partial_failures(self):
        b, _ = _breaker(failure_threshold=3)
        b.record_failure()
        b.record_failure()
        b.record_success()  # reset the partial count
        b.record_failure()
        b.record_failure()
        assert b.state == CircuitState.CLOSED  # only 2 failures since the reset

    def test_half_open_watchdog_recovers_from_lost_trial(self):
        b, clock = _breaker(failure_threshold=1, recovery_timeout=30.0)
        b.record_failure()            # OPEN at t=0
        clock["t"] = 30.0
        assert b.allow() is True      # grant trial -> HALF_OPEN, anchored at t=30
        assert b.state == CircuitState.HALF_OPEN
        assert b.allow() is False     # concurrent caller fast-fails (single trial)
        # trial coroutine is "lost" (cancelled) and never records; watchdog kicks in:
        clock["t"] = 60.0
        assert b.allow() is True      # fresh trial granted after another cooldown
        assert b.state == CircuitState.HALF_OPEN

    def test_open_ignores_late_failures(self):
        b, clock = _breaker(failure_threshold=1, recovery_timeout=30.0)
        b.record_failure()            # OPEN at t=0
        clock["t"] = 10.0
        b.record_failure()            # late in-flight failure; must NOT push the timer
        clock["t"] = 30.0
        assert b.allow() is True      # 30 - 0 >= 30 -> trial granted (timer stayed at t=0)
