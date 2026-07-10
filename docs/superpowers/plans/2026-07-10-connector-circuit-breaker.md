# Connector Circuit Breaker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-connector circuit breaker so `RESTConnector` fast-fails (zero HTTP attempts, zero backoff sleeps) while an upstream is down, instead of paying the full retry+backoff budget on every call.

**Architecture:** A self-contained 3-state `CircuitBreaker` (CLOSED → OPEN → HALF_OPEN) in a new `databot/connectors/circuit_breaker.py`, with a synchronous (await-free, atomic under asyncio) `allow()` and an injectable clock for deterministic tests. `RESTConnector` owns one instance, gates `request()` at entry, and reports each call's outcome (success / 4xx = success signal / transport-error = failure) to the breaker.

**Tech Stack:** Python 3, `httpx` (0.28.1, `MockTransport` for tests), pytest (`python3 -m pytest`), ruff. Interpreter is `python3` (NOT `python`).

## Global Constraints

- Interpreter is `python3`. Tests: `python3 -m pytest`. Lint: `ruff check databot/` and `ruff format --check databot/` (CI lints `databot/` only, not `tests/`).
- `CircuitBreaker.allow()` MUST be synchronous — no `await` inside — so its state transitions are atomic under asyncio. This is the concurrency guarantee.
- The HALF_OPEN state itself is the single-trial reservation: the one caller that flips OPEN → HALF_OPEN gets the trial; every other caller sees HALF_OPEN and returns `False`. Do NOT add a separate in-flight flag.
- Defaults: `failure_threshold=5`, `recovery_timeout=30.0`, `enabled=True`. Config is read from `connector config["circuit_breaker"]` (`enabled`, `failure_threshold`, `recovery_timeout`).
- Breaker contract: for every `allow()` that returns `True`, the caller calls exactly one of `record_success()` / `record_failure()`.
- Failure classification: 2xx → success; 4xx non-429 → success (service reachable); `ConnectError`/`TimeoutException`/5xx/429 (retries exhausted) → failure.
- REST-only. No change to `BaseConnector` or any non-REST connector. No Prometheus metric.
- Baseline: **497** tests passing. Keep every existing test green.
- Local-only: branch `feat/connector-circuit-breaker` off `main`; ff-merge to `main` at the end; nothing pushed to `origin`.

---

### Task 1: The `CircuitBreaker` module

**Files:**
- Create: `databot/connectors/circuit_breaker.py`
- Test: `tests/test_circuit_breaker.py`

**Interfaces:**
- Produces: `CircuitBreaker(name, *, failure_threshold=5, recovery_timeout=30.0, enabled=True, time_func=time.monotonic)` with properties `.state -> CircuitState`, `.recovery_timeout -> float`, and methods `allow() -> bool`, `record_success() -> None`, `record_failure() -> None`; and `CircuitState` (str Enum: `CLOSED`/`OPEN`/`HALF_OPEN`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_circuit_breaker.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_circuit_breaker.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'databot.connectors.circuit_breaker'`

- [ ] **Step 3: Write the implementation**

Create `databot/connectors/circuit_breaker.py`:

```python
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
        if self._state == CircuitState.OPEN:
            if self._time_func() - self._opened_at >= self._recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                logger.info(
                    f"Circuit breaker '{self._name}': half-open — trialing one request."
                )
                return True
            return False
        # HALF_OPEN: a trial is already outstanding.
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_circuit_breaker.py -q`
Expected: PASS (9 tests)

- [ ] **Step 5: Lint**

Run: `ruff check databot/ && ruff format --check databot/`
Expected: no errors. If format flags the new file, run `ruff format databot/connectors/circuit_breaker.py` and re-check.

- [ ] **Step 6: Commit**

```bash
git add databot/connectors/circuit_breaker.py tests/test_circuit_breaker.py
git commit -m "feat: add per-connector CircuitBreaker (3-state, injectable clock)"
```

---

### Task 2: Integrate the breaker into `RESTConnector`

**Files:**
- Modify: `databot/connectors/rest_connector.py` (`__init__` ~lines 26-33; `request()` ~lines 75-150)
- Test: `tests/test_connectors.py` (append a new test class)

**Interfaces:**
- Consumes: `CircuitBreaker`, `CircuitState` from Task 1 (`databot/connectors/circuit_breaker.py`).
- Produces: `RESTConnector` now has a `self._breaker: CircuitBreaker` attribute; `request()` fast-fails with `ConnectorResult(success=False, metadata={"circuit": "open"})` when the breaker is OPEN.

- [ ] **Step 1: Write the failing integration tests**

Append to `tests/test_connectors.py` (the file already imports `pytest`, `asyncio`, and connector base types — add the three new imports at the top of the file with the others):

```python
import httpx

from databot.connectors.circuit_breaker import CircuitBreaker, CircuitState
from databot.connectors.rest_connector import RESTConnector
```

Then append this test class at the end of the file:

```python
# ---------------------------------------------------------------------------
# REST connector circuit breaker
# ---------------------------------------------------------------------------


def _rest_with_transport(handler, *, clock=None, **cb):
    """Build a RESTConnector whose HTTP client uses a MockTransport handler.

    `cb` becomes the `circuit_breaker` config. When `clock` (a {"t": float} dict)
    is given, the breaker is rebuilt with an injectable time_func so recovery can
    be tested without real sleeps. max_retries=1 => the retry loop never sleeps.
    """
    conn = RESTConnector(
        "api",
        {"base_url": "http://test.local", "max_retries": 1, "circuit_breaker": cb},
    )
    conn._client = httpx.AsyncClient(
        base_url="http://test.local", transport=httpx.MockTransport(handler)
    )
    if clock is not None:
        conn._breaker = CircuitBreaker(
            "api",
            failure_threshold=cb.get("failure_threshold", 5),
            recovery_timeout=cb.get("recovery_timeout", 30.0),
            time_func=lambda: clock["t"],
        )
    return conn


class TestRESTCircuitBreaker:
    @pytest.mark.asyncio
    async def test_opens_and_fast_fails(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            raise httpx.ConnectError("down")

        conn = _rest_with_transport(handler, failure_threshold=2)
        r1 = await conn.request("GET", "/x")
        r2 = await conn.request("GET", "/x")
        assert r1.success is False and r2.success is False
        assert conn._breaker.state == CircuitState.OPEN
        r3 = await conn.request("GET", "/x")
        assert r3.success is False
        assert r3.metadata.get("circuit") == "open"
        assert calls["n"] == 2  # the third call never reached the transport
        await conn._client.aclose()

    @pytest.mark.asyncio
    async def test_4xx_does_not_trip(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(404, text="nope")

        conn = _rest_with_transport(handler, failure_threshold=2)
        for _ in range(4):
            r = await conn.request("GET", "/missing")
            assert r.success is False
        assert conn._breaker.state == CircuitState.CLOSED
        assert calls["n"] == 4  # every call reached the transport (never tripped)
        await conn._client.aclose()

    @pytest.mark.asyncio
    async def test_recovers_after_cooldown(self):
        clock = {"t": 0.0}
        state = {"fail": True}

        def handler(request):
            if state["fail"]:
                raise httpx.ConnectError("down")
            return httpx.Response(200, json={"ok": True})

        conn = _rest_with_transport(
            handler, clock=clock, failure_threshold=2, recovery_timeout=30.0
        )
        await conn.request("GET", "/x")
        await conn.request("GET", "/x")
        assert conn._breaker.state == CircuitState.OPEN
        state["fail"] = False           # upstream recovers
        clock["t"] = 30.0               # advance past cooldown
        r = await conn.request("GET", "/x")
        assert r.success is True
        assert conn._breaker.state == CircuitState.CLOSED
        await conn._client.aclose()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_connectors.py::TestRESTCircuitBreaker -q`
Expected: FAIL — `RESTConnector` has no `_breaker` attribute yet (`AttributeError`), because the breaker is not wired in.

- [ ] **Step 3: Add the import and build the breaker in `__init__`**

In `databot/connectors/rest_connector.py`, add the import after the existing `from databot.connectors.base import (...)` block:

```python
from databot.connectors.circuit_breaker import CircuitBreaker
```

At the end of `__init__` (right after `self._client: httpx.AsyncClient | None = None`), add:

```python
        cb = self._config.get("circuit_breaker", {})
        self._breaker = CircuitBreaker(
            name,
            failure_threshold=cb.get("failure_threshold", 5),
            recovery_timeout=cb.get("recovery_timeout", 30.0),
            enabled=cb.get("enabled", True),
        )
```

- [ ] **Step 4: Gate `request()` at entry**

In `request()`, immediately after the docstring and BEFORE the `client = self._client or httpx.AsyncClient(...)` line, insert the breaker gate:

```python
        if not self._breaker.allow():
            return ConnectorResult(
                success=False,
                error=(
                    f"Circuit breaker open for '{self._name}': upstream repeatedly failing. "
                    f"Retries are paused for up to {self._breaker.recovery_timeout:.0f}s."
                ),
                metadata={"circuit": "open"},
            )
```

- [ ] **Step 5: Instrument the three return points**

Still in `request()`, add one breaker call immediately before each of the three existing returns:

(a) Before the **2xx success** return `return ConnectorResult(success=True, data=body, metadata={"status": resp.status_code})`:

```python
                self._breaker.record_success()
```

(b) Before the **4xx (non-429)** early return inside `except httpx.HTTPStatusError` (the `return ConnectorResult(success=False, error=f"HTTP {e.response.status_code}: ...")`):

```python
                    self._breaker.record_success()
```

(c) Before the **final "failed after N attempts"** return at the end of the method (`return ConnectorResult(success=False, error=f"Request failed after {self._max_retries} attempts: {last_error}")`), and AFTER the existing `if own_client: await client.aclose()` block:

```python
        self._breaker.record_failure()
```

Match the indentation of each surrounding return exactly (return (a) and the success path are inside the `try`, (b) is one level deeper inside the `except`, (c) is at method-body level).

- [ ] **Step 6: Run the integration tests to verify they pass**

Run: `python3 -m pytest tests/test_connectors.py::TestRESTCircuitBreaker -q`
Expected: PASS (3 tests)

- [ ] **Step 7: Full regression + lint**

Run: `python3 -m pytest -q && ruff check databot/ && ruff format --check databot/`
Expected: all pass (~509 tests). If format flags `rest_connector.py`, run `ruff format databot/connectors/rest_connector.py`.

- [ ] **Step 8: Commit**

```bash
git add databot/connectors/rest_connector.py tests/test_connectors.py
git commit -m "feat: wire CircuitBreaker into RESTConnector.request()"
```

---

### Task 3: CHANGELOG

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add the changelog entry**

Under the existing `### Changed — Reliability` section in `CHANGELOG.md`, add:

```markdown
- **Connector circuit breaker:** `RESTConnector` now fast-fails while an upstream is
  down instead of paying the full retry+backoff budget on every call. A per-connector
  3-state breaker (`databot/connectors/circuit_breaker.py`) opens after
  `failure_threshold` consecutive failures (default 5), fast-fails for
  `recovery_timeout` seconds (default 30), then trials one request to test recovery.
  A 4xx response counts as reachable and never trips it. On by default; configure or
  disable via a connector's `circuit_breaker` config (`enabled`, `failure_threshold`,
  `recovery_timeout`).
```

- [ ] **Step 2: Full regression + lint (final gate)**

Run: `python3 -m pytest -q && ruff check databot/ && ruff format --check databot/`
Expected: all pass; ~509 tests (497 baseline + ~12).

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: changelog for connector circuit breaker"
```

---

## Self-Review

**Spec coverage:**
- New `circuit_breaker.py` with `CircuitBreaker` + `CircuitState`, injectable clock → Task 1. ✅
- `allow()` synchronous/atomic; HALF_OPEN state is the reservation (no flag) → Task 1 impl + `test_concurrent_half_open_second_allow_false`. ✅
- All 8 spec unit-test bullets (closed→open, open fast-fail, open→half-open, half-open success→closed, half-open fail→reopen+reset timer, concurrent half-open, disabled no-op, success resets partial) → Task 1 (9 tests). ✅
- `__init__` builds breaker from `config["circuit_breaker"]` with defaults → Task 2 Step 3. ✅
- `request()` entry gate with `metadata={"circuit":"open"}`, zero attempts → Task 2 Step 4 + `test_opens_and_fast_fails` (asserts `calls["n"] == 2`). ✅
- Three return-point instrumentation (2xx→success, 4xx→success, final→failure) → Task 2 Step 5. ✅
- 4xx does not trip → `test_4xx_does_not_trip`. ✅
- Recovery after cooldown → `test_recovers_after_cooldown`. ✅
- `health_check()` unchanged (naturally fast-fails via `request()`) → no task needed; not modified. ✅
- No BaseConnector/other-connector change, no metric → no such task exists. ✅
- CHANGELOG → Task 3. ✅

**Placeholder scan:** No TBD/TODO. Every code step carries complete code. The three return-point edits (Task 2 Step 5) reference the exact existing return statements by their literal text so the implementer can locate them without re-deriving.

**Type consistency:** `CircuitBreaker(name, *, failure_threshold, recovery_timeout, enabled, time_func)`, `.state`, `.recovery_timeout`, `allow()/record_success()/record_failure()`, and `CircuitState.{CLOSED,OPEN,HALF_OPEN}` are used identically in Task 1 (definition + tests) and Task 2 (construction + assertions). `metadata={"circuit": "open"}` key matches between Task 2 Step 4 and `test_opens_and_fast_fails`. ✅
