# Sub-spec #4 — Connector Circuit Breaker

**Date:** 2026-07-10
**Milestone:** Robustness & Polish (sub-spec #4 of 4)
**Status:** Design approved

## Problem

`RESTConnector.request()` (`databot/connectors/rest_connector.py`) retries up to
`max_retries` (default 3) with exponential backoff (1s, 2s) on the "service
unhealthy" class: `httpx.ConnectError`, `httpx.TimeoutException`, 5xx, and 429.
When an upstream is down, **every** call pays the full budget — ~3 attempts plus
1s+2s of `asyncio.sleep` plus connect timeouts — before returning a failure, with
no memory that the service was already failing a moment ago. A down Airflow (or
Spark Livy, Kafka REST, dbt Cloud — all inherit `RESTConnector`) makes every tool
call slow and wasteful.

4xx (non-429) already returns immediately without retry, so the waste is specific
to the transport-failure / 5xx / 429 path.

## Goal

Add a per-connector circuit breaker so that after a run of consecutive failures the
connector **fast-fails** — zero HTTP attempts, zero backoff sleeps — for a cooldown
window, then allows a single trial request to test recovery.

## Decisions (from brainstorming)

- **On by default**, with conservative thresholds; disableable via config.
- **REST connector only** — the one place with a retry+backoff budget. SQL/catalog
  connectors have no retry loop and different failure semantics; leaving `BaseConnector`
  untouched keeps their surface clean.
- **Structured logs only** on state transitions — no Prometheus metric (can be added
  later).

## Design

### New module: `databot/connectors/circuit_breaker.py`

A self-contained 3-state breaker. No I/O. Fully unit-testable via an injected clock.

```python
class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        enabled: bool = True,
        time_func: Callable[[], float] = time.monotonic,
    ) -> None: ...

    @property
    def state(self) -> CircuitState: ...
    @property
    def recovery_timeout(self) -> float: ...

    def allow(self) -> bool: ...          # synchronous check-and-set
    def record_success(self) -> None: ...
    def record_failure(self) -> None: ...
```

**`allow()` — synchronous (no `await` inside), so atomic under asyncio.** This is
where the concurrency sensitivity lives; keeping it await-free is the guarantee. The
HALF_OPEN **state itself is the single-trial reservation** — the one caller that flips
OPEN → HALF_OPEN receives the trial; every other caller sees HALF_OPEN and fast-fails.
No separate in-flight flag is needed (it would be redundant with the state).

- `enabled=False` → always `True` (breaker is a no-op).
- CLOSED → `True`.
- OPEN → if `time_func() - _opened_at >= recovery_timeout`: transition to HALF_OPEN,
  log INFO, return `True` (this caller is the trial). Otherwise return `False`
  (fast-fail).
- HALF_OPEN → `False` (a trial is already outstanding; wait for it to record a result).

**`record_failure()`** (no-op when disabled):
- HALF_OPEN → the trial failed → `_trip()` (back to OPEN, timer reset).
- CLOSED → `_failures += 1`; if `_failures >= failure_threshold` → `_trip()`.

**`record_success()`** (no-op when disabled): set CLOSED, `_failures = 0`. Log INFO if
transitioning out of OPEN/HALF_OPEN.

**`_trip()`**: set OPEN, `_opened_at = time_func()`, log WARNING with name + failure
count.

Contract: for every `allow()` that returns `True`, the caller MUST call exactly one
of `record_success()` / `record_failure()`.

### Integration in `rest_connector.py`

**`__init__`** — build the breaker from config:

```python
cb = self._config.get("circuit_breaker", {})
self._breaker = CircuitBreaker(
    name,
    failure_threshold=cb.get("failure_threshold", 5),
    recovery_timeout=cb.get("recovery_timeout", 30.0),
    enabled=cb.get("enabled", True),
)
```

**`request()`** — gate at entry, then instrument the three existing return points:

1. At the very top (before building the client / retry loop):

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

2. The **2xx success** return → call `self._breaker.record_success()` immediately
   before returning the success result.
3. The **4xx (non-429) early** return → call `self._breaker.record_success()` before
   returning (the service is reachable; a bad request must not trip the breaker, and a
   4xx during a half-open trial should close it).
4. The **final "failed after N attempts"** return → call `self._breaker.record_failure()`
   before returning.

`own_client` cleanup (`await client.aclose()`) stays exactly where it is; the breaker
gate returns before a client is created, so no client leaks on the fast-fail path.

**`health_check()`** — no change. It calls `request()`, so when OPEN it fast-fails to
`UNREACHABLE`, and a periodic health probe can itself serve as a half-open trial.

### Failure classification (summary)

| Outcome | Breaker call |
|---|---|
| 2xx success | `record_success()` |
| 4xx, non-429 | `record_success()` (service reachable) |
| `ConnectError` / `TimeoutException` (retries exhausted) | `record_failure()` |
| 5xx / 429 (retries exhausted) | `record_failure()` |

### Out of scope (YAGNI)

- `success_threshold` fixed at 1 (half-open closes on first success) — not configurable.
- No change to `BaseConnector` or any non-REST connector.
- No Prometheus metric.

## Testing

**`tests/test_circuit_breaker.py`** (new, pure unit tests with an injected clock — a
small mutable `{"t": 0.0}` and `time_func=lambda: clock["t"]`):

- CLOSED → OPEN after exactly `failure_threshold` consecutive `record_failure()`.
- OPEN: `allow()` returns `False` before `recovery_timeout` elapses.
- OPEN → HALF_OPEN: after advancing the clock past `recovery_timeout`, `allow()`
  returns `True` and reserves the trial.
- HALF_OPEN + `record_success()` → CLOSED (`allow()` True again, counter reset).
- HALF_OPEN + `record_failure()` → OPEN, and the cooldown timer restarts (an
  immediate `allow()` is `False`).
- Concurrent half-open: after `allow()` reserves the trial, a second `allow()` before
  any record returns `False`.
- `enabled=False`: `allow()` always `True`; `record_failure()` never opens.
- A single `record_success()` in CLOSED resets a partial failure count (interleaved
  failure/success does not accumulate toward the threshold).

**REST integration** (added to `tests/test_connectors.py`, using
`httpx.MockTransport` that counts requests, and `max_retries=1` so no real
`asyncio.sleep` runs):

- With `failure_threshold=2, max_retries=1` and a transport that always raises
  `httpx.ConnectError`: two consecutive failing `request()` calls open the breaker;
  the third call makes **zero** transport requests (assert the mock's call count is
  exactly 2) and returns `metadata={"circuit": "open"}`.
- A transport returning 404 does **not** trip the breaker (repeated 404s keep
  `allow()` returning `True` / the breaker CLOSED).
- Recovery: after opening, advance the connector's breaker clock past
  `recovery_timeout` (inject via `connector._breaker` — construct it with a
  controllable `time_func`, or replace it in the test) and assert the next call is
  allowed through to the transport again.

Baseline is **497** passing; expect roughly **+14**. `ruff check databot/` and
`ruff format --check databot/` must stay clean.

## Execution & integration

- Subagent-driven (fresh subagent per task, two-stage review per task, broad final
  review), consistent with sub-specs #1–#3.
- Branch `feat/connector-circuit-breaker` off `main`; on completion **merge to `main`
  locally** with `--ff-only`. **Nothing is pushed to `origin`.**
