# Gateway App Factory + HTTP Endpoint Tests — Design

**Date:** 2026-07-04
**Status:** Approved (design)
**Sub-spec:** #1 of the "Robustness & Polish" milestone

---

## Milestone context

This is the first of four sequenced, independent sub-specs in a robustness effort.
Each gets its own spec → plan → implementation cycle. Recorded here so the queued work
is not lost:

| # | Sub-spec | Depends on | Status |
|---|----------|------------|--------|
| **1** | **App-factory extraction + HTTP endpoint tests** | — | **this doc** |
| 2 | Structured tool errors (`ToolError`: code/message/hint/retryable) | — | queued |
| 3 | Error UX in chat (clean, truncated, traceback-free failures) | #2 | queued |
| 4 | Connector circuit breaker (fast-fail on repeatedly-down services) | (soft) #2 | queued |

The four are *not* one design — they are decomposed deliberately. Sub-spec #1 is
orthogonal to #2–#4 (tool errors never surface through `/livez`, `/metrics`, etc.), and it
is scheduled first because it is behavior-preserving, closes the top known follow-up, and
produces a `TestClient` harness that later sub-specs reuse for integration coverage.

---

## Problem

The gateway's FastAPI app and all ~30 route closures are constructed **inline inside the
`gateway()` Typer command** (`databot/cli/commands.py`, ~lines 1008–1685), capturing local
variables (`tools`, `sessions`, `bus`, `connector_registry`, `monitor_service`,
`skill_registry`, `usage_tracker`, `metrics`, `audit_log`, …) via closure.

Because the app is welded to the CLI command's body — which also connects connectors,
starts background services, and runs `uvicorn` — the HTTP surface **cannot be instantiated
for a test** without standing up the entire gateway. As a result these endpoints have
**zero HTTP-level coverage** today:

`/livez` · `/readyz` · `/metrics` · `/usage` · `/audit` · `/monitors` · `/queries`

They are thin wrappers over unit-tested facades/stores, but the wiring, status codes,
serialization, and middleware interaction are unverified. This is the top known follow-up.

## Goals

1. Extract the FastAPI app construction into a **pure, module-level factory**
   `build_api(...) -> FastAPI` that takes already-built components and returns the app.
2. Add HTTP-level tests via FastAPI `TestClient` covering the seven untested endpoints
   above (plus a few free neighbors once the harness exists).
3. **Preserve behavior exactly** — `databot gateway` must start and serve identically.

## Non-goals

- No change to route handler logic or response shapes.
- No change to `_build_components` or any component wiring (the 14-tuple stays as-is).
- No `GatewayComponents` dataclass yet (deferred; see Approach B).
- No new endpoints, no auth/rate-limit redesign.
- Nothing from sub-specs #2–#4.

---

## Approach

**Chosen: A — pure factory with explicit keyword params.**

Move the `FastAPI(...)` construction, the three `add_middleware` calls, and every
`@api.get/post/put/delete` route closure verbatim into a module-level function. The route
bodies are unchanged; they simply close over the factory's parameters instead of the CLI
command's locals. `gateway()` builds components exactly as it does now, calls
`build_api(...)`, and then proceeds to startup/uvicorn/shutdown unchanged.

Rejected alternatives:

- **B — bundle components into a `GatewayComponents` dataclass** (single param). Cleaner
  for a ~12-arg signature and would tidy the existing 14-tuple smell, but converting the
  tuple edges into a *wiring* refactor — out of scope for a behavior-preserving extraction.
  Deferred to a later cleanup.
- **C — inner `create_app()` closure returning `api`.** Rejected: it still captures locals,
  so tests cannot build it without standing up the whole gateway. Does not deliver
  testability.

---

## Design

### Module boundary

New file **`databot/cli/api.py`** exposing:

```python
def build_api(
    *,
    cfg,                     # DatabotConfig
    tools,                   # ToolRegistry
    sessions,                # SessionManager
    bus,                     # MessageBus
    loop,                    # AgentLoop
    delegator,               # Delegator | None
    connector_registry,      # ConnectorRegistry | None
    skill_registry,          # SkillRegistry | None
    usage_tracker,           # UsageTracker | None
    monitor_service,         # MonitorService
    workspace,               # Path
    channels=None,           # list of channel objects exposing get_fastapi_routes()
) -> FastAPI:
    ...
```

The exact parameter set is whatever the moved route closures reference — enumerated
precisely during implementation, not guessed here. `build_api` returns a fully-configured
`FastAPI` (middleware + routes + any channel routers), and does **nothing else** (no
`connect`, no `.start()`, no `uvicorn`).

`commands.py:gateway()` changes from ~680 lines of inline app to:

```python
api = build_api(cfg=cfg, tools=tools, sessions=sessions, bus=bus, loop=loop,
                delegator=delegator, connector_registry=connector_registry,
                skill_registry=skill_registry, usage_tracker=usage_tracker,
                monitor_service=monitor_service, workspace=workspace,
                channels=[c for c in (_gchat, _whatsapp, _telegram) if c])
```

### What moves vs. what stays

| Moves into `build_api` | Stays in `gateway()` |
|------------------------|----------------------|
| `FastAPI(title=..., version=...)` | `_build_components(cfg)` |
| CORS / API-key / rate-limit middleware | connector `connect_all()` |
| All `@api.*` route closures (verbatim) | cron/monitor service construction |
| `include_router(...)` for channel webhooks | channel `.start()` (slack/discord/webhook) |
| `/ui` mount (guarded by `cfg.ui.enabled`) | startup health checks |
| | background tasks (`loop.run`, cron, monitor) |
| | `uvicorn.serve()` + shutdown cleanup |

### Channel handling

Channel objects (gchat app-mode, whatsapp, telegram) currently are constructed and have
their routers included *and* are `.start()`-ed interleaved in the route section. Split:
**construction + `.start()` stay in `gateway()`**; `build_api` receives the already-built
channel objects via the `channels=` param and calls `include_router(c.get_fastapi_routes())`
for each. Tests pass `channels=None` — the seven target endpoints do not need channels.

---

## Test plan

New file **`tests/test_gateway_api.py`** using `fastapi.testclient.TestClient` (httpx is
already a dependency).

**Harness.** A pytest fixture builds a minimal component set:
- Real, cheap components: `SessionManager` on a `tmp_path` SQLite, `ToolRegistry`,
  `MessageBus`, configured metrics + audit log.
- Small stubs where the real thing is heavy: `connector_registry` (health controllable),
  `usage_tracker`, `monitor_service`.
- Config with **empty `api_keys`** (auth middleware no-ops) and a **high `rate_limit_rpm`**
  so tests are not throttled. Both middleware behaviors are verified first before relying
  on them.

**Cases:**

| Endpoint | Assertion |
|----------|-----------|
| `/livez` | 200, process-up body |
| `/readyz` | 200 when connectors healthy; **503** when a stub connector reports `unreachable` |
| `/metrics` | 200 — exercised both with and without `prometheus_client` installed (graceful no-op) |
| `/usage` | 200, cost/usage JSON shape |
| `/audit` | 200, returns recorded tool executions |
| `/monitors` | 200, monitor list |
| `/queries` | 200, saved-query list |
| `/health`, `/api/v1/tools`, `/api/v1/skills`, `/api/v1/stats` | folded in — free once the harness exists |

---

## Scope guardrails

- No route logic changes — closures move verbatim.
- No `_build_components` change; the 14-tuple is untouched.
- No component-wiring refactor (the advisor's explicit caution: a "behavior-preserving"
  extraction must not silently change startup).
- Pure extraction + new tests only.

## Verification / success criteria

1. Full `pytest` suite green (all existing tests + new gateway-API tests).
2. `ruff check databot/` and `ruff format --check databot/` clean.
3. Manual `databot gateway` smoke: starts, serves `/health`, and logs are equivalent to
   pre-change (byte-for-byte equivalent startup behavior).
4. The seven previously-untested endpoints now have passing HTTP-level assertions.

## Risks & mitigations

| Risk | Mitigation |
|------|------------|
| A moved closure references a local not passed as a param → `NameError` at import/first-request | Enumerate captured names during implementation; smoke-run the gateway; tests exercise each route |
| Channel router inclusion order/behavior changes | Keep `include_router` calls and their order identical; pass channels in the same order |
| Middleware assumptions break tests (auth/rate-limit) | Verify no-op-on-empty-keys and rpm behavior first; configure the fixture accordingly |
| `commands.py` import cycle when importing `api.py` | `build_api` imports are lazy/local where needed, mirroring current in-function imports |
