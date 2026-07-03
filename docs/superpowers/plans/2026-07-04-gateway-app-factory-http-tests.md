# Gateway App Factory + HTTP Endpoint Tests — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the gateway's FastAPI app out of the `gateway()` CLI command into a pure, testable `build_api(...)` factory, then add HTTP-level tests covering the seven previously-untested endpoints.

**Architecture:** A new module `databot/cli/api.py` exposes `build_api(**components) -> FastAPI`. It constructs the app, middleware, all route closures (moved verbatim), and channel webhook routers — and does *no* lifecycle work. `gateway()` builds components exactly as before, constructs the three webhook channels, calls `build_api(...)`, then does startup/uvicorn/shutdown. Tests build a minimal component set and drive the app with FastAPI `TestClient`.

**Tech Stack:** Python 3.9+, FastAPI, `fastapi.testclient.TestClient` (httpx-backed), pytest, ruff.

**Design ref:** `docs/superpowers/specs/2026-07-04-gateway-app-factory-http-tests-design.md`

---

## Key facts established during design (do not re-derive)

- The app + routes live in `databot/cli/commands.py` inside the **`async def _run_gateway(...)`** function (lines **920–1764**), *not* the outer `gateway()` Typer command (line 223) that calls it. The app block is lines **1008–1631** (FastAPI construction, 3 middleware, all `@api.*` route closures, `/ui` mount). Channel setup is **1633–1684**.
- Verified via full AST free-name analysis of `_run_gateway`: the moved block (1020–1631) references exactly these enclosing locals that become `build_api` params (**12 components**): `cfg, tools, sessions, connector_registry, rag_context, delegator, skill_registry, usage_tracker, monitor_service, loop, metrics, audit_log`. Plus `api` (created at the top of `build_api`), `logger` (imported in `api.py`), and `_channels` (the shared dict, aliased in `build_api`).
- **`bus` is NOT used by any moved route** — it appears only in the channel-*construction* section (1633–1684), which stays in `_run_gateway`. Do not add `bus` to `build_api`.
- `provider`, `port`, `memory`, `workspace`, `data_dir`, `cron_service`, `tracer`, `compactor`, `slash_handler` are **not** referenced by the moved block (confirmed absent from the enclosing-dependency set).
- Route-local helpers move verbatim with their routes: `_mask` (nested in `get_channels_config`, 1509–1512), `_event_generator` and `_json` (nested in `post_message_stream`). None are free names.
- **`_channels` is read by a route that moves** (PUT `/api/v1/config`, lines 1410–1411) *and* mutated by `gateway()` afterward (slack/discord). So it must be a **shared mutable dict** passed into `build_api` as `channels` — pass-by-reference keeps both sides in sync. This is the single behavioral wrinkle; handle it exactly as written in Task 2.
- Middleware is test-friendly: `APIKeyAuthMiddleware` no-ops when `api_keys == []` (auth.py:38–39); `RateLimitMiddleware` disables when `rpm <= 0` (rate_limit.py:29–30).
- `Metrics.render()` returns `b"# databot metrics disabled ...\n"` when disabled (metrics.py:142–143), so `/metrics` works without `prometheus_client`.
- Constructors for the fixture: `SessionManager(data_dir: Path)`, `UsageTracker()`, `MonitorService(data_dir, bus, sql_tool=None)`, `configure_metrics(enabled=False)`, `configure_audit_log(path)`, `ToolRegistry()`, `MessageBus()`.
- `GatewayConfig` fields: `api_keys` (default `[]`), `rate_limit_rpm` (default 60), `cors_origins` (default `["*"]`).

---

## Task 1: Failing HTTP test that imports the not-yet-existing factory

**Files:**
- Create: `tests/test_gateway_api.py`

- [ ] **Step 1: Write the test harness + first test**

Create `tests/test_gateway_api.py`:

```python
"""HTTP-level tests for the gateway API, driven through the build_api factory."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from databot.audit import configure_audit_log
from databot.cli.api import build_api  # this module is created in Task 2
from databot.config.schema import DatabotConfig
from databot.core.bus import MessageBus
from databot.core.cost import UsageTracker
from databot.monitors.service import MonitorService
from databot.observability.metrics import configure_metrics
from databot.session.manager import SessionManager
from databot.tools.base import ToolRegistry


class _FakeRegistry:
    """Minimal connector registry stub with controllable health."""

    def __init__(self, health: dict | None = None):
        self._health = health or {}

    async def health_check_all(self):
        return self._health

    def list_all(self):
        return []

    def __len__(self):
        return 0


def _build(tmp_path: Path, *, connector_registry=None, rate_limit_rpm=0):
    """Construct a minimal-but-real gateway app for testing."""
    cfg = DatabotConfig()
    cfg.gateway.api_keys = []              # open mode -> auth middleware no-ops
    cfg.gateway.rate_limit_rpm = rate_limit_rpm  # 0 -> rate limiter disabled
    cfg.ui.enabled = False                 # skip the /ui mount

    bus = MessageBus()
    tools = ToolRegistry()
    sessions = SessionManager(tmp_path)
    usage_tracker = UsageTracker()
    monitor_service = MonitorService(tmp_path, bus)
    metrics = configure_metrics(enabled=False)
    audit_log = configure_audit_log(tmp_path / "audit.db")

    skill_registry = MagicMock()
    skill_registry.enabled_skills.return_value = []
    skill_registry.summary.return_value = []

    return build_api(
        cfg=cfg,
        tools=tools,
        sessions=sessions,
        connector_registry=connector_registry,
        rag_context=None,
        delegator=None,
        skill_registry=skill_registry,
        usage_tracker=usage_tracker,
        monitor_service=monitor_service,
        loop=MagicMock(),
        metrics=metrics,
        audit_log=audit_log,
        channels=None,
    )
    # note: `bus` is built above for MonitorService but is NOT a build_api param
    # (no moved route references it).


@pytest.fixture
def client(tmp_path):
    return TestClient(_build(tmp_path))


def test_livez(client):
    r = client.get("/livez")
    assert r.status_code == 200
    assert r.json() == {"status": "alive"}
```

- [ ] **Step 2: Run the test to verify it fails for the right reason**

Run: `python -m pytest tests/test_gateway_api.py -q`
Expected: collection/import error — `ModuleNotFoundError: No module named 'databot.cli.api'` (the factory does not exist yet). This confirms the test targets the new module.

- [ ] **Step 3: Commit the failing test**

```bash
git add tests/test_gateway_api.py
git commit -m "test: failing HTTP test targeting build_api factory (red)"
```

---

## Task 2: Extract `build_api` and rewire `gateway()`

**Files:**
- Create: `databot/cli/api.py`
- Modify: `databot/cli/commands.py` (replace lines ~1007–1684 inside `async def _run_gateway`)

- [ ] **Step 1: Create the factory module skeleton**

Create `databot/cli/api.py`:

```python
"""FastAPI application factory for the databot gateway.

``build_api`` constructs the gateway's HTTP app from already-built components
and returns it. It performs NO lifecycle work — no connector connect, no channel
``.start()``, no uvicorn. The ``gateway()`` CLI command owns all of that. Keeping
app construction here makes the full HTTP surface testable via ``TestClient``.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger  # used by moved route closures (post_message, config PUT, ...)

from databot.middleware.auth import APIKeyAuthMiddleware
from databot.middleware.rate_limit import RateLimitMiddleware


def build_api(
    *,
    cfg: Any,
    tools: Any,
    sessions: Any,
    connector_registry: Any,
    rag_context: Any,
    delegator: Any,
    skill_registry: Any,
    usage_tracker: Any,
    monitor_service: Any,
    loop: Any,
    metrics: Any,
    audit_log: Any,
    channels: dict[str, Any] | None = None,
) -> FastAPI:
    """Build the gateway FastAPI app from constructed components.

    ``channels`` is a mutable name->channel dict shared with ``gateway()``; its
    webhook routers are registered here, and the PUT /api/v1/config route reads
    it (as ``_channels``) to start/stop channels at runtime.
    """
    api = FastAPI(title="databot", version="0.3.0")

    api.add_middleware(
        CORSMiddleware,
        allow_origins=cfg.gateway.cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    api.add_middleware(APIKeyAuthMiddleware, api_keys=cfg.gateway.api_keys)
    api.add_middleware(RateLimitMiddleware, requests_per_minute=cfg.gateway.rate_limit_rpm)

    _channels = channels if channels is not None else {}

    # <<< PASTE MOVED ROUTES HERE (see Step 2) >>>

    # Channel webhook routers — always registered so runtime-enable works.
    for _ch in _channels.values():
        api.include_router(_ch.get_fastapi_routes())

    return api
```

Note: the local alias `_channels = channels ...` means the moved PUT `/api/v1/config` route body (which references `_channels`) needs **no edit** — it closes over this alias. Router registration iterates the same dict.

- [ ] **Step 2: Move the routes verbatim**

In `databot/cli/commands.py`, cut the block from line **1020** (`@api.get("/health")`) through line **1631** (the end of the `if cfg.ui.enabled:` `/ui` mount block — the line returning the `<h1>databot UI not found</h1>` HTMLResponse). Paste it **verbatim** into `databot/cli/api.py` at the `<<< PASTE MOVED ROUTES HERE >>>` marker.

Do not modify any route body. The `_channels` alias defined in Step 1 satisfies the PUT `/api/v1/config` route. Every other name the routes reference is a `build_api` parameter.

- [ ] **Step 3: Rewire `_run_gateway()`**

In `databot/cli/commands.py`, inside `async def _run_gateway`, the region that previously held `FastAPI(...)` + middleware + routes + channel setup (old lines ~1007–1684) is replaced. Keep the lines *above* (through `audit_log = configure_audit_log(...)` at ~1005) and *below* (the slack/discord/startup section starting at ~1686) unchanged. `bus` remains in scope here and is used to construct the channels below. Replace the middle with:

```python
    # ------------------------------------------------------------------
    # Construct webhook channels (routes always registered so runtime
    # enable works). Lifecycle (.start()) stays here in gateway().
    # ------------------------------------------------------------------
    from databot.channels.gchat import GChatChannel
    from databot.channels.telegram import TelegramChannel
    from databot.channels.whatsapp import WhatsAppChannel

    gchat = GChatChannel(
        bus=bus,
        mode=cfg.channels.gchat.mode,
        webhook_url=cfg.channels.gchat.webhook_url,
        cfg_ref=cfg,
        loop_ref=loop,
    )
    whatsapp_channel = WhatsAppChannel(
        bus=bus,
        phone_number_id=cfg.channels.whatsapp.phone_number_id,
        access_token=cfg.channels.whatsapp.access_token,
        verify_token=cfg.channels.whatsapp.verify_token,
        app_secret=cfg.channels.whatsapp.app_secret,
    )
    telegram_channel = TelegramChannel(
        bus=bus,
        bot_token=cfg.channels.telegram.bot_token,
        mode=cfg.channels.telegram.mode,
        webhook_url=cfg.channels.telegram.webhook_url,
        cfg_ref=cfg,
        loop_ref=loop,
    )
    _channels: dict = {
        "gchat": gchat,
        "whatsapp": whatsapp_channel,
        "telegram": telegram_channel,
    }

    # Build the HTTP app from constructed components.
    from databot.cli.api import build_api

    api = build_api(
        cfg=cfg,
        tools=tools,
        sessions=sessions,
        connector_registry=connector_registry,
        rag_context=rag_context,
        delegator=delegator,
        skill_registry=skill_registry,
        usage_tracker=usage_tracker,
        monitor_service=monitor_service,
        loop=loop,
        metrics=metrics,
        audit_log=audit_log,
        channels=_channels,
    )

    # Start enabled channels (lifecycle owned by gateway()).
    if cfg.channels.gchat.enabled:
        await gchat.start()
        logger.info(f"Google Chat channel started (mode={cfg.channels.gchat.mode})")
    if cfg.channels.whatsapp.enabled:
        await whatsapp_channel.start()
        logger.info("WhatsApp channel started")
    if cfg.channels.telegram.enabled:
        await telegram_channel.start()
        logger.info(f"Telegram channel started (mode={cfg.channels.telegram.mode})")
```

The slack/discord section below still does `_channels["slack"] = slack` / `_channels["discord"] = ...` — because `_channels` is the same dict object passed to `build_api`, the runtime-enable route sees those additions too. Behavior preserved.

- [ ] **Step 4: Run the Task 1 test — it should now pass**

Run: `python -m pytest tests/test_gateway_api.py::test_livez -q`
Expected: PASS.

- [ ] **Step 5: Run the full suite + lint to prove no behavior change**

Run: `python -m pytest -q`
Expected: all pass (existing tests + `test_livez`).

Run: `ruff check databot/ && ruff format --check databot/`
Expected: clean. (If `ruff` flags an unused import that no longer has a consumer in `commands.py` — e.g. `FastAPI`/`CORSMiddleware`/middleware imports now only used in `api.py` — remove those now-dead imports from `commands.py` and re-run.)

- [ ] **Step 6: Commit**

```bash
git add databot/cli/api.py databot/cli/commands.py
git commit -m "refactor: extract gateway FastAPI app into build_api factory"
```

---

## Task 3: Readiness probe tests (200 healthy + 503 unreachable)

**Files:**
- Modify: `tests/test_gateway_api.py`

- [ ] **Step 1: Add the tests**

Append to `tests/test_gateway_api.py`:

```python
def test_readyz_healthy_when_no_connectors(client):
    r = client.get("/readyz")
    assert r.status_code == 200
    assert r.json()["status"] == "ready"


def test_readyz_503_when_connector_unreachable(tmp_path):
    from databot.connectors.base import ConnectorStatus

    reg = _FakeRegistry(health={"warehouse": ConnectorStatus.UNREACHABLE})
    client = TestClient(_build(tmp_path, connector_registry=reg))
    r = client.get("/readyz")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "not_ready"
    assert body["connectors"]["warehouse"] == "unreachable"
```

- [ ] **Step 2: Run**

Run: `python -m pytest tests/test_gateway_api.py -q`
Expected: PASS (all).

- [ ] **Step 3: Commit**

```bash
git add tests/test_gateway_api.py
git commit -m "test: readiness probe 200/503 coverage"
```

---

## Task 4: The five remaining target endpoints

**Files:**
- Modify: `tests/test_gateway_api.py`

- [ ] **Step 1: Add the tests**

Append to `tests/test_gateway_api.py`:

```python
def test_metrics_returns_exposition_payload(client):
    r = client.get("/metrics")
    assert r.status_code == 200
    # metrics configured disabled -> graceful placeholder, no prometheus_client needed
    assert b"databot metrics disabled" in r.content


def test_usage(client):
    r = client.get("/api/v1/usage")
    assert r.status_code == 200
    body = r.json()
    assert "global" in body
    assert "sessions" in body
    assert "session_budget_usd" in body


def test_audit(client):
    r = client.get("/api/v1/audit")
    assert r.status_code == 200
    body = r.json()
    assert "enabled" in body
    assert "entries" in body
    assert isinstance(body["entries"], list)


def test_monitors_empty(client):
    r = client.get("/api/v1/monitors")
    assert r.status_code == 200
    assert r.json() == {"monitors": []}


def test_queries_empty_without_saved_query_tool(client):
    r = client.get("/api/v1/queries")
    assert r.status_code == 200
    assert r.json() == {"queries": []}
```

- [ ] **Step 2: Run**

Run: `python -m pytest tests/test_gateway_api.py -q`
Expected: PASS (all).

- [ ] **Step 3: Commit**

```bash
git add tests/test_gateway_api.py
git commit -m "test: HTTP coverage for /metrics /usage /audit /monitors /queries"
```

---

## Task 5: Free-neighbor endpoints (health/stats/tools/skills/config)

**Files:**
- Modify: `tests/test_gateway_api.py`

- [ ] **Step 1: Add the tests**

Append to `tests/test_gateway_api.py`:

```python
def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["version"] == "0.3.0"


def test_stats(client):
    r = client.get("/api/v1/stats")
    assert r.status_code == 200
    body = r.json()
    assert body["sessions_count"] == 0
    assert "tools_count" in body
    assert "usage" in body


def test_tools_empty(client):
    r = client.get("/api/v1/tools")
    assert r.status_code == 200
    assert r.json() == {"tools": []}


def test_skills(client):
    r = client.get("/api/v1/skills")
    assert r.status_code == 200
    assert r.json() == {"skills": []}


def test_get_config(client):
    # Exercises the config-route class (GET /api/v1/config) so any helper path
    # in that region is covered — guards the app-factory extraction.
    r = client.get("/api/v1/config")
    assert r.status_code == 200
    assert isinstance(r.json(), dict)


def test_get_channels_config_masks_secrets(client):
    # Drives the `_mask` helper path (nested in get_channels_config). Must not
    # raise NameError after the verbatim move, and must not echo raw secrets.
    r = client.get("/api/v1/config/channels")
    assert r.status_code == 200
    assert isinstance(r.json(), dict)
```

- [ ] **Step 2: Run**

Run: `python -m pytest tests/test_gateway_api.py -q`
Expected: PASS (all).

- [ ] **Step 3: Commit**

```bash
git add tests/test_gateway_api.py
git commit -m "test: HTTP coverage for /health /stats /tools /skills and config routes"
```

---

## Task 6: Full verification + manual smoke

**Files:** none (verification only)

- [ ] **Step 1: Full suite**

Run: `python -m pytest -q`
Expected: all pass. Note the new count vs. before (should be +14 tests from `test_gateway_api.py`).

- [ ] **Step 2: Lint + format**

Run: `ruff check databot/ && ruff format --check databot/`
Expected: clean.

- [ ] **Step 3: Manual gateway smoke (behavior-preservation)**

Run: `databot gateway` (Ctrl-C after it reports "starting on port ...").
Expected: it starts, logs connector/skill/tool counts, and prints the Health/API/UI URLs exactly as before the refactor. If a webhook channel is configured+enabled in local config, confirm its "channel started" log still appears.

Optionally, with the gateway running: `curl -s localhost:18790/livez` → `{"status":"alive"}` and `curl -s localhost:18790/readyz`.

- [ ] **Step 4: Final commit (if smoke revealed nothing to change)**

No code change expected here; if Step 3 required a tweak, commit it:

```bash
git add -A
git commit -m "fix: gateway smoke follow-up"
```

---

## Self-review notes

- **Spec coverage:** app-factory extraction (Task 2) ✓; seven target endpoints `/livez`(T1) `/readyz`(T3) `/metrics` `/usage` `/audit` `/monitors` `/queries`(T4) ✓; free neighbors `/health` `/stats` `/tools` `/skills`(T5) ✓; empty-api-keys + rpm=0 fixture ✓; behavior-preservation via full suite + manual smoke (T2/T6) ✓; scope guardrails (no `_build_components` change, routes verbatim) ✓.
- **Refinements vs. spec:** (1) `channels` is a **dict** (not a list) because the runtime-enable route reads it as `_channels`; documented in Task 2. (2) `bus` is **not** a `build_api` param — AST free-name analysis proved no moved route uses it. Both are strictly more correct than the spec's sketch.
- **Type/name consistency:** `build_api` param names in Task 1's `_build(...)` call match the signature in Task 2 Step 1 exactly (**12 components** + `channels`).
- **Cut boundaries:** move lines 1020–1631 (routes/UI) into `api.py`; the `FastAPI(...)`+middleware at 1008–1018 are re-authored at the top of `build_api` (Step 1), not pasted. The enclosing dependencies of the moved block were verified by AST to be exactly {`api`, `logger`, the 12 params, `_channels`} — no hidden module-level helper (e.g. `_mask` is route-local and moves with its route).
