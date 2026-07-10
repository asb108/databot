# Error UX in Chat Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route every user-facing error through one shared, traceback-free renderer that consumes the structured `ToolError` fields, and close the four remaining chat leak points.

**Architecture:** Add a pure `render_user_error(exc)` function in a new `databot/core/error_ux.py` — a 5-class classifier (auth · rate-limit · timeout · network · generic, checked in that order) that also unpacks `ToolError.hint`/`retryable`. Then replace the scattered inline error literals and raw `str(e)` leaks (loop `run()`, streaming guard, GChat, Telegram, CLI single-shot, slash `/status`, HTTP `/message` + `/stream`) with calls to it. No change to `ToolRegistry.execute` or how tool errors reach the model.

**Tech Stack:** Python 3, pytest (`python3 -m pytest`), ruff. Interpreter is `python3` (NOT `python`).

## Global Constraints

- Interpreter is `python3`. Run tests with `python3 -m pytest`. Lint with `ruff check databot/` and `ruff format --check databot/` (CI lints `databot/` only, not `tests/`).
- `render_user_error(exc: BaseException) -> str` is the ONLY place a user-facing error string is produced. It is pure/deterministic — no I/O, no logging inside it.
- The classifier order is fixed: **(1) auth → (2) rate-limit → (3) timeout → (4) network → (5) generic**. First match wins. Checks are case-insensitive against both the exception type name and `str(exc)`.
- For the non-`ToolError` path, `str(exc)` internals MUST NOT appear in the output — only the fixed class message. Each call site still `logger.error(...)`s the raw exception (unchanged).
- The retry nudge is a single fixed suffix reused for retryable classes and for `ToolError.retryable=True`.
- No change to `databot/tools/errors.py` or `ToolRegistry.execute`. No change to how tool-result strings reach the model.
- Baseline: **477** tests passing. Keep every existing test green.
- Local-only: branch `feat/error-ux-in-chat` off `main`; ff-merge to `main` at the end; nothing pushed to `origin`.

---

### Task 1: The `render_user_error` renderer + classifier

**Files:**
- Create: `databot/core/error_ux.py`
- Test: `tests/test_error_ux.py`

**Interfaces:**
- Consumes: `databot.tools.errors.ToolError` (fields `code`, `message`, `hint`, `retryable`).
- Produces: `render_user_error(exc: BaseException) -> str` — the single user-facing error renderer used by all later tasks.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_error_ux.py`:

```python
"""Tests for the shared user-facing error renderer."""

from __future__ import annotations

import asyncio

import pytest

from databot.core.error_ux import render_user_error
from databot.tools.errors import ErrorCode, ToolError

RETRY_SUFFIX = "You can try again in a moment."


class TestClassifier:
    def test_auth_by_type(self):
        out = render_user_error(PermissionError("nope"))
        assert "authentication failed" in out.lower()
        assert "administrator" in out.lower()
        assert RETRY_SUFFIX not in out

    def test_auth_by_message(self):
        out = render_user_error(RuntimeError("Invalid API Key provided"))
        assert "authentication failed" in out.lower()

    def test_rate_limit(self):
        out = render_user_error(RuntimeError("429 Too Many Requests"))
        assert "busy" in out.lower()
        assert RETRY_SUFFIX in out

    def test_timeout_by_type(self):
        out = render_user_error(asyncio.TimeoutError())
        assert "too long" in out.lower()
        assert RETRY_SUFFIX in out

    def test_network(self):
        out = render_user_error(ConnectionError("connection refused"))
        assert "reaching a service" in out.lower()
        assert RETRY_SUFFIX in out

    def test_generic_default(self):
        out = render_user_error(ValueError("something weird"))
        assert "internal error" in out.lower()
        assert RETRY_SUFFIX not in out

    def test_order_rate_limit_beats_network(self):
        # "429 ... connection reset" contains a network substring but must classify as rate-limit
        out = render_user_error(RuntimeError("HTTP 429 on connection to api"))
        assert "busy" in out.lower()

    def test_order_auth_beats_network(self):
        # auth message that also contains "connection" must classify as auth
        out = render_user_error(RuntimeError("AuthenticationError: connection to api failed"))
        assert "authentication failed" in out.lower()


class TestToolErrorRendering:
    def test_plain(self):
        out = render_user_error(ToolError(ErrorCode.NOT_FOUND, "Table 'x' was not found."))
        assert "Table 'x' was not found." in out
        assert "Error [" not in out  # never the LLM-facing prefix
        assert RETRY_SUFFIX not in out

    def test_with_hint(self):
        out = render_user_error(
            ToolError(ErrorCode.NOT_FOUND, "Unknown connection 'db'.", hint="Available: main")
        )
        assert "Unknown connection 'db'." in out
        assert "Available: main" in out

    def test_retryable_appends_nudge(self):
        out = render_user_error(
            ToolError(ErrorCode.CONNECTION_ERROR, "Database unavailable.", retryable=True)
        )
        assert RETRY_SUFFIX in out

    def test_not_retryable_no_nudge(self):
        out = render_user_error(ToolError(ErrorCode.INVALID_ARGUMENT, "Bad column name."))
        assert RETRY_SUFFIX not in out


class TestSecretSafety:
    def test_generic_path_never_leaks_str(self):
        secret = "sk-super-secret-token-98765"
        out = render_user_error(RuntimeError(f"boom with {secret}"))
        assert secret not in out

    def test_no_traceback_words(self):
        out = render_user_error(ValueError("x"))
        assert "Traceback" not in out
        assert "ValueError" not in out
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_error_ux.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'databot.core.error_ux'`

- [ ] **Step 3: Write the implementation**

Create `databot/core/error_ux.py`:

```python
"""Shared user-facing error rendering.

``render_user_error`` is the ONLY place a user-facing error string is produced.
It is pure and deterministic: given an exception it returns a clean, actionable,
traceback-free message. Raw exception internals (``str(exc)``) never appear in the
output for non-``ToolError`` exceptions — callers log the raw exception themselves.

It consumes the structured :class:`~databot.tools.errors.ToolError` fields
(``message`` / ``hint`` / ``retryable``) when the exception carries them.
"""

from __future__ import annotations

from databot.tools.errors import ToolError

# Reused for every retryable class and for ToolError(retryable=True).
_RETRY_NUDGE = "You can try again in a moment."

_GENERIC = "Sorry, I encountered an internal error. Please try again or contact an administrator."

# (substrings, type-names, message, retryable) — checked in order; first match wins.
# `substrings` and `type_names` are matched case-insensitively.
_CLASSES: list[tuple[tuple[str, ...], tuple[str, ...], str, bool]] = [
    # 1. auth / config
    (
        ("authenticationerror", "api key", "unauthorized", "permission denied", "forbidden"),
        ("permissionerror",),
        "I'm not configured correctly to do that (authentication failed). "
        "Please contact an administrator.",
        False,
    ),
    # 2. rate-limit
    (
        ("rate limit", "too many requests", "429"),
        (),
        "The service is busy at the moment. Please wait a few seconds and try again.",
        True,
    ),
    # 3. timeout
    (
        ("timed out", "timeout"),
        ("timeouterror",),
        "That took too long to complete. Please try again.",
        True,
    ),
    # 4. network / connection
    (
        ("connection", "network", "unreachable", "refused"),
        ("connectionerror", "connecterror", "connecttimeout", "readtimeout", "transporterror"),
        "I'm having trouble reaching a service right now. Please try again shortly.",
        True,
    ),
]


def _with_nudge(message: str, retryable: bool) -> str:
    return f"{message} {_RETRY_NUDGE}" if retryable else message


def render_user_error(exc: BaseException) -> str:
    """Render a clean, traceback-free, user-facing string for any exception."""
    if isinstance(exc, ToolError):
        parts = [exc.message]
        if exc.hint:
            parts.append(exc.hint)
        text = " ".join(parts)
        return _with_nudge(text, exc.retryable)

    type_name = type(exc).__name__.lower()
    text = str(exc).lower()
    for substrings, type_names, message, retryable in _CLASSES:
        if any(t in type_name for t in type_names) or any(s in text for s in substrings):
            return _with_nudge(message, retryable)
    return _GENERIC
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_error_ux.py -q`
Expected: PASS (all tests green)

- [ ] **Step 5: Lint**

Run: `ruff check databot/ && ruff format --check databot/`
Expected: no errors. (If format flags the new file, run `ruff format databot/core/error_ux.py` and re-check.)

- [ ] **Step 6: Commit**

```bash
git add databot/core/error_ux.py tests/test_error_ux.py
git commit -m "feat: shared user-facing error renderer (render_user_error)"
```

---

### Task 2: Wire into the core loop (`run()` + streaming guard)

**Files:**
- Modify: `databot/core/loop.py:85-98` (the `run()` except) and `databot/core/loop.py:275-425` (`process_message_stream`)
- Test: `tests/test_loop_error_ux.py` (create)

**Interfaces:**
- Consumes: `render_user_error` from Task 1; `StreamEvent(channel, chat_id, event_type, data="", tool_name="")` from `databot/core/bus.py`.
- Produces: `process_message_stream` now yields a single `event_type="error"` `StreamEvent` (never raises) when the turn fails.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_loop_error_ux.py`:

```python
"""Streaming error-path tests for AgentLoop."""

from __future__ import annotations

import pytest

from databot.core.bus import InboundMessage, MessageBus
from databot.core.loop import AgentLoop


class _BoomProvider:
    """Provider whose stream raises on first use."""

    def get_definitions(self):
        return None

    async def chat_stream(self, **kwargs):
        raise RuntimeError("boom secret-xyz")
        yield  # pragma: no cover — makes this an async generator


class _NoTools:
    def get_definitions(self):
        return None


def _make_loop():
    return AgentLoop(
        bus=MessageBus(),
        provider=_BoomProvider(),
        tools=_NoTools(),
        workspace="/tmp",
        sessions=_Sessions(),
        memory=None,
        system_prompt="test",
        max_iterations=3,
    )


class _Session:
    summary = ""

    def get_history(self):
        return []

    def add_message(self, *a, **k):
        pass


class _Sessions:
    def get_or_create(self, key):
        return _Session()

    def save(self, s):
        pass


@pytest.mark.asyncio
async def test_stream_yields_clean_error_event():
    loop = _make_loop()
    msg = InboundMessage(channel="api", sender_id="u", chat_id="c", content="hi", stream=True)
    events = [e async for e in loop.process_message_stream(msg)]
    error_events = [e for e in events if e.event_type == "error"]
    assert len(error_events) == 1
    body = error_events[0].data
    assert "secret-xyz" not in body  # no raw exception text
    assert "Traceback" not in body
    assert "internal error" in body.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_loop_error_ux.py -q`
Expected: FAIL — the current `process_message_stream` has no guard, so `RuntimeError("boom secret-xyz")` propagates out of the async iteration instead of producing an `error` event.

- [ ] **Step 3: Add the import**

In `databot/core/loop.py`, add near the other `databot.core` imports at the top of the file:

```python
from databot.core.error_ux import render_user_error
```

- [ ] **Step 4: Replace the `run()` inline literal**

In `databot/core/loop.py`, the `run()` except block currently reads:

```python
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # Sanitize error message to avoid leaking internals
                    safe_msg = (
                        "Sorry, I encountered an internal error. "
                        "Please try again or contact an administrator."
                    )
                    await self.bus.publish_outbound(
                        OutboundMessage(
                            channel=msg.channel,
                            chat_id=msg.chat_id,
                            content=safe_msg,
                        )
                    )
```

Replace the `safe_msg` assignment so it uses the shared renderer:

```python
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    safe_msg = render_user_error(e)
                    await self.bus.publish_outbound(
                        OutboundMessage(
                            channel=msg.channel,
                            chat_id=msg.chat_id,
                            content=safe_msg,
                        )
                    )
```

- [ ] **Step 5: Guard `process_message_stream`**

In `databot/core/loop.py`, wrap the body of `process_message_stream` (everything AFTER the `logger.info(...)` line at the top of the method) in a `try/except`. The method currently starts:

```python
    async def process_message_stream(self, msg: InboundMessage) -> AsyncIterator[StreamEvent]:
        """Process a message with streaming — yields StreamEvent for SSE delivery."""
        logger.info(f"Processing streaming message from {msg.channel}:{msg.sender_id}")

        # Deterministic slash commands bypass the LLM entirely.
        if self._slash:
```

Restructure to a helper so the guard wraps all yields. Rename the existing body to `_process_message_stream_inner` (same code, unchanged) and add a thin guarded wrapper:

```python
    async def process_message_stream(self, msg: InboundMessage) -> AsyncIterator[StreamEvent]:
        """Process a message with streaming — yields StreamEvent for SSE delivery.

        Never raises to the caller: on failure it yields a single sanitized
        ``error`` event (via ``render_user_error``) and stops.
        """
        try:
            async for event in self._process_message_stream_inner(msg):
                yield event
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            yield StreamEvent(
                channel=msg.channel,
                chat_id=msg.chat_id,
                event_type="error",
                data=render_user_error(e),
            )

    async def _process_message_stream_inner(
        self, msg: InboundMessage
    ) -> AsyncIterator[StreamEvent]:
        logger.info(f"Processing streaming message from {msg.channel}:{msg.sender_id}")

        # Deterministic slash commands bypass the LLM entirely.
        if self._slash:
```

Leave the entire remaining body of the original method (from `if self._slash:` through the final `done` event yield) exactly as-is under `_process_message_stream_inner`.

- [ ] **Step 6: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_loop_error_ux.py tests/test_error_ux.py -q`
Expected: PASS

- [ ] **Step 7: Full regression + lint**

Run: `python3 -m pytest -q && ruff check databot/ && ruff format --check databot/`
Expected: all pass (478+ tests). If format flags `loop.py`, run `ruff format databot/core/loop.py`.

- [ ] **Step 8: Commit**

```bash
git add databot/core/loop.py tests/test_loop_error_ux.py
git commit -m "feat: route loop run() + streaming errors through render_user_error"
```

---

### Task 3: Wire into GChat + Telegram channels

**Files:**
- Modify: `databot/channels/gchat.py:146-148` (generic except)
- Modify: `databot/channels/telegram.py:238-240` (generic except)

**Interfaces:**
- Consumes: `render_user_error` from Task 1.

No new test file — these are one-line literal swaps behind network handlers already covered by the renderer's own unit tests. The timeout branches (which already produce a friendly line) stay unchanged.

- [ ] **Step 1: GChat — add import and swap the literal**

In `databot/channels/gchat.py`, add to the imports at the top of the file:

```python
from databot.core.error_ux import render_user_error
```

Change the generic-except branch from:

```python
                    except Exception as e:
                        logger.error(f"GChat: error processing message: {e}")
                        return {"text": "Sorry, I encountered an error processing your request."}
```

to:

```python
                    except Exception as e:
                        logger.error(f"GChat: error processing message: {e}")
                        return {"text": render_user_error(e)}
```

- [ ] **Step 2: Telegram — add import and swap the literal**

In `databot/channels/telegram.py`, add to the imports at the top of the file:

```python
from databot.core.error_ux import render_user_error
```

Change the generic-except branch from:

```python
            except Exception as e:
                logger.error(f"Telegram: error processing message: {e}")
                await self.send(chat_id, "Sorry, I encountered an error processing your request.")
```

to:

```python
            except Exception as e:
                logger.error(f"Telegram: error processing message: {e}")
                await self.send(chat_id, render_user_error(e))
```

- [ ] **Step 3: Run regression + lint**

Run: `python3 -m pytest -q && ruff check databot/ && ruff format --check databot/`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add databot/channels/gchat.py databot/channels/telegram.py
git commit -m "feat: render channel errors via render_user_error (gchat, telegram)"
```

---

### Task 4: Wire into slash `/status` + CLI single-shot

**Files:**
- Modify: `databot/core/slash.py:107-113` (`_status_report`)
- Modify: `databot/cli/commands.py:799-860` (`_process_single`)
- Test: `tests/test_slash_error_ux.py` (create)

**Interfaces:**
- Consumes: `render_user_error` from Task 1.

- [ ] **Step 1: Write the failing test**

Create `tests/test_slash_error_ux.py`:

```python
"""Slash-command error rendering."""

from __future__ import annotations

import pytest

from databot.core.slash import SlashCommandHandler


class _BoomConnectors:
    async def health_check_all(self):
        raise RuntimeError("psycopg2 OperationalError: password=secret123 failed")


@pytest.mark.asyncio
async def test_status_report_hides_raw_error():
    handler = SlashCommandHandler(tools=None, skills=None, connectors=_BoomConnectors())
    out = await handler._status_report()
    assert "secret123" not in out
    assert "reaching a service" in out.lower()  # network class
```

Note: confirm `SlashCommandHandler`'s constructor kwarg names by reading `databot/core/slash.py` (it stores `self._tools`, `self._skills`, `self._connectors`). Adjust the constructor call to match the real signature if it differs; keep the assertions.

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_slash_error_ux.py -q`
Expected: FAIL — current code returns `f"Could not check connector health: {e}"`, so `secret123` is present in the output.

- [ ] **Step 3: Fix `_status_report`**

In `databot/core/slash.py`, add the import at the top of the file:

```python
from databot.core.error_ux import render_user_error
```

Change the except branch from:

```python
        try:
            health = await self._connectors.health_check_all()
        except Exception as e:
            return f"Could not check connector health: {e}"
```

to:

```python
        try:
            health = await self._connectors.health_check_all()
        except Exception as e:
            logger.error(f"Slash /status health check failed: {e}")
            return render_user_error(e)
```

If `loguru`'s `logger` is not already imported in `slash.py`, add `from loguru import logger` at the top; otherwise reuse the existing import.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_slash_error_ux.py -q`
Expected: PASS

- [ ] **Step 5: Guard the CLI single-shot path**

In `databot/cli/commands.py`, `_process_single` (lines ~799-860) currently calls `delegator.handle(...)` / `loop.process_direct(...)` with no guard, so an exception yields a raw traceback in the terminal. Wrap the two call sites. Add the import near the top of `commands.py` (with the other `databot` imports):

```python
from databot.core.error_ux import render_user_error
```

Wrap the delegator branch:

```python
    # If multi-agent is enabled, use the delegator directly
    if delegator:
        extra_context = ""
        if rag_context:
            extra_context = rag_context.enrich_prompt(message)
        try:
            result = await delegator.handle(
                message, extra_context=extra_context, session_key="cli:direct"
            )
        except Exception as e:
            logger.error(f"CLI single-shot error: {e}")
            result = render_user_error(e)
        if connector_registry:
            await connector_registry.disconnect_all()
        return result
```

And wrap the loop branch:

```python
    try:
        result = await loop.process_direct(message)
    except Exception as e:
        logger.error(f"CLI single-shot error: {e}")
        result = render_user_error(e)

    if connector_registry:
        await connector_registry.disconnect_all()
    if tracer:
        tracer.shutdown()

    return result
```

Confirm `logger` is imported in `commands.py` (it is used elsewhere in the file); if not, add `from loguru import logger`.

- [ ] **Step 6: Run regression + lint**

Run: `python3 -m pytest -q && ruff check databot/ && ruff format --check databot/`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add databot/core/slash.py databot/cli/commands.py tests/test_slash_error_ux.py
git commit -m "feat: render slash /status + CLI single-shot errors via render_user_error"
```

---

### Task 5: Wire into HTTP API (`/message` + `/stream`)

**Files:**
- Modify: `databot/cli/api.py:141-149` (`/api/v1/message` except) and `databot/cli/api.py:193-203` (SSE `_event_generator` except)
- Test: `tests/test_gateway_api.py` (extend — this file already exists from sub-spec #1)

**Interfaces:**
- Consumes: `render_user_error` from Task 1. The `/message` endpoint keeps HTTP status **500**; the SSE path keeps emitting the `error` event.

- [ ] **Step 1: Write the failing test**

In `tests/test_gateway_api.py`, add a test that a failing `process_message` returns a sanitized 500 body. Use the existing `_build(...)` helper and `TestClient` patterns already in that file. Add:

```python
def test_message_endpoint_sanitizes_error(tmp_path):
    client = _build(tmp_path)

    class _BoomLoop:
        async def process_message(self, msg):
            raise RuntimeError("driver failure token-abc123")

    # Replace the loop the route closes over. The build helper exposes the app's
    # loop via app.state or the closure; if not directly reachable, monkeypatch
    # loop.process_message on the instance the factory created.
    app = client.app
    app.state.loop = _BoomLoop()  # see note below

    resp = client.post("/api/v1/message", json={"message": "hi"})
    assert resp.status_code == 500
    body = resp.json()
    assert "token-abc123" not in body["error"]
    assert "internal error" in body["error"].lower()
```

Note for the implementer: `build_api(...)` closes over the `loop` argument rather than reading `app.state`. Read `tests/test_gateway_api.py`'s `_build` helper and `databot/cli/api.py` to see how the route accesses `loop`; the cleanest approach is to have `_build` accept/return the loop stub, or pass a loop whose `process_message` raises. Wire the test to whichever injection point `_build` already supports (the sub-spec #1 tests use a `_FakeRegistry`; follow the same injection style for a fake loop). Keep the two assertions (`status_code == 500`, secret absent, generic body).

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_gateway_api.py -q -k sanitizes`
Expected: FAIL — current code returns `str(e)` (containing `token-abc123`) for non-API-key errors.

- [ ] **Step 3: Fix the `/message` endpoint**

In `databot/cli/api.py`, add the import near the top of the file (module level, with the other `databot` imports):

```python
from databot.core.error_ux import render_user_error
```

Replace the `/api/v1/message` except block:

```python
        except Exception as e:
            logger.error(f"Message processing error: {e}")
            err_msg = str(e)
            if "API Key" in err_msg or "AuthenticationError" in err_msg:
                err_msg = (
                    "LLM API key not configured. Set ANTHROPIC_API_KEY "
                    "environment variable or run 'databot init'."
                )
            return JSONResponse({"error": err_msg}, status_code=500)
```

with:

```python
        except Exception as e:
            logger.error(f"Message processing error: {e}")
            return JSONResponse({"error": render_user_error(e)}, status_code=500)
```

- [ ] **Step 4: Fix the SSE `_event_generator` except**

Replace:

```python
            except Exception as e:
                err_msg = str(e)
                if "API Key" in err_msg or "AuthenticationError" in err_msg:
                    err_msg = (
                        "LLM API key not configured. Set ANTHROPIC_API_KEY "
                        "environment variable or run 'databot init'."
                    )
                yield {
                    "event": "error",
                    "data": _json.dumps({"type": "error", "data": err_msg}),
                }
```

with:

```python
            except Exception as e:
                logger.error(f"Streaming endpoint error: {e}")
                yield {
                    "event": "error",
                    "data": _json.dumps({"type": "error", "data": render_user_error(e)}),
                }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python3 -m pytest tests/test_gateway_api.py -q`
Expected: PASS (all gateway tests, including the new one).

- [ ] **Step 6: Full regression + lint**

Run: `python3 -m pytest -q && ruff check databot/ && ruff format --check databot/`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add databot/cli/api.py tests/test_gateway_api.py
git commit -m "feat: render HTTP /message + /stream errors via render_user_error"
```

---

### Task 6: CHANGELOG + docs finish

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add the changelog entry**

Under the existing `### Changed — Reliability` section (added in sub-specs #1/#2) in `CHANGELOG.md`, add:

```markdown
- **Error UX in chat:** all user-facing failures now render through a single
  `render_user_error` helper (`databot/core/error_ux.py`) — traceback-free,
  consistent across channels, and classified into actionable messages
  (auth / rate-limit / timeout / network / generic) that consume `ToolError`'s
  `hint`/`retryable`. Closes raw-error leaks in the CLI single-shot path, the
  HTTP `/message` + `/stream` endpoints, and the `/status` slash command; adds a
  top-level guard to the streaming loop.
```

- [ ] **Step 2: Full regression + lint (final gate)**

Run: `python3 -m pytest -q && ruff check databot/ && ruff format --check databot/`
Expected: all pass; ~489 tests (477 baseline + ~12).

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: changelog for error UX in chat"
```

---

## Self-Review

**Spec coverage:**
- New `databot/core/error_ux.py` + `render_user_error` + 5-class classifier → Task 1. ✅
- `ToolError` field consumption (message/hint/retryable) → Task 1 (`TestToolErrorRendering`). ✅
- Classifier precedence (auth/rate-limit before network/timeout) → Task 1 (order tests). ✅
- Secret-safety guarantee → Task 1 (`TestSecretSafety`). ✅
- Loop `run()` literal + streaming guard → Task 2. ✅
- GChat + Telegram literals → Task 3. ✅
- Slash `/status` leak → Task 4. ✅
- CLI single-shot traceback → Task 4. ✅
- HTTP `/message` + `/stream` leaks (status stays 500, API-key case subsumed) → Task 5. ✅
- Non-goals (MCP, config-admin endpoints, message chunking) → untouched by any task. ✅
- CHANGELOG → Task 6. ✅

**Placeholder scan:** Task 5 Step 1 intentionally defers the exact loop-injection mechanism to the implementer (it depends on the pre-existing `_build` helper from sub-spec #1, which the plan author cannot restate verbatim without re-reading it); the assertions and behavior are fully specified. All other steps carry complete code. No TBD/TODO literals.

**Type consistency:** `render_user_error(exc: BaseException) -> str` used identically in Tasks 2–5. `StreamEvent(channel, chat_id, event_type, data)` matches `databot/core/bus.py:42-49`. `_RETRY_NUDGE` string ("You can try again in a moment.") matches the `RETRY_SUFFIX` constant asserted in Task 1 tests. ✅
