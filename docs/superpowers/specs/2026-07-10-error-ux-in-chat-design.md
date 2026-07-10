# Sub-spec #3 — Error UX in Chat

**Date:** 2026-07-10
**Milestone:** Robustness & Polish (sub-spec #3 of 4)
**Depends on:** #2 (structured tool errors — `databot/tools/errors.py`)
**Status:** Design approved

## Problem

Sub-spec #2 made the *tool* boundary traceback-free: `ToolRegistry.execute` never
raises and its `Error [code]: …` string is fed back to the **model**, not the user.
But several turn-level and model-bypassing paths still let a raw `str(exception)` —
or a full Python traceback — reach an end user, and the "safe" paths each use their
own inline literal, so nothing is consistent and nothing consumes the structured
`ToolError` fields (`code` / `hint` / `retryable`).

### Confirmed leak points (a raw error/traceback reaches a person)

1. **CLI single-shot** `databot agent -m "…"` → `_process_single` (`databot/cli/commands.py`)
   has no guard; an LLM/session exception produces a **full traceback** in the terminal.
2. **HTTP API** `POST /api/v1/message` → returns `str(e)` in the 500 JSON body
   (only an API-key special-case is cleaned up).
3. **HTTP API** SSE `/api/v1/stream` → yields `str(e)` as an `error` event;
   `process_message_stream` has no top-level guard around the LLM stream.
4. **Slash commands** — `databot/core/slash.py` `_status_report` returns
   `f"Could not check connector health: {e}"` verbatim; slash output bypasses the
   model and is returned to the user across **all** channels.

### Inconsistent (safe but divergent) paths

- `loop.py` `run()` except uses one inline generic literal.
- `gchat.py` / `telegram.py` generic-except branches each use their own literal.
- None of these consume `ToolError.hint` / `retryable`, so a retryable failure never
  tells the user "you can retry."

## Goals

- One shared, pure, deterministic helper that turns any exception (or `ToolError`)
  into a clean, traceback-free, actionable user-facing string.
- Consume `ToolError.code` / `hint` / `retryable` when the error carries them.
- Close leak points 1–4 (chat surfaces) and unify the divergent literals.

## Non-goals (deferred)

- **MCP** `call_tool` error path (non-chat surface; `ToolRegistry` already guards it).
- **Config / connector-admin** API endpoints (`api.py` config routes — not chat).
- **Message-length chunking** for non-Discord channels (Telegram/WhatsApp/Slack may
  silently fail on over-limit replies). This is a message-*delivery* concern, not
  error UX; track as a separate follow-up.

## Design

### New module: `databot/core/error_ux.py`

A single public function is the **only** place a user-facing error string is produced:

```python
def render_user_error(exc: BaseException) -> str: ...
```

Behavior:

- **`exc` is a `ToolError`** → build from its structured fields:
  - lead with a friendly sentence derived from `message` (not the `Error [code]:`
    LLM-facing form — that stays internal),
  - append `hint` if present,
  - append a retry nudge if `retryable`.
- **Otherwise** → classify into a small fixed set; each class has a **fixed**,
  actionable, traceback-free message. `str(exc)` internals never appear in the output
  for this path.

Classifier table (**first match wins; the table order IS the check order**; checks are
case-insensitive against the exception type name and `str(exc)`):

| # | Class | Trigger | Message | Retry nudge |
|---|---|---|---|---|
| 1 | auth/config | type `PermissionError`; substrings `authenticationerror`, `api key`, `unauthorized`, `permission denied`, `forbidden` | "I'm not configured correctly to do that (authentication failed). Please contact an administrator." | no |
| 2 | rate-limit | substrings `rate limit`, `too many requests`, `429` | "The service is busy at the moment. Please wait a few seconds and try again." | yes |
| 3 | timeout | type `asyncio.TimeoutError` / `TimeoutError`; substrings `timed out`, `timeout` | "That took too long to complete. Please try again." | yes |
| 4 | network | type `ConnectionError` and `httpx` transport errors; substrings `connection`, `network`, `unreachable`, `refused` | "I'm having trouble reaching a service right now. Please try again shortly." | yes |
| 5 | generic (default) | anything else | "Sorry, I encountered an internal error. Please try again or contact an administrator." | no |

Notes:

- Order matters and is fixed by the `#` column above: **auth and rate-limit are checked
  before the broader timeout/network substrings**, so a `"429 Too Many Requests"`
  classifies as rate-limit and an `AuthenticationError("connection to api failed")`
  classifies as auth (its `connection` substring does not win). Implement the checks in
  exactly this order.
- The retry nudge is a fixed suffix (e.g. " You can try again in a moment.") appended
  to the class message; identical wording is reused for the `ToolError.retryable` case.
- The raw exception is still `logger.error(...)`'d at each call site (unchanged) — only
  the *user-facing* text is sanitized.

### Wiring (chat surfaces only)

1. **`databot/core/loop.py`** `run()` except → `render_user_error(e)` for `safe_msg`.
2. **`databot/core/loop.py`** `process_message_stream` → wrap the turn in `try/except`;
   on exception, `logger.error(...)` then `yield` one `error` `StreamEvent` carrying
   `render_user_error(e)` and stop. (Confirm the exact `StreamEvent` error shape used
   by `api.py` and reuse it.)
3. **`databot/channels/gchat.py`** and **`databot/channels/telegram.py`** — generic-except
   literals → `render_user_error(e)`. Existing timeout branches either stay or route
   through the timeout class; make wording consistent with the classifier.
4. **`databot/cli/commands.py`** `_process_single` → wrap the `delegator.handle` /
   `loop.process_direct` call in `try/except`; on exception, `logger.error(...)` then
   `console.print(render_user_error(e))`. No traceback.
5. **`databot/core/slash.py`** `_status_report` → replace the `f"…: {e}"` return with
   `render_user_error(e)` (still `logger`-log the raw `e`).
6. **`databot/cli/api.py`** `/api/v1/message` and `/api/v1/stream` — the error body/event
   text uses `render_user_error(e)`; **HTTP status stays 500** for `/message`. The
   existing API-key special-case is subsumed by the auth class and removed. SSE emits
   the rendered text via the existing `error` event.

### Contract preservation

- `render_user_error` is pure and deterministic → directly unit-testable, no I/O.
- **No change** to `ToolRegistry.execute` (still returns the LLM-facing string) and
  **no change** to how tool errors reach the model. This sub-spec touches only the
  turn-level / model-bypassing paths where an error reaches a person directly.
- Channels that already catch (GChat/Telegram) keep their `try/except`; only the
  produced string changes. The `run()` except remains the backstop for bus channels.

## Testing

New: **`tests/test_error_ux.py`**

- One assertion per classifier row (auth / timeout / network / rate-limit / generic),
  each driven by a representative exception (by type and by message substring).
- Ordering tests: a `"429 Too Many Requests"` → rate-limit (not network); an
  `AuthenticationError("connection to api failed")` → auth (not network).
- `ToolError` rendering: with/without `hint`; `retryable=True` appends the nudge,
  `retryable=False` does not; the `Error [code]:` prefix never appears in output.
- **Secret-safety guarantee:** an exception whose `str()` embeds a secret token never
  has that substring appear in `render_user_error` output on the generic path.

Integration:

- `process_message_stream` yields a clean `error` event (no `Traceback`, no `str(e)`
  internals) when the provider call raises.
- CLI single-shot: a forced failure prints the friendly line and no traceback.
- Slash `/status` with a failing `health_check_all` returns no raw `str(e)`.

Baseline is **477** passing; expect roughly **+12** tests. `ruff check databot/` and
`ruff format --check databot/` must stay clean (CI lints `databot/` only).

## Execution & integration

- Subagent-driven per the established milestone pattern (fresh subagent per task,
  two-stage review: spec-compliance then code-quality).
- Branch `feat/error-ux-in-chat` off `main`; on completion **merge to `main` locally**
  with `--ff-only`. **Nothing is pushed to `origin`** (local-only, consistent with #1/#2).
