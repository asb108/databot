# Structured Tool Errors — Design

**Date:** 2026-07-04
**Status:** Approved (design)
**Sub-spec:** #2 of the "Robustness & Polish" milestone

---

## Milestone context

Second of four sequenced robustness sub-specs (see the milestone table in
`2026-07-04-gateway-app-factory-http-tests-design.md`). #1 (gateway app-factory +
HTTP tests) is merged. This sub-spec is the **error backbone** that #3 (error UX in
chat) will consume; #4 (connector circuit breaker) will emit these errors too.

## Problem

Tool failures are stringly-typed and inconsistent. Across 21 tool files there are
~86 `return "Error: …"` / `return "❌ …"` sites, each with an ad-hoc message. The
agent loop feeds whatever string comes back to the LLM (`loop.py:242,402` →
`add_tool_result`) and emits it as a `tool_result` stream event — with **no way to
tell an error from a success**, no error code, and no retry signal.

There is also a **latent accuracy bug**: `ToolRegistry.execute` (base.py:95–109)
only marks `status="error"` when a tool *raises*. A tool that `return`s an error
*string* is recorded in audit + metrics as `status="ok"`. So the audit log and the
`databot_tool_calls_total{status}` metric under-count failures.

## Goals

1. A single structured error type, `ToolError(code, message, hint, retryable)`, that
   tools raise instead of returning ad-hoc error strings.
2. `ToolRegistry.execute` catches it, renders **one consistent** LLM-facing string,
   and records the structured `code` into audit + metrics — fixing the accuracy bug.
3. Migrate **all** tool error sites to `raise ToolError(...)` so failures are coded
   and consistently formatted everywhere.

## Non-goals

- **No change to `execute`'s return type** — it still returns `str`. The loop,
  multi-agent path, and MCP server callers are untouched. (This is the "minimal"
  fork, chosen over returning a `ToolResult` object.)
- No structured error data in the streaming events yet — that is sub-spec #3's call
  if it needs it. This sub-spec keeps the wire format a string.
- No connector-layer changes (that's #4).
- No new user-facing endpoints.

---

## Design

### 1. The `ToolError` contract — new `databot/tools/errors.py`

```python
class ToolError(Exception):
    """Structured, LLM-facing tool failure.

    code:      short machine-readable slug (see ErrorCode).
    message:   human/LLM-readable description of what went wrong.
    hint:      optional actionable next step ("remove the INSERT and retry").
    retryable: True if a later identical call might succeed (transient).
    """

    def __init__(self, code, message, *, hint=None, retryable=False):
        self.code = code
        self.message = message
        self.hint = hint
        self.retryable = retryable
        super().__init__(message)

    def to_text(self) -> str:
        parts = [f"Error [{self.code}]: {self.message}"]
        if self.hint:
            parts.append(f"Hint: {self.hint}")
        if self.retryable:
            parts.append("This looks transient; a retry may succeed.")
        return " ".join(parts)
```

Standard codes are provided as constants so tools stay consistent, but `code` is a
plain string — a tool may pass a more specific slug when warranted:

```python
class ErrorCode:
    INVALID_ARGUMENT = "invalid_argument"
    NOT_FOUND = "not_found"
    READ_ONLY_VIOLATION = "read_only_violation"
    PERMISSION_DENIED = "permission_denied"
    CONNECTION_ERROR = "connection_error"
    NOT_CONFIGURED = "not_configured"
    DEPENDENCY_MISSING = "dependency_missing"
    UNSUPPORTED = "unsupported"
    # registry-owned:
    UNKNOWN_TOOL = "unknown_tool"
    TIMEOUT = "timeout"
    INTERNAL_ERROR = "internal_error"
```

The rendered text intentionally **starts with `"Error"`** so existing
`assert "Error" in result` tests keep passing; only exact-string assertions change.

### 2. Registry integration — `ToolRegistry.execute` (base.py)

Rewrite the try/except so every failure path produces a `ToolError`, renders it to
the returned string, and records `code` as the status:

```python
if not tool:
    err = ToolError(ErrorCode.UNKNOWN_TOOL, f"Unknown tool '{name}'.")
    metrics.tool_call(name, err.code)
    audit.record(name, arguments, err.code)
    return err.to_text()

status = "ok"
try:
    return await asyncio.wait_for(tool.execute(**arguments), timeout=timeout)
except ToolError as e:
    status = e.code
    return e.to_text()
except asyncio.TimeoutError:
    e = ToolError(ErrorCode.TIMEOUT,
                  f"Tool '{name}' timed out after {timeout} seconds.", retryable=True)
    status = e.code
    return e.to_text()
except Exception as e:
    err = ToolError(ErrorCode.INTERNAL_ERROR, f"Tool '{name}' failed: {e}")
    status = err.code
    return err.to_text()
finally:
    elapsed = loop.time() - start
    metrics.tool_call(name, status, elapsed)
    audit.record(name, arguments, status, duration_ms=int(elapsed * 1000))
```

- Success keeps `status="ok"`.
- The `code` flows into both `metrics.tool_call(status=code)` (a Prometheus label —
  codes are a bounded static set, so cardinality is fine) and `audit.record(status=code)`.
  This is what fixes the under-counting bug.
- Back-compat: the metrics/audit status value `"error"` becomes `"internal_error"`
  (generic) or a specific code. Success/`"ok"`, `"timeout"`, `"unknown"`→`"unknown_tool"`.

### 3. Tool migration (all 21 files, ~86 sites)

Mechanical, one pattern per site: `return "Error: <msg>"` → `raise ToolError(<code>,
"<msg>", hint=…, retryable=…)`, choosing an apt `ErrorCode`. Guidelines:

- Bad/absent params → `INVALID_ARGUMENT`. Missing table/dag/topic/file → `NOT_FOUND`.
- Non-SELECT SQL when read-only → `READ_ONLY_VIOLATION`. Path outside workspace →
  `PERMISSION_DENIED`. Unreachable service / connector error → `CONNECTION_ERROR`
  (usually `retryable=True`). Connector/feature not set up → `NOT_CONFIGURED`.
  Optional dep not installed → `DEPENDENCY_MISSING`. Unsupported mode/op → `UNSUPPORTED`.
- Domain `raise`s that map cleanly are migrated too: `filesystem.py` `PermissionError`
  → `PERMISSION_DENIED`; `lineage.py` `ImportError` → `DEPENDENCY_MISSING`. Other bare
  `raise Exception(...)` that already surface as `internal_error` may stay unless a
  clear code applies.

Each tool file is independent, so migration is done one file per task (pipelined in
execution). The registry's generic `except Exception` remains a safety net for
anything not explicitly migrated.

### 4. Testing

New `tests/test_tool_errors.py`:
- `ToolError.to_text()` — with/without hint, with/without retryable.
- `ToolRegistry.execute` behavior for each branch, asserting **both** the returned
  string (starts with `"Error ["`, contains the code) **and** the recorded
  audit/metrics status equals the code:
  - a stub tool that raises `ToolError("read_only_violation", …)` → status `read_only_violation`;
  - a stub tool that raises a generic `Exception` → `internal_error`;
  - a stub tool that hangs past its timeout → `timeout`, retryable note present;
  - unknown tool name → `unknown_tool`.
- Representative migrated paths on real tools: SQL write blocked → `read_only_violation`;
  filesystem path outside workspace → `permission_denied`.

Update the handful of existing exact-match assertions (≈4 test files:
`test_profiling`, `test_integration_wiring`, `test_metrics`, `test_web`) to the new
format / status codes. Substring `"Error"` assertions are unaffected by design.

---

## File structure

| File | Change |
|------|--------|
| `databot/tools/errors.py` | **New** — `ToolError` + `ErrorCode`. |
| `databot/tools/base.py` | Modify `ToolRegistry.execute` (import `ToolError`, restructure try/except, status=code). |
| `databot/tools/*.py` (21 files) | Migrate `return "Error…"` → `raise ToolError(...)`; a few domain raises. |
| `tests/test_tool_errors.py` | **New** — errors + registry + representative tools. |
| `tests/test_{profiling,integration_wiring,metrics,web}.py` | Update exact-match error assertions. |

## Verification / success criteria

1. Full `pytest` green (updated assertions included).
2. `ruff check databot/` + `ruff format --check databot/` clean.
3. No `return "Error` / `return "❌` string left in `databot/tools/*.py` (grep proves
   the migration is complete).
4. Audit log and `databot_tool_calls_total{status}` now record failure codes (not `ok`)
   when a tool returns/raises an error — verified by a registry test that inspects the
   recorded status.

## Risks & mitigations

| Risk | Mitigation |
|------|------------|
| A migrated `raise` breaks control flow that previously `return`ed mid-function | `raise` exits the tool the same way a `return "Error"` did (both stop execution); registry catches it. Per-file tests + full suite catch regressions. |
| Wrong/over-specific error `code` chosen for a site | Codes are advisory; the standard set covers the common cases. Reviewer checks code choices during execution. |
| Existing exact-string test assertions break | Enumerated up front (≈4 files); updated as part of the work. Substring `"Error"` assertions survive by keeping the prefix. |
| Prometheus label cardinality from codes | Codes are a bounded static set (~11); negligible series growth. |
| A tool `return`s a non-error string starting with "Error" (false positive during grep) | Migration is per-site human/reviewer-checked, not a blind sed; only genuine error returns become raises. |
