# Structured Tool Errors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace ~86 ad-hoc `return "Error…"` sites across the tools with a structured `ToolError(code, message, hint, retryable)` that the registry renders consistently and records into audit/metrics by code.

**Architecture:** New `databot/tools/errors.py` defines `ToolError` (an `Exception`) + `ErrorCode` constants. `ToolRegistry.execute` catches `ToolError`, renders `.to_text()` for the LLM, and records `code` as the audit/metrics status — `execute` still returns `str`, so loop/agents/MCP callers are untouched. All 20 tool files migrate their error returns to `raise ToolError(...)`.

**Tech Stack:** Python 3.9+, pytest, ruff. Interpreter is `python3` (NOT `python`).

**Design ref:** `docs/superpowers/specs/2026-07-04-structured-tool-errors-design.md`

---

## Key facts (established during design — do not re-derive)

- `python3 -m pytest` and `ruff check databot/` / `ruff format --check databot/`. Baseline is GREEN: **468 passed, 10 skipped**. CI lints only `databot/` (not `tests/`).
- The current `ToolRegistry.execute` lives in `databot/tools/base.py:77-109`. Its `unknown`/`timeout`/generic-`error` strings are what Task 2 rewrites.
- **20 tool files** have error-return sites (base.py's 3 are the registry strings, handled in Task 2). Site counts: filesystem 9, data_quality 8, ticket 6, shell 5, saved_query 5, catalog 5, airflow 5, sql 4, spark 4, lineage 4, kafka 4, dbt 4, schema_tool 3, profiling 3, monitor 3, cron 3, chart 3, web 2, export 2, nl_to_sql 1.
- `to_text()` output MUST start with `"Error"` so existing `assert "Error" in result` tests (test_integration_wiring.py:283, test_web.py:29, test_profiling.py:43) keep passing.
- **Only `test_metrics.py` breaks from the registry change** (lines 84, 89, 90 assert `"Error executing tool"`, `status="error"`, `status="unknown"`). Those are updated in Task 2. Tool-specific test breakage (if any) is handled inside each file's migration in Task 3.
- `metrics.tool_call(tool, status, latency)` uses `status` as a Prometheus label; `audit.record(tool, arguments, status, ...)` stores it as text. Error codes are a bounded static set → cardinality is fine.
- Exception ordering matters: `except ToolError` MUST precede `except Exception`.
- **In-process tool composition (critical):** six call sites invoke the sql *tool* directly as `self._sql.execute(...)` (not via the registry), so once `sql.py` raises `ToolError`, those raises propagate into the caller instead of being a string. Verified: no consumer branches on the error string (`grep "Error" in result` is empty), and MCP (`mcp/__init__.py:235`) + agents (`agents/__init__.py:141`) go through `registry.execute` (safe). The six direct consumers and their handling:
  - `monitors/service.py:104` — parses via `parse_metric_value(...) → None` for a graceful "error" run. **A raise escapes and breaks this. Must wrap.**
  - `profiling.py:96,134,156` — parses via `_parse_single_row`/`splitlines`, degrades to `None`/error-dict. **Must wrap.**
  - `data_quality.py:121,134,146,156,160` — embeds result in markdown. **Wrap** (preserves embedded-error output).
  - `nl_to_sql.py:164`, `saved_query.py:128` — embed result. **Wrap** (keeps generated-SQL + error visible).
  - `schema_tool.py:136` — pure `return await self._sql.execute(...)` passthrough; a raise propagates cleanly to the registry. **No wrap needed.**
  The wrap is behavior-preserving: `try: <var> = await self._sql.execute(...) except ToolError as e: <var> = e.to_text()`. It is landed BEFORE the sql migration (Task 3), where it is a harmless no-op until sql actually raises (Task 4) — so every commit stays green.

---

## Task 1: The `ToolError` type

**Files:**
- Create: `databot/tools/errors.py`
- Test: `tests/test_tool_errors.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_tool_errors.py`:

```python
"""Tests for the structured ToolError type and its registry integration."""

from __future__ import annotations

from databot.tools.errors import ErrorCode, ToolError


def test_to_text_minimal():
    err = ToolError(ErrorCode.NOT_FOUND, "Table 'x' does not exist.")
    assert err.to_text() == "Error [not_found]: Table 'x' does not exist."


def test_to_text_with_hint():
    err = ToolError("read_only_violation", "Only SELECT is allowed.", hint="Remove the INSERT.")
    assert err.to_text() == (
        "Error [read_only_violation]: Only SELECT is allowed. Hint: Remove the INSERT."
    )


def test_to_text_retryable_note():
    err = ToolError(ErrorCode.CONNECTION_ERROR, "Airflow unreachable.", retryable=True)
    text = err.to_text()
    assert text.startswith("Error [connection_error]: Airflow unreachable.")
    assert "transient" in text


def test_is_exception_and_carries_fields():
    err = ToolError(ErrorCode.INVALID_ARGUMENT, "bad arg", hint="fix it", retryable=False)
    assert isinstance(err, Exception)
    assert err.code == "invalid_argument"
    assert err.message == "bad arg"
    assert err.hint == "fix it"
    assert err.retryable is False
    assert str(err) == "bad arg"


def test_error_code_constants_are_slugs():
    assert ErrorCode.INTERNAL_ERROR == "internal_error"
    assert ErrorCode.UNKNOWN_TOOL == "unknown_tool"
    assert ErrorCode.TIMEOUT == "timeout"
```

- [ ] **Step 2: Run to verify it fails**

Run: `python3 -m pytest tests/test_tool_errors.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'databot.tools.errors'`.

- [ ] **Step 3: Implement `errors.py`**

Create `databot/tools/errors.py`:

```python
"""Structured tool errors.

Tools raise ``ToolError`` instead of returning ad-hoc ``"Error: …"`` strings.
``ToolRegistry.execute`` catches it, renders one consistent LLM-facing string via
``to_text()``, and records ``code`` as the audit/metrics status. ``execute`` still
returns ``str``, so callers (loop, agents, MCP) are unchanged.
"""

from __future__ import annotations


class ErrorCode:
    """Standard error-code slugs. ``code`` is a plain string; tools may pass a more
    specific slug, but prefer these for consistent audit/metrics grouping."""

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


class ToolError(Exception):
    """A structured, LLM-facing tool failure."""

    def __init__(self, code: str, message: str, *, hint: str | None = None,
                 retryable: bool = False) -> None:
        self.code = code
        self.message = message
        self.hint = hint
        self.retryable = retryable
        super().__init__(message)

    def to_text(self) -> str:
        """Render a consistent, LLM-readable error string (starts with 'Error')."""
        parts = [f"Error [{self.code}]: {self.message}"]
        if self.hint:
            parts.append(f"Hint: {self.hint}")
        if self.retryable:
            parts.append("This looks transient; a retry may succeed.")
        return " ".join(parts)
```

- [ ] **Step 4: Run to verify it passes**

Run: `python3 -m pytest tests/test_tool_errors.py -q`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add databot/tools/errors.py tests/test_tool_errors.py
git commit -m "feat: add structured ToolError type"
```

---

## Task 2: Integrate `ToolError` into `ToolRegistry.execute`

**Files:**
- Modify: `databot/tools/base.py` (the `execute` method, lines 77-109)
- Modify: `tests/test_metrics.py` (lines 84, 89, 90 — status/message assertions)
- Test: `tests/test_tool_errors.py` (append registry tests)

- [ ] **Step 1: Write the failing registry tests**

Append to `tests/test_tool_errors.py`:

```python
import asyncio

import pytest

from databot.audit import configure_audit_log
from databot.observability.metrics import configure_metrics
from databot.tools.base import ToolRegistry


class _RaisingTool:
    timeout = None  # registry reads tool.timeout; duck-typed stubs must define it

    def __init__(self, name, exc):
        self._name, self._exc = name, exc

    @property
    def name(self):
        return self._name

    @property
    def description(self):
        return "test"

    def parameters(self):
        return {"type": "object", "properties": {}}

    async def execute(self, **kwargs):
        raise self._exc


class _SlowTool(_RaisingTool):
    timeout = 1

    async def execute(self, **kwargs):
        await asyncio.sleep(5)


@pytest.fixture
def audited(tmp_path):
    configure_metrics(enabled=False)
    return configure_audit_log(tmp_path / "audit.db")


@pytest.mark.asyncio
async def test_execute_tool_error_returns_text_and_records_code(audited):
    reg = ToolRegistry()
    reg.register(_RaisingTool("boom", ToolError(ErrorCode.READ_ONLY_VIOLATION, "no writes")))
    out = await reg.execute("boom", {})
    assert out == "Error [read_only_violation]: no writes"
    assert audited.recent(limit=1)[0]["status"] == "read_only_violation"


@pytest.mark.asyncio
async def test_execute_generic_exception_is_internal_error(audited):
    reg = ToolRegistry()
    reg.register(_RaisingTool("boom", ValueError("kaboom")))
    out = await reg.execute("boom", {})
    assert out.startswith("Error [internal_error]:")
    assert "kaboom" in out
    assert audited.recent(limit=1)[0]["status"] == "internal_error"


@pytest.mark.asyncio
async def test_execute_timeout_is_retryable(audited):
    reg = ToolRegistry(default_timeout=1)
    reg.register(_SlowTool("slow", None))
    out = await reg.execute("slow", {})
    assert out.startswith("Error [timeout]:")
    assert "transient" in out
    assert audited.recent(limit=1)[0]["status"] == "timeout"


@pytest.mark.asyncio
async def test_execute_unknown_tool(audited):
    reg = ToolRegistry()
    out = await reg.execute("nope", {})
    assert out == "Error [unknown_tool]: Unknown tool 'nope'."
    assert audited.recent(limit=1)[0]["status"] == "unknown_tool"
```

- [ ] **Step 2: Run to verify the new tests fail**

Run: `python3 -m pytest tests/test_tool_errors.py -q`
Expected: FAIL — current `execute` returns `"Error: Unknown tool 'nope'"` / `"Error executing tool…"` and records status `"unknown"`/`"error"`, not the new codes.

- [ ] **Step 3: Rewrite `ToolRegistry.execute`**

In `databot/tools/base.py`, add the import near the top (after the existing imports):

```python
from databot.tools.errors import ErrorCode, ToolError
```

Replace the body of `execute` (currently lines 77-109) with:

```python
    async def execute(self, name: str, arguments: dict[str, Any]) -> str:
        """Execute a tool by name with timeout enforcement and structured errors."""
        from databot.audit import get_audit_log
        from databot.observability.metrics import get_metrics

        metrics = get_metrics()
        audit = get_audit_log()
        tool = self._tools.get(name)
        if not tool:
            err = ToolError(ErrorCode.UNKNOWN_TOOL, f"Unknown tool '{name}'.")
            metrics.tool_call(name, err.code)
            audit.record(name, arguments, err.code)
            return err.to_text()

        timeout = tool.timeout if tool.timeout is not None else self._default_timeout

        loop = asyncio.get_event_loop()
        start = loop.time()
        status = "ok"
        try:
            return await asyncio.wait_for(tool.execute(**arguments), timeout=timeout)
        except ToolError as e:
            logger.warning(f"Tool '{name}' error [{e.code}]: {e.message}")
            status = e.code
            return e.to_text()
        except asyncio.TimeoutError:
            logger.error(f"Tool '{name}' timed out after {timeout}s")
            e = ToolError(
                ErrorCode.TIMEOUT,
                f"Tool '{name}' timed out after {timeout} seconds.",
                retryable=True,
            )
            status = e.code
            return e.to_text()
        except Exception as e:
            logger.error(f"Tool '{name}' failed: {e}")
            err = ToolError(ErrorCode.INTERNAL_ERROR, f"Tool '{name}' failed: {e}")
            status = err.code
            return err.to_text()
        finally:
            elapsed = loop.time() - start
            metrics.tool_call(name, status, elapsed)
            audit.record(name, arguments, status, duration_ms=int(elapsed * 1000))
```

- [ ] **Step 4: Update `tests/test_metrics.py`**

The registry status semantics changed. In `tests/test_metrics.py`, update the three assertions:
- Line 84: `assert "Error executing tool" in result` → `assert "Error [internal_error]" in result`
- Line 89: `assert 'status="error"' in payload` → `assert 'status="internal_error"' in payload`
- Line 90: `assert 'status="unknown"' in payload` → `assert 'status="unknown_tool"' in payload`

(Read the surrounding lines first to confirm the failing tool in that test raises a generic exception — it does; the `"echo"` ok-path assertion on line 82/88 is unchanged.)

- [ ] **Step 5: Run to verify all pass**

Run: `python3 -m pytest tests/test_tool_errors.py tests/test_metrics.py -q`
Expected: PASS.
Then full suite: `python3 -m pytest -q` — expect all pass. No tool has been migrated yet, so tool tests are unaffected at this point; only the `test_metrics` updates above matter here. (Tool-specific exact-string assertions are handled within Tasks 4–5 as each file migrates.) If any NON-tool test fails, fix it here.

- [ ] **Step 6: Commit**

```bash
git add databot/tools/base.py tests/test_tool_errors.py tests/test_metrics.py
git commit -m "feat: registry renders ToolError and records error code as status"
```

---

## Task 3: Protect in-process SQL consumers (wrap before sql migrates)

**Files:**
- Modify: `databot/tools/data_quality.py`, `databot/tools/profiling.py`, `databot/tools/nl_to_sql.py`, `databot/tools/saved_query.py`, `databot/monitors/service.py`
- (Not `schema_tool.py:136` — it is a passthrough `return await self._sql.execute(...)`; a raise propagates cleanly to the registry.)

This lands BEFORE `sql.py` migrates. Until then `self._sql.execute` returns strings, so the added `except ToolError` is a dormant no-op — the suite stays green — and once Task 4 makes sql raise, these wraps preserve the exact prior behavior.

- [ ] **Step 1: Add the import + wrap each `self._sql.execute(...)` site**

In each of the five files add `from databot.tools.errors import ToolError` (top-level import), then wrap every `self._sql.execute(...)` call so a `ToolError` becomes its rendered string, assigned to the **same variable** the code already uses. Grep the exact sites: `grep -nE 'self\._sql\.execute\(' databot/tools/{data_quality,profiling,nl_to_sql,saved_query}.py databot/monitors/service.py`.

Worked example — `monitors/service.py:104` (the critical parsing consumer):

```python
        try:
            result_md = await self._sql.execute(query=query, connection=monitor["connection"])
        except ToolError as e:
            result_md = e.to_text()
        value = parse_metric_value(result_md)
```

Worked example — `data_quality.py._row_count` (an embedding consumer):

```python
        try:
            result = await self._sql.execute(query=query, connection=connection)
        except ToolError as e:
            result = e.to_text()
        return f"**Row count for `{table}`:**\n{result}"
```

Apply the identical `try/except ToolError → to_text()` wrap to every `self._sql.execute(...)` in the five files (data_quality has 5, profiling 3, nl_to_sql 1, saved_query 1, monitors/service 1).

- [ ] **Step 2: Run — suite must stay green (wrap is a dormant no-op)**

Run: `python3 -m pytest -q` → expect **468 passed, 10 skipped** (unchanged; sql doesn't raise yet).
Run: `ruff check databot/ && ruff format --check databot/` → clean.

- [ ] **Step 3: Commit**

```bash
git add databot/tools/data_quality.py databot/tools/profiling.py databot/tools/nl_to_sql.py databot/tools/saved_query.py databot/monitors/service.py
git commit -m "refactor: guard in-process sql consumers against ToolError"
```

---

## Task 4: Migrate `sql.py`

**Files:**
- Modify: `databot/tools/sql.py` (4 error sites: lines 91, 112, 134, 143)
- Test: existing `tests/test_sql*.py` / `tests/test_smoke_sqlite.py` if they assert exact strings.

- [ ] **Step 1: Migrate the 4 sites**

Add `from databot.tools.errors import ErrorCode, ToolError`, then:

```python
# line 143 — multiple statements under read-only
- return "Error: Multiple SQL statements are not allowed in read-only mode."
+ raise ToolError(ErrorCode.READ_ONLY_VIOLATION,
+                 "Multiple SQL statements are not allowed in read-only mode.")

# line 91 — unknown connection
- return f"Error: Unknown connection '{connection}'. Available: {available}"
+ raise ToolError(ErrorCode.NOT_FOUND, f"Unknown connection '{connection}'.",
+                 hint=f"Available: {available}")

# line 112 — connector returned an error
- return f"Error: {result.error}"
+ raise ToolError(ErrorCode.CONNECTION_ERROR, str(result.error), retryable=True)

# line 134 — generic except Exception as e
- return f"Error executing query: {str(e)}"
+ raise ToolError(ErrorCode.INTERNAL_ERROR, f"executing query: {e}")
```

Also check `sql.py` for a read-only-violation on single statements (non-SELECT) — if a
`return "Error: ...read-only..."` exists beyond line 143, migrate it to `READ_ONLY_VIOLATION` too.

- [ ] **Step 2: Verify the wraps hold and nothing escapes**

Run: `python3 -m pytest -q` — expect **all pass** (the Task 3 wraps convert sql's raises back into strings inside monitors/data_quality/profiling/nl_to_sql/saved_query; schema_tool's passthrough raise is caught by the registry). Pay attention to `test_monitors`, `test_data_quality`, `test_profiling`, `test_nl_to_sql`, `test_saved_query`, `test_smoke_sqlite` — all must stay green.
Run: `grep -nE 'return f?"(Error|❌)' databot/tools/sql.py` → empty.
Run: `ruff check databot/ && ruff format --check databot/` → clean.

- [ ] **Step 3: Commit**

```bash
git add databot/tools/sql.py tests/
git commit -m "refactor: migrate sql tool to ToolError"
```

---

## Task 5: Migrate the remaining tool files to `raise ToolError`

**Files:** the remaining tool files under `databot/tools/` — everything except `sql.py` (done in Task 4): `filesystem.py`, `data_quality.py`, `ticket.py`, `shell.py`, `saved_query.py`, `catalog.py`, `airflow.py`, `spark.py`, `lineage.py`, `kafka.py`, `dbt.py`, `schema_tool.py`, `profiling.py`, `monitor.py`, `cron.py`, `chart.py`, `web.py`, `export.py`, `nl_to_sql.py` — plus any tool-specific test that asserts an exact error string.

This migrates each file's **own** `return "Error…"` sites (independent of the sql-consumer wraps added in Task 3, which stay). Applied **once per tool file** — each an independent commit. `filesystem.py` is fully worked below as the reference; apply the identical procedure to every other file.

### Code-selection guidance

| Situation | `ErrorCode` | `retryable` |
|-----------|-------------|-------------|
| Missing/invalid parameter, malformed input | `INVALID_ARGUMENT` | False |
| Named table/dag/topic/file/connection not found | `NOT_FOUND` | False |
| Write/DDL attempted under read-only | `READ_ONLY_VIOLATION` | False |
| Path outside workspace / denied | `PERMISSION_DENIED` | False |
| Remote service unreachable / connector error | `CONNECTION_ERROR` | True |
| Connector/feature not set up | `NOT_CONFIGURED` | False |
| Optional dependency not installed | `DEPENDENCY_MISSING` | False |
| Unsupported mode/operation | `UNSUPPORTED` | False |
| Caught generic `except Exception as e` adding context | `INTERNAL_ERROR` | False |

Preserve the original human wording of each message (so substring assertions like
`"Invalid"` in test_profiling.py:43 still hold). Add `import` at the top of each file:
`from databot.tools.errors import ErrorCode, ToolError`.

### Per-file procedure (repeat for every file)

- [ ] Add the `from databot.tools.errors import ErrorCode, ToolError` import.
- [ ] Convert each `return "Error…"` / `return "❌…"` to `raise ToolError(<code>, "<message>", hint=…, retryable=…)`, choosing the code from the table. Also convert domain `raise`s that map cleanly (e.g. `raise PermissionError(...)` → `raise ToolError(ErrorCode.PERMISSION_DENIED, ...)`; `raise ImportError("… not installed")` → `raise ToolError(ErrorCode.DEPENDENCY_MISSING, ...)`).
- [ ] `grep -nE 'return f?"(Error|❌)' databot/tools/<file>.py` → must return nothing.
- [ ] Run that tool's tests (e.g. `python3 -m pytest tests/test_<tool>.py -q` where one exists) and update any exact-string assertion to the new format.
- [ ] `ruff check databot/tools/<file>.py && ruff format databot/tools/<file>.py`
- [ ] Commit: `git add databot/tools/<file>.py tests/… && git commit -m "refactor: migrate <file> to ToolError"`

### Worked reference: `filesystem.py` (all 9 sites)

Current → migrated (preserve surrounding code; only the error lines change):

```python
# line 20 — shared path validator
- raise PermissionError(f"Path '{path}' is outside the allowed workspace")
+ raise ToolError(ErrorCode.PERMISSION_DENIED,
+                 f"Path '{path}' is outside the allowed workspace.")

# line 54 / 137 — file missing (read / edit)
- return f"Error: File '{path}' does not exist"
+ raise ToolError(ErrorCode.NOT_FOUND, f"File '{path}' does not exist.")

# line 75 — read failure (inside except Exception as e)
- return f"Error reading file: {str(e)}"
+ raise ToolError(ErrorCode.INTERNAL_ERROR, f"reading file: {e}")

# line 107 — write failure (inside except Exception as e)
- return f"Error writing file: {str(e)}"
+ raise ToolError(ErrorCode.INTERNAL_ERROR, f"writing file: {e}")

# line 142 — edit: old_string absent
- return "Error: old_string not found in file"
+ raise ToolError(ErrorCode.NOT_FOUND, "old_string not found in file.")

# line 144 — edit: old_string ambiguous
- return f"Error: old_string found {count} times; must be unique"
+ raise ToolError(ErrorCode.INVALID_ARGUMENT,
+                 f"old_string found {count} times; must be unique.")

# line 152 — edit failure (inside except Exception as e)
- return f"Error editing file: {str(e)}"
+ raise ToolError(ErrorCode.INTERNAL_ERROR, f"editing file: {e}")

# line 177 — list target not a directory
- return f"Error: '{path}' is not a directory"
+ raise ToolError(ErrorCode.INVALID_ARGUMENT, f"'{path}' is not a directory.")

# line 193 — list failure (inside except Exception as e)
- return f"Error listing directory: {str(e)}"
+ raise ToolError(ErrorCode.INTERNAL_ERROR, f"listing directory: {e}")
```

Then: `grep -nE 'return f?"(Error|❌)' databot/tools/filesystem.py` → empty; run any filesystem tests; ruff; commit.

### Files to migrate (each its own commit)

`filesystem.py`, `data_quality.py`, `ticket.py`, `shell.py`, `saved_query.py`, `catalog.py`, `airflow.py`, `spark.py`, `lineage.py`, `kafka.py`, `dbt.py`, `schema_tool.py`, `profiling.py`, `monitor.py`, `cron.py`, `chart.py`, `web.py`, `export.py`, `nl_to_sql.py`. (`sql.py` was done in Task 4.)

Note: the five sql-consumer files (`data_quality`, `profiling`, `nl_to_sql`, `saved_query`, and the non-tool `monitors/service.py`) already got their `except ToolError` wraps in Task 3 — here you migrate only their **own** `return "Error…"` sites; leave the wraps in place.

Worked examples for a few tricky ones:
- `data_quality.py:74` `return "Error: SQL tool not configured. Cannot run DQ checks."` → `raise ToolError(ErrorCode.NOT_CONFIGURED, "SQL tool not configured. Cannot run DQ checks.")`
- `data_quality.py:117` outer `except Exception as e: return f"DQ check error: {str(e)}"` → `except Exception as e: raise ToolError(ErrorCode.INTERNAL_ERROR, f"DQ check error: {e}")` (place it AFTER an `except ToolError: raise` so structured sql/sub errors keep their code).
- `lineage.py:36` `raise ImportError("networkx not installed…")` → `raise ToolError(ErrorCode.DEPENDENCY_MISSING, "networkx not installed.", hint="pip install databot[lineage]")`
- `nl_to_sql.py` / `saved_query.py`: a bare `except Exception` that wraps a broad block should re-raise `ToolError` unchanged before wrapping generic ones — `except ToolError: raise` then `except Exception as e: raise ToolError(ErrorCode.INTERNAL_ERROR, ...)`.

---

## Task 6: Final sweep

**Files:** none (verification only)

- [ ] **Step 1: Prove the migration is complete**

Run: `grep -rnE 'return f?"(Error|❌)' databot/tools/*.py`
Expected: **no output** (base.py's registry strings were replaced with `ToolError` in Task 2; every tool now raises).
Also confirm no unguarded in-process sql consumer remains:
`grep -rnE 'self\._sql\.execute\(' databot/tools/*.py databot/monitors/service.py` — every hit except the `schema_tool.py` passthrough should sit under a `try:` with an `except ToolError`.

- [ ] **Step 2: Full suite + lint**

Run: `python3 -m pytest -q` — expect all pass (baseline 468 + new `test_tool_errors.py` tests; no net loss).
Run: `ruff check databot/ && ruff format --check databot/` — expect clean.

- [ ] **Step 3: Confirm the audit-status bug is fixed**

This is covered by `test_execute_tool_error_returns_text_and_records_code` (Task 2) — a tool error now records its `code`, never `"ok"`. No extra work; just confirm that test is green.

- [ ] **Step 4: Commit any residual**

If Steps 1-2 required a fixup (a missed site, a ruff nit), commit it:

```bash
git add -A
git commit -m "chore: structured-tool-errors migration sweep"
```

---

## Self-review notes

- **Spec coverage:** ToolError type + codes (Task 1) ✓; registry integration + status-by-code + audit-bug fix (Task 2) ✓; in-process sql-consumer safety (Task 3) ✓; sql migration (Task 4) ✓; migrate all remaining tool files incl. domain raises (Task 5) ✓; tests — errors, registry branches, representative tools (Tasks 1-5) ✓; update exact-match assertions (test_metrics in Task 2; tool-specific within Tasks 4-5) ✓; grep-verified completeness (Task 6) ✓.
- **`execute` return type unchanged** (still `str`) — no loop/agents/MCP edits, matching the chosen minimal fork. MCP + agents verified to call `registry.execute` (not raw `tool.execute`), so they are unaffected.
- **Composition safety:** the six direct `self._sql.execute` consumers are handled — five wrapped with `except ToolError → to_text()` (Task 3, landed before sql raises so every commit is green), `schema_tool` passthrough left to propagate to the registry. This is the same "passes-tests-but-breaks-in-prod" class caught in sub-spec #1; the wraps preserve monitors' graceful `None` path and profiling's degrade-to-error-dict path.
- **Sequencing keeps every commit green:** wraps (dormant no-op) → sql raises (wraps activate) → remaining tools. No intermediate broken state.
- **Type/name consistency:** `ToolError(code, message, *, hint, retryable)` and `ErrorCode.*` slugs are identical across all tasks; `.to_text()` used in Tasks 2-3 matches Task 1's definition; `.code` recorded as status in Task 2 matches the field defined in Task 1.
- **Ordering:** `except ToolError` precedes `except Exception` in Task 2 and in consumer files that re-raise (Task 5), else structured errors would be swallowed as internal_error.
