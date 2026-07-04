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
