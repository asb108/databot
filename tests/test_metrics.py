"""Tests for Prometheus metrics facade and tool-registry instrumentation."""

from __future__ import annotations

import pytest

from databot.observability.metrics import Metrics, configure_metrics, get_metrics
from databot.tools.base import BaseTool, ToolRegistry


class _EchoTool(BaseTool):
    @property
    def name(self) -> str:
        return "echo"

    @property
    def description(self) -> str:
        return "echo"

    def parameters(self):
        return {"type": "object", "properties": {}}

    async def execute(self, **kwargs):
        return "ok"


class _BoomTool(_EchoTool):
    @property
    def name(self) -> str:
        return "boom"

    async def execute(self, **kwargs):
        raise RuntimeError("kaboom")


class TestMetricsFacade:
    def test_enabled_records_and_renders(self):
        m = Metrics(enabled=True)
        assert m.enabled is True
        m.message("api", 0.1)
        m.tool_call("sql", "ok", 0.2)
        m.llm_call(100, 50, 0.003)
        m.error("loop")
        payload = m.render().decode()
        assert "databot_messages_total" in payload
        assert "databot_tool_calls_total" in payload
        assert "databot_llm_tokens_total" in payload
        assert 'channel="api"' in payload

    def test_disabled_is_noop(self):
        m = Metrics(enabled=False)
        assert m.enabled is False
        # No exceptions; render returns the disabled note.
        m.message("api", 0.1)
        m.tool_call("sql", "ok")
        m.llm_call(1, 1, 1.0)
        assert b"disabled" in m.render()

    def test_separate_registries_no_duplicate_error(self):
        # Constructing twice must not raise duplicate-timeseries errors.
        a = Metrics(enabled=True)
        b = Metrics(enabled=True)
        a.message("x")
        b.message("y")
        assert a.enabled and b.enabled

    def test_global_configure(self):
        m = configure_metrics(enabled=True)
        assert get_metrics() is m
        assert m.enabled is True
        # Reset global to a disabled instance so other tests aren't affected.
        configure_metrics(enabled=False)


class TestToolRegistryInstrumentation:
    async def test_records_ok_and_error(self):
        m = configure_metrics(enabled=True)
        reg = ToolRegistry()
        reg.register(_EchoTool())
        reg.register(_BoomTool())

        assert await reg.execute("echo", {}) == "ok"
        result = await reg.execute("boom", {})
        assert "Error executing tool" in result
        assert "Unknown tool" in await reg.execute("missing", {})

        payload = m.render().decode()
        assert 'tool="echo",status="ok"' in payload.replace(" ", "") or 'status="ok"' in payload
        assert 'status="error"' in payload
        assert 'status="unknown"' in payload
        configure_metrics(enabled=False)


@pytest.fixture(autouse=True)
def _reset_metrics():
    yield
    configure_metrics(enabled=False)
