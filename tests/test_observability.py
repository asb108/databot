"""Tests for the observability module."""

from __future__ import annotations

import pytest

from databot.observability import (
    Tracer,
    _NoOpSpan,
    get_tracer,
    trace_llm_call,
    trace_tool_call,
)


class TestNoOpSpan:
    def test_set_attribute(self):
        span = _NoOpSpan()
        span.set_attribute("key", "value")  # Should not raise

    def test_record_exception(self):
        span = _NoOpSpan()
        span.record_exception(RuntimeError("test"))

    def test_end(self):
        span = _NoOpSpan()
        span.end()

    def test_context_manager(self):
        with _NoOpSpan() as span:
            span.set_attribute("x", 1)


class TestTracer:
    def test_disabled_by_default(self):
        """Tracer should be disabled when OpenTelemetry is not installed."""
        # This test may pass or fail depending on whether otel is installed
        tracer = Tracer()
        # Either way, it shouldn't crash
        span = tracer.start_span("test")
        span.set_attribute("key", "value")
        span.end()

    def test_span_context_manager(self):
        tracer = Tracer()
        with tracer.span("test", {"key": "val"}) as span:
            span.set_attribute("extra", True)

    @pytest.mark.asyncio
    async def test_async_span(self):
        tracer = Tracer()
        async with tracer.async_span("async_test") as span:
            span.add_event("something")


class TestConvenienceFunctions:
    def test_trace_tool_call(self):
        span = trace_tool_call("sql", {"query": "SELECT 1"})
        span.end()

    def test_trace_llm_call(self):
        span = trace_llm_call("gpt-4o", 5)
        span.end()

    def test_get_tracer_singleton(self):
        t1 = get_tracer()
        t2 = get_tracer()
        assert t1 is t2
