"""Prometheus metrics with graceful fallback.

Mirrors the tracer's design: when ``prometheus_client`` is not installed every
method is a no-op, so the rest of the codebase can record metrics freely without
conditional imports. When it *is* installed, metrics are registered on a private
``CollectorRegistry`` (so repeated construction in tests never raises duplicate
registration errors) and exposed via :meth:`render` for a ``/metrics`` endpoint.

Metrics:
* ``databot_messages_total{channel}`` — messages processed
* ``databot_message_latency_seconds{channel}`` — end-to-end processing latency
* ``databot_tool_calls_total{tool,status}`` — tool invocations by outcome
* ``databot_tool_latency_seconds{tool}`` — tool execution latency
* ``databot_llm_calls_total`` — LLM completion calls
* ``databot_llm_tokens_total{type}`` — prompt/completion tokens
* ``databot_llm_cost_usd_total`` — estimated spend
* ``databot_errors_total{component}`` — handled errors
"""

from __future__ import annotations

from typing import Any

from loguru import logger

_CONTENT_TYPE_FALLBACK = "text/plain; version=0.0.4; charset=utf-8"


class Metrics:
    """Prometheus metric facade with a no-op fallback."""

    def __init__(self, enabled: bool = True, namespace: str = "databot"):
        self._enabled = False
        self._registry = None
        self._m: dict[str, Any] = {}
        self._content_type = _CONTENT_TYPE_FALLBACK
        if enabled:
            self._setup(namespace)

    def _setup(self, namespace: str) -> None:
        try:
            from prometheus_client import (
                CONTENT_TYPE_LATEST,
                CollectorRegistry,
                Counter,
                Histogram,
            )
        except ImportError:
            logger.debug("prometheus_client not installed — metrics disabled")
            return

        reg = CollectorRegistry()
        self._registry = reg
        self._content_type = CONTENT_TYPE_LATEST
        self._m = {
            "messages": Counter(
                "databot_messages_total",
                "Messages processed",
                ["channel"],
                registry=reg,
            ),
            "message_latency": Histogram(
                "databot_message_latency_seconds",
                "End-to-end message processing latency",
                ["channel"],
                registry=reg,
            ),
            "tool_calls": Counter(
                "databot_tool_calls_total",
                "Tool invocations by outcome",
                ["tool", "status"],
                registry=reg,
            ),
            "tool_latency": Histogram(
                "databot_tool_latency_seconds",
                "Tool execution latency",
                ["tool"],
                registry=reg,
            ),
            "llm_calls": Counter("databot_llm_calls_total", "LLM completion calls", registry=reg),
            "llm_tokens": Counter(
                "databot_llm_tokens_total",
                "LLM tokens consumed",
                ["type"],
                registry=reg,
            ),
            "llm_cost": Counter(
                "databot_llm_cost_usd_total",
                "Estimated LLM spend in USD",
                registry=reg,
            ),
            "errors": Counter(
                "databot_errors_total",
                "Handled errors by component",
                ["component"],
                registry=reg,
            ),
        }
        self._enabled = True
        logger.info("Prometheus metrics enabled")

    @property
    def enabled(self) -> bool:
        return self._enabled

    # -- recording helpers (all no-op when disabled) --------------------
    def message(self, channel: str, latency_seconds: float | None = None) -> None:
        if not self._enabled:
            return
        self._m["messages"].labels(channel=channel).inc()
        if latency_seconds is not None:
            self._m["message_latency"].labels(channel=channel).observe(latency_seconds)

    def tool_call(self, tool: str, status: str, latency_seconds: float | None = None) -> None:
        if not self._enabled:
            return
        self._m["tool_calls"].labels(tool=tool, status=status).inc()
        if latency_seconds is not None:
            self._m["tool_latency"].labels(tool=tool).observe(latency_seconds)

    def llm_call(self, prompt_tokens: int, completion_tokens: int, cost_usd: float) -> None:
        if not self._enabled:
            return
        self._m["llm_calls"].inc()
        self._m["llm_tokens"].labels(type="prompt").inc(prompt_tokens or 0)
        self._m["llm_tokens"].labels(type="completion").inc(completion_tokens or 0)
        if cost_usd:
            self._m["llm_cost"].inc(cost_usd)

    def error(self, component: str) -> None:
        if not self._enabled:
            return
        self._m["errors"].labels(component=component).inc()

    # -- exposition -----------------------------------------------------
    @property
    def content_type(self) -> str:
        return self._content_type

    def render(self) -> bytes:
        """Return the Prometheus exposition payload."""
        if not self._enabled or self._registry is None:
            return b"# databot metrics disabled (prometheus_client not installed)\n"
        from prometheus_client import generate_latest

        return generate_latest(self._registry)


# Module-level singleton so the tool registry and loop can record without
# threading the instance through every call site.
_global_metrics: Metrics | None = None


def get_metrics() -> Metrics:
    """Return the global metrics facade (disabled no-op until configured)."""
    global _global_metrics
    if _global_metrics is None:
        _global_metrics = Metrics(enabled=False)
    return _global_metrics


def configure_metrics(enabled: bool = True, namespace: str = "databot") -> Metrics:
    """(Re)configure and return the global metrics facade."""
    global _global_metrics
    _global_metrics = Metrics(enabled=enabled, namespace=namespace)
    return _global_metrics
