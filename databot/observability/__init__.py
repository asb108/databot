"""Observability module — OpenTelemetry tracing for tool calls and LLM interactions."""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncIterator, Iterator

from loguru import logger


class _NoOpSpan:
    """No-op span when OpenTelemetry is not available."""

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def set_status(self, status: Any) -> None:
        pass

    def record_exception(self, exc: Exception) -> None:
        pass

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        pass

    def end(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class Tracer:
    """Wrapper around OpenTelemetry tracer with graceful fallback.

    When ``opentelemetry`` is not installed, all spans are no-ops so the
    rest of the codebase can instrument freely without conditional imports.
    """

    def __init__(self, service_name: str = "databot", endpoint: str = ""):
        self._service_name = service_name
        self._endpoint = endpoint
        self._tracer = None
        self._provider = None
        self._enabled = False
        self._setup()

    def _setup(self) -> None:
        try:
            from opentelemetry import trace
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import (
                BatchSpanProcessor,
                ConsoleSpanExporter,
            )

            resource = Resource.create({"service.name": self._service_name})
            self._provider = TracerProvider(resource=resource)

            if self._endpoint:
                try:
                    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                        OTLPSpanExporter,
                    )

                    exporter = OTLPSpanExporter(endpoint=self._endpoint)
                except ImportError:
                    logger.warning("OTLP exporter not available, falling back to console")
                    exporter = ConsoleSpanExporter()
            else:
                exporter = ConsoleSpanExporter()

            self._provider.add_span_processor(BatchSpanProcessor(exporter))
            trace.set_tracer_provider(self._provider)
            self._tracer = trace.get_tracer(self._service_name)
            self._enabled = True
            logger.info(f"OpenTelemetry tracing enabled for {self._service_name}")
        except ImportError:
            logger.debug("OpenTelemetry not installed — tracing disabled")
            self._enabled = False

    @property
    def enabled(self) -> bool:
        return self._enabled

    def start_span(self, name: str, attributes: dict[str, Any] | None = None) -> Any:
        """Start a new span. Returns a span (or no-op)."""
        if not self._enabled or not self._tracer:
            return _NoOpSpan()

        span = self._tracer.start_span(name)
        if attributes:
            for k, v in attributes.items():
                span.set_attribute(k, str(v) if not isinstance(v, (int, float, bool, str)) else v)
        return span

    @contextmanager
    def span(self, name: str, attributes: dict[str, Any] | None = None) -> Iterator[Any]:
        """Context manager for spans."""
        s = self.start_span(name, attributes)
        try:
            yield s
        except Exception as e:
            s.record_exception(e)
            raise
        finally:
            s.end()

    @asynccontextmanager
    async def async_span(
        self, name: str, attributes: dict[str, Any] | None = None
    ) -> AsyncIterator[Any]:
        """Async context manager for spans."""
        s = self.start_span(name, attributes)
        try:
            yield s
        except Exception as e:
            s.record_exception(e)
            raise
        finally:
            s.end()

    def shutdown(self) -> None:
        """Flush pending spans and shut down."""
        if self._provider:
            self._provider.shutdown()


# Module-level singleton — created lazily
_global_tracer: Tracer | None = None


def get_tracer(service_name: str = "databot", endpoint: str = "") -> Tracer:
    """Get or create the global tracer instance."""
    global _global_tracer
    if _global_tracer is None:
        _global_tracer = Tracer(service_name, endpoint)
    return _global_tracer


def trace_tool_call(tool_name: str, arguments: dict[str, Any]) -> Any:
    """Start a span for a tool call. Call ``.end()`` when done."""
    tracer = get_tracer()
    return tracer.start_span(
        f"tool.{tool_name}",
        attributes={
            "tool.name": tool_name,
            "tool.arguments": str(arguments)[:500],
        },
    )


def trace_llm_call(model: str, message_count: int) -> Any:
    """Start a span for an LLM call. Call ``.end()`` when done."""
    tracer = get_tracer()
    return tracer.start_span(
        "llm.chat",
        attributes={
            "llm.model": model,
            "llm.message_count": message_count,
        },
    )
