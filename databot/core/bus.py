"""Message bus for inter-component communication."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable

from loguru import logger

# Sensible default limits — prevent unbounded memory growth under load.
DEFAULT_MAX_QUEUE_SIZE: int = 1000


@dataclass
class InboundMessage:
    """A message received from a channel."""

    channel: str
    sender_id: str
    chat_id: str
    content: str
    media: list[dict[str, Any]] = field(default_factory=list)
    stream: bool = False  # request streaming response

    @property
    def session_key(self) -> str:
        return f"{self.channel}:{self.chat_id}"


@dataclass
class OutboundMessage:
    """A message to send to a channel."""

    channel: str
    chat_id: str
    content: str
    thread_id: str | None = None


@dataclass
class StreamEvent:
    """A streaming event for SSE / websocket delivery."""

    channel: str
    chat_id: str
    event_type: str  # "delta" | "tool_start" | "tool_result" | "done" | "error"
    data: str = ""
    tool_name: str = ""


class MessageBus:
    """Async message bus with bounded queues and parallel handler dispatch."""

    def __init__(self, max_queue_size: int = DEFAULT_MAX_QUEUE_SIZE):
        self._inbound: asyncio.Queue[InboundMessage] = asyncio.Queue(
            maxsize=max_queue_size,
        )
        self._outbound: asyncio.Queue[OutboundMessage] = asyncio.Queue(
            maxsize=max_queue_size,
        )
        self._outbound_handlers: list[Callable] = []
        self._stream_handlers: list[Callable] = []

    # ------------------------------------------------------------------
    # Queue stats
    # ------------------------------------------------------------------

    @property
    def inbound_size(self) -> int:
        return self._inbound.qsize()

    @property
    def outbound_size(self) -> int:
        return self._outbound.qsize()

    # ------------------------------------------------------------------
    # Inbound
    # ------------------------------------------------------------------

    async def publish_inbound(self, msg: InboundMessage) -> None:
        try:
            self._inbound.put_nowait(msg)
        except asyncio.QueueFull:
            logger.warning("Inbound queue full — dropping oldest message")
            # Drop oldest to make room (back-pressure strategy)
            try:
                self._inbound.get_nowait()
            except asyncio.QueueEmpty:
                pass
            await self._inbound.put(msg)

    async def consume_inbound(self) -> InboundMessage:
        return await self._inbound.get()

    # ------------------------------------------------------------------
    # Outbound — handlers run in parallel via asyncio.gather
    # ------------------------------------------------------------------

    async def publish_outbound(self, msg: OutboundMessage) -> None:
        try:
            self._outbound.put_nowait(msg)
        except asyncio.QueueFull:
            logger.warning("Outbound queue full — awaiting space")
            await self._outbound.put(msg)

        if self._outbound_handlers:
            results = await asyncio.gather(
                *(handler(msg) for handler in self._outbound_handlers),
                return_exceptions=True,
            )
            for handler, result in zip(self._outbound_handlers, results):
                if isinstance(result, Exception):
                    logger.error(f"Outbound handler {handler.__qualname__} failed: {result}")

    def on_outbound(self, handler: Callable) -> None:
        """Register a handler for outbound messages."""
        self._outbound_handlers.append(handler)

    async def consume_outbound(self) -> OutboundMessage:
        return await self._outbound.get()

    # ------------------------------------------------------------------
    # Streaming — handlers run in parallel
    # ------------------------------------------------------------------

    async def publish_stream_event(self, event: StreamEvent) -> None:
        """Publish a streaming event to all stream handlers in parallel."""
        if not self._stream_handlers:
            return
        results = await asyncio.gather(
            *(handler(event) for handler in self._stream_handlers),
            return_exceptions=True,
        )
        for handler, result in zip(self._stream_handlers, results):
            if isinstance(result, Exception):
                logger.error(f"Stream handler {handler.__qualname__} failed: {result}")

    def on_stream(self, handler: Callable) -> None:
        """Register a handler for stream events."""
        self._stream_handlers.append(handler)
