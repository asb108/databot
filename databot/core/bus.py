"""Message bus for inter-component communication."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable

from loguru import logger


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
    """Async message bus using queues."""

    def __init__(self):
        self._inbound: asyncio.Queue[InboundMessage] = asyncio.Queue()
        self._outbound: asyncio.Queue[OutboundMessage] = asyncio.Queue()
        self._outbound_handlers: list[Callable] = []
        self._stream_handlers: list[Callable] = []

    async def publish_inbound(self, msg: InboundMessage) -> None:
        await self._inbound.put(msg)

    async def consume_inbound(self) -> InboundMessage:
        return await self._inbound.get()

    async def publish_outbound(self, msg: OutboundMessage) -> None:
        await self._outbound.put(msg)
        for handler in self._outbound_handlers:
            try:
                await handler(msg)
            except Exception as e:
                logger.error(f"Outbound handler {handler.__qualname__} failed: {e}")

    def on_outbound(self, handler: Callable) -> None:
        """Register a handler for outbound messages."""
        self._outbound_handlers.append(handler)

    async def consume_outbound(self) -> OutboundMessage:
        return await self._outbound.get()

    async def publish_stream_event(self, event: StreamEvent) -> None:
        """Publish a streaming event to all stream handlers."""
        for handler in self._stream_handlers:
            try:
                await handler(event)
            except Exception as e:
                logger.error(f"Stream handler {handler.__qualname__} failed: {e}")

    def on_stream(self, handler: Callable) -> None:
        """Register a handler for stream events."""
        self._stream_handlers.append(handler)
