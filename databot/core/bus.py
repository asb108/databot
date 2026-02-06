"""Message bus for inter-component communication."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine


@dataclass
class InboundMessage:
    """A message received from a channel."""

    channel: str
    sender_id: str
    chat_id: str
    content: str
    media: list[dict[str, Any]] = field(default_factory=list)

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


class MessageBus:
    """Async message bus using queues."""

    def __init__(self):
        self._inbound: asyncio.Queue[InboundMessage] = asyncio.Queue()
        self._outbound: asyncio.Queue[OutboundMessage] = asyncio.Queue()
        self._outbound_handlers: list[Callable] = []

    async def publish_inbound(self, msg: InboundMessage) -> None:
        await self._inbound.put(msg)

    async def consume_inbound(self) -> InboundMessage:
        return await self._inbound.get()

    async def publish_outbound(self, msg: OutboundMessage) -> None:
        await self._outbound.put(msg)
        for handler in self._outbound_handlers:
            try:
                await handler(msg)
            except Exception:
                pass

    def on_outbound(self, handler: Callable) -> None:
        """Register a handler for outbound messages."""
        self._outbound_handlers.append(handler)

    async def consume_outbound(self) -> OutboundMessage:
        return await self._outbound.get()
