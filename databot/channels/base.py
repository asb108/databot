"""Base channel interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

from databot.core.bus import MessageBus


class BaseChannel(ABC):
    """Abstract base class for messaging channels."""

    def __init__(self, bus: MessageBus):
        self.bus = bus

    @property
    @abstractmethod
    def name(self) -> str:
        """Channel name identifier."""
        ...

    @abstractmethod
    async def start(self) -> None:
        """Start the channel (connect, listen, etc.)."""
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Stop the channel."""
        ...

    @abstractmethod
    async def send(self, chat_id: str, content: str, thread_id: str | None = None) -> None:
        """Send a message to a specific chat."""
        ...
