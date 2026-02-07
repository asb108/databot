"""Base LLM provider interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, AsyncIterator


@dataclass
class ToolCall:
    """A tool call from the LLM."""

    id: str
    name: str
    arguments: dict[str, Any]


@dataclass
class TokenUsage:
    """Token usage statistics from an LLM call."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


@dataclass
class LLMResponse:
    """Response from an LLM provider."""

    content: str | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)
    usage: TokenUsage | None = None

    @property
    def has_tool_calls(self) -> bool:
        return len(self.tool_calls) > 0


@dataclass
class StreamChunk:
    """A single chunk in a streaming response."""

    delta: str = ""
    is_tool_call: bool = False
    tool_call_id: str = ""
    tool_name: str = ""
    tool_arguments_delta: str = ""
    finish_reason: str | None = None
    usage: TokenUsage | None = None


class LLMProvider(ABC):
    """Abstract base class for LLM providers."""

    @abstractmethod
    async def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        model: str | None = None,
    ) -> LLMResponse:
        """Send a chat completion request."""
        ...

    async def chat_stream(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        model: str | None = None,
    ) -> AsyncIterator[StreamChunk]:
        """Send a streaming chat completion request.

        Default implementation falls back to non-streaming ``chat()``,
        yielding a single chunk with the full response.
        """
        response = await self.chat(messages, tools, model)
        if response.has_tool_calls:
            for tc in response.tool_calls:
                import json
                yield StreamChunk(
                    is_tool_call=True,
                    tool_call_id=tc.id,
                    tool_name=tc.name,
                    tool_arguments_delta=json.dumps(tc.arguments),
                    finish_reason="tool_calls",
                )
        else:
            yield StreamChunk(
                delta=response.content or "",
                finish_reason="stop",
                usage=response.usage,
            )

    @abstractmethod
    def get_default_model(self) -> str:
        """Get the default model name."""
        ...
