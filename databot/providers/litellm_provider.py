"""LiteLLM provider implementation with retry, token tracking, and streaming."""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator

from loguru import logger

from databot.providers.base import (
    LLMProvider,
    LLMResponse,
    StreamChunk,
    TokenUsage,
    ToolCall,
)


class LiteLLMProvider(LLMProvider):
    """LLM provider using LiteLLM for multi-provider support.

    Includes automatic retry with exponential backoff for transient failures
    and token usage tracking.
    """

    def __init__(
        self,
        default_model: str = "anthropic/claude-sonnet-4-5-20250929",
        api_key: str | None = None,
        api_base: str | None = None,
        retry_attempts: int = 3,
        retry_delay: float = 1.0,
    ):
        self._default_model = default_model
        self._api_key = api_key
        self._api_base = api_base
        self._retry_attempts = max(1, retry_attempts)
        self._retry_delay = retry_delay
        # Cumulative token usage tracking
        self._total_usage = TokenUsage()

    async def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        model: str | None = None,
    ) -> LLMResponse:
        """Send a chat completion request via LiteLLM with retry."""
        import litellm

        model = model or self._default_model

        kwargs: dict[str, Any] = {
            "model": model,
            "messages": messages,
        }

        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"

        if self._api_key:
            kwargs["api_key"] = self._api_key

        if self._api_base:
            kwargs["api_base"] = self._api_base

        last_error: Exception | None = None
        for attempt in range(self._retry_attempts):
            try:
                response = await litellm.acompletion(**kwargs)
                return self._parse_response(response)
            except Exception as e:
                last_error = e
                # Don't retry on non-retryable errors
                error_str = str(e).lower()
                if any(
                    kw in error_str
                    for kw in ["invalid api key", "authentication", "invalid_api_key", "401"]
                ):
                    logger.error(f"LLM auth error (not retrying): {e}")
                    raise

                if attempt < self._retry_attempts - 1:
                    delay = self._retry_delay * (2**attempt)
                    logger.warning(
                        f"LLM call failed (attempt {attempt + 1}/{self._retry_attempts}), "
                        f"retrying in {delay:.1f}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"LLM call failed after {self._retry_attempts} attempts: {e}")

        raise last_error  # type: ignore[misc]

    def get_default_model(self) -> str:
        return self._default_model

    @property
    def total_usage(self) -> TokenUsage:
        """Get cumulative token usage across all calls."""
        return self._total_usage

    def _parse_response(self, response: Any) -> LLMResponse:
        """Parse LiteLLM response into our format."""
        choice = response.choices[0]
        message = choice.message

        content = message.content
        tool_calls = []

        if message.tool_calls:
            for tc in message.tool_calls:
                args = tc.function.arguments
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except json.JSONDecodeError:
                        args = {"raw": args}

                tool_calls.append(
                    ToolCall(
                        id=tc.id,
                        name=tc.function.name,
                        arguments=args,
                    )
                )

        # Extract token usage
        usage = None
        if hasattr(response, "usage") and response.usage:
            usage = TokenUsage(
                prompt_tokens=getattr(response.usage, "prompt_tokens", 0) or 0,
                completion_tokens=getattr(response.usage, "completion_tokens", 0) or 0,
                total_tokens=getattr(response.usage, "total_tokens", 0) or 0,
            )
            # Accumulate
            self._total_usage.prompt_tokens += usage.prompt_tokens
            self._total_usage.completion_tokens += usage.completion_tokens
            self._total_usage.total_tokens += usage.total_tokens

        return LLMResponse(content=content, tool_calls=tool_calls, usage=usage)

    async def chat_stream(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        model: str | None = None,
    ) -> AsyncIterator[StreamChunk]:
        """Native streaming via LiteLLM."""
        import litellm

        model = model or self._default_model

        kwargs: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "stream": True,
        }

        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"
        if self._api_key:
            kwargs["api_key"] = self._api_key
        if self._api_base:
            kwargs["api_base"] = self._api_base

        # Accumulate tool calls across chunks
        tool_call_accumulators: dict[int, dict[str, str]] = {}

        response = await litellm.acompletion(**kwargs)

        async for chunk in response:
            delta = chunk.choices[0].delta if chunk.choices else None
            finish_reason = chunk.choices[0].finish_reason if chunk.choices else None

            if delta is None:
                continue

            # Text delta
            if delta.content:
                yield StreamChunk(delta=delta.content)

            # Tool call deltas
            if delta.tool_calls:
                for tc_delta in delta.tool_calls:
                    idx = tc_delta.index if hasattr(tc_delta, "index") else 0
                    if idx not in tool_call_accumulators:
                        tool_call_accumulators[idx] = {
                            "id": "",
                            "name": "",
                            "arguments": "",
                        }
                    acc = tool_call_accumulators[idx]
                    if tc_delta.id:
                        acc["id"] = tc_delta.id
                    if tc_delta.function:
                        if tc_delta.function.name:
                            acc["name"] = tc_delta.function.name
                        if tc_delta.function.arguments:
                            acc["arguments"] += tc_delta.function.arguments

            # Emit finish
            if finish_reason:
                if finish_reason == "tool_calls":
                    for _idx, acc in sorted(tool_call_accumulators.items()):
                        yield StreamChunk(
                            is_tool_call=True,
                            tool_call_id=acc["id"],
                            tool_name=acc["name"],
                            tool_arguments_delta=acc["arguments"],
                            finish_reason="tool_calls",
                        )
                else:
                    # Extract usage from final chunk
                    usage = None
                    if hasattr(chunk, "usage") and chunk.usage:
                        usage = TokenUsage(
                            prompt_tokens=getattr(chunk.usage, "prompt_tokens", 0) or 0,
                            completion_tokens=getattr(chunk.usage, "completion_tokens", 0) or 0,
                            total_tokens=getattr(chunk.usage, "total_tokens", 0) or 0,
                        )
                        self._total_usage.prompt_tokens += usage.prompt_tokens
                        self._total_usage.completion_tokens += usage.completion_tokens
                        self._total_usage.total_tokens += usage.total_tokens

                    yield StreamChunk(finish_reason=finish_reason, usage=usage)
