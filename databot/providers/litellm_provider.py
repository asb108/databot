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
        enable_prompt_caching: bool = True,
    ):
        self._default_model = default_model
        self._api_key = api_key
        self._api_base = api_base
        self._retry_attempts = max(1, retry_attempts)
        self._retry_delay = retry_delay
        self._enable_prompt_caching = enable_prompt_caching
        # Cumulative token usage tracking
        self._total_usage = TokenUsage()

    def _prepare_request(
        self,
        model: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]] | None]:
        """Apply provider-side request transforms (prompt caching)."""
        if self._enable_prompt_caching:
            from databot.providers.caching import apply_anthropic_cache_control

            return apply_anthropic_cache_control(model, messages, tools)
        return messages, tools

    def _build_kwargs(
        self,
        model: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        stream: bool = False,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"model": model, "messages": messages}
        if stream:
            kwargs["stream"] = True
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"
        if self._api_key:
            kwargs["api_key"] = self._api_key
        if self._api_base:
            kwargs["api_base"] = self._api_base
        return kwargs

    @staticmethod
    def _is_auth_error(e: Exception) -> bool:
        error_str = str(e).lower()
        return any(
            kw in error_str
            for kw in ["invalid api key", "authentication", "invalid_api_key", "401"]
        )

    async def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        model: str | None = None,
    ) -> LLMResponse:
        """Send a chat completion request via LiteLLM with retry.

        Prompt caching is an optimization, not correctness: if a cached request
        fails (e.g. a provider/litellm version rejects the ``cache_control``
        shape) we fall back to an uncached request rather than failing the turn.
        """
        import litellm

        model = model or self._default_model

        # Optimistic single attempt with prompt caching; on any non-auth error,
        # fall through to the uncached retry loop so caching can never break the
        # core path.
        cached_messages, cached_tools = self._prepare_request(model, messages, tools)
        caching_applied = cached_messages is not messages or cached_tools is not tools
        if caching_applied:
            try:
                kwargs = self._build_kwargs(model, cached_messages, cached_tools)
                return self._parse_response(await litellm.acompletion(**kwargs))
            except Exception as e:
                if self._is_auth_error(e):
                    logger.error(f"LLM auth error (not retrying): {e}")
                    raise
                logger.warning(f"Prompt-cached request failed; retrying without caching: {e}")

        last_error: Exception | None = None
        for attempt in range(self._retry_attempts):
            try:
                kwargs = self._build_kwargs(model, messages, tools)
                response = await litellm.acompletion(**kwargs)
                return self._parse_response(response)
            except Exception as e:
                last_error = e
                if self._is_auth_error(e):
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

        # Open the stream with prompt caching; if that fails (and it isn't an
        # auth error) reopen without caching. The error surfaces here, before
        # any chunk is yielded, so the fallback is safe.
        cached_messages, cached_tools = self._prepare_request(model, messages, tools)
        caching_applied = cached_messages is not messages or cached_tools is not tools
        response = None
        if caching_applied:
            try:
                response = await litellm.acompletion(
                    **self._build_kwargs(model, cached_messages, cached_tools, stream=True)
                )
            except Exception as e:
                if self._is_auth_error(e):
                    raise
                logger.warning(f"Prompt-cached stream failed; retrying without caching: {e}")
        if response is None:
            response = await litellm.acompletion(
                **self._build_kwargs(model, messages, tools, stream=True)
            )

        # Accumulate tool calls across chunks
        tool_call_accumulators: dict[int, dict[str, str]] = {}

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
