"""LiteLLM provider implementation."""

from __future__ import annotations

import json
from typing import Any

from loguru import logger

from databot.providers.base import LLMProvider, LLMResponse, ToolCall


class LiteLLMProvider(LLMProvider):
    """LLM provider using LiteLLM for multi-provider support."""

    def __init__(
        self,
        default_model: str = "anthropic/claude-sonnet-4-5-20250929",
        api_key: str | None = None,
        api_base: str | None = None,
    ):
        self._default_model = default_model
        self._api_key = api_key
        self._api_base = api_base

    async def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        model: str | None = None,
    ) -> LLMResponse:
        """Send a chat completion request via LiteLLM."""
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

        try:
            response = await litellm.acompletion(**kwargs)
            return self._parse_response(response)
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            raise

    def get_default_model(self) -> str:
        return self._default_model

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

        return LLMResponse(content=content, tool_calls=tool_calls)
