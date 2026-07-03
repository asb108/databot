"""Anthropic prompt-caching helpers.

Anthropic supports prompt caching: marking a stable prefix of the request with
``cache_control`` lets the API reuse it across turns, cutting cost (cached input
tokens are ~10% the price) and latency. The stable prefix in databot is the
system prompt and the tool definitions, which are identical on every turn.

This module rewrites those parts to carry an ``ephemeral`` cache breakpoint.
LiteLLM forwards ``cache_control`` to the Anthropic API. The transform is a
no-op for non-Anthropic models, so it is always safe to call.
"""

from __future__ import annotations

import copy
from typing import Any

_EPHEMERAL = {"type": "ephemeral"}


def is_anthropic_model(model: str) -> bool:
    name = (model or "").lower()
    return "claude" in name or "anthropic" in name


def apply_anthropic_cache_control(
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]] | None]:
    """Return copies of ``messages``/``tools`` with cache breakpoints set.

    Anthropic allows at most 4 cache breakpoints. We use up to two: one at the
    end of the tool list and one on the system prompt — together these cover the
    large, unchanging request prefix.
    """
    if not is_anthropic_model(model):
        return messages, tools

    new_messages = copy.deepcopy(messages)
    for msg in new_messages:
        if msg.get("role") != "system":
            continue
        content = msg.get("content")
        if isinstance(content, str):
            msg["content"] = [{"type": "text", "text": content, "cache_control": _EPHEMERAL}]
        elif isinstance(content, list) and content:
            # Mark the last text block as the cache breakpoint.
            for part in reversed(content):
                if isinstance(part, dict) and part.get("type") == "text":
                    part["cache_control"] = _EPHEMERAL
                    break
        break  # only the first system message

    new_tools = tools
    if tools:
        new_tools = copy.deepcopy(tools)
        new_tools[-1]["cache_control"] = _EPHEMERAL

    return new_messages, new_tools
