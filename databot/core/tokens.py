"""Token estimation and context-window budgeting.

The agent loop can accumulate a large message list (long conversation history,
big tool outputs, RAG context). Without a budget, a single turn can exceed the
model's context window and the LLM call fails outright. This module provides:

* :func:`estimate_tokens` — best-effort token count for a message list, using
  ``litellm.token_counter`` when available and a cheap character heuristic
  otherwise (so it works with no heavy dependencies installed).
* :func:`get_context_window` — the model's input-token capacity.
* :func:`fit_messages_to_budget` — trim the oldest messages so the list fits in
  a budget, while preserving a valid message sequence (never orphaning a tool
  result and never starting the retained body with a dangling assistant
  tool-call).
"""

from __future__ import annotations

import json
from typing import Any

# Per-model input-token capacity fallback (used when litellm metadata is absent).
# Conservative values; the real window is looked up from litellm when possible.
_CONTEXT_WINDOWS: dict[str, int] = {
    "claude-3-5-sonnet": 200_000,
    "claude-3-5-haiku": 200_000,
    "claude-sonnet-4": 200_000,
    "claude-opus-4": 200_000,
    "claude-haiku-4": 200_000,
    "gpt-4o": 128_000,
    "gpt-4o-mini": 128_000,
    "gpt-4-turbo": 128_000,
    "o1": 200_000,
    "o3": 200_000,
    "deepseek-chat": 64_000,
    "gemini-2.0-flash": 1_000_000,
    "gemini-1.5-pro": 2_000_000,
    "mistral-large": 128_000,
    "llama-3.3-70b": 128_000,
}
_DEFAULT_CONTEXT_WINDOW = 128_000

# Average characters per token for the heuristic fallback. ~4 is the common
# rule of thumb for English + code.
_CHARS_PER_TOKEN = 4


def _heuristic_tokens(messages: list[dict[str, Any]]) -> int:
    """Estimate tokens from serialized content length (no dependencies)."""
    total_chars = 0
    for msg in messages:
        content = msg.get("content")
        if isinstance(content, str):
            total_chars += len(content)
        elif isinstance(content, list):
            for part in content:
                if isinstance(part, dict):
                    total_chars += len(part.get("text", "")) or len(json.dumps(part))
                else:
                    total_chars += len(str(part))
        # Tool calls add tokens too.
        for tc in msg.get("tool_calls") or []:
            total_chars += len(json.dumps(tc))
        # A few tokens of per-message role/formatting overhead.
        total_chars += 12
    return max(1, total_chars // _CHARS_PER_TOKEN)


def estimate_tokens(messages: list[dict[str, Any]], model: str = "") -> int:
    """Estimate the number of tokens a message list will consume.

    Tries ``litellm.token_counter`` for an accurate, model-aware count and falls
    back to a character heuristic if litellm is unavailable or errors.
    """
    if not messages:
        return 0
    try:
        import litellm

        return int(litellm.token_counter(model=model or "gpt-4o", messages=messages))
    except Exception:
        return _heuristic_tokens(messages)


def get_context_window(model: str) -> int:
    """Return the model's input-token capacity (best effort)."""
    try:
        import litellm

        info = litellm.get_model_info(model)
        window = info.get("max_input_tokens") or info.get("max_tokens")
        if window:
            return int(window)
    except Exception:
        pass

    name = (model or "").split("/")[-1].lower()
    for key, window in _CONTEXT_WINDOWS.items():
        if name.startswith(key) or key in name:
            return window
    return _DEFAULT_CONTEXT_WINDOW


def _is_orphan_leading(msg: dict[str, Any]) -> bool:
    """A leading body message that would be invalid without its predecessor."""
    role = msg.get("role")
    if role == "tool":
        return True  # tool result with no preceding assistant tool_calls
    if role == "assistant" and msg.get("tool_calls"):
        return True  # assistant tool_calls whose tool results may be trimmed
    return False


def fit_messages_to_budget(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    min_recent: int = 2,
) -> tuple[list[dict[str, Any]], int]:
    """Trim oldest messages so the list fits within ``max_tokens``.

    Leading ``system`` messages are always preserved. The most recent messages
    are kept (at least ``min_recent`` when possible). Trimming never leaves an
    orphaned tool result or a dangling assistant tool-call at the start of the
    retained body.

    Returns ``(fitted_messages, dropped_count)``.
    """
    if not messages or max_tokens <= 0:
        return messages, 0

    head: list[dict[str, Any]] = []
    i = 0
    while i < len(messages) and messages[i].get("role") == "system":
        head.append(messages[i])
        i += 1
    body = list(messages[i:])

    dropped = 0
    # Drop oldest body messages until within budget (keep at least min_recent).
    while len(body) > min_recent and estimate_tokens(head + body, model) > max_tokens:
        body.pop(0)
        dropped += 1

    # Repair: the retained body must not begin with an orphaned tool/assistant.
    while body and _is_orphan_leading(body[0]):
        body.pop(0)
        dropped += 1

    return head + body, dropped
