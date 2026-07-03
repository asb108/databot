"""Conversation compaction via LLM summarization.

Without compaction, long conversations are handled by simply dropping the oldest
messages once a cap is hit (``Session.add_message``), silently losing context.
The compactor instead distills the oldest turns into a compact running summary
and keeps the most recent turns verbatim, so older context is preserved in
condensed form rather than discarded.

The summary is carried separately from the user/assistant history (see
``Session.summary``) and injected as an extra system message at build time, so
message-role alternation is never broken.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from databot.core.tokens import estimate_tokens

_SUMMARY_INSTRUCTION = (
    "You are compacting a conversation between a user and 'databot', a data "
    "platform assistant. Summarize the messages below into a dense brief that "
    "preserves everything needed to continue the conversation: the user's goals, "
    "key facts and entities (tables, DAGs, connections, metrics), decisions, "
    "answers already given, and any unfinished tasks. Use terse bullet points. "
    "Do not invent details. If a previous summary is provided, merge it in."
)


class ConversationCompactor:
    """Summarizes old conversation turns when history grows too large."""

    def __init__(
        self,
        provider: Any,
        model: str | None = None,
        *,
        trigger_tokens: int = 6000,
        keep_recent_messages: int = 12,
        max_summary_tokens: int = 1024,
    ):
        self._provider = provider
        self._model = model
        self._trigger_tokens = trigger_tokens
        # Keep an even number so the retained window starts on a user turn.
        self._keep_recent = max(2, keep_recent_messages - (keep_recent_messages % 2))
        self._max_summary_tokens = max_summary_tokens

    def should_compact(self, history: list[dict[str, Any]], model: str = "") -> bool:
        if len(history) <= self._keep_recent:
            return False
        return estimate_tokens(history, model or self._model or "") > self._trigger_tokens

    async def compact(
        self,
        history: list[dict[str, Any]],
        previous_summary: str = "",
        model: str = "",
    ) -> tuple[list[dict[str, Any]], str] | None:
        """Compact ``history`` if it is over the trigger.

        Returns ``(recent_history, new_summary)`` when compaction happened, or
        ``None`` when nothing was done (history small, or summarization failed —
        in which case the caller keeps the original history unchanged).
        """
        if not self.should_compact(history, model):
            return None

        older = history[: -self._keep_recent]
        recent = history[-self._keep_recent :]
        if not older:
            return None

        try:
            summary = await self._summarize(older, previous_summary, model)
        except Exception as e:  # never let compaction break a turn
            logger.warning(f"Conversation compaction failed, keeping full history: {e}")
            return None

        if not summary:
            return None
        logger.info(f"Compacted {len(older)} old messages into a {len(summary)}-char summary")
        return recent, summary

    async def _summarize(
        self, messages: list[dict[str, Any]], previous_summary: str, model: str
    ) -> str:
        transcript_parts: list[str] = []
        if previous_summary:
            transcript_parts.append(f"## Previous summary\n{previous_summary}\n")
        transcript_parts.append("## Conversation to summarize")
        for msg in messages:
            role = msg.get("role", "?")
            content = msg.get("content", "")
            if isinstance(content, list):
                content = " ".join(p.get("text", "") for p in content if isinstance(p, dict))
            transcript_parts.append(f"{role}: {content}")
        transcript = "\n".join(transcript_parts)

        prompt_messages = [
            {"role": "system", "content": _SUMMARY_INSTRUCTION},
            {"role": "user", "content": transcript},
        ]
        response = await self._provider.chat(
            messages=prompt_messages,
            tools=None,
            model=model or self._model or self._provider.get_default_model(),
        )
        return (response.content or "").strip()
