"""Context builder for LLM conversations."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databot.memory.manager import MemoryManager

DEFAULT_SYSTEM_PROMPT = """\
You are databot, an AI assistant for data platform operations. You help data \
engineers monitor pipelines, diagnose data quality issues, query databases, \
and manage infrastructure.

You have access to tools for:
- Executing SQL queries against configured databases
- Checking Airflow DAG and task status
- Running data quality checks
- Querying data lineage (upstream/downstream dependencies)
- Executing shell commands
- Reading and writing files
- Searching the web

When using tools:
- Always prefer read-only operations unless explicitly asked to modify something
- Format query results as readable tables
- Explain your reasoning before and after tool use
- If a query might be expensive, warn the user first

Be concise, technical, and helpful. Use markdown formatting in your responses.\
"""


class ContextBuilder:
    """Builds LLM message context from history, memory, and current message."""

    def __init__(
        self,
        workspace: Path,
        memory: MemoryManager | None = None,
        system_prompt: str = "",
    ):
        self.workspace = workspace
        self.memory = memory
        self.system_prompt = system_prompt or DEFAULT_SYSTEM_PROMPT

    def build_messages(
        self,
        history: list[dict[str, Any]],
        current_message: str,
        channel: str = "cli",
        chat_id: str = "direct",
        media: list[dict] | None = None,
    ) -> list[dict[str, Any]]:
        """Build the full message list for the LLM."""
        messages: list[dict[str, Any]] = []

        # System prompt with memory context
        system = self._build_system_prompt()
        messages.append({"role": "system", "content": system})

        # Conversation history
        for msg in history:
            messages.append(msg)

        # Current user message
        if media:
            parts: list[dict[str, Any]] = [{"type": "text", "text": current_message}]
            for m in media:
                parts.append(m)
            messages.append({"role": "user", "content": parts})
        else:
            messages.append({"role": "user", "content": current_message})

        return messages

    def _build_system_prompt(self) -> str:
        """Build system prompt with memory context."""
        parts = [self.system_prompt]

        if self.memory:
            memories = self.memory.get_all()
            if memories:
                parts.append("\n## Persistent Memory")
                for key, value in memories.items():
                    parts.append(f"- {key}: {value}")

        parts.append(f"\nWorkspace: {self.workspace}")
        return "\n".join(parts)

    @staticmethod
    def add_assistant_message(
        messages: list[dict],
        content: str | None,
        tool_calls: list[dict] | None = None,
    ) -> list[dict]:
        """Add an assistant message (possibly with tool calls) to the list."""
        msg: dict[str, Any] = {"role": "assistant"}
        if content:
            msg["content"] = content
        if tool_calls:
            msg["tool_calls"] = tool_calls
        messages.append(msg)
        return messages

    @staticmethod
    def add_tool_result(
        messages: list[dict],
        tool_call_id: str,
        name: str,
        result: str,
    ) -> list[dict]:
        """Add a tool result to the message list."""
        messages.append(
            {
                "role": "tool",
                "tool_call_id": tool_call_id,
                "name": name,
                "content": result,
            }
        )
        return messages
