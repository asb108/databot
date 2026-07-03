"""Slash commands — fast, deterministic chat operations with no LLM round-trip.

Messages that begin with a recognized ``/command`` are handled directly (clear
the session, show usage/cost, list tools/skills, check connector status) instead
of being sent to the model. This works across every channel because it runs at
the agent-loop chokepoint that all inbound messages pass through. A leading "/"
that isn't a known command is left untouched so normal messages (and file paths)
still reach the model.
"""

from __future__ import annotations

from typing import Any

_HELP = {
    "help": "Show this help",
    "clear": "Clear this conversation's history",
    "usage": "Show this session's token usage and cost",
    "tools": "List available tools",
    "skills": "List skills and whether they're enabled",
    "status": "Show connector health",
}


class SlashCommandHandler:
    """Dispatches ``/command`` messages to deterministic handlers."""

    def __init__(
        self,
        sessions: Any | None = None,
        tools: Any | None = None,
        usage_tracker: Any | None = None,
        skill_registry: Any | None = None,
        connector_registry: Any | None = None,
    ):
        self._sessions = sessions
        self._tools = tools
        self._usage = usage_tracker
        self._skills = skill_registry
        self._connectors = connector_registry

    def is_command(self, content: str) -> bool:
        text = (content or "").strip()
        if not text.startswith("/"):
            return False
        cmd = text[1:].split(maxsplit=1)[0].lower() if len(text) > 1 else ""
        return cmd in _HELP

    async def handle(self, content: str, session_key: str = "") -> str | None:
        """Return the command's response, or None if not a slash command."""
        if not self.is_command(content):
            return None
        cmd = content.strip()[1:].split(maxsplit=1)[0].lower()
        if cmd == "help":
            return self._help()
        if cmd == "clear":
            return self._clear(session_key)
        if cmd == "usage":
            return self._usage_report(session_key)
        if cmd == "tools":
            return self._tools_report()
        if cmd == "skills":
            return self._skills_report()
        if cmd == "status":
            return await self._status_report()
        return None

    def _help(self) -> str:
        lines = ["**Available commands:**"]
        lines.extend(f"- `/{name}` — {desc}" for name, desc in _HELP.items())
        return "\n".join(lines)

    def _clear(self, session_key: str) -> str:
        if not self._sessions or not session_key:
            return "No active session to clear."
        session = self._sessions.get_or_create(session_key)
        session.clear()
        self._sessions.save(session)
        return "Conversation history cleared."

    def _usage_report(self, session_key: str) -> str:
        if not self._usage or not session_key:
            return "Usage tracking is not available."
        stats = self._usage.session_stats(session_key)
        budget = self._usage.session_budget_usd
        budget_line = f" / ${budget:.2f} budget" if budget else ""
        return (
            f"**Session usage:** {stats['total_tokens']:,} tokens across "
            f"{stats['calls']} call(s), ${stats['cost_usd']:.4f}{budget_line}."
        )

    def _tools_report(self) -> str:
        if not self._tools:
            return "No tools available."
        names = sorted(self._tools.tool_names)
        return f"**{len(names)} tools:** " + ", ".join(f"`{n}`" for n in names)

    def _skills_report(self) -> str:
        if not self._skills:
            return "Skills are not available."
        lines = ["**Skills:**"]
        for s in self._skills.summary():
            mark = "✅" if s.get("enabled") else "⬜"
            lines.append(f"{mark} `{s['name']}` — {s.get('description', '')}")
        return "\n".join(lines)

    async def _status_report(self) -> str:
        if not self._connectors:
            return "No connectors configured."
        try:
            health = await self._connectors.health_check_all()
        except Exception as e:
            return f"Could not check connector health: {e}"
        if not health:
            return "No connectors configured."
        lines = ["**Connector status:**"]
        lines.extend(f"- `{name}`: {status.value}" for name, status in health.items())
        return "\n".join(lines)
