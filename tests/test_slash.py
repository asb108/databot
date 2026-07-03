"""Tests for slash commands."""

from __future__ import annotations

from databot.core.cost import UsageTracker
from databot.core.slash import SlashCommandHandler
from databot.session.manager import SessionManager
from databot.skills import SkillRegistry
from databot.tools.base import BaseTool, ToolRegistry


class _Tool(BaseTool):
    @property
    def name(self):
        return "demo"

    @property
    def description(self):
        return "demo"

    def parameters(self):
        return {"type": "object", "properties": {}}

    async def execute(self, **kwargs):
        return "ok"


class TestSlashHandler:
    def _handler(self, tmp_path):
        tools = ToolRegistry()
        tools.register(_Tool())
        return SlashCommandHandler(
            sessions=SessionManager(tmp_path),
            tools=tools,
            usage_tracker=UsageTracker(),
            skill_registry=SkillRegistry.from_config(["filesystem"]),
        )

    def test_is_command(self, tmp_path):
        h = self._handler(tmp_path)
        assert h.is_command("/help")
        assert h.is_command("/usage")
        assert not h.is_command("hello")
        assert not h.is_command("/unknowncmd")  # unknown -> not a command
        assert not h.is_command("/path/to/file")  # not hijacked

    async def test_non_command_returns_none(self, tmp_path):
        h = self._handler(tmp_path)
        assert await h.handle("what is the row count?") is None
        # An unknown slash is left for the model.
        assert await h.handle("/notacommand") is None

    async def test_help(self, tmp_path):
        out = await self._handler(tmp_path).handle("/help")
        assert "/clear" in out and "/usage" in out

    async def test_tools(self, tmp_path):
        out = await self._handler(tmp_path).handle("/tools")
        assert "demo" in out

    async def test_skills(self, tmp_path):
        out = await self._handler(tmp_path).handle("/skills")
        assert "filesystem" in out

    async def test_usage(self, tmp_path):
        h = self._handler(tmp_path)
        h._usage.record("gpt-4o", 100, 50, session_key="s1")
        out = await h.handle("/usage", session_key="s1")
        assert "150" in out  # total tokens
        assert "$" in out

    async def test_clear(self, tmp_path):
        h = self._handler(tmp_path)
        session = h._sessions.get_or_create("s1")
        session.add_message("user", "hi")
        h._sessions.save(session)
        out = await h.handle("/clear", session_key="s1")
        assert "cleared" in out.lower()
        assert h._sessions.get_or_create("s1").get_history() == []

    async def test_status_no_connectors(self, tmp_path):
        out = await self._handler(tmp_path).handle("/status")
        assert "No connectors" in out


class TestLoopSlashIntegration:
    async def test_loop_handles_slash_without_llm(self, tmp_path):
        from unittest.mock import AsyncMock, MagicMock

        from databot.core.bus import InboundMessage, MessageBus
        from databot.core.loop import AgentLoop
        from databot.memory.manager import MemoryManager

        provider = MagicMock()
        provider.get_default_model.return_value = "gpt-4o"
        provider.chat = AsyncMock()

        sessions = SessionManager(tmp_path)
        tools = ToolRegistry()
        tools.register(_Tool())
        loop = AgentLoop(
            bus=MessageBus(),
            provider=provider,
            tools=tools,
            workspace=tmp_path,
            sessions=sessions,
            memory=MemoryManager(tmp_path / "m.db"),
            slash_handler=SlashCommandHandler(sessions=sessions, tools=tools),
        )
        msg = InboundMessage(channel="cli", sender_id="u", chat_id="c", content="/tools")
        resp = await loop.process_message(msg)
        assert "demo" in resp.content
        provider.chat.assert_not_called()  # no LLM round-trip
