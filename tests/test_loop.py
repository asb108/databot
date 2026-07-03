from __future__ import annotations

"""Tests for the core agent loop."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from databot.core.bus import InboundMessage, MessageBus
from databot.core.loop import AgentLoop
from databot.memory.manager import MemoryManager
from databot.providers.base import LLMResponse, ToolCall
from databot.session.manager import SessionManager
from databot.tools.base import ToolRegistry


@pytest.fixture
def tmp_data_dir(tmp_path):
    return tmp_path


@pytest.fixture
def mock_provider():
    provider = MagicMock()
    provider.get_default_model.return_value = "test-model"
    provider.chat = AsyncMock(
        return_value=LLMResponse(content="Hello! I'm databot.", tool_calls=[])
    )
    return provider


@pytest.fixture
def agent_loop(tmp_data_dir, mock_provider):
    bus = MessageBus()
    tools = ToolRegistry()
    sessions = SessionManager(tmp_data_dir)
    memory = MemoryManager(tmp_data_dir / "memory.db")

    return AgentLoop(
        bus=bus,
        provider=mock_provider,
        tools=tools,
        workspace=tmp_data_dir,
        sessions=sessions,
        memory=memory,
    )


class TestAgentLoop:
    @pytest.mark.asyncio
    async def test_process_direct(self, agent_loop):
        result = await agent_loop.process_direct("Hello")
        assert "databot" in result

    @pytest.mark.asyncio
    async def test_process_message(self, agent_loop):
        msg = InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="test",
            content="What is 2+2?",
        )
        response = await agent_loop.process_message(msg)
        assert response is not None
        assert response.channel == "cli"
        assert response.chat_id == "test"

    @pytest.mark.asyncio
    async def test_tool_execution(self, agent_loop, mock_provider):
        # First call returns a tool call, second call returns text
        mock_provider.chat = AsyncMock(
            side_effect=[
                LLMResponse(
                    content=None,
                    tool_calls=[
                        ToolCall(
                            id="call_1",
                            name="nonexistent_tool",
                            arguments={"arg": "value"},
                        )
                    ],
                ),
                LLMResponse(content="Done!", tool_calls=[]),
            ]
        )

        result = await agent_loop.process_direct("Do something")
        assert "Done!" in result
        assert mock_provider.chat.call_count == 2

    @pytest.mark.asyncio
    async def test_session_persistence(self, agent_loop, tmp_data_dir):
        # First message
        await agent_loop.process_direct("Hello")
        # Check session was saved
        sessions = agent_loop.sessions.list_sessions()
        assert len(sessions) > 0


class TestContextCostIntegration:
    @pytest.fixture
    def loop_with_usage(self, tmp_data_dir, mock_provider):
        from databot.core.cost import UsageTracker
        from databot.providers.base import LLMResponse, TokenUsage

        mock_provider.chat = AsyncMock(
            return_value=LLMResponse(
                content="hi",
                tool_calls=[],
                usage=TokenUsage(prompt_tokens=100, completion_tokens=20, total_tokens=120),
            )
        )
        tracker = UsageTracker(session_budget_usd=0.0)
        loop = AgentLoop(
            bus=MessageBus(),
            provider=mock_provider,
            tools=ToolRegistry(),
            workspace=tmp_data_dir,
            sessions=SessionManager(tmp_data_dir),
            memory=MemoryManager(tmp_data_dir / "memory.db"),
            usage_tracker=tracker,
        )
        return loop, tracker

    @pytest.mark.asyncio
    async def test_records_usage(self, loop_with_usage):
        loop, tracker = loop_with_usage
        msg = InboundMessage(channel="cli", sender_id="u", chat_id="c", content="hi")
        await loop.process_message(msg)
        stats = tracker.session_stats(msg.session_key)
        assert stats["prompt_tokens"] == 100
        assert stats["completion_tokens"] == 20
        assert stats["calls"] == 1

    @pytest.mark.asyncio
    async def test_budget_blocks_before_llm(self, tmp_data_dir, mock_provider):
        from databot.core.cost import UsageTracker

        tracker = UsageTracker(session_budget_usd=0.01)
        tracker.record("gpt-4o", 1_000_000, 0, session_key="cli:direct")  # over budget
        loop = AgentLoop(
            bus=MessageBus(),
            provider=mock_provider,
            tools=ToolRegistry(),
            workspace=tmp_data_dir,
            sessions=SessionManager(tmp_data_dir),
            memory=MemoryManager(tmp_data_dir / "memory.db"),
            usage_tracker=tracker,
        )
        msg = InboundMessage(channel="cli", sender_id="u", chat_id="direct", content="hi")
        response = await loop.process_message(msg)
        assert "budget" in response.content.lower()
        mock_provider.chat.assert_not_called()

    @pytest.mark.asyncio
    async def test_stream_budget_blocks_before_llm(self, tmp_data_dir, mock_provider):
        from databot.core.cost import UsageTracker

        tracker = UsageTracker(session_budget_usd=0.01)
        tracker.record("gpt-4o", 1_000_000, 0, session_key="cli:stream")
        mock_provider.chat_stream = MagicMock()
        loop = AgentLoop(
            bus=MessageBus(),
            provider=mock_provider,
            tools=ToolRegistry(),
            workspace=tmp_data_dir,
            sessions=SessionManager(tmp_data_dir),
            memory=MemoryManager(tmp_data_dir / "memory.db"),
            usage_tracker=tracker,
        )
        msg = InboundMessage(
            channel="cli", sender_id="u", chat_id="stream", content="hi"
        )
        events = [e async for e in loop.process_message_stream(msg)]
        assert any("budget" in (e.data or "").lower() for e in events)
        mock_provider.chat_stream.assert_not_called()

    @pytest.mark.asyncio
    async def test_compaction_invoked(self, tmp_data_dir, mock_provider):
        from databot.core.compaction import ConversationCompactor

        compactor = ConversationCompactor(
            mock_provider, trigger_tokens=10, keep_recent_messages=2
        )
        sessions = SessionManager(tmp_data_dir)
        loop = AgentLoop(
            bus=MessageBus(),
            provider=mock_provider,
            tools=ToolRegistry(),
            workspace=tmp_data_dir,
            sessions=sessions,
            memory=MemoryManager(tmp_data_dir / "memory.db"),
            compactor=compactor,
        )
        # Seed a long history so compaction triggers.
        session = sessions.get_or_create("cli:direct")
        for i in range(20):
            session.add_message("user" if i % 2 == 0 else "assistant", f"old turn {i} " * 20)
        sessions.save(session)

        await loop.process_direct("new question")
        # The session now carries a compaction summary.
        reloaded = sessions.get_or_create("cli:direct")
        assert reloaded.summary != ""


class TestSessionSummaryPersistence:
    def test_summary_round_trip(self, tmp_path):
        sessions = SessionManager(tmp_path)
        s = sessions.get_or_create("k")
        s.add_message("user", "hello")
        s.add_message("assistant", "hi there")
        s.set_summary("- user greeted the bot")
        sessions.save(s)

        # Drop from cache to force a reload from disk.
        sessions._cache.clear()
        reloaded = sessions.get_or_create("k")
        assert reloaded.summary == "- user greeted the bot"
        # History excludes the summary (no stray system message).
        history = reloaded.get_history()
        assert all(m["role"] != "system" for m in history)
        assert len(history) == 2


class TestMessageBus:
    @pytest.mark.asyncio
    async def test_publish_consume(self):
        bus = MessageBus()
        msg = InboundMessage(channel="cli", sender_id="user", chat_id="test", content="hello")
        await bus.publish_inbound(msg)
        received = await bus.consume_inbound()
        assert received.content == "hello"

    @pytest.mark.asyncio
    async def test_outbound_handler(self):
        bus = MessageBus()
        received = []

        async def handler(msg):
            received.append(msg)

        bus.on_outbound(handler)
        from databot.core.bus import OutboundMessage

        await bus.publish_outbound(
            OutboundMessage(channel="cli", chat_id="test", content="response")
        )
        assert len(received) == 1
        assert received[0].content == "response"

    @pytest.mark.asyncio
    async def test_bounded_queue(self):
        """Queue rejects messages beyond maxsize (backpressure)."""
        bus = MessageBus(max_queue_size=3)
        for i in range(5):
            msg = InboundMessage(channel="cli", sender_id="u", chat_id="t", content=f"m{i}")
            await bus.publish_inbound(msg)
        # Queue should not exceed maxsize
        assert bus.inbound_size <= 3

    @pytest.mark.asyncio
    async def test_parallel_outbound_handlers(self):
        """Multiple outbound handlers execute concurrently."""
        bus = MessageBus()
        call_order = []

        async def slow_handler(msg):
            await asyncio.sleep(0.1)
            call_order.append("slow")

        async def fast_handler(msg):
            call_order.append("fast")

        bus.on_outbound(slow_handler)
        bus.on_outbound(fast_handler)

        from databot.core.bus import OutboundMessage

        await bus.publish_outbound(OutboundMessage(channel="cli", chat_id="t", content="hi"))
        assert len(call_order) == 2
        # Both handlers were called (order may vary due to parallelism)
        assert "slow" in call_order
        assert "fast" in call_order

    @pytest.mark.asyncio
    async def test_handler_error_isolation(self):
        """A failing handler doesn't block others."""
        bus = MessageBus()
        results = []

        async def bad_handler(msg):
            raise RuntimeError("boom")

        async def good_handler(msg):
            results.append("ok")

        bus.on_outbound(bad_handler)
        bus.on_outbound(good_handler)

        from databot.core.bus import OutboundMessage

        await bus.publish_outbound(OutboundMessage(channel="cli", chat_id="t", content="hi"))
        # Good handler still executed despite bad handler raising
        assert results == ["ok"]
