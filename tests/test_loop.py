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
