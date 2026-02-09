"""Tests for streaming support in providers and agent loop."""

from __future__ import annotations

import pytest

from databot.core.bus import InboundMessage, MessageBus, StreamEvent
from databot.providers.base import LLMProvider, LLMResponse, StreamChunk, TokenUsage, ToolCall

# ---------------------------------------------------------------------------
# StreamChunk model
# ---------------------------------------------------------------------------


class TestStreamChunk:
    def test_text_chunk(self):
        c = StreamChunk(delta="hello")
        assert c.delta == "hello"
        assert c.is_tool_call is False

    def test_tool_call_chunk(self):
        c = StreamChunk(
            is_tool_call=True,
            tool_call_id="tc_1",
            tool_name="sql",
            tool_arguments_delta='{"query": "SELECT 1"}',
            finish_reason="tool_calls",
        )
        assert c.is_tool_call is True
        assert c.tool_name == "sql"


# ---------------------------------------------------------------------------
# Default chat_stream fallback
# ---------------------------------------------------------------------------


class DummyProvider(LLMProvider):
    """Minimal provider that returns a canned response for testing."""

    def __init__(self, response: LLMResponse):
        self._response = response

    async def chat(self, messages, tools=None, model=None) -> LLMResponse:
        return self._response

    def get_default_model(self) -> str:
        return "test-model"


class TestDefaultChatStream:
    @pytest.mark.asyncio
    async def test_text_response_stream(self):
        resp = LLMResponse(content="Hello world", usage=TokenUsage(10, 5, 15))
        provider = DummyProvider(resp)

        chunks = []
        async for chunk in provider.chat_stream(messages=[]):
            chunks.append(chunk)

        assert len(chunks) == 1
        assert chunks[0].delta == "Hello world"
        assert chunks[0].finish_reason == "stop"
        assert chunks[0].usage.total_tokens == 15

    @pytest.mark.asyncio
    async def test_tool_call_stream(self):
        resp = LLMResponse(
            content=None,
            tool_calls=[
                ToolCall(id="tc1", name="sql", arguments={"query": "SELECT 1"}),
                ToolCall(id="tc2", name="shell", arguments={"command": "ls"}),
            ],
        )
        provider = DummyProvider(resp)

        chunks = []
        async for chunk in provider.chat_stream(messages=[]):
            chunks.append(chunk)

        assert len(chunks) == 2
        assert chunks[0].is_tool_call is True
        assert chunks[0].tool_name == "sql"
        assert chunks[1].tool_name == "shell"


# ---------------------------------------------------------------------------
# StreamEvent on bus
# ---------------------------------------------------------------------------


class TestStreamEventBus:
    @pytest.mark.asyncio
    async def test_publish_stream_event(self):
        bus = MessageBus()
        received = []

        async def handler(event: StreamEvent):
            received.append(event)

        bus.on_stream(handler)

        event = StreamEvent(
            channel="cli",
            chat_id="test",
            event_type="delta",
            data="chunk data",
        )
        await bus.publish_stream_event(event)

        assert len(received) == 1
        assert received[0].data == "chunk data"

    @pytest.mark.asyncio
    async def test_inbound_message_stream_flag(self):
        msg = InboundMessage(
            channel="api",
            sender_id="user1",
            chat_id="chat1",
            content="hello",
            stream=True,
        )
        assert msg.stream is True
        assert msg.session_key == "api:chat1"
