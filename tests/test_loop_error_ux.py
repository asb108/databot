"""Streaming error-path tests for AgentLoop."""

from __future__ import annotations

import pytest

from databot.core.bus import InboundMessage, MessageBus
from databot.core.loop import AgentLoop


class _BoomProvider:
    """Provider whose stream raises on first use."""

    def get_definitions(self):
        return None

    def get_default_model(self) -> str:
        return "test-model"

    async def chat_stream(self, **kwargs):
        raise RuntimeError("boom secret-xyz")
        yield  # pragma: no cover — makes this an async generator


class _NoTools:
    def get_definitions(self):
        return None


def _make_loop():
    return AgentLoop(
        bus=MessageBus(),
        provider=_BoomProvider(),
        tools=_NoTools(),
        workspace="/tmp",
        sessions=_Sessions(),
        memory=None,
        system_prompt="test",
        max_iterations=3,
    )


class _Session:
    summary = ""

    def get_history(self):
        return []

    def add_message(self, *a, **k):
        pass


class _Sessions:
    def get_or_create(self, key):
        return _Session()

    def save(self, s):
        pass


@pytest.mark.asyncio
async def test_stream_yields_clean_error_event():
    loop = _make_loop()
    msg = InboundMessage(channel="api", sender_id="u", chat_id="c", content="hi", stream=True)
    events = [e async for e in loop.process_message_stream(msg)]
    error_events = [e for e in events if e.event_type == "error"]
    assert len(error_events) == 1
    body = error_events[0].data
    assert "secret-xyz" not in body  # no raw exception text
    assert "Traceback" not in body
    assert "internal error" in body.lower()
