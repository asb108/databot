from __future__ import annotations

"""Tests for channel implementations."""

import pytest

from databot.core.bus import MessageBus


class TestGChatChannel:
    def test_channel_name(self):
        from databot.channels.gchat import GChatChannel

        bus = MessageBus()
        channel = GChatChannel(bus=bus, mode="webhook", webhook_url="https://example.com")
        assert channel.name == "gchat"

    @pytest.mark.asyncio
    async def test_start_stop(self):
        from databot.channels.gchat import GChatChannel

        bus = MessageBus()
        channel = GChatChannel(bus=bus)
        await channel.start()
        assert channel._running is True
        await channel.stop()
        assert channel._running is False

    def test_fastapi_routes(self):
        from databot.channels.gchat import GChatChannel

        bus = MessageBus()
        channel = GChatChannel(bus=bus, mode="app")
        router = channel.get_fastapi_routes()
        assert router is not None
        # Should have the /webhooks/gchat route
        routes = [r.path for r in router.routes]
        assert "/webhooks/gchat" in routes


class TestCLIChannel:
    def test_channel_name(self):
        from databot.channels.cli_channel import CLIChannel

        bus = MessageBus()
        channel = CLIChannel(bus=bus)
        assert channel.name == "cli"
