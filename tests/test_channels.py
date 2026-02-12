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


# ===================================================================
# WhatsApp channel tests
# ===================================================================


class TestWhatsAppChannel:
    def test_channel_name(self):
        from databot.channels.whatsapp import WhatsAppChannel

        bus = MessageBus()
        channel = WhatsAppChannel(bus=bus)
        assert channel.name == "whatsapp"

    @pytest.mark.asyncio
    async def test_start_stop(self):
        from databot.channels.whatsapp import WhatsAppChannel

        bus = MessageBus()
        channel = WhatsAppChannel(bus=bus)
        await channel.start()
        assert channel._running is True
        await channel.stop()
        assert channel._running is False

    def test_fastapi_routes(self):
        from databot.channels.whatsapp import WhatsAppChannel

        bus = MessageBus()
        channel = WhatsAppChannel(bus=bus, verify_token="test-token")
        router = channel.get_fastapi_routes()
        routes = [r.path for r in router.routes]
        assert "/webhooks/whatsapp" in routes

    @pytest.mark.asyncio
    async def test_send_without_config(self):
        """Sending without phone_number_id should not raise."""
        from databot.channels.whatsapp import WhatsAppChannel

        bus = MessageBus()
        channel = WhatsAppChannel(bus=bus)
        # Should just log a warning, no exception
        await channel.send("123", "hello")

    @pytest.mark.asyncio
    async def test_outbound_filters_channel(self):
        """Outbound handler only fires for whatsapp channel."""
        from unittest.mock import AsyncMock

        from databot.channels.whatsapp import WhatsAppChannel
        from databot.core.bus import OutboundMessage

        bus = MessageBus()
        channel = WhatsAppChannel(bus=bus)
        channel.send = AsyncMock()

        msg_other = OutboundMessage(channel="slack", chat_id="x", content="hi")
        await channel._handle_outbound(msg_other)
        channel.send.assert_not_called()

        msg_wa = OutboundMessage(channel="whatsapp", chat_id="123", content="hello")
        await channel._handle_outbound(msg_wa)
        channel.send.assert_called_once_with("123", "hello", None)

    def test_webhook_verification_success(self):
        """GET /webhooks/whatsapp with correct verify_token returns challenge."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from databot.channels.whatsapp import WhatsAppChannel

        bus = MessageBus()
        channel = WhatsAppChannel(bus=bus, verify_token="my-secret")
        app = FastAPI()
        app.include_router(channel.get_fastapi_routes())

        with TestClient(app) as client:
            resp = client.get(
                "/webhooks/whatsapp",
                params={
                    "hub.mode": "subscribe",
                    "hub.verify_token": "my-secret",
                    "hub.challenge": "ABC123",
                },
            )
            assert resp.status_code == 200
            assert resp.text == "ABC123"

    def test_webhook_verification_failure(self):
        """GET /webhooks/whatsapp with wrong token returns 403."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from databot.channels.whatsapp import WhatsAppChannel

        bus = MessageBus()
        channel = WhatsAppChannel(bus=bus, verify_token="my-secret")
        app = FastAPI()
        app.include_router(channel.get_fastapi_routes())

        with TestClient(app) as client:
            resp = client.get(
                "/webhooks/whatsapp",
                params={
                    "hub.mode": "subscribe",
                    "hub.verify_token": "wrong",
                    "hub.challenge": "ABC123",
                },
            )
            assert resp.status_code == 403

    def test_webhook_inbound_message(self):
        """POST /webhooks/whatsapp with a text message publishes to bus."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from databot.channels.whatsapp import WhatsAppChannel

        bus = MessageBus()
        channel = WhatsAppChannel(bus=bus)
        app = FastAPI()
        app.include_router(channel.get_fastapi_routes())

        with TestClient(app) as client:
            payload = {
                "entry": [
                    {
                        "changes": [
                            {
                                "value": {
                                    "messages": [
                                        {
                                            "type": "text",
                                            "from": "628123456789",
                                            "text": {"body": "What tables are failing?"},
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
            resp = client.post("/webhooks/whatsapp", json=payload)
            assert resp.status_code == 200
            assert resp.json()["status"] == "ok"


# ===================================================================
# Telegram channel tests
# ===================================================================


class TestTelegramChannel:
    def test_channel_name(self):
        from databot.channels.telegram import TelegramChannel

        bus = MessageBus()
        channel = TelegramChannel(bus=bus)
        assert channel.name == "telegram"

    @pytest.mark.asyncio
    async def test_start_stop_polling(self):
        """Start/stop in polling mode without a real token should be safe."""
        from databot.channels.telegram import TelegramChannel

        bus = MessageBus()
        channel = TelegramChannel(bus=bus, mode="polling")
        # No token — start will log warning and return
        await channel.start()
        assert channel._running is False  # token is empty so it stays inactive
        await channel.stop()

    @pytest.mark.asyncio
    async def test_start_stop_with_token(self):
        """With a token, start sets _running=True."""
        from unittest.mock import AsyncMock, patch

        from databot.channels.telegram import TelegramChannel

        bus = MessageBus()
        channel = TelegramChannel(bus=bus, bot_token="fake:token", mode="polling")

        # Mock the poll loop so it doesn't actually call Telegram
        with patch.object(channel, "_poll_loop", new_callable=AsyncMock):
            await channel.start()
            assert channel._running is True
            await channel.stop()
            assert channel._running is False

    def test_fastapi_routes(self):
        from databot.channels.telegram import TelegramChannel

        bus = MessageBus()
        channel = TelegramChannel(bus=bus, bot_token="fake:token", mode="webhook")
        router = channel.get_fastapi_routes()
        routes = [r.path for r in router.routes]
        assert "/webhooks/telegram" in routes

    @pytest.mark.asyncio
    async def test_outbound_filters_channel(self):
        """Outbound handler only fires for telegram channel."""
        from unittest.mock import AsyncMock

        from databot.channels.telegram import TelegramChannel
        from databot.core.bus import OutboundMessage

        bus = MessageBus()
        channel = TelegramChannel(bus=bus)
        channel.send = AsyncMock()

        msg_other = OutboundMessage(channel="slack", chat_id="x", content="hi")
        await channel._handle_outbound(msg_other)
        channel.send.assert_not_called()

        msg_tg = OutboundMessage(channel="telegram", chat_id="123", content="hello")
        await channel._handle_outbound(msg_tg)
        channel.send.assert_called_once_with("123", "hello", None)

    @pytest.mark.asyncio
    async def test_send_without_token(self):
        """Sending without bot_token should not raise."""
        from databot.channels.telegram import TelegramChannel

        bus = MessageBus()
        channel = TelegramChannel(bus=bus)
        await channel.send("123", "hello")

    @pytest.mark.asyncio
    async def test_process_update_text(self):
        """_process_update extracts text and publishes to bus."""
        from unittest.mock import AsyncMock

        from databot.channels.telegram import TelegramChannel

        bus = MessageBus()
        bus.publish_inbound = AsyncMock()

        channel = TelegramChannel(bus=bus, bot_token="fake:token")

        update = {
            "update_id": 1,
            "message": {
                "text": "show me table sizes",
                "chat": {"id": 999},
                "from": {"id": 42},
            },
        }
        await channel._process_update(update)
        bus.publish_inbound.assert_called_once()
        msg = bus.publish_inbound.call_args[0][0]
        assert msg.channel == "telegram"
        assert msg.content == "show me table sizes"
        assert msg.chat_id == "999"
        assert msg.sender_id == "42"

    @pytest.mark.asyncio
    async def test_process_update_no_text(self):
        """Non-text updates are silently ignored."""
        from unittest.mock import AsyncMock

        from databot.channels.telegram import TelegramChannel

        bus = MessageBus()
        bus.publish_inbound = AsyncMock()

        channel = TelegramChannel(bus=bus, bot_token="fake:token")

        update = {"update_id": 2, "message": {"chat": {"id": 1}, "from": {"id": 1}}}
        await channel._process_update(update)
        bus.publish_inbound.assert_not_called()

    def test_webhook_inbound(self):
        """POST /webhooks/telegram with a message update works."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from databot.channels.telegram import TelegramChannel

        bus = MessageBus()
        channel = TelegramChannel(bus=bus, bot_token="fake:token", mode="webhook")
        app = FastAPI()
        app.include_router(channel.get_fastapi_routes())

        with TestClient(app) as client:
            update = {
                "update_id": 100,
                "message": {
                    "text": "Hello bot",
                    "chat": {"id": 555},
                    "from": {"id": 77},
                },
            }
            resp = client.post("/webhooks/telegram", json=update)
            assert resp.status_code == 200
            assert resp.json()["ok"] is True


# ===================================================================
# Config model tests
# ===================================================================


class TestWhatsAppConfig:
    def test_defaults(self):
        from databot.config.schema import WhatsAppConfig

        cfg = WhatsAppConfig()
        assert cfg.enabled is False
        assert cfg.phone_number_id == ""
        assert cfg.access_token == ""
        assert cfg.verify_token == ""

    def test_enabled(self):
        from databot.config.schema import WhatsAppConfig

        cfg = WhatsAppConfig(enabled=True, phone_number_id="123", access_token="tok")
        assert cfg.enabled is True
        assert cfg.phone_number_id == "123"


class TestTelegramConfig:
    def test_defaults(self):
        from databot.config.schema import TelegramConfig

        cfg = TelegramConfig()
        assert cfg.enabled is False
        assert cfg.bot_token == ""
        assert cfg.mode == "polling"
        assert cfg.webhook_url == ""

    def test_webhook_mode(self):
        from databot.config.schema import TelegramConfig

        cfg = TelegramConfig(
            enabled=True, bot_token="fake:tok", mode="webhook", webhook_url="https://example.com"
        )
        assert cfg.mode == "webhook"
        assert cfg.webhook_url == "https://example.com"


class TestChannelsConfigIncludesNewChannels:
    def test_whatsapp_and_telegram_in_channels(self):
        from databot.config.schema import ChannelsConfig

        cfg = ChannelsConfig()
        assert hasattr(cfg, "whatsapp")
        assert hasattr(cfg, "telegram")
        assert cfg.whatsapp.enabled is False
        assert cfg.telegram.enabled is False

    def test_save_load_new_channels(self, tmp_path):
        from databot.config.schema import DatabotConfig

        cfg = DatabotConfig()
        cfg.channels.whatsapp.enabled = True
        cfg.channels.whatsapp.phone_number_id = "ph123"
        cfg.channels.telegram.enabled = True
        cfg.channels.telegram.bot_token = "tg:token"
        cfg.channels.telegram.mode = "webhook"

        path = tmp_path / "config.yaml"
        cfg.save(path)

        loaded = DatabotConfig.load(path)
        assert loaded.channels.whatsapp.enabled is True
        assert loaded.channels.whatsapp.phone_number_id == "ph123"
        assert loaded.channels.telegram.enabled is True
        assert loaded.channels.telegram.bot_token == "tg:token"
        assert loaded.channels.telegram.mode == "webhook"

    def test_env_var_resolution(self, tmp_path, monkeypatch):
        import yaml

        from databot.config.schema import DatabotConfig

        monkeypatch.setenv("WA_TOKEN", "secret-wa")
        monkeypatch.setenv("TG_TOKEN", "secret-tg")
        data = {
            "channels": {
                "whatsapp": {"enabled": True, "access_token": "${WA_TOKEN}"},
                "telegram": {"enabled": True, "bot_token": "${TG_TOKEN}"},
            }
        }
        path = tmp_path / "config.yaml"
        with open(path, "w") as f:
            yaml.dump(data, f)

        loaded = DatabotConfig.load(path)
        assert loaded.channels.whatsapp.access_token == "secret-wa"
        assert loaded.channels.telegram.bot_token == "secret-tg"
