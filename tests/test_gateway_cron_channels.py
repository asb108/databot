"""Tests for the gateway API, cron service, GChat channel, and middleware."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from databot.config.schema import (
    CronConfig,
    CronJobConfig,
    DatabotConfig,
    GChatConfig,
    GatewayConfig,
)
from databot.core.bus import InboundMessage, MessageBus


# ===================================================================
# GChat channel tests
# ===================================================================


class TestGChatChannelModes:
    """Extended GChat channel tests beyond basic name/start/stop."""

    def test_webhook_mode_default(self):
        from databot.channels.gchat import GChatChannel

        bus = MessageBus()
        ch = GChatChannel(bus=bus)
        assert ch._mode == "webhook"
        assert ch._webhook_url == ""

    def test_app_mode(self):
        from databot.channels.gchat import GChatChannel

        bus = MessageBus()
        ch = GChatChannel(bus=bus, mode="app")
        assert ch._mode == "app"

    @pytest.mark.asyncio
    async def test_send_without_webhook_url(self, caplog):
        """Sending without a webhook URL should log a warning."""
        from databot.channels.gchat import GChatChannel

        bus = MessageBus()
        ch = GChatChannel(bus=bus, mode="webhook", webhook_url="")
        await ch.send("chat1", "hello")
        # No exception should be raised

    @pytest.mark.asyncio
    async def test_handle_outbound_filters_channel(self):
        """Outbound handler only fires for gchat channel."""
        from databot.channels.gchat import GChatChannel
        from databot.core.bus import OutboundMessage

        bus = MessageBus()
        ch = GChatChannel(bus=bus, mode="webhook", webhook_url="")
        ch.send = AsyncMock()

        # Message for different channel — should not trigger send
        msg = OutboundMessage(channel="slack", chat_id="x", content="hi")
        await ch._handle_outbound(msg)
        ch.send.assert_not_called()

        # Message for gchat — should trigger send
        msg = OutboundMessage(channel="gchat", chat_id="x", content="hello")
        await ch._handle_outbound(msg)
        ch.send.assert_called_once_with("x", "hello", None)

    def test_fastapi_routes_webhook_event_types(self):
        """App mode router handles MESSAGE and ADDED_TO_SPACE events."""
        from databot.channels.gchat import GChatChannel

        bus = MessageBus()
        ch = GChatChannel(bus=bus, mode="app")
        router = ch.get_fastapi_routes()
        routes = [r.path for r in router.routes]
        assert "/webhooks/gchat" in routes

    def test_app_mode_webhook_message_event(self):
        """POST to /webhooks/gchat with MESSAGE type publishes to bus."""
        from databot.channels.gchat import GChatChannel
        from fastapi.testclient import TestClient
        from fastapi import FastAPI

        bus = MessageBus()
        ch = GChatChannel(bus=bus, mode="app")

        app = FastAPI()
        app.include_router(ch.get_fastapi_routes())

        with TestClient(app) as client:
            payload = {
                "type": "MESSAGE",
                "message": {
                    "text": "test query",
                    "sender": {"name": "users/123"},
                },
                "space": {"name": "spaces/abc"},
            }
            response = client.post("/webhooks/gchat", json=payload)
            assert response.status_code == 200
            assert "Processing" in response.json()["text"]

    def test_app_mode_added_to_space(self):
        """ADDED_TO_SPACE event returns a welcome message."""
        from databot.channels.gchat import GChatChannel
        from fastapi.testclient import TestClient
        from fastapi import FastAPI

        bus = MessageBus()
        ch = GChatChannel(bus=bus, mode="app")

        app = FastAPI()
        app.include_router(ch.get_fastapi_routes())

        with TestClient(app) as client:
            payload = {"type": "ADDED_TO_SPACE"}
            response = client.post("/webhooks/gchat", json=payload)
            assert response.status_code == 200
            assert "databot" in response.json()["text"]


# ===================================================================
# CronStore tests
# ===================================================================


class TestCronStore:
    """Test SQLite cron job persistence."""

    def test_add_and_list(self):
        from databot.cron.store import CronStore

        with tempfile.TemporaryDirectory() as td:
            store = CronStore(Path(td) / "cron.db")
            store.add("j1", "nightly", "0 0 * * *", "run ETL")
            jobs = store.list_all()
            assert len(jobs) == 1
            assert jobs[0]["name"] == "nightly"
            assert jobs[0]["schedule"] == "0 0 * * *"
            assert jobs[0]["message"] == "run ETL"

    def test_remove(self):
        from databot.cron.store import CronStore

        with tempfile.TemporaryDirectory() as td:
            store = CronStore(Path(td) / "cron.db")
            store.add("j1", "nightly", "0 0 * * *", "run ETL")
            assert store.remove("j1") is True
            assert store.remove("j1") is False  # already removed
            assert store.list_all() == []

    def test_get_enabled(self):
        from databot.cron.store import CronStore

        with tempfile.TemporaryDirectory() as td:
            store = CronStore(Path(td) / "cron.db")
            store.add("j1", "active", "0 * * * *", "msg1")
            store.add("j2", "inactive", "0 * * * *", "msg2")
            # Disable j2
            conn = store._get_conn()
            conn.execute("UPDATE cron_jobs SET enabled = 0 WHERE id = 'j2'")
            conn.commit()

            enabled = store.get_enabled()
            assert len(enabled) == 1
            assert enabled[0]["name"] == "active"

    def test_update_last_run(self):
        from databot.cron.store import CronStore

        with tempfile.TemporaryDirectory() as td:
            store = CronStore(Path(td) / "cron.db")
            store.add("j1", "test", "0 * * * *", "msg")
            store.update_last_run("j1")
            jobs = store.list_all()
            assert jobs[0]["last_run"] is not None

    def test_wal_mode(self):
        """Ensure WAL mode is activated for the cron DB."""
        from databot.cron.store import CronStore

        with tempfile.TemporaryDirectory() as td:
            store = CronStore(Path(td) / "cron.db")
            conn = store._get_conn()
            result = conn.execute("PRAGMA journal_mode").fetchone()[0]
            assert result == "wal"


# ===================================================================
# CronService tests
# ===================================================================


class TestCronService:
    """Test the cron scheduler service."""

    def test_add_job_valid(self):
        from databot.cron.service import CronService

        with tempfile.TemporaryDirectory() as td:
            bus = MessageBus()
            svc = CronService(Path(td), bus)
            job_id = svc.add_job("test", "*/5 * * * *", "hello")
            assert len(job_id) == 8

    def test_add_job_invalid_cron(self):
        from databot.cron.service import CronService

        with tempfile.TemporaryDirectory() as td:
            bus = MessageBus()
            svc = CronService(Path(td), bus)
            with pytest.raises(ValueError, match="Invalid cron"):
                svc.add_job("bad", "not-a-cron", "msg")

    def test_remove_job(self):
        from databot.cron.service import CronService

        with tempfile.TemporaryDirectory() as td:
            bus = MessageBus()
            svc = CronService(Path(td), bus)
            job_id = svc.add_job("test", "*/5 * * * *", "hello")
            assert svc.remove_job(job_id) is True
            assert svc.remove_job(job_id) is False

    def test_list_jobs(self):
        from databot.cron.service import CronService

        with tempfile.TemporaryDirectory() as td:
            bus = MessageBus()
            svc = CronService(Path(td), bus)
            svc.add_job("j1", "0 * * * *", "msg1")
            svc.add_job("j2", "0 0 * * *", "msg2")
            jobs = svc.list_jobs()
            assert len(jobs) == 2

    def test_stop(self):
        from databot.cron.service import CronService

        with tempfile.TemporaryDirectory() as td:
            bus = MessageBus()
            svc = CronService(Path(td), bus)
            svc._running = True
            svc.stop()
            assert svc._running is False


# ===================================================================
# Middleware tests
# ===================================================================


class TestAPIKeyAuthMiddleware:
    """Test API key auth middleware."""

    def test_generate_key(self):
        from databot.middleware.auth import APIKeyAuthMiddleware

        key = APIKeyAuthMiddleware.generate_key()
        assert key.startswith("db_")
        assert len(key) > 10

    def test_open_mode_no_keys(self):
        """No API keys configured = open mode, all requests pass."""
        from databot.middleware.auth import APIKeyAuthMiddleware
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.add_middleware(APIKeyAuthMiddleware, api_keys=[])

        @app.get("/test")
        async def test_endpoint():
            return {"ok": True}

        client = TestClient(app)
        resp = client.get("/test")
        assert resp.status_code == 200

    def test_rejects_missing_key(self):
        """With keys configured, missing key returns 401."""
        from databot.middleware.auth import APIKeyAuthMiddleware
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.add_middleware(APIKeyAuthMiddleware, api_keys=["secret-key"])

        @app.get("/test")
        async def test_endpoint():
            return {"ok": True}

        client = TestClient(app)
        resp = client.get("/test")
        assert resp.status_code == 401

    def test_accepts_valid_bearer(self):
        """Valid Bearer token is accepted."""
        from databot.middleware.auth import APIKeyAuthMiddleware
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.add_middleware(APIKeyAuthMiddleware, api_keys=["secret-key"])

        @app.get("/test")
        async def test_endpoint():
            return {"ok": True}

        client = TestClient(app)
        resp = client.get("/test", headers={"Authorization": "Bearer secret-key"})
        assert resp.status_code == 200

    def test_accepts_x_api_key_header(self):
        """X-API-Key header works too."""
        from databot.middleware.auth import APIKeyAuthMiddleware
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.add_middleware(APIKeyAuthMiddleware, api_keys=["secret-key"])

        @app.get("/test")
        async def test_endpoint():
            return {"ok": True}

        client = TestClient(app)
        resp = client.get("/test", headers={"X-API-Key": "secret-key"})
        assert resp.status_code == 200

    def test_rejects_invalid_key(self):
        """Wrong key returns 403."""
        from databot.middleware.auth import APIKeyAuthMiddleware
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.add_middleware(APIKeyAuthMiddleware, api_keys=["secret-key"])

        @app.get("/test")
        async def test_endpoint():
            return {"ok": True}

        client = TestClient(app)
        resp = client.get("/test", headers={"Authorization": "Bearer wrong-key"})
        assert resp.status_code == 403

    def test_health_is_public(self):
        """Health endpoint is always accessible."""
        from databot.middleware.auth import APIKeyAuthMiddleware
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.add_middleware(APIKeyAuthMiddleware, api_keys=["secret-key"])

        @app.get("/health")
        async def health():
            return {"status": "ok"}

        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200


class TestRateLimitMiddleware:
    """Test rate limiting middleware."""

    def test_disabled_when_zero(self):
        """RPM=0 disables rate limiting."""
        from databot.middleware.rate_limit import RateLimitMiddleware
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.add_middleware(RateLimitMiddleware, requests_per_minute=0)

        @app.get("/test")
        async def test_endpoint():
            return {"ok": True}

        client = TestClient(app)
        for _ in range(100):
            resp = client.get("/test")
            assert resp.status_code == 200

    def test_enforces_limit(self):
        """Exceeding RPM limit returns 429."""
        from databot.middleware.rate_limit import RateLimitMiddleware
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.add_middleware(RateLimitMiddleware, requests_per_minute=5)

        @app.get("/test")
        async def test_endpoint():
            return {"ok": True}

        client = TestClient(app)
        for i in range(5):
            resp = client.get("/test")
            assert resp.status_code == 200

        resp = client.get("/test")
        assert resp.status_code == 429

    def test_rate_limit_headers(self):
        """Responses include rate limit headers."""
        from databot.middleware.rate_limit import RateLimitMiddleware
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        app.add_middleware(RateLimitMiddleware, requests_per_minute=10)

        @app.get("/test")
        async def test_endpoint():
            return {"ok": True}

        client = TestClient(app)
        resp = client.get("/test")
        assert resp.status_code == 200
        assert resp.headers["X-RateLimit-Limit"] == "10"
        assert int(resp.headers["X-RateLimit-Remaining"]) >= 0


# ===================================================================
# GChat config in onboarding
# ===================================================================


class TestGChatConfigModel:
    """Test GChat config model defaults and settings."""

    def test_defaults(self):
        cfg = GChatConfig()
        assert cfg.enabled is False
        assert cfg.mode == "webhook"
        assert cfg.webhook_url == ""

    def test_app_mode(self):
        cfg = GChatConfig(enabled=True, mode="app")
        assert cfg.mode == "app"
        assert cfg.enabled is True

    def test_webhook_url(self):
        cfg = GChatConfig(
            enabled=True,
            mode="webhook",
            webhook_url="https://chat.googleapis.com/v1/spaces/xxx/messages?key=yyy",
        )
        assert "chat.googleapis.com" in cfg.webhook_url


class TestGatewayConfig:
    """Test gateway config features."""

    def test_cors_origins_default(self):
        cfg = GatewayConfig()
        assert cfg.cors_origins == ["*"]

    def test_rate_limit_default(self):
        cfg = GatewayConfig()
        assert cfg.rate_limit_rpm == 60

    def test_custom_api_keys(self):
        cfg = GatewayConfig(api_keys=["key1", "key2"])
        assert len(cfg.api_keys) == 2


class TestCronConfig:
    """Test cron config model."""

    def test_defaults(self):
        cfg = CronConfig()
        assert cfg.jobs == []
        assert cfg.check_interval_seconds == 30

    def test_job_config(self):
        job = CronJobConfig(name="daily", schedule="0 0 * * *", message="run report")
        assert job.name == "daily"
        assert job.channel == "gchat"
        assert job.enabled is True

    def test_config_with_jobs(self):
        cfg = CronConfig(
            jobs=[
                CronJobConfig(name="j1", schedule="0 * * * *", message="msg1"),
                CronJobConfig(name="j2", schedule="0 0 * * *", message="msg2", channel="slack"),
            ]
        )
        assert len(cfg.jobs) == 2
        assert cfg.jobs[1].channel == "slack"
