"""Tests for middleware (auth and rate limiting)."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from databot.middleware.auth import APIKeyAuthMiddleware
from databot.middleware.rate_limit import RateLimitMiddleware


def _create_app(api_keys: list[str] | None = None, rpm: int = 60) -> FastAPI:
    """Create a test FastAPI app with middleware."""
    app = FastAPI()
    app.add_middleware(APIKeyAuthMiddleware, api_keys=api_keys)
    app.add_middleware(RateLimitMiddleware, requests_per_minute=rpm)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/api/v1/message")
    async def post_message():
        return {"response": "hello"}

    return app


class TestAPIKeyAuth:
    def test_health_always_public(self):
        app = _create_app(api_keys=["secret_key"])
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_protected_endpoint_without_key(self):
        app = _create_app(api_keys=["secret_key"])
        client = TestClient(app)
        resp = client.post("/api/v1/message")
        assert resp.status_code == 401

    def test_protected_endpoint_with_bearer_key(self):
        app = _create_app(api_keys=["secret_key"])
        client = TestClient(app)
        resp = client.post(
            "/api/v1/message",
            headers={"Authorization": "Bearer secret_key"},
        )
        assert resp.status_code == 200

    def test_protected_endpoint_with_x_api_key(self):
        app = _create_app(api_keys=["secret_key"])
        client = TestClient(app)
        resp = client.post(
            "/api/v1/message",
            headers={"X-API-Key": "secret_key"},
        )
        assert resp.status_code == 200

    def test_invalid_key(self):
        app = _create_app(api_keys=["secret_key"])
        client = TestClient(app)
        resp = client.post(
            "/api/v1/message",
            headers={"Authorization": "Bearer wrong_key"},
        )
        assert resp.status_code == 403

    def test_no_keys_configured_open_mode(self):
        app = _create_app(api_keys=[])
        client = TestClient(app)
        resp = client.post("/api/v1/message")
        assert resp.status_code == 200

    def test_generate_key(self):
        key = APIKeyAuthMiddleware.generate_key()
        assert key.startswith("db_")
        assert len(key) > 20


class TestRateLimiting:
    def test_allows_requests_within_limit(self):
        app = _create_app(rpm=10)
        client = TestClient(app)
        for _ in range(10):
            resp = client.get("/health")
            assert resp.status_code == 200

    def test_rate_limit_headers(self):
        app = _create_app(rpm=100)
        client = TestClient(app)
        resp = client.get("/health")
        assert "X-RateLimit-Limit" in resp.headers
        assert "X-RateLimit-Remaining" in resp.headers

    def test_disabled_when_zero(self):
        app = _create_app(rpm=0)
        client = TestClient(app)
        for _ in range(100):
            resp = client.get("/health")
            assert resp.status_code == 200
