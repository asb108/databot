"""Authentication middleware for the databot gateway."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from typing import Callable

from fastapi import HTTPException, Request, Response
from fastapi.responses import JSONResponse
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware


class APIKeyAuthMiddleware(BaseHTTPMiddleware):
    """Middleware that enforces API key authentication on protected routes.

    Public paths (like /health) are excluded from authentication.
    API keys can be provided via the ``Authorization: Bearer <key>`` header
    or the ``X-API-Key`` header.
    """

    PUBLIC_PATHS: set[str] = {"/health", "/docs", "/openapi.json", "/redoc"}

    def __init__(self, app, api_keys: list[str] | None = None):
        super().__init__(app)
        self._api_keys = api_keys or []
        # Pre-hash keys for constant-time comparison
        self._key_hashes = [
            hashlib.sha256(k.encode()).hexdigest() for k in self._api_keys
        ]

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip auth for public paths and OPTIONS requests
        if request.url.path in self.PUBLIC_PATHS or request.method == "OPTIONS":
            return await call_next(request)

        # Skip auth if no keys configured (open mode — backward compatible)
        if not self._api_keys:
            return await call_next(request)

        # Extract token from headers
        token = self._extract_token(request)
        if not token:
            logger.warning(f"Unauthenticated request to {request.url.path} from {request.client}")
            return JSONResponse(
                status_code=401,
                content={
                    "detail": "Missing API key. Provide via 'Authorization: Bearer <key>' or 'X-API-Key' header."
                },
            )

        if not self._verify_key(token):
            logger.warning(f"Invalid API key used for {request.url.path} from {request.client}")
            return JSONResponse(status_code=403, content={"detail": "Invalid API key."})

        return await call_next(request)

    @staticmethod
    def _extract_token(request: Request) -> str | None:
        """Extract API key from Authorization header or X-API-Key header."""
        # Try Authorization: Bearer <key>
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            return auth_header[7:].strip()

        # Try X-API-Key header
        return request.headers.get("x-api-key")

    def _verify_key(self, token: str) -> bool:
        """Verify an API key using constant-time comparison."""
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        return any(
            hmac.compare_digest(token_hash, key_hash)
            for key_hash in self._key_hashes
        )

    @staticmethod
    def generate_key() -> str:
        """Generate a secure random API key."""
        return f"db_{secrets.token_urlsafe(32)}"
