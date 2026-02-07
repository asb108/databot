"""Rate limiting middleware for the databot gateway."""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Callable

from fastapi import HTTPException, Request, Response
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Simple in-memory rate limiter using a sliding window.

    Limits requests per client IP. Configure via ``requests_per_minute``.
    Set to 0 to disable rate limiting.
    """

    def __init__(self, app, requests_per_minute: int = 60):
        super().__init__(app)
        self._rpm = requests_per_minute
        self._window = 60.0  # seconds
        self._requests: dict[str, list[float]] = defaultdict(list)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if self._rpm <= 0:
            return await call_next(request)

        client_ip = request.client.host if request.client else "unknown"
        now = time.monotonic()

        # Prune old entries
        timestamps = self._requests[client_ip]
        cutoff = now - self._window
        self._requests[client_ip] = [t for t in timestamps if t > cutoff]
        timestamps = self._requests[client_ip]

        if len(timestamps) >= self._rpm:
            logger.warning(f"Rate limit exceeded for {client_ip}: {len(timestamps)}/{self._rpm} rpm")
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Maximum {self._rpm} requests per minute.",
            )

        timestamps.append(now)
        response = await call_next(request)

        # Add rate limit headers
        remaining = max(0, self._rpm - len(timestamps))
        response.headers["X-RateLimit-Limit"] = str(self._rpm)
        response.headers["X-RateLimit-Remaining"] = str(remaining)

        return response
