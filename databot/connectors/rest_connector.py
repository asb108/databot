"""REST API connector — generic HTTP client for REST services.

Provides authenticated, paginated, retry-capable HTTP calls to any REST API.
Used as the base for Airflow, Spark Livy, Kafka REST, dbt Cloud, and custom APIs.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
from loguru import logger

from databot.connectors.base import (
    BaseConnector,
    ConnectorResult,
    ConnectorStatus,
    ConnectorType,
)


class RESTConnector(BaseConnector):
    """Generic REST API connector with auth, pagination, and retry support."""

    def __init__(self, name: str, config: dict[str, Any] | None = None):
        super().__init__(name, config)
        self._base_url = self._config.get("base_url", "").rstrip("/")
        self._timeout = self._config.get("timeout", 30)
        self._max_retries = self._config.get("max_retries", 3)
        self._auth = self._build_auth()
        self._headers = self._build_headers()
        self._client: httpx.AsyncClient | None = None

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.REST_API

    def capabilities(self) -> list[str]:
        return ["request", "get", "post", "put", "delete", "health_check"]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            auth=self._auth,
            headers=self._headers,
        )
        self._connected = True

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
        self._connected = False

    async def health_check(self) -> ConnectorStatus:
        if not self._base_url:
            return ConnectorStatus.NOT_CONFIGURED
        health_path = self._config.get("health_path", "/")
        try:
            resp = await self.request("GET", health_path)
            return ConnectorStatus.HEALTHY if resp.success else ConnectorStatus.DEGRADED
        except Exception:
            return ConnectorStatus.UNREACHABLE

    # ------------------------------------------------------------------
    # Core HTTP methods
    # ------------------------------------------------------------------

    async def request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: Any = None,
        data: Any = None,
        headers: dict[str, str] | None = None,
        raw_response: bool = False,
    ) -> ConnectorResult:
        """Make an HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, PATCH).
            path: URL path relative to ``base_url``.
            params: Query parameters.
            json: JSON body.
            data: Form data.
            headers: Extra headers.
            raw_response: If True, return the raw response text/json in ``data``.
        """
        client = self._client or httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            auth=self._auth,
            headers=self._headers,
        )
        own_client = self._client is None

        last_error = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = await client.request(
                    method,
                    path,
                    params=params,
                    json=json,
                    data=data,
                    headers=headers,
                )
                resp.raise_for_status()

                # Parse body
                content_type = resp.headers.get("content-type", "")
                if "application/json" in content_type:
                    body = resp.json()
                else:
                    body = resp.text

                return ConnectorResult(success=True, data=body, metadata={"status": resp.status_code})

            except httpx.HTTPStatusError as e:
                # Don't retry 4xx (except 429)
                if 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                    return ConnectorResult(
                        success=False,
                        error=f"HTTP {e.response.status_code}: {e.response.text[:500]}",
                        metadata={"status": e.response.status_code},
                    )
                last_error = str(e)
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_error = str(e)

            if attempt < self._max_retries:
                delay = 2 ** (attempt - 1)
                logger.debug(f"REST retry {attempt}/{self._max_retries} for {path} in {delay}s")
                await asyncio.sleep(delay)

        if own_client:
            await client.aclose()

        return ConnectorResult(success=False, error=f"Request failed after {self._max_retries} attempts: {last_error}")

    # ------------------------------------------------------------------
    # Convenience operations (dispatched by BaseConnector.execute)
    # ------------------------------------------------------------------

    async def _op_request(self, method: str = "GET", path: str = "/", **kwargs: Any) -> ConnectorResult:
        return await self.request(method, path, **kwargs)

    async def _op_get(self, path: str = "/", **kwargs: Any) -> ConnectorResult:
        return await self.request("GET", path, **kwargs)

    async def _op_post(self, path: str = "/", **kwargs: Any) -> ConnectorResult:
        return await self.request("POST", path, **kwargs)

    async def _op_put(self, path: str = "/", **kwargs: Any) -> ConnectorResult:
        return await self.request("PUT", path, **kwargs)

    async def _op_delete(self, path: str = "/", **kwargs: Any) -> ConnectorResult:
        return await self.request("DELETE", path, **kwargs)

    # ------------------------------------------------------------------
    # Pagination helper
    # ------------------------------------------------------------------

    async def paginate(
        self,
        method: str,
        path: str,
        items_key: str = "items",
        params: dict[str, Any] | None = None,
        limit: int = 100,
        offset_key: str = "offset",
        limit_key: str = "limit",
        page_size: int = 25,
        max_pages: int = 20,
    ) -> ConnectorResult:
        """Fetch paginated results using offset/limit pattern."""
        all_items: list[Any] = []
        offset = 0

        for _ in range(max_pages):
            p = dict(params or {})
            p[offset_key] = offset
            p[limit_key] = min(page_size, limit - len(all_items))

            result = await self.request(method, path, params=p)
            if not result.success:
                return result

            items = result.data.get(items_key, []) if isinstance(result.data, dict) else []
            if not items:
                break
            all_items.extend(items)

            if len(all_items) >= limit or len(items) < page_size:
                break
            offset += len(items)

        return ConnectorResult(success=True, data=all_items, metadata={"total_fetched": len(all_items)})

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _build_auth(self) -> httpx.BasicAuth | None:
        auth_cfg = self._config.get("auth", {})
        auth_type = auth_cfg.get("type", "")

        if auth_type == "basic":
            return httpx.BasicAuth(
                auth_cfg.get("username", ""),
                auth_cfg.get("password", ""),
            )

        # For "bearer" / "api_key", we handle via headers instead
        # Also support top-level username/password for backward compat
        username = self._config.get("username", "")
        password = self._config.get("password", "")
        if username and not auth_type:
            return httpx.BasicAuth(username, password)

        return None

    def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        auth_cfg = self._config.get("auth", {})
        auth_type = auth_cfg.get("type", "")

        if auth_type == "bearer":
            token = auth_cfg.get("token", "")
            if token:
                headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "api_key":
            header_name = auth_cfg.get("header", "X-API-Key")
            key = auth_cfg.get("key", "")
            if key:
                headers[header_name] = key

        # Merge custom headers
        extra_headers = self._config.get("headers", {})
        headers.update(extra_headers)
        return headers
