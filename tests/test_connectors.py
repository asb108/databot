"""Tests for the connector framework."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from databot.connectors.base import (
    BaseConnector,
    ConnectorResult,
    ConnectorStatus,
    ConnectorType,
)
from databot.connectors.factory import create_connector
from databot.connectors.registry import ConnectorRegistry


# ---------------------------------------------------------------------------
# ConnectorResult
# ---------------------------------------------------------------------------


class TestConnectorResult:
    def test_success_default(self):
        r = ConnectorResult(data={"key": "val"})
        assert r.success is True
        assert r.error is None

    def test_failure(self):
        r = ConnectorResult(success=False, error="something broke")
        assert r.success is False
        assert "something broke" in r.error

    def test_to_markdown_table(self):
        r = ConnectorResult(
            columns=["name", "age"],
            rows=[["Alice", 30], ["Bob", 25]],
            row_count=2,
        )
        md = r.to_markdown_table()
        assert "| name | age |" in md
        assert "Alice" in md
        assert "Bob" in md

    def test_to_markdown_table_empty(self):
        r = ConnectorResult(columns=["a"], rows=[], row_count=0)
        md = r.to_markdown_table()
        assert "No data" in md or md == "No data returned."


# ---------------------------------------------------------------------------
# Concrete test connector for unit testing
# ---------------------------------------------------------------------------


class DummyConnector(BaseConnector):
    def __init__(self, name: str, config: dict | None = None):
        super().__init__(name, config)
        self._connected = False

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.SQL

    def capabilities(self) -> list[str]:
        return ["ping", "echo"]

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    async def health_check(self) -> ConnectorStatus:
        return ConnectorStatus.HEALTHY if self._connected else ConnectorStatus.UNREACHABLE

    async def _op_ping(self, **kwargs: Any) -> ConnectorResult:
        return ConnectorResult(data="pong")

    async def _op_echo(self, message: str = "", **kwargs: Any) -> ConnectorResult:
        return ConnectorResult(data=message)


# ---------------------------------------------------------------------------
# BaseConnector dispatch
# ---------------------------------------------------------------------------


class TestBaseConnectorDispatch:
    @pytest.mark.asyncio
    async def test_execute_known_operation(self):
        conn = DummyConnector("test")
        result = await conn.execute("ping")
        assert result.success
        assert result.data == "pong"

    @pytest.mark.asyncio
    async def test_execute_with_args(self):
        conn = DummyConnector("test")
        result = await conn.execute("echo", message="hello")
        assert result.data == "hello"

    @pytest.mark.asyncio
    async def test_execute_unknown_operation(self):
        conn = DummyConnector("test")
        result = await conn.execute("unknown_op")
        assert result.success is False
        assert "not supported" in result.error.lower() or "unknown" in result.error.lower()

    @pytest.mark.asyncio
    async def test_connect_disconnect(self):
        conn = DummyConnector("test")
        await conn.connect()
        assert await conn.health_check() == ConnectorStatus.HEALTHY
        await conn.disconnect()
        assert await conn.health_check() == ConnectorStatus.UNREACHABLE


# ---------------------------------------------------------------------------
# ConnectorRegistry
# ---------------------------------------------------------------------------


class TestConnectorRegistry:
    def test_register_and_get(self):
        reg = ConnectorRegistry()
        conn = DummyConnector("db1")
        reg.register(conn)
        assert reg.get("db1") is conn

    def test_get_nonexistent(self):
        reg = ConnectorRegistry()
        assert reg.get("nope") is None

    def test_get_by_type(self):
        reg = ConnectorRegistry()
        conn = DummyConnector("db1")
        reg.register(conn)
        found = reg.get_by_type(ConnectorType.SQL)
        assert len(found) == 1
        assert found[0].name == "db1"

    def test_list_all(self):
        reg = ConnectorRegistry()
        reg.register(DummyConnector("a"))
        reg.register(DummyConnector("b"))
        assert len(reg.list_all()) == 2

    @pytest.mark.asyncio
    async def test_connect_all(self):
        reg = ConnectorRegistry()
        conn = DummyConnector("db1")
        reg.register(conn)
        await reg.connect_all()
        assert await conn.health_check() == ConnectorStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_disconnect_all(self):
        reg = ConnectorRegistry()
        conn = DummyConnector("db1")
        reg.register(conn)
        await conn.connect()
        await reg.disconnect_all()
        assert await conn.health_check() == ConnectorStatus.UNREACHABLE

    @pytest.mark.asyncio
    async def test_health_check_all(self):
        reg = ConnectorRegistry()
        conn = DummyConnector("db1")
        reg.register(conn)
        await conn.connect()
        health = await reg.health_check_all()
        assert health["db1"] == ConnectorStatus.HEALTHY


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


class TestConnectorFactory:
    def test_create_sql_connector(self):
        conn = create_connector("test_sql", {"type": "sql", "driver": "mysql"})
        assert conn.name == "test_sql"
        assert conn.connector_type == ConnectorType.SQL

    def test_create_rest_connector(self):
        conn = create_connector("test_rest", {"type": "rest_api", "base_url": "http://example.com"})
        assert conn.name == "test_rest"
        assert conn.connector_type == ConnectorType.REST_API

    def test_create_catalog_connector(self):
        conn = create_connector("test_cat", {"type": "catalog", "protocol": "iceberg"})
        assert conn.name == "test_cat"
        assert conn.connector_type == ConnectorType.CATALOG

    def test_create_spark_connector(self):
        conn = create_connector("test_spark", {"type": "spark", "mode": "livy"})
        assert conn.name == "test_spark"
        assert conn.connector_type == ConnectorType.PROCESSING

    def test_create_kafka_connector(self):
        conn = create_connector("test_kafka", {"type": "kafka", "base_url": "http://kafka:8082"})
        assert conn.name == "test_kafka"
        assert conn.connector_type == ConnectorType.STREAMING

    def test_create_unknown_type(self):
        with pytest.raises(ValueError, match="Unknown connector type"):
            create_connector("bad", {"type": "foobar"})

    def test_aliases(self):
        conn_s = create_connector("s", {"type": "processing"})
        assert conn_s.connector_type == ConnectorType.PROCESSING
        conn_k = create_connector("k", {"type": "streaming"})
        assert conn_k.connector_type == ConnectorType.STREAMING
