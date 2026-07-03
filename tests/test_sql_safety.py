"""Tests for SQL identifier safety and connector injection protection."""

from __future__ import annotations

import pytest

from databot.connectors.identifiers import (
    IdentifierError,
    quote_identifier,
    validate_identifier,
)
from databot.connectors.sql_connector import SQLConnector


class TestValidateIdentifier:
    def test_simple(self):
        assert validate_identifier("orders") == "orders"

    def test_qualified(self):
        assert validate_identifier("sales.orders") == "sales.orders"

    def test_strips_surrounding_quotes(self):
        assert validate_identifier("`orders`") == "orders"
        assert validate_identifier('"orders"') == "orders"

    @pytest.mark.parametrize(
        "bad",
        [
            "orders; DROP TABLE x",
            "orders UNION SELECT *",
            "(SELECT 1)",
            "orders -- c",
            "orders'",
            "orders OR 1=1",
            "ord ers",
            "1orders",  # must start with letter/underscore
            "",
            "  ",
        ],
    )
    def test_rejects_injection(self, bad):
        with pytest.raises(IdentifierError):
            validate_identifier(bad, "table")

    def test_qualified_rejected_when_disallowed(self):
        with pytest.raises(IdentifierError):
            validate_identifier("sales.orders", "schema", allow_qualified=False)

    def test_injection_in_one_component(self):
        with pytest.raises(IdentifierError):
            validate_identifier("sales.orders; DROP TABLE x", "table")


class TestQuoteIdentifier:
    def test_mysql_backticks(self):
        assert quote_identifier("orders", "mysql") == "`orders`"
        assert quote_identifier("sales.orders", "mysql") == "`sales`.`orders`"

    def test_ansi_double_quotes(self):
        assert quote_identifier("sales.orders", "trino") == '"sales"."orders"'
        assert quote_identifier("orders", "postgresql") == '"orders"'

    def test_quote_rejects_injection(self):
        with pytest.raises(IdentifierError):
            quote_identifier("orders`; DROP TABLE x; --", "mysql")


class TestConnectorInjectionProtection:
    """The connector must reject injection in list_tables / get_schema params."""

    async def test_list_tables_rejects_injection(self):
        conn = SQLConnector("c", {"driver": "mysql", "host": "h", "database": "db"})
        result = await conn.execute("list_tables", database="db`; DROP TABLE users; --")
        assert result.success is False
        assert "Invalid" in (result.error or "")

    async def test_get_schema_rejects_injection(self):
        conn = SQLConnector("c", {"driver": "postgresql", "host": "h"})
        result = await conn.execute(
            "get_schema", table="users' UNION SELECT password FROM secrets --"
        )
        assert result.success is False
        assert "Invalid" in (result.error or "")

    async def test_get_schema_trino_rejects_schema_injection(self):
        conn = SQLConnector("c", {"driver": "trino", "host": "h", "catalog": "hive"})
        result = await conn.execute(
            "get_schema", table="orders", schema='public"; DROP SCHEMA x; --'
        )
        assert result.success is False
        assert "Invalid" in (result.error or "")
