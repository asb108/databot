"""Tests for SQL tool, including read-only enforcement."""

from __future__ import annotations

import pytest

from databot.tools.sql import SQLTool


@pytest.fixture
def sql_tool():
    """SQL tool with a dummy connection for testing validation logic."""
    return SQLTool(
        connections={"test": {"driver": "mysql", "host": "localhost"}},
        read_only=True,
        max_rows=100,
    )


class TestReadOnlyEnforcement:
    """Test SQL read-only enforcement logic."""

    @pytest.mark.asyncio
    async def test_blocks_insert(self, sql_tool):
        result = await sql_tool.execute("INSERT INTO t VALUES (1)", "test")
        assert "Write operations are not allowed" in result
        assert "INSERT" in result

    @pytest.mark.asyncio
    async def test_blocks_update(self, sql_tool):
        result = await sql_tool.execute("UPDATE t SET x=1", "test")
        assert "Write operations are not allowed" in result

    @pytest.mark.asyncio
    async def test_blocks_delete(self, sql_tool):
        result = await sql_tool.execute("DELETE FROM t WHERE id=1", "test")
        assert "Write operations are not allowed" in result

    @pytest.mark.asyncio
    async def test_blocks_drop(self, sql_tool):
        result = await sql_tool.execute("DROP TABLE t", "test")
        assert "Write operations are not allowed" in result

    @pytest.mark.asyncio
    async def test_blocks_truncate(self, sql_tool):
        result = await sql_tool.execute("TRUNCATE TABLE t", "test")
        assert "Write operations are not allowed" in result

    @pytest.mark.asyncio
    async def test_blocks_multi_statement(self, sql_tool):
        result = await sql_tool.execute("SELECT 1; DROP TABLE t", "test")
        assert "Multiple SQL statements" in result

    @pytest.mark.asyncio
    async def test_blocks_cte_wrapping_write(self, sql_tool):
        result = await sql_tool.execute(
            "WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte", "test"
        )
        assert "Write operations are not allowed" in result
        assert "CTE" in result

    @pytest.mark.asyncio
    async def test_allows_select(self, sql_tool):
        # This will fail at connection level, but should pass read-only check
        result = await sql_tool.execute("SELECT * FROM t", "test")
        assert "Write operations are not allowed" not in result
        assert "Multiple SQL statements" not in result

    @pytest.mark.asyncio
    async def test_allows_cte_select(self, sql_tool):
        result = await sql_tool.execute(
            "WITH cte AS (SELECT 1 as x) SELECT * FROM cte", "test"
        )
        assert "Write operations are not allowed" not in result

    @pytest.mark.asyncio
    async def test_blocks_merge(self, sql_tool):
        result = await sql_tool.execute("MERGE INTO t USING s ON t.id=s.id", "test")
        assert "Write operations are not allowed" in result

    @pytest.mark.asyncio
    async def test_blocks_call(self, sql_tool):
        result = await sql_tool.execute("CALL my_procedure()", "test")
        assert "Write operations are not allowed" in result


class TestConnectionValidation:
    @pytest.mark.asyncio
    async def test_unknown_connection(self, sql_tool):
        result = await sql_tool.execute("SELECT 1", "nonexistent")
        assert "Unknown connection" in result
        assert "test" in result


class TestFormatTable:
    def test_format_empty(self, sql_tool):
        result = sql_tool._format_table(["col1"], [], 0)
        assert "0 rows" in result

    def test_format_basic(self, sql_tool):
        result = sql_tool._format_table(["name", "age"], [("Alice", 30)], 1)
        assert "name" in result
        assert "Alice" in result
        assert "30" in result
        assert "1 row" in result

    def test_format_truncation(self, sql_tool):
        long_val = "x" * 100
        result = sql_tool._format_table(["val"], [(long_val,)], 1)
        assert "..." in result

    def test_format_null(self, sql_tool):
        result = sql_tool._format_table(["val"], [(None,)], 1)
        assert "NULL" in result

    def test_format_shows_truncation_message(self, sql_tool):
        rows = [(i,) for i in range(100)]
        result = sql_tool._format_table(["id"], rows, 200)
        assert "of 200 total" in result


class TestToolSchema:
    def test_schema_shape(self, sql_tool):
        schema = sql_tool.schema()
        assert schema["type"] == "function"
        assert schema["function"]["name"] == "sql"
        assert "query" in schema["function"]["parameters"]["properties"]
        assert "connection" in schema["function"]["parameters"]["properties"]
