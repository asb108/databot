"""Tests for data quality tool SQL injection protection."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from databot.tools.data_quality import DataQualityTool, _validate_identifier


class TestIdentifierValidation:
    """Test SQL identifier validation."""

    def test_valid_table_name(self):
        assert _validate_identifier("my_table", "table") == "my_table"

    def test_valid_schema_table(self):
        assert _validate_identifier("schema.table", "table") == "schema.table"

    def test_valid_backtick(self):
        assert _validate_identifier("`my_table`", "table") == "`my_table`"

    def test_rejects_semicolon(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("table; DROP TABLE x", "table")

    def test_rejects_parentheses(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("table()", "table")

    def test_rejects_subquery(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("(SELECT 1)", "table")

    def test_rejects_comment(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("table -- comment", "table")

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("", "table")

    def test_rejects_too_long(self):
        with pytest.raises(ValueError, match="exceeds maximum"):
            _validate_identifier("a" * 200, "table")

    def test_rejects_union_injection(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("table UNION SELECT *", "table")


class TestDataQualityTool:
    """Test DQ tool execution with mocked SQL."""

    @pytest.fixture
    def dq_tool(self):
        mock_sql = AsyncMock()
        mock_sql.execute = AsyncMock(return_value="| cnt |\n|---|\n| 100 |")
        return DataQualityTool(sql_tool=mock_sql)

    @pytest.mark.asyncio
    async def test_row_count(self, dq_tool):
        result = await dq_tool.execute(check_type="row_count", connection="test", table="my_table")
        assert "Row count" in result
        assert "my_table" in result

    @pytest.mark.asyncio
    async def test_null_check(self, dq_tool):
        result = await dq_tool.execute(
            check_type="null_check", connection="test", table="my_table", column="col1"
        )
        assert "Null check" in result

    @pytest.mark.asyncio
    async def test_null_check_missing_column(self, dq_tool):
        result = await dq_tool.execute(check_type="null_check", connection="test", table="my_table")
        assert "column is required" in result

    @pytest.mark.asyncio
    async def test_rejects_malicious_table(self, dq_tool):
        result = await dq_tool.execute(
            check_type="row_count", connection="test", table="t; DROP TABLE x"
        )
        assert "Invalid table" in result

    @pytest.mark.asyncio
    async def test_rejects_malicious_column(self, dq_tool):
        result = await dq_tool.execute(
            check_type="null_check",
            connection="test",
            table="my_table",
            column="col1; --",
        )
        assert "Invalid column" in result

    @pytest.mark.asyncio
    async def test_compare(self, dq_tool):
        result = await dq_tool.execute(
            check_type="compare",
            connection="target_conn",
            table="target_table",
            source_connection="source_conn",
            source_table="source_table",
        )
        assert "Source-target" in result

    @pytest.mark.asyncio
    async def test_no_sql_tool(self):
        tool = DataQualityTool(sql_tool=None)
        result = await tool.execute(check_type="row_count", connection="x", table="t")
        assert "SQL tool not configured" in result

    @pytest.mark.asyncio
    async def test_unknown_check_type(self, dq_tool):
        result = await dq_tool.execute(check_type="unknown", connection="test", table="my_table")
        assert "Unknown check type" in result
