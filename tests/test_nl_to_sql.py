"""Tests for schema introspection and NL-to-SQL tools."""

from __future__ import annotations

import pytest

from databot.connectors.base import ConnectorResult, ConnectorType
from databot.providers.base import LLMResponse
from databot.tools.errors import ToolError
from databot.tools.nl_to_sql import NLToSQLTool, _is_read_only, _strip_sql
from databot.tools.schema_tool import SchemaTool


class _FakeSQLConnector:
    connector_type = ConnectorType.SQL

    def __init__(self, tables, columns):
        self._tables = tables
        self._columns = columns  # {table: [(col, type), ...]}

    async def execute(self, operation, **params):
        if operation == "list_tables":
            return ConnectorResult(
                success=True, columns=["table_name"], rows=[[t] for t in self._tables]
            )
        if operation == "get_schema":
            cols = self._columns.get(params.get("table"), [])
            return ConnectorResult(
                success=True,
                columns=["column_name", "data_type"],
                rows=[[c, t] for c, t in cols],
            )
        return ConnectorResult(success=False, error="unsupported")


class _Registry:
    def __init__(self, connector):
        self._c = connector

    def get(self, name):
        return self._c


class _FakeProvider:
    def __init__(self, sql):
        self._sql = sql

    async def chat(self, messages, tools=None, model=None):
        return LLMResponse(content=self._sql)

    def get_default_model(self):
        return "fake/model"


class _RecordingSQL:
    def __init__(self, result="| n |\n| --- |\n| 5 |"):
        self._result = result
        self.executed = []

    async def execute(self, query, connection):
        self.executed.append(query)
        return self._result


class TestHelpers:
    def test_strip_fences(self):
        assert _strip_sql("```sql\nSELECT 1;\n```") == "SELECT 1"
        assert _strip_sql("SELECT 1") == "SELECT 1"

    def test_read_only(self):
        assert _is_read_only("SELECT * FROM t")
        assert _is_read_only("WITH x AS (SELECT 1) SELECT * FROM x")
        assert not _is_read_only("DELETE FROM t")
        assert not _is_read_only("DROP TABLE t")
        assert not _is_read_only("WITH x AS (SELECT 1) DELETE FROM t")
        assert not _is_read_only("update t set a=1")

    def test_read_only_rejects_multi_statement(self):
        # A second statement smuggled after a SELECT must be rejected.
        assert not _is_read_only("SELECT 1; DROP TABLE x")
        assert not _is_read_only("SELECT 1; DELETE FROM y")
        # A single statement with a trailing semicolon is fine.
        assert _is_read_only("SELECT 1;")


class TestSchemaTool:
    async def test_list_tables_via_connector(self):
        conn = _FakeSQLConnector(["orders", "users"], {})
        tool = SchemaTool(connector_registry=_Registry(conn))
        out = await tool.execute(action="list_tables", connection="wh")
        assert "orders" in out and "users" in out

    async def test_describe_table_requires_table(self):
        tool = SchemaTool(connector_registry=_Registry(_FakeSQLConnector([], {})))
        with pytest.raises(ToolError) as exc_info:
            await tool.execute(action="describe_table", connection="wh")
        assert "required" in exc_info.value.message

    async def test_describe_table(self):
        conn = _FakeSQLConnector(["orders"], {"orders": [("id", "int"), ("amt", "decimal")]})
        tool = SchemaTool(connector_registry=_Registry(conn))
        out = await tool.execute(action="describe_table", connection="wh", table="orders")
        assert "id" in out and "amt" in out


class TestNLToSQLTool:
    def _tool(self, generated_sql, sql=None):
        conn = _FakeSQLConnector(
            ["orders"], {"orders": [("id", "int"), ("amount", "decimal"), ("status", "varchar")]}
        )
        return NLToSQLTool(
            provider=_FakeProvider(generated_sql),
            sql_tool=sql or _RecordingSQL(),
            connector_registry=_Registry(conn),
        )

    async def test_generates_sql(self):
        tool = self._tool("SELECT COUNT(*) FROM orders WHERE status = 'active'")
        out = await tool.execute(connection="wh", question="how many active orders?")
        assert "SELECT COUNT(*)" in out
        assert "```sql" in out

    async def test_executes_when_requested(self):
        sql = _RecordingSQL()
        tool = self._tool("SELECT COUNT(*) FROM orders", sql=sql)
        out = await tool.execute(connection="wh", question="count orders", execute=True)
        assert "Result" in out
        assert sql.executed and "orders" in sql.executed[0]

    async def test_refuses_non_read_only(self):
        sql = _RecordingSQL()
        tool = self._tool("DELETE FROM orders", sql=sql)
        out = await tool.execute(connection="wh", question="remove orders", execute=True)
        assert "not read-only" in out
        assert not sql.executed  # never ran the destructive query

    async def test_no_schema_available(self):
        tool = NLToSQLTool(
            provider=_FakeProvider("SELECT 1"),
            sql_tool=_RecordingSQL(),
            connector_registry=None,
        )
        out = await tool.execute(connection="wh", question="anything")
        assert "Could not read the schema" in out

    async def test_records_usage_of_internal_call(self):
        from databot.core.cost import UsageTracker
        from databot.providers.base import LLMResponse, TokenUsage

        class _UsageProvider:
            async def chat(self, messages, tools=None, model=None):
                return LLMResponse(
                    content="SELECT COUNT(*) FROM orders",
                    usage=TokenUsage(prompt_tokens=80, completion_tokens=12, total_tokens=92),
                )

            def get_default_model(self):
                return "gpt-4o"

        conn = _FakeSQLConnector(["orders"], {"orders": [("id", "int")]})
        tracker = UsageTracker()
        tool = NLToSQLTool(
            provider=_UsageProvider(),
            sql_tool=_RecordingSQL(),
            connector_registry=_Registry(conn),
            usage_tracker=tracker,
        )
        await tool.execute(connection="wh", question="count orders")
        # The nested generation call's tokens are now accounted for globally.
        assert tracker.global_stats()["prompt_tokens"] == 80
        assert tracker.global_stats()["calls"] == 1
