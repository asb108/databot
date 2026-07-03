"""Tests for the saved query library."""

from __future__ import annotations

import pytest

from databot.queries.store import QueryStore, placeholders, render_query
from databot.tools.saved_query import SavedQueryTool


class _SQL:
    def __init__(self):
        self.executed = []

    async def execute(self, query, connection):
        self.executed.append((connection, query))
        return "| n |\n| --- |\n| 1 |"


class TestRenderQuery:
    def test_no_params(self):
        assert render_query("SELECT 1", {}) == "SELECT 1"

    def test_numeric_and_string(self):
        out = render_query(
            "SELECT * FROM t WHERE r = {region} AND amt > {min}",
            {"region": "APAC", "min": 1000},
        )
        assert "r = 'APAC'" in out
        assert "amt > 1000" in out

    def test_escapes_quotes(self):
        # An injection attempt is neutralized by quote-doubling.
        out = render_query("SELECT {x}", {"x": "a' OR '1'='1"})
        assert out == "SELECT 'a'' OR ''1''=''1'"

    def test_rejects_backslash_injection(self):
        # On MySQL, backslash escapes the next char inside a literal, so a
        # backslash-quote would smuggle a closing quote past quote-doubling.
        # Such values are rejected outright.
        with pytest.raises(ValueError, match="backslash"):
            render_query("SELECT {x}", {"x": "\\' OR 1=1 -- "})
        with pytest.raises(ValueError, match="backslash|null"):
            render_query("SELECT {x}", {"x": "path\\to\\file"})

    def test_rejects_null_byte(self):
        with pytest.raises(ValueError):
            render_query("SELECT {x}", {"x": "a\x00b"})

    def test_bool(self):
        assert render_query("SELECT {b}", {"b": True}) == "SELECT TRUE"

    def test_missing_param_raises(self):
        with pytest.raises(ValueError, match="Missing parameter"):
            render_query("SELECT {a}, {b}", {"a": 1})

    def test_placeholders(self):
        assert placeholders("SELECT {a} FROM t WHERE x = {b} AND y = {a}") == ["a", "b"]


class TestQueryStore:
    def test_save_get_list_delete(self, tmp_path):
        store = QueryStore(tmp_path / "q.db")
        store.save("daily", "SELECT * FROM t WHERE d = {day}", connection="wh", description="x")
        assert store.get("daily")["sql"].startswith("SELECT")
        assert len(store.list_all()) == 1
        # Upsert overwrites.
        store.save("daily", "SELECT 2", connection="wh2")
        assert store.get("daily")["connection"] == "wh2"
        assert store.delete("daily") is True
        assert store.get("daily") is None


class TestSavedQueryTool:
    def _tool(self, tmp_path, sql=None):
        return SavedQueryTool(store=QueryStore(tmp_path / "q.db"), sql_tool=sql)

    async def test_save_reports_params(self, tmp_path):
        tool = self._tool(tmp_path)
        out = await tool.execute(
            action="save", name="rev", sql="SELECT * FROM r WHERE region = {region}",
            connection="wh",
        )
        assert "Saved query 'rev'" in out
        assert "region" in out

    async def test_run_substitutes_and_executes(self, tmp_path):
        sql = _SQL()
        tool = self._tool(tmp_path, sql=sql)
        await tool.execute(
            action="save", name="rev", sql="SELECT * FROM r WHERE region = {region}",
            connection="wh",
        )
        out = await tool.execute(action="run", name="rev", params={"region": "APAC"})
        assert sql.executed
        assert "region = 'APAC'" in sql.executed[0][1]
        assert "APAC" in out

    async def test_run_missing_param(self, tmp_path):
        tool = self._tool(tmp_path, sql=_SQL())
        await tool.execute(action="save", name="q", sql="SELECT {a}", connection="wh")
        out = await tool.execute(action="run", name="q", params={})
        assert "Missing parameter" in out

    async def test_run_unknown_query(self, tmp_path):
        out = await self._tool(tmp_path, sql=_SQL()).execute(action="run", name="nope")
        assert "No saved query" in out

    async def test_run_needs_connection(self, tmp_path):
        tool = self._tool(tmp_path, sql=_SQL())
        await tool.execute(action="save", name="q", sql="SELECT 1")  # no connection
        out = await tool.execute(action="run", name="q")
        assert "no connection" in out

    async def test_list_and_get(self, tmp_path):
        tool = self._tool(tmp_path)
        await tool.execute(action="save", name="q", sql="SELECT {a}", connection="wh")
        assert "q" in await tool.execute(action="list")
        got = await tool.execute(action="get", name="q")
        assert "```sql" in got and "parameters: a" in got

    async def test_delete(self, tmp_path):
        tool = self._tool(tmp_path)
        await tool.execute(action="save", name="q", sql="SELECT 1", connection="wh")
        assert "Deleted" in await tool.execute(action="delete", name="q")
