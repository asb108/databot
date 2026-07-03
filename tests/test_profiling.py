"""Tests for the data profiling tool."""

from __future__ import annotations

from databot.tools.profiling import DataProfileTool, _parse_single_row


def _md(headers, row):
    h = "| " + " | ".join(headers) + " |"
    sep = "| " + " | ".join("---" for _ in headers) + " |"
    r = "| " + " | ".join(str(c) for c in row) + " |"
    return f"{h}\n{sep}\n{r}\n\n*1 row(s) returned.*"


class _ScriptedSQL:
    """Returns canned markdown keyed by substrings in the query."""

    def __init__(self, responses):
        self._responses = responses
        self.queries = []

    async def execute(self, query, connection):
        self.queries.append(query)
        for needle, resp in self._responses.items():
            if needle in query:
                return resp
        return "Error: unexpected query"


class TestParseSingleRow:
    def test_parses(self):
        md = _md(["a", "b"], [1, 2])
        assert _parse_single_row(md) == {"a": "1", "b": "2"}

    def test_returns_none_on_no_data(self):
        assert _parse_single_row("Query returned 0 rows.") is None


class TestDataProfileTool:
    async def test_rejects_injection_in_table(self):
        tool = DataProfileTool(sql_tool=_ScriptedSQL({}))
        out = await tool.execute(connection="c", table="t; DROP TABLE x")
        assert "Error" in out and "Invalid" in out

    async def test_rejects_injection_in_columns(self):
        tool = DataProfileTool(sql_tool=_ScriptedSQL({"COUNT(*)": _md(["total"], [100])}))
        out = await tool.execute(connection="c", table="orders", columns=["id', (SELECT 1)"])
        assert "Invalid" in out

    async def test_profiles_explicit_columns(self):
        sql = _ScriptedSQL(
            {
                "COUNT(*)": _md(["total"], [1000]),
                "COUNT(id)": _md(
                    ["non_null", "distinct_count", "min_val", "max_val"],
                    [1000, 1000, 1, 1000],
                ),
                "COUNT(status)": _md(
                    ["non_null", "distinct_count", "min_val", "max_val"],
                    [400, 1, "active", "active"],
                ),
            }
        )
        tool = DataProfileTool(sql_tool=sql)
        out = await tool.execute(connection="c", table="orders", columns=["id", "status"])
        assert "1,000 total rows" in out
        assert "id" in out and "status" in out
        # status: 600/1000 null = 60% -> flagged; single distinct -> constant flag
        assert "60.0%" in out
        assert "Potential issues" in out
        assert "60.0% null" in out
        assert "constant" in out

    async def test_auto_discovers_columns(self):
        sql = _ScriptedSQL(
            {
                "COUNT(*)": _md(["total"], [50]),
                "SELECT * FROM orders LIMIT 1": _md(["id", "name"], [1, "a"]),
                "COUNT(id)": _md(
                    ["non_null", "distinct_count", "min_val", "max_val"], [50, 50, 1, 50]
                ),
                "COUNT(name)": _md(
                    ["non_null", "distinct_count", "min_val", "max_val"], [50, 50, "a", "z"]
                ),
            }
        )
        tool = DataProfileTool(sql_tool=sql)
        out = await tool.execute(connection="c", table="orders")
        assert "id" in out and "name" in out
        assert "50 total rows" in out

    async def test_surfaces_row_count_error(self):
        sql = _ScriptedSQL({"COUNT(*)": "Error: Unknown connection 'c'."})
        tool = DataProfileTool(sql_tool=sql)
        out = await tool.execute(connection="c", table="orders")
        assert "could not read row count" in out

    async def test_no_sql_tool(self):
        tool = DataProfileTool(sql_tool=None)
        out = await tool.execute(connection="c", table="orders")
        assert "not configured" in out
