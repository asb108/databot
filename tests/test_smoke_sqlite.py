"""Real end-to-end smoke test against an actual SQLite database (no mocks).

The rest of the suite mocks the SQL layer, which can't catch real-engine string
semantics, quoting, or type handling. This exercises the SQL connector + the
new data tools against a live SQLite file so those bugs surface.
"""

from __future__ import annotations

import sqlite3

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "shop.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE orders (id INTEGER, region TEXT, amount REAL, status TEXT);
        INSERT INTO orders VALUES
            (1, 'APAC', 100.0, 'active'),
            (2, 'APAC', 250.0, 'active'),
            (3, 'EMEA', 50.0, NULL),
            (4, 'EMEA', 75.0, 'active'),
            (5, 'APAC', NULL, 'cancelled');
        """
    )
    conn.commit()
    conn.close()
    return str(path)


@pytest.fixture
async def sql_tool(db):
    from databot.connectors.registry import ConnectorRegistry
    from databot.connectors.sql_connector import SQLConnector
    from databot.tools.sql import SQLTool

    connector = SQLConnector("wh", {"driver": "sqlite", "database": db, "read_only": True})
    await connector.connect()
    registry = ConnectorRegistry()
    registry.register(connector)
    return SQLTool(
        connections={"wh": {"driver": "sqlite", "database": db}},
        connector_registry=registry,
    ), registry


class TestRealSqlite:
    async def test_query(self, sql_tool):
        tool, _ = sql_tool
        out = await tool.execute(query="SELECT COUNT(*) AS n FROM orders", connection="wh")
        assert "5" in out

    async def test_read_only_blocks_write(self, sql_tool):
        from databot.tools.errors import ToolError

        tool, _ = sql_tool
        with pytest.raises(ToolError) as exc_info:
            await tool.execute(query="DELETE FROM orders", connection="wh")
        msg = exc_info.value.to_text().lower()
        assert "read" in msg or "blocked" in msg

    async def test_schema_introspection(self, sql_tool):
        from databot.tools.schema_tool import SchemaTool

        tool, registry = sql_tool
        schema = SchemaTool(connector_registry=registry, sql_tool=tool)
        out = await schema.execute(action="describe_table", connection="wh", table="orders")
        assert "region" in out and "amount" in out

    async def test_profiling(self, sql_tool):
        from databot.tools.profiling import DataProfileTool

        tool, _ = sql_tool
        prof = DataProfileTool(sql_tool=tool)
        out = await prof.execute(connection="wh", table="orders", columns=["status", "amount"])
        assert "5 total rows" in out
        # status has 1 null of 5 = 20%, amount has 1 null = 20%
        assert "20.0%" in out

    async def test_export_csv(self, sql_tool, tmp_path):
        from databot.tools.export import ExportTool

        tool, registry = sql_tool
        export = ExportTool(connector_registry=registry, workspace=tmp_path)
        out = await export.execute(
            connection="wh", query="SELECT id, region FROM orders", filename="orders.csv"
        )
        assert "Exported 5 row" in out
        content = (tmp_path / "orders.csv").read_text()
        assert "region" in content and "APAC" in content

    async def test_saved_query_with_params(self, sql_tool, tmp_path):
        from databot.queries.store import QueryStore
        from databot.tools.saved_query import SavedQueryTool

        tool, _ = sql_tool
        sq = SavedQueryTool(store=QueryStore(tmp_path / "q.db"), sql_tool=tool)
        await sq.execute(
            action="save",
            name="by_region",
            sql="SELECT COUNT(*) AS n FROM orders WHERE region = {region}",
            connection="wh",
        )
        out = await sq.execute(action="run", name="by_region", params={"region": "APAC"})
        assert "3" in out  # 3 APAC orders

    async def test_monitor_breach_against_real_db(self, sql_tool, tmp_path):
        from databot.core.bus import MessageBus
        from databot.monitors.service import MonitorService

        tool, _ = sql_tool
        svc = MonitorService(tmp_path, MessageBus(), sql_tool=tool)
        mid = svc.add_monitor(
            "low_rows", "wh", "row_count", "lt", 10, "0 * * * *", table="orders"
        )
        result = await svc.run_monitor(svc.store.get(mid))
        # 5 rows < 10 threshold -> breached
        assert result["value"] == 5.0
        assert result["breached"] is True

    async def test_list_tables_and_databases(self, sql_tool):
        from databot.tools.schema_tool import SchemaTool

        tool, registry = sql_tool
        schema = SchemaTool(connector_registry=registry, sql_tool=tool)
        tables = await schema.execute(action="list_tables", connection="wh")
        assert "orders" in tables

    async def test_data_quality_null_check(self, sql_tool):
        from databot.tools.data_quality import DataQualityTool

        tool, _ = sql_tool
        dq = DataQualityTool(sql_tool=tool)
        # status has 1 NULL out of 5 rows = 20%.
        out = await dq.execute(
            check_type="null_check", connection="wh", table="orders", column="status"
        )
        assert "20" in out  # null_pct

    async def test_data_quality_row_count(self, sql_tool):
        from databot.tools.data_quality import DataQualityTool

        tool, _ = sql_tool
        dq = DataQualityTool(sql_tool=tool)
        out = await dq.execute(check_type="row_count", connection="wh", table="orders")
        assert "5" in out

    async def test_nl_to_sql_schema_grounding(self, sql_tool):
        # text_to_sql gathers the real schema via get_schema; with the SQLite
        # pragma_table_info aliasing, columns come through as (name, type) —
        # this would have surfaced the PRAGMA column-order bug.
        from databot.tools.nl_to_sql import NLToSQLTool

        tool, registry = sql_tool

        class _StubProvider:
            def __init__(self):
                self.schema_seen = ""

            async def chat(self, messages, tools=None, model=None):
                from databot.providers.base import LLMResponse

                self.schema_seen = messages[-1]["content"]
                return LLMResponse(content="SELECT COUNT(*) FROM orders")

            def get_default_model(self):
                return "x"

        provider = _StubProvider()
        nl = NLToSQLTool(provider=provider, sql_tool=tool, connector_registry=registry)
        out = await nl.execute(connection="wh", question="count orders", execute=True)
        # The schema handed to the model names the real columns (not pragma cids).
        assert "region" in provider.schema_seen and "amount" in provider.schema_seen
        assert "orders(" in provider.schema_seen
        assert "5" in out  # executed against the real DB

    async def test_audit_records_real_executions(self, sql_tool, tmp_path):
        from databot.audit import configure_audit_log, get_audit_log
        from databot.tools.base import ToolRegistry

        tool, _ = sql_tool
        # Drive the SQL tool through the registry so the audit hook fires.
        configure_audit_log(tmp_path / "audit.db")
        reg = ToolRegistry()
        reg.register(tool)
        await reg.execute("sql", {"query": "SELECT 1", "connection": "wh"})
        entries = get_audit_log().recent()
        assert any(e["tool"] == "sql" and e["status"] == "ok" for e in entries)
        configure_audit_log(None)
