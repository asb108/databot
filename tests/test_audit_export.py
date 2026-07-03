"""Tests for the audit log and export tool."""

from __future__ import annotations

import csv
import json

import pytest

from databot.audit import AuditLog, configure_audit_log, get_audit_log
from databot.connectors.base import ConnectorResult, ConnectorType
from databot.tools.base import BaseTool, ToolRegistry
from databot.tools.export import ExportTool


class _Tool(BaseTool):
    @property
    def name(self):
        return "demo"

    @property
    def description(self):
        return "demo"

    def parameters(self):
        return {"type": "object", "properties": {}}

    async def execute(self, **kwargs):
        return "ok"


class TestAuditLog:
    def test_disabled_is_noop(self):
        log = AuditLog(None)
        assert log.enabled is False
        log.record("sql", {"q": "SELECT 1"}, "ok")
        assert log.recent() == []

    def test_records_and_reads(self, tmp_path):
        log = AuditLog(tmp_path / "audit.db")
        log.record("sql", {"query": "SELECT 1"}, "ok", session_key="s1", duration_ms=5)
        log.record("shell", {"command": "ls"}, "error")
        entries = log.recent()
        assert len(entries) == 2
        assert entries[0]["tool"] == "shell"  # most recent first
        assert entries[1]["status"] == "ok"

    def test_redacts_secrets(self, tmp_path):
        log = AuditLog(tmp_path / "audit.db")
        log.record("connect", {"host": "db", "password": "hunter2", "api_key": "k"}, "ok")
        args = json.loads(log.recent()[0]["arguments"])
        assert args["host"] == "db"
        assert args["password"] == "***redacted***"
        assert args["api_key"] == "***redacted***"

    def test_filter_by_tool(self, tmp_path):
        log = AuditLog(tmp_path / "audit.db")
        log.record("sql", {}, "ok")
        log.record("shell", {}, "ok")
        assert all(e["tool"] == "sql" for e in log.recent(tool="sql"))

    async def test_registry_records_executions(self, tmp_path):
        configure_audit_log(tmp_path / "audit.db")
        reg = ToolRegistry()
        reg.register(_Tool())
        await reg.execute("demo", {"x": 1})
        await reg.execute("missing", {})
        entries = get_audit_log().recent()
        tools = {e["tool"] for e in entries}
        assert "demo" in tools and "missing" in tools
        configure_audit_log(None)  # reset global


class _Registry:
    def __init__(self, result):
        self._result = result

    def get(self, name):
        return self

    connector_type = ConnectorType.SQL

    async def execute(self, op, **kwargs):
        return self._result


class TestExportTool:
    def _tool(self, tmp_path, result):
        return ExportTool(connector_registry=_Registry(result), workspace=tmp_path)

    async def test_csv_export(self, tmp_path):
        result = ConnectorResult(
            success=True, columns=["id", "name"], rows=[[1, "a"], [2, "b"]]
        )
        tool = self._tool(tmp_path, result)
        out = await tool.execute(connection="wh", query="SELECT *", filename="out.csv")
        assert "Exported 2 row" in out
        rows = list(csv.reader((tmp_path / "out.csv").open()))
        assert rows[0] == ["id", "name"]
        assert rows[1] == ["1", "a"]

    async def test_json_export(self, tmp_path):
        result = ConnectorResult(success=True, columns=["id"], rows=[[1], [2]])
        tool = self._tool(tmp_path, result)
        out = await tool.execute(
            connection="wh", query="SELECT id", filename="out.json", format="json"
        )
        assert "Exported 2 row" in out
        data = json.loads((tmp_path / "out.json").read_text())
        assert data == [{"id": 1}, {"id": 2}]

    @pytest.mark.parametrize("bad", ["../escape.csv", "/etc/passwd", "a/b.csv", "we ird.csv"])
    async def test_rejects_unsafe_filename(self, tmp_path, bad):
        result = ConnectorResult(success=True, columns=["id"], rows=[[1]])
        tool = self._tool(tmp_path, result)
        out = await tool.execute(connection="wh", query="SELECT 1", filename=bad)
        assert "invalid filename" in out

    async def test_requires_connector(self, tmp_path):
        tool = ExportTool(connector_registry=None, workspace=tmp_path)
        out = await tool.execute(connection="wh", query="SELECT 1", filename="x.csv")
        assert "connector-backed" in out
