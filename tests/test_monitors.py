"""Tests for scheduled data monitors."""

from __future__ import annotations

import pytest

from databot.core.bus import MessageBus
from databot.monitors.evaluator import (
    MonitorError,
    build_check_query,
    evaluate_breach,
    parse_metric_value,
)
from databot.monitors.service import MonitorService


def _md(value):
    return f"| value |\n| --- |\n| {value} |\n\n*1 row(s) returned.*"


class _SQL:
    def __init__(self, result):
        self._result = result
        self.queries = []

    async def execute(self, query, connection):
        self.queries.append((connection, query))
        return self._result


class TestEvaluator:
    def test_build_row_count(self):
        assert build_check_query("row_count", table="orders") == "SELECT COUNT(*) AS value FROM orders"

    def test_build_null_rate_requires_column(self):
        with pytest.raises(MonitorError):
            build_check_query("null_rate", table="orders")

    def test_build_null_rate(self):
        q = build_check_query("null_rate", table="orders", column="status")
        assert "status IS NULL" in q and "orders" in q

    def test_build_sql_metric_passthrough(self):
        assert build_check_query("sql_metric", query="SELECT 5") == "SELECT 5"

    def test_build_rejects_injection(self):
        with pytest.raises(MonitorError):
            build_check_query("row_count", table="orders; DROP TABLE x")

    def test_parse_value(self):
        assert parse_metric_value(_md(42)) == 42.0
        assert parse_metric_value("no table") is None

    def test_evaluate_breach(self):
        assert evaluate_breach(500, "lt", 1000) is True
        assert evaluate_breach(1500, "lt", 1000) is False
        assert evaluate_breach(10, "gt", 5) is True
        assert evaluate_breach(5, "ge", 5) is True
        with pytest.raises(MonitorError):
            evaluate_breach(1, "bogus", 0)


class TestMonitorService:
    def _service(self, tmp_path, sql_result):
        return MonitorService(tmp_path, MessageBus(), sql_tool=_SQL(sql_result))

    def test_add_validates(self, tmp_path):
        svc = self._service(tmp_path, _md(100))
        with pytest.raises(MonitorError):
            svc.add_monitor("m", "wh", "row_count", "bad_op", 1, "0 * * * *", table="t")
        with pytest.raises(MonitorError):
            svc.add_monitor("m", "wh", "row_count", "lt", 1, "not a cron", table="t")

    def test_add_and_list(self, tmp_path):
        svc = self._service(tmp_path, _md(100))
        mid = svc.add_monitor("rows", "wh", "row_count", "lt", 1000, "0 * * * *", table="orders")
        monitors = svc.list_monitors()
        assert len(monitors) == 1 and monitors[0]["id"] == mid

    async def test_run_monitor_breach_alerts(self, tmp_path):
        bus = MessageBus()
        svc = MonitorService(tmp_path, bus, sql_tool=_SQL(_md(500)))
        mid = svc.add_monitor(
            "low_rows", "wh", "row_count", "lt", 1000, "0 * * * *", table="orders", channel="slack"
        )
        monitor = svc.store.get(mid)
        result = await svc.run_monitor(monitor)
        assert result["breached"] is True
        assert result["value"] == 500.0
        # History recorded.
        history = svc.store.history(mid)
        assert history[0]["status"] == "breached"

    async def test_run_monitor_ok(self, tmp_path):
        svc = MonitorService(tmp_path, MessageBus(), sql_tool=_SQL(_md(5000)))
        mid = svc.add_monitor("rows", "wh", "row_count", "lt", 1000, "0 * * * *", table="orders")
        result = await svc.run_monitor(svc.store.get(mid))
        assert result["breached"] is False
        assert result["status"] == "ok"

    async def test_alert_published_on_due_breach(self, tmp_path):
        bus = MessageBus()
        received = []

        async def _capture(m):
            received.append(m)

        bus.on_outbound(_capture)
        # hours_stale = 48, threshold gt 24 -> breach.
        svc = MonitorService(tmp_path, bus, sql_tool=_SQL(_md(48)))
        svc.add_monitor(
            "freshness", "wh", "sql_metric", "gt", 24, "* * * * *",
            query="SELECT hours_stale", channel="slack",
        )
        await svc._check_and_execute()
        assert received and "Monitor alert" in received[0].content
        assert received[0].channel == "slack"

    async def test_run_monitor_handles_bad_value(self, tmp_path):
        svc = MonitorService(tmp_path, MessageBus(), sql_tool=_SQL("Error: no such table"))
        mid = svc.add_monitor("rows", "wh", "row_count", "lt", 1, "0 * * * *", table="orders")
        result = await svc.run_monitor(svc.store.get(mid))
        assert result["status"] == "error"
