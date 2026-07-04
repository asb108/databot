"""Tests for the chart visualization tool."""

from __future__ import annotations

import pytest

from databot.tools.chart import (
    ChartTool,
    render_bar_chart,
    render_line_chart,
    render_sparkline,
)
from databot.tools.errors import ToolError


class TestRenderers:
    def test_bar_chart(self):
        out = render_bar_chart(["a", "b", "c"], [10, 20, 5], title="Counts")
        assert "Counts" in out
        assert "a" in out and "b" in out
        assert "█" in out  # has bars
        assert out.startswith("```")

    def test_bar_chart_empty(self):
        assert "no data" in render_bar_chart([], [])

    def test_sparkline(self):
        out = render_sparkline([1, 2, 3, 4, 5], title="trend")
        assert "trend" in out
        assert any(c in out for c in "▁▂▃▄▅▆▇█")

    def test_line_chart(self):
        out = render_line_chart([1, 3, 2, 5, 4, 6], title="series")
        assert "series" in out
        assert "•" in out
        assert "n=6" in out

    def test_line_downsamples(self):
        out = render_line_chart(list(range(500)), width=20)
        assert "n=500" in out


class TestChartTool:
    async def test_bar(self):
        tool = ChartTool()
        out = await tool.execute(chart_type="bar", values=[3, 1, 2], labels=["x", "y", "z"])
        assert "x" in out and "█" in out

    async def test_sparkline(self):
        tool = ChartTool()
        out = await tool.execute(chart_type="sparkline", values=[1, 5, 2, 8])
        assert "`" in out

    async def test_rejects_non_numeric(self):
        tool = ChartTool()
        with pytest.raises(ToolError) as exc_info:
            await tool.execute(chart_type="bar", values=["a", "b"])
        assert "must be numbers" in exc_info.value.message

    async def test_empty(self):
        tool = ChartTool()
        with pytest.raises(ToolError) as exc_info:
            await tool.execute(chart_type="bar", values=[])
        assert "no values" in exc_info.value.message

    async def test_labels_padded_when_short(self):
        tool = ChartTool()
        out = await tool.execute(chart_type="bar", values=[1, 2, 3], labels=["only"])
        # Should not raise and should render all three bars.
        assert out.count("│") >= 3

    async def test_unknown_type(self):
        tool = ChartTool()
        with pytest.raises(ToolError) as exc_info:
            await tool.execute(chart_type="pie", values=[1, 2])
        assert "unknown chart_type" in exc_info.value.message
