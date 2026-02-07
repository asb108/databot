"""Tests for the web tools."""

from __future__ import annotations

import pytest

from databot.tools.web import WebFetchTool, WebSearchTool


class TestWebFetchTool:
    def test_name(self):
        tool = WebFetchTool()
        assert tool.name == "web_fetch"

    def test_configurable_max_length(self):
        tool = WebFetchTool(max_length=5000)
        assert tool._max_length == 5000

    def test_schema(self):
        tool = WebFetchTool()
        schema = tool.schema()
        assert schema["function"]["name"] == "web_fetch"
        assert "url" in schema["function"]["parameters"]["properties"]

    @pytest.mark.asyncio
    async def test_invalid_url(self):
        tool = WebFetchTool()
        result = await tool.execute(url="not-a-real-url")
        assert "Error" in result


class TestWebSearchTool:
    def test_name(self):
        tool = WebSearchTool()
        assert tool.name == "web_search"

    def test_no_api_key(self):
        tool = WebSearchTool()
        assert tool._api_key is None

    @pytest.mark.asyncio
    async def test_search_without_key(self):
        tool = WebSearchTool()
        result = await tool.execute(query="test")
        assert "not configured" in result

    def test_configurable_results_count(self):
        tool = WebSearchTool(api_key="key", results_count=10)
        assert tool._results_count == 10
