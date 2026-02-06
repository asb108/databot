"""Web tools: search and fetch."""
from __future__ import annotations

from typing import Any

import httpx

from databot.tools.base import BaseTool


class WebFetchTool(BaseTool):
    """Fetch content from a URL."""

    @property
    def name(self) -> str:
        return "web_fetch"

    @property
    def description(self) -> str:
        return "Fetch content from a URL and return it as text."

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "The URL to fetch."},
            },
            "required": ["url"],
        }

    async def execute(self, url: str) -> str:
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                resp = await client.get(url, headers={"User-Agent": "databot/0.1"})
                resp.raise_for_status()

                content = resp.text
                max_len = 15000
                if len(content) > max_len:
                    content = content[:max_len] + "\n... (truncated)"
                return content
        except Exception as e:
            return f"Error fetching URL: {str(e)}"


class WebSearchTool(BaseTool):
    """Search the web using Brave Search API."""

    def __init__(self, api_key: str | None = None):
        self._api_key = api_key

    @property
    def name(self) -> str:
        return "web_search"

    @property
    def description(self) -> str:
        return "Search the web for information. Returns a summary of search results."

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The search query."},
            },
            "required": ["query"],
        }

    async def execute(self, query: str) -> str:
        if not self._api_key:
            return (
                "Error: Web search API key not configured. "
                "Set tools.web.search_api_key in config."
            )

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    "https://api.search.brave.com/res/v1/web/search",
                    params={"q": query, "count": 5},
                    headers={
                        "Accept": "application/json",
                        "X-Subscription-Token": self._api_key,
                    },
                )
                resp.raise_for_status()
                data = resp.json()

                results = []
                for item in data.get("web", {}).get("results", [])[:5]:
                    results.append(
                        f"**{item.get('title', 'No title')}**\n"
                        f"{item.get('url', '')}\n"
                        f"{item.get('description', 'No description')}\n"
                    )

                if not results:
                    return "No search results found."
                return "\n---\n".join(results)
        except Exception as e:
            return f"Error searching: {str(e)}"
