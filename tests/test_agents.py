"""Tests for the multi-agent framework."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

from databot.agents import (
    AgentSpec,
    Delegator,
    Router,
    SpecialistAgent,
    build_default_agents,
)
from databot.providers.base import LLMProvider, LLMResponse, ToolCall
from databot.tools.base import ToolRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class MockProvider(LLMProvider):
    """Provider that returns preconfigured responses."""

    def __init__(self, responses: list[LLMResponse] | None = None):
        self._responses = list(responses or [])
        self._call_count = 0

    async def chat(self, messages, tools=None, model=None) -> LLMResponse:
        if self._call_count < len(self._responses):
            resp = self._responses[self._call_count]
        else:
            resp = LLMResponse(content="default response")
        self._call_count += 1
        return resp

    def get_default_model(self) -> str:
        return "mock-model"


# ---------------------------------------------------------------------------
# AgentSpec
# ---------------------------------------------------------------------------


class TestAgentSpec:
    def test_defaults(self):
        spec = AgentSpec(name="test", description="desc", system_prompt="prompt")
        assert spec.tool_names == []
        assert spec.priority == 0

    def test_custom(self):
        spec = AgentSpec(
            name="sql",
            description="SQL agent",
            system_prompt="You are a SQL expert.",
            tool_names=["sql"],
            priority=1,
        )
        assert spec.tool_names == ["sql"]


# ---------------------------------------------------------------------------
# SpecialistAgent
# ---------------------------------------------------------------------------


class TestSpecialistAgent:
    @pytest.mark.asyncio
    async def test_simple_run(self):
        provider = MockProvider([LLMResponse(content="I can help with SQL!")])
        spec = AgentSpec(name="sql", description="SQL", system_prompt="You are a SQL agent.")
        tools = ToolRegistry()
        agent = SpecialistAgent(spec, provider, tools)

        result = await agent.run("Show me all tables")
        assert "SQL" in result

    @pytest.mark.asyncio
    async def test_run_with_tool_calls(self):
        """Agent that makes a tool call, then responds."""
        provider = MockProvider([
            LLMResponse(
                content=None,
                tool_calls=[ToolCall(id="tc1", name="sql", arguments={"query": "SELECT 1"})],
            ),
            LLMResponse(content="The result is 1."),
        ])
        spec = AgentSpec(name="sql", description="SQL", system_prompt="sys", tool_names=["sql"])
        tools = ToolRegistry()

        # Mock a tool
        from databot.tools.base import BaseTool

        class FakeSQLTool(BaseTool):
            @property
            def name(self):
                return "sql"

            @property
            def description(self):
                return "SQL tool"

            def parameters(self):
                return {"type": "object", "properties": {}}

            async def execute(self, **kwargs):
                return "| col |\n| --- |\n| 1 |"

        tools.register(FakeSQLTool())
        agent = SpecialistAgent(spec, provider, tools)

        result = await agent.run("Run SELECT 1")
        assert "result is 1" in result.lower() or "1" in result

    @pytest.mark.asyncio
    async def test_tool_filtering(self):
        """Agent only gets tools it's scoped to."""
        from databot.tools.base import BaseTool as BT

        spec = AgentSpec(name="sql", description="SQL", system_prompt="sys", tool_names=["sql"])
        provider = MockProvider([LLMResponse(content="ok")])
        tools = ToolRegistry()

        class FakeTool1(BT):
            @property
            def name(self):
                return "sql"

            @property
            def description(self):
                return "sql"

            def parameters(self):
                return {"type": "object", "properties": {}}

            async def execute(self, **kwargs):
                return "ok"

        class FakeTool2(BT):
            @property
            def name(self):
                return "shell"

            @property
            def description(self):
                return "shell"

            def parameters(self):
                return {"type": "object", "properties": {}}

            async def execute(self, **kwargs):
                return "ok"

        tools.register(FakeTool1())
        tools.register(FakeTool2())

        agent = SpecialistAgent(spec, provider, tools)
        defs = agent._get_tool_definitions()
        assert len(defs) == 1
        assert defs[0]["function"]["name"] == "sql"


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------


class TestRouter:
    @pytest.mark.asyncio
    async def test_routes_to_correct_agent(self):
        """Router returns a valid agent name from JSON response."""
        provider = MockProvider([
            LLMResponse(content='{"agent": "sql", "reasoning": "user asked SQL"}')
        ])
        agents = {
            "sql": SpecialistAgent(
                AgentSpec(name="sql", description="SQL", system_prompt="sys"),
                provider,
                ToolRegistry(),
            ),
            "general": SpecialistAgent(
                AgentSpec(name="general", description="General", system_prompt="sys"),
                provider,
                ToolRegistry(),
            ),
        }
        router = Router(agents, provider)
        result = await router.route("Show me all tables")
        assert result == "sql"

    @pytest.mark.asyncio
    async def test_falls_back_to_general(self):
        """Unknown agent name falls back to general."""
        provider = MockProvider([
            LLMResponse(content='{"agent": "unknown_agent"}')
        ])
        agents = {
            "general": SpecialistAgent(
                AgentSpec(name="general", description="General", system_prompt="sys"),
                provider,
                ToolRegistry(),
            ),
        }
        router = Router(agents, provider)
        result = await router.route("something weird")
        assert result == "general"

    @pytest.mark.asyncio
    async def test_handles_non_json_response(self):
        """Router handles plain text response gracefully."""
        provider = MockProvider([
            LLMResponse(content="I think the sql agent should handle this")
        ])
        agents = {
            "sql": SpecialistAgent(
                AgentSpec(name="sql", description="SQL", system_prompt="sys"),
                provider,
                ToolRegistry(),
            ),
            "general": SpecialistAgent(
                AgentSpec(name="general", description="General", system_prompt="sys"),
                provider,
                ToolRegistry(),
            ),
        }
        router = Router(agents, provider)
        result = await router.route("query")
        assert result == "sql"


# ---------------------------------------------------------------------------
# Delegator
# ---------------------------------------------------------------------------


class TestDelegator:
    @pytest.mark.asyncio
    async def test_handle(self):
        """Delegator routes and gets response."""
        provider = MockProvider([
            # Router response
            LLMResponse(content='{"agent": "general"}'),
            # Agent response
            LLMResponse(content="Hello! How can I help?"),
        ])
        agents = {
            "general": SpecialistAgent(
                AgentSpec(name="general", description="General", system_prompt="sys"),
                provider,
                ToolRegistry(),
            ),
        }
        router = Router(agents, provider)
        delegator = Delegator(agents, router)

        result = await delegator.handle("hi")
        assert "help" in result.lower() or "hello" in result.lower()

    @pytest.mark.asyncio
    async def test_handle_with_metadata(self):
        provider = MockProvider([
            LLMResponse(content='{"agent": "general"}'),
            LLMResponse(content="response text"),
        ])
        agents = {
            "general": SpecialistAgent(
                AgentSpec(name="general", description="General assistant", system_prompt="sys"),
                provider,
                ToolRegistry(),
            ),
        }
        router = Router(agents, provider)
        delegator = Delegator(agents, router)

        result = await delegator.handle_with_metadata("hi")
        assert result["agent"] == "general"
        assert result["response"] == "response text"


# ---------------------------------------------------------------------------
# build_default_agents
# ---------------------------------------------------------------------------


class TestBuildDefaults:
    def test_build_default_agents(self):
        provider = MockProvider()
        tools = ToolRegistry()
        agents, router, delegator = build_default_agents(provider, tools)

        assert "sql" in agents
        assert "pipeline" in agents
        assert "quality" in agents
        assert "catalog" in agents
        assert "streaming" in agents
        assert "general" in agents
        assert isinstance(router, Router)
        assert isinstance(delegator, Delegator)
