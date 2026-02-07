"""Multi-agent framework — Router/Delegator pattern for domain-specific agents.

Architecture
------------
1. **Router** — examines the user's message and decides which *specialist*
   agent should handle it (SQL, Pipeline, Quality, Catalog, Streaming,
   General).
2. **Specialist agents** — each wraps a subset of tools and a tailored system
   prompt so the LLM has the right context for the domain.
3. **Delegator** — orchestrates multi-step workflows that span several
   specialists (e.g., "find the table in the catalog, then run a quality
   check, then query it with SQL").
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from databot.providers.base import LLMProvider
from databot.tools.base import ToolRegistry


# ---------------------------------------------------------------------------
# Specialist Agent
# ---------------------------------------------------------------------------

@dataclass
class AgentSpec:
    """Specification for a specialist agent."""

    name: str
    description: str
    system_prompt: str
    tool_names: list[str] = field(default_factory=list)
    # If empty, agent has access to all tools in the registry
    priority: int = 0  # Lower = higher priority for tie-breaking


class SpecialistAgent:
    """A specialist agent scoped to a subset of tools and a domain prompt."""

    def __init__(
        self,
        spec: AgentSpec,
        provider: LLMProvider,
        tools: ToolRegistry,
        model: str = "",
        max_iterations: int = 10,
    ):
        self.spec = spec
        self._provider = provider
        self._all_tools = tools
        self._model = model or provider.get_default_model()
        self._max_iterations = max_iterations

    def _get_tool_definitions(self) -> list[dict[str, Any]]:
        """Get tool definitions filtered to this agent's scope."""
        all_defs = self._all_tools.get_definitions()
        if not self.spec.tool_names:
            return all_defs
        return [d for d in all_defs if d["function"]["name"] in self.spec.tool_names]

    async def run(
        self,
        user_message: str,
        history: list[dict[str, Any]] | None = None,
        extra_context: str = "",
    ) -> str:
        """Run the specialist agent on a message. Returns the final response."""
        messages: list[dict[str, Any]] = []

        # System prompt
        sys_prompt = self.spec.system_prompt
        if extra_context:
            sys_prompt += f"\n\nAdditional context:\n{extra_context}"
        messages.append({"role": "system", "content": sys_prompt})

        # History
        if history:
            messages.extend(history)

        # User message
        messages.append({"role": "user", "content": user_message})

        tool_defs = self._get_tool_definitions() or None

        for iteration in range(self._max_iterations):
            response = await self._provider.chat(
                messages=messages,
                tools=tool_defs,
                model=self._model,
            )

            if response.has_tool_calls:
                # Add assistant message
                tc_dicts = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.name,
                            "arguments": json.dumps(tc.arguments),
                        },
                    }
                    for tc in response.tool_calls
                ]
                messages.append({
                    "role": "assistant",
                    "content": response.content,
                    "tool_calls": tc_dicts,
                })

                # Execute tools
                for tc in response.tool_calls:
                    result = await self._all_tools.execute(tc.name, tc.arguments)
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "name": tc.name,
                        "content": result,
                    })
            else:
                return response.content or ""

        return "Agent reached maximum iterations."


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

# Default routing prompt
_ROUTER_SYSTEM_PROMPT = """\
You are a routing agent. Your job is to analyze the user's message and decide
which specialist agent should handle it. Respond with ONLY a JSON object:

{
  "agent": "<agent_name>",
  "reasoning": "<brief explanation>"
}

Available agents:
{agent_descriptions}

If the message is ambiguous or spans multiple domains, choose the most
relevant primary agent. If no specialist fits, use "general".
"""


class Router:
    """Routes incoming messages to the appropriate specialist agent."""

    def __init__(
        self,
        agents: dict[str, SpecialistAgent],
        provider: LLMProvider,
        model: str = "",
    ):
        self._agents = agents
        self._provider = provider
        self._model = model or provider.get_default_model()
        self._default_agent = "general"

    async def route(self, user_message: str, history: list[dict[str, Any]] | None = None) -> str:
        """Determine which agent should handle the message. Returns agent name."""
        agent_desc = "\n".join(
            f"- {name}: {agent.spec.description}"
            for name, agent in self._agents.items()
        )
        sys_prompt = _ROUTER_SYSTEM_PROMPT.replace("{agent_descriptions}", agent_desc)

        messages = [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": user_message},
        ]

        try:
            response = await self._provider.chat(messages=messages, model=self._model)
            content = (response.content or "").strip()

            # Try to parse JSON
            if content.startswith("{"):
                data = json.loads(content)
                agent_name = data.get("agent", self._default_agent)
            else:
                # Fallback: look for agent name in text
                agent_name = self._default_agent
                for name in self._agents:
                    if name.lower() in content.lower():
                        agent_name = name
                        break

            if agent_name not in self._agents:
                logger.warning(f"Router selected unknown agent '{agent_name}', using default")
                agent_name = self._default_agent

            logger.info(f"Router → {agent_name} for: {user_message[:80]}")
            return agent_name

        except Exception as e:
            logger.error(f"Router failed: {e}, defaulting to {self._default_agent}")
            return self._default_agent


# ---------------------------------------------------------------------------
# Delegator (Multi-Agent Orchestrator)
# ---------------------------------------------------------------------------

class Delegator:
    """Orchestrates multi-agent workflows.

    Combines the Router with specialist agents. Can be used as a drop-in
    replacement for the simple AgentLoop when multi-agent routing is desired.
    """

    def __init__(
        self,
        agents: dict[str, SpecialistAgent],
        router: Router,
    ):
        self._agents = agents
        self._router = router

    async def handle(
        self,
        user_message: str,
        history: list[dict[str, Any]] | None = None,
        extra_context: str = "",
    ) -> str:
        """Route and handle a user message. Returns the specialist's response."""
        agent_name = await self._router.route(user_message, history)
        agent = self._agents[agent_name]

        logger.info(f"Delegating to '{agent_name}': {user_message[:80]}")
        response = await agent.run(user_message, history, extra_context)
        return response

    async def handle_with_metadata(
        self,
        user_message: str,
        history: list[dict[str, Any]] | None = None,
        extra_context: str = "",
    ) -> dict[str, Any]:
        """Like handle() but returns metadata about routing."""
        agent_name = await self._router.route(user_message, history)
        agent = self._agents[agent_name]

        response = await agent.run(user_message, history, extra_context)
        return {
            "agent": agent_name,
            "response": response,
            "agent_description": agent.spec.description,
        }


# ---------------------------------------------------------------------------
# Default agent specs for data platform operations
# ---------------------------------------------------------------------------

DEFAULT_AGENT_SPECS: dict[str, AgentSpec] = {
    "sql": AgentSpec(
        name="sql",
        description="Handles SQL queries, database schema exploration, and data analysis.",
        system_prompt=(
            "You are a SQL expert agent. Help users write and execute SQL queries, "
            "explore database schemas, and analyze query results. Always validate SQL "
            "safety and suggest optimizations when possible."
        ),
        tool_names=["sql"],
    ),
    "pipeline": AgentSpec(
        name="pipeline",
        description="Manages data pipelines, Airflow DAGs, Spark jobs, and orchestration.",
        system_prompt=(
            "You are a data pipeline expert. Help users manage Airflow DAGs, submit "
            "and monitor Spark jobs, and orchestrate data workflows. Provide "
            "actionable guidance on pipeline debugging and optimization."
        ),
        tool_names=["airflow", "spark", "shell"],
    ),
    "quality": AgentSpec(
        name="quality",
        description="Handles data quality checks, validation rules, and monitoring.",
        system_prompt=(
            "You are a data quality expert. Help users define and run data quality "
            "checks, set up monitoring rules, and diagnose data issues. Use SQL "
            "queries to validate data when needed."
        ),
        tool_names=["data_quality", "sql"],
    ),
    "catalog": AgentSpec(
        name="catalog",
        description="Browses data catalogs, discovers schemas, and manages metadata.",
        system_prompt=(
            "You are a data catalog expert. Help users discover tables, explore "
            "schemas, search across data catalogs, and understand table relationships. "
            "You can query Iceberg, Glue, and Unity Catalog."
        ),
        tool_names=["catalog", "lineage"],
    ),
    "streaming": AgentSpec(
        name="streaming",
        description="Manages Kafka topics, consumer groups, schemas, and streaming pipelines.",
        system_prompt=(
            "You are a streaming data expert. Help users manage Kafka topics, "
            "monitor consumer lag, query Schema Registry, and manage Kafka Connect "
            "connectors. Provide guidance on streaming architecture."
        ),
        tool_names=["kafka"],
    ),
    "general": AgentSpec(
        name="general",
        description="General-purpose assistant for questions that don't fit other specialists.",
        system_prompt=(
            "You are a helpful data platform assistant. Answer general questions, "
            "provide guidance, and help users navigate the data platform. You have "
            "access to all tools."
        ),
        tool_names=[],  # All tools
    ),
}


def build_default_agents(
    provider: LLMProvider,
    tools: ToolRegistry,
    model: str = "",
    specs: dict[str, AgentSpec] | None = None,
) -> tuple[dict[str, SpecialistAgent], Router, Delegator]:
    """Build the default multi-agent system.

    Returns (agents_dict, router, delegator).
    """
    specs = specs or DEFAULT_AGENT_SPECS

    agents = {
        name: SpecialistAgent(spec, provider, tools, model)
        for name, spec in specs.items()
    }

    router = Router(agents, provider, model)
    delegator = Delegator(agents, router)

    return agents, router, delegator
