"""Tool base class and registry."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from loguru import logger


class BaseTool(ABC):
    """Base class for all tools."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique tool name."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description of what this tool does."""
        ...

    @abstractmethod
    def parameters(self) -> dict[str, Any]:
        """JSON Schema for the tool's parameters."""
        ...

    @abstractmethod
    async def execute(self, **kwargs: Any) -> str:
        """Execute the tool and return a string result."""
        ...

    def schema(self) -> dict[str, Any]:
        """Return the OpenAI-compatible tool schema."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters(),
            },
        }


class ToolRegistry:
    """Registry for managing tools."""

    def __init__(self):
        self._tools: dict[str, BaseTool] = {}

    def register(self, tool: BaseTool) -> None:
        """Register a tool."""
        self._tools[tool.name] = tool
        logger.debug(f"Registered tool: {tool.name}")

    def get(self, name: str) -> BaseTool | None:
        """Get a tool by name."""
        return self._tools.get(name)

    def get_definitions(self) -> list[dict[str, Any]]:
        """Get all tool definitions for the LLM."""
        return [tool.schema() for tool in self._tools.values()]

    async def execute(self, name: str, arguments: dict[str, Any]) -> str:
        """Execute a tool by name."""
        tool = self._tools.get(name)
        if not tool:
            return f"Error: Unknown tool '{name}'"

        try:
            result = await tool.execute(**arguments)
            return result
        except Exception as e:
            logger.error(f"Tool '{name}' failed: {e}")
            return f"Error executing tool '{name}': {str(e)}"

    def load_plugins(self, **kwargs: Any) -> int:
        """Load and register tools from entry points.

        Args:
            **kwargs: Arguments passed to tool constructors (e.g., workspace, config).

        Returns:
            Number of plugins loaded.
        """
        from databot.plugins.loader import discover_tools

        loaded = 0
        for tool_class in discover_tools():
            try:
                # Try to instantiate with kwargs, fall back to no-args
                try:
                    tool = tool_class(**kwargs)
                except TypeError:
                    tool = tool_class()
                self.register(tool)
                loaded += 1
            except Exception as e:
                logger.warning(f"Failed to instantiate tool plugin {tool_class}: {e}")

        if loaded > 0:
            logger.info(f"Loaded {loaded} tool plugin(s)")
        return loaded

    @property
    def tool_names(self) -> list[str]:
        return list(self._tools.keys())
