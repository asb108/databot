from __future__ import annotations
"""Example plugin tool for databot.

This demonstrates how to create a custom tool that can be installed
as a plugin via Python entry points.

To use this as a plugin:

1. Install as editable: pip install -e examples/example_plugin
2. The tool will be auto-discovered by databot
"""

from databot.tools.base import BaseTool


class HelloWorldTool(BaseTool):
    """An example tool that greets the user."""

    @property
    def name(self) -> str:
        return "hello_world"

    @property
    def description(self) -> str:
        return "A simple example tool that greets the user by name."

    def parameters(self):
        return {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "The name to greet",
                },
            },
            "required": ["name"],
        }

    async def execute(self, name: str) -> str:
        return f"Hello, {name}! Welcome to databot."
