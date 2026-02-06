# Contributing to databot

Thanks for your interest in contributing to databot! This document provides
guidelines for contributing to the project.

## Development Setup

```bash
# Clone the repository
git clone https://github.com/asb108/databot.git
cd databot

# Install in development mode with all extras
pip install -e ".[all]"

# Run tests
pytest

# Run linter
ruff check databot/
ruff format databot/
```

## Project Structure

```
databot/
  cli/          # Typer CLI commands
  config/       # Pydantic config schema + YAML loader
  core/         # Agent loop, message bus, context builder
  providers/    # LLM provider abstraction (LiteLLM)
  tools/        # Pluggable tools (SQL, Airflow, DQ, lineage, shell, fs, web, cron)
  channels/     # Messaging channels (Google Chat, CLI)
  session/      # SQLite-backed conversation history
  memory/       # Persistent key-value memory
  cron/         # Scheduled task execution
```

## Adding a New Tool

Tools are the primary extension point. To add a new tool:

1. Create a new file in `databot/tools/` (e.g., `databot/tools/my_tool.py`)
2. Subclass `BaseTool` from `databot.tools.base`
3. Implement the required properties and methods:

```python
from typing import Any
from databot.tools.base import BaseTool

class MyTool(BaseTool):
    @property
    def name(self) -> str:
        return "my_tool"

    @property
    def description(self) -> str:
        return "Description of what this tool does."

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "param1": {"type": "string", "description": "..."},
            },
            "required": ["param1"],
        }

    async def execute(self, param1: str) -> str:
        # Your tool logic here
        return "result"
```

4. Register the tool in `databot/cli/commands.py` in the `_register_tools` function.
5. Add tests in `tests/`.

## Adding a New Channel

1. Create a new file in `databot/channels/` (e.g., `databot/channels/slack.py`)
2. Subclass `BaseChannel` from `databot.channels.base`
3. Implement `start()`, `stop()`, and `send()` methods
4. Wire it into the gateway in `databot/cli/commands.py`

## Code Style

- Python 3.11+ with type hints
- Formatted with `ruff format`
- Linted with `ruff check`
- Line length: 100 characters
- Async-first (use `async def` for tool execution)

## Pull Request Process

1. Fork the repository and create a feature branch
2. Write tests for new functionality
3. Ensure `ruff check` and `pytest` pass
4. Open a PR with a clear description of the changes
5. Reference any related issues

## Commit Messages

Follow conventional commits:

- `feat: add Slack channel support`
- `fix: handle empty SQL results gracefully`
- `docs: update README with Docker instructions`
- `refactor: simplify agent loop error handling`
- `test: add tests for SQL tool read-only enforcement`
