# databot

[![CI](https://github.com/asb108/databot/actions/workflows/ci.yml/badge.svg)](https://github.com/asb108/databot/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![PyPI](https://img.shields.io/pypi/v/databot.svg)](https://pypi.org/project/databot/)

A lightweight, open-source AI assistant for data platform operations.

**~4,000 lines of core code** -- built for data engineers who need an intelligent assistant
for monitoring pipelines, diagnosing data quality issues, and querying infrastructure.

## Features

- **SQL Queries**: Execute read-only queries against Clickzetta, TiDB, Trino, or any SQL database
- **Airflow Integration**: Check DAG status, view task logs, trigger runs via REST API
- **Data Quality**: Run DQ checks -- row counts, null rates, freshness, source-target comparison
- **Data Lineage**: Query upstream/downstream dependencies using NetworkX graphs
- **Scheduled Tasks**: Cron-based proactive monitoring with Google Chat alerts
- **Google Chat**: Webhook (send-only) and App (bidirectional) modes
- **Shell & Filesystem**: Execute commands, read/write files with workspace sandboxing
- **Multi-Provider LLM**: Anthropic, OpenAI, DeepSeek, Gemini, local vLLM via LiteLLM
- **Persistent Memory**: SQLite-backed sessions and key-value memory (zero external deps)

## Quick Start

### Install

```bash
# From PyPI
pip install databot

# From source (recommended for development)
git clone https://github.com/asb108/databot.git
cd databot
pip install -e ".[all]"
```

### Initialize

```bash
databot onboard
```

### Configure

Edit `~/.databot/config.yaml`:

```yaml
providers:
  default: anthropic
  anthropic:
    api_key: ${ANTHROPIC_API_KEY}
    model: claude-sonnet-4-5-20250929

channels:
  gchat:
    enabled: true
    mode: webhook
    webhook_url: ${GCHAT_WEBHOOK_URL}

tools:
  sql:
    connections:
      clickzetta:
        driver: clickzetta
        host: ${CZ_HOST}
        schema_name: data_warehouse
        virtual_cluster: ${CZ_VC}
    read_only: true
    max_rows: 1000

  airflow:
    base_url: ${AIRFLOW_URL}
    username: ${AIRFLOW_USER}
    password: ${AIRFLOW_PASSWORD}

security:
  restrict_to_workspace: true
  allowed_commands: ["kubectl", "airflow", "trino-cli"]
```

### Chat

```bash
# Single message
databot agent -m "How many rows in pricing.rate_cards?"

# Interactive mode
databot agent

# Start gateway (always-on with cron + Google Chat)
databot gateway
```

## Architecture

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

## Tools

| Tool | Description |
|------|-------------|
| `sql` | Execute SQL queries against configured databases |
| `airflow` | Check DAG status, view logs, trigger runs |
| `data_quality` | Row counts, null checks, freshness, source-target comparison |
| `lineage` | Upstream/downstream dependencies, path finding |
| `shell` | Execute shell commands (sandboxed) |
| `read_file` | Read file contents |
| `write_file` | Write/create files |
| `edit_file` | Find-and-replace edits |
| `list_dir` | List directory contents |
| `web_fetch` | Fetch URL content |
| `web_search` | Search the web (Brave API) |
| `cron` | Manage scheduled tasks |

## CLI Reference

| Command | Description |
|---------|-------------|
| `databot onboard` | Initialize config and workspace |
| `databot agent -m "..."` | Send a single message |
| `databot agent` | Interactive chat mode |
| `databot gateway` | Start always-on service (API + channels + cron) |
| `databot status` | Show status and configuration |
| `databot cron list` | List scheduled jobs |
| `databot cron add --name "..." --schedule "..." --message "..."` | Add a cron job |
| `databot cron remove --id "..."` | Remove a cron job |

## Docker

```bash
# Build
docker build -t databot .

# Initialize (first time)
docker run -v ~/.databot:/root/.databot --rm databot onboard

# Run gateway
docker run -v ~/.databot:/root/.databot -p 18790:18790 databot gateway
```

## Kubernetes

See the `k8s/` directory for example Kubernetes deployment manifests.

## Security

- **Read-only SQL by default**: Write operations blocked unless explicitly enabled
- **Workspace sandboxing**: Filesystem and shell restricted to workspace directory
- **Command allowlist**: Only whitelisted shell commands can execute
## Plugins

Databot supports plugins via Python entry points. Third-party packages can add custom tools, channels, and LLM providers.

### Creating a Plugin

1. Create a Python package with your custom tool:

```python
# my_databot_plugin/tools.py
from databot.tools.base import BaseTool

class MyCustomTool(BaseTool):
    @property
    def name(self) -> str:
        return "my_tool"
    
    @property
    def description(self) -> str:
        return "Description of what this tool does"
    
    def parameters(self):
        return {
            "type": "object",
            "properties": {
                "param1": {"type": "string", "description": "First parameter"}
            },
            "required": ["param1"]
        }
    
    async def execute(self, param1: str) -> str:
        return f"Executed with {param1}"
```

2. Register it in your `pyproject.toml`:

```toml
[project.entry-points."databot.tools"]
my_tool = "my_databot_plugin.tools:MyCustomTool"
```

3. Install your package and databot will auto-discover it.

### Entry Point Groups

| Group | Base Class | Description |
|-------|------------|-------------|
| `databot.tools` | `BaseTool` | Custom tools for the agent |
| `databot.channels` | `BaseChannel` | Messaging integrations (Slack, Discord, etc.) |
| `databot.providers` | `LLMProvider` | LLM provider adapters |

## License

MIT
