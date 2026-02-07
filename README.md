# databot

[![CI](https://github.com/asb108/databot/actions/workflows/ci.yml/badge.svg)](https://github.com/asb108/databot/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![PyPI](https://img.shields.io/pypi/v/databot-ai.svg)](https://pypi.org/project/databot-ai/)

A lightweight, open-source AI agent platform for data engineering and platform operations.

**~8,000 lines of core code** -- built for data engineers who need an intelligent assistant
for monitoring pipelines, diagnosing data quality issues, querying infrastructure, and
managing the full big-data stack.

#### Deepwiki: https://deepwiki.com/asb108/databot/1-overview

## Features

### Core
- **Multi-Provider LLM**: Anthropic, OpenAI, DeepSeek, Gemini, local vLLM via LiteLLM
- **Streaming Responses**: Token-by-token streaming via SSE for real-time feedback
- **Persistent Memory**: SQLite-backed sessions and key-value memory (zero external deps)
- **Plugin System**: Extend with custom tools, channels, and providers via Python entry points

### Data Tools
- **SQL Queries**: Execute read-only queries against MySQL, Trino, Presto, ClickHouse, StarRocks, Hive, and more
- **Airflow Integration**: Check DAG status, view task logs, trigger runs via REST API
- **Data Quality**: Row counts, null checks, freshness, source-target comparison with SQL injection protection
- **Data Lineage**: Upstream/downstream dependencies via NetworkX graphs or Marquez REST API
- **Spark Management**: Submit batch jobs, manage interactive sessions via Livy/YARN/K8s
- **Kafka Ecosystem**: Topics, consumer groups, Schema Registry, Kafka Connect management
- **Data Catalog**: Browse Iceberg REST, AWS Glue, or Databricks Unity Catalog

### Connectors
- **Connector Framework**: Unified `BaseConnector` abstraction for SQL, REST, Spark, Kafka, and Catalog
- **Connector Registry**: Centralized lifecycle management — auto-discovery, health checks, connect/disconnect
- **Connector Factory**: Declarative connector instantiation from YAML config

### Intelligence
- **Multi-Agent Architecture**: Router + Delegator pattern with 6 specialist agents (SQL, Pipeline, Quality, Catalog, Streaming, General)
- **RAG (Retrieval-Augmented Generation)**: ChromaDB-backed vector store for schema-aware, context-grounded answers
- **MCP Server**: Expose tools and connectors via Model Context Protocol for Claude Desktop, Cursor, and VS Code

### Channels & Gateway
- **Google Chat**: Webhook (send-only) and App (bidirectional) modes
- **Slack**: Bot with slash commands and thread-aware conversations
- **Discord**: Bot with prefix commands
- **REST Gateway**: FastAPI gateway with auth middleware, rate limiting, SSE streaming endpoint
- **Scheduled Tasks**: Cron-based proactive monitoring with channel alerts

### Operations
- **Observability**: OpenTelemetry tracing for tool calls and LLM interactions
- **Security**: Read-only SQL, workspace sandboxing, command allowlist, API key auth, rate limiting
- **Shell & Filesystem**: Execute commands, read/write files with workspace sandboxing

## Quick Start

### Install

```bash
# From PyPI
pip install databot-ai

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
  agents/       # Multi-agent framework (Router, Delegator, Specialists)
  channels/     # Messaging channels (CLI, Google Chat, Slack, Discord)
  cli/          # Typer CLI commands
  config/       # Pydantic config schema + YAML loader
  connectors/   # Connector framework (SQL, REST, Spark, Kafka, Catalog)
  core/         # Agent loop, message bus, context builder, streaming
  cron/         # Scheduled task execution
  mcp/          # MCP server (Model Context Protocol)
  memory/       # Persistent key-value memory
  middleware/   # Gateway middleware (API key auth, rate limiting)
  observability/# OpenTelemetry tracing
  plugins/      # Plugin discovery and loading
  providers/    # LLM provider abstraction (LiteLLM)
  rag/          # RAG module (ChromaDB vector store)
  session/      # SQLite-backed conversation history
  tools/        # Pluggable tools (SQL, Airflow, DQ, lineage, spark, kafka, catalog, ...)
```

## Tools

| Tool | Description |
|------|-------------|
| `sql` | Execute SQL queries against configured databases (connector-backed) |
| `airflow` | Check DAG status, view logs, trigger runs (connector-backed) |
| `data_quality` | Row counts, null checks, freshness, source-target comparison |
| `lineage` | Upstream/downstream dependencies via graphs or Marquez API |
| `spark` | Submit batch jobs, manage sessions via Livy/YARN/K8s connectors |
| `kafka` | Topics, consumer groups, Schema Registry, Kafka Connect |
| `catalog` | Browse Iceberg REST, AWS Glue, Unity Catalog |
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
| `databot mcp` | Start MCP server (stdio transport, for Claude Desktop / Cursor) |
| `databot mcp --transport sse` | Start MCP server over HTTP (SSE transport) |
| `databot status` | Show status and configuration |
| `databot cron list` | List scheduled jobs |
| `databot cron add --name "..." --schedule "..." --message "..."` | Add a cron job |
| `databot cron remove --id "..."` | Remove a cron job |

## MCP Server

Databot implements the [Model Context Protocol](https://modelcontextprotocol.io/) so external LLM clients can discover and invoke its tools.

### Claude Desktop / Cursor

Add to your MCP config (`claude_desktop_config.json` or Cursor settings):

```json
{
  "mcpServers": {
    "databot": {
      "command": "databot",
      "args": ["mcp"]
    }
  }
}
```

### SSE (HTTP) Transport

```bash
databot mcp --transport sse --port 18791
```

### Exposed Resources

- All registered tools are exposed as MCP tools
- Each connector is exposed as an MCP resource (`connector://<name>`)
- Health overview available at `databot://health`

## Connector Configuration

Connectors are declared in `config.yaml` under the `connectors` key:

```yaml
connectors:
  instances:
    my_warehouse:
      type: sql
      driver: trino
      host: trino.internal
      port: 8080
      catalog: hive
      schema_name: analytics
    airflow:
      type: rest_api
      base_url: http://airflow:8080/api/v1
      auth:
        type: basic
        username: ${AIRFLOW_USER}
        password: ${AIRFLOW_PASSWORD}
    spark_livy:
      type: spark
      mode: livy
      base_url: http://livy:8998
    kafka_prod:
      type: kafka
      base_url: http://kafka-rest:8082
      schema_registry_url: http://schema-registry:8081
      connect_url: http://kafka-connect:8083
    iceberg:
      type: catalog
      protocol: iceberg
      base_url: http://iceberg-rest:8181
```

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
- **API Key Authentication**: Gateway endpoints protected with Bearer / X-API-Key auth
- **Rate Limiting**: Configurable per-IP request rate limiting with sliding window
- **SQL Injection Protection**: Identifier validation on data quality checks
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
