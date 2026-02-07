"""Configuration schema for databot."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


def _resolve_env_vars(value: Any) -> Any:
    """Resolve ${ENV_VAR} patterns in string values."""
    if isinstance(value, str):
        pattern = re.compile(r"\$\{([^}]+)\}")

        def replacer(match):
            env_var = match.group(1)
            return os.environ.get(env_var, match.group(0))

        return pattern.sub(replacer, value)
    elif isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_resolve_env_vars(v) for v in value]
    return value


# ---------------------------------------------------------------------------
# Provider configuration
# ---------------------------------------------------------------------------


class ProviderConfig(BaseModel):
    api_key: str = ""
    model: str = ""
    api_base: str | None = None


class ProvidersConfig(BaseModel):
    default: str = "anthropic"
    anthropic: ProviderConfig = Field(
        default_factory=lambda: ProviderConfig(model="claude-sonnet-4-5-20250929")
    )
    openai: ProviderConfig = Field(default_factory=lambda: ProviderConfig(model="gpt-4o"))
    deepseek: ProviderConfig = Field(default_factory=lambda: ProviderConfig(model="deepseek-chat"))
    custom: dict[str, ProviderConfig] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Channel configuration
# ---------------------------------------------------------------------------


class GChatConfig(BaseModel):
    enabled: bool = False
    mode: str = "webhook"  # "webhook" or "app"
    webhook_url: str = ""
    project_id: str = ""
    service_account: str = ""


class SlackConfig(BaseModel):
    enabled: bool = False
    bot_token: str = ""
    app_token: str = ""
    signing_secret: str = ""
    default_channel: str = ""


class DiscordConfig(BaseModel):
    enabled: bool = False
    bot_token: str = ""
    command_prefix: str = "!"


class ChannelsConfig(BaseModel):
    gchat: GChatConfig = Field(default_factory=GChatConfig)
    slack: SlackConfig = Field(default_factory=SlackConfig)
    discord: DiscordConfig = Field(default_factory=DiscordConfig)


# ---------------------------------------------------------------------------
# Tool configuration
# ---------------------------------------------------------------------------


class SQLConnectionConfig(BaseModel):
    driver: str = "mysql"
    host: str = ""
    port: int = 3306
    database: str = ""
    schema_name: str = ""
    username: str = ""
    password: str = ""
    virtual_cluster: str = ""
    extra: dict[str, str] = Field(default_factory=dict)


class SQLToolConfig(BaseModel):
    connections: dict[str, SQLConnectionConfig] = Field(default_factory=dict)
    read_only: bool = True
    max_rows: int = 1000


class AirflowToolConfig(BaseModel):
    base_url: str = ""
    username: str = ""
    password: str = ""


class LineageToolConfig(BaseModel):
    graph_path: str = ""
    marquez_url: str = ""


class ShellToolConfig(BaseModel):
    enabled: bool = True
    restrict_to_workspace: bool = True
    timeout: int = 30
    allowed_commands: list[str] = Field(default_factory=list)
    max_output_length: int = 10000


class WebToolConfig(BaseModel):
    search_api_key: str = ""
    search_results_count: int = 5
    max_fetch_length: int = 15000


class DataQualityToolConfig(BaseModel):
    enabled: bool = True


class ToolsConfig(BaseModel):
    sql: SQLToolConfig = Field(default_factory=SQLToolConfig)
    airflow: AirflowToolConfig = Field(default_factory=AirflowToolConfig)
    lineage: LineageToolConfig = Field(default_factory=LineageToolConfig)
    shell: ShellToolConfig = Field(default_factory=ShellToolConfig)
    web: WebToolConfig = Field(default_factory=WebToolConfig)
    data_quality: DataQualityToolConfig = Field(default_factory=DataQualityToolConfig)


# ---------------------------------------------------------------------------
# Connector configuration
# ---------------------------------------------------------------------------


class ConnectorConfig(BaseModel):
    """Configuration for a single connector instance."""

    type: str = "sql"
    driver: str = ""
    host: str = ""
    port: int = 0
    database: str = ""
    catalog: str = ""
    schema_name: str = ""
    username: str = ""
    password: str = ""
    base_url: str = ""
    auth: dict[str, str] = Field(default_factory=dict)
    extra: dict[str, Any] = Field(default_factory=dict)

    # Spark-specific
    mode: str = ""  # livy | yarn | k8s
    namespace: str = ""
    image: str = ""
    spark_version: str = ""
    default_conf: dict[str, str] = Field(default_factory=dict)

    # Kafka-specific
    schema_registry_url: str = ""
    connect_url: str = ""

    # Catalog-specific
    protocol: str = ""  # iceberg | glue | unity
    region: str = ""
    workspace_url: str = ""
    access_token: str = ""

    # General options
    timeout: int = 30
    read_only: bool = True
    max_rows: int = 1000
    retry_on: list[int] = Field(default_factory=lambda: [429, 502, 503, 504])
    max_retries: int = 3

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dict suitable for passing to the connector factory."""
        return self.model_dump(exclude_defaults=False)


class ConnectorsConfig(BaseModel):
    """Named connector instances. Keys are connector names."""

    instances: dict[str, ConnectorConfig] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# RAG configuration
# ---------------------------------------------------------------------------


class RAGConfig(BaseModel):
    enabled: bool = False
    persist_directory: str = ""
    collection_name: str = "databot"
    embedding_model: str = ""
    api_key: str = ""
    max_context_docs: int = 5
    max_context_chars: int = 4000


# ---------------------------------------------------------------------------
# Observability configuration
# ---------------------------------------------------------------------------


class ObservabilityConfig(BaseModel):
    enabled: bool = False
    service_name: str = "databot"
    otlp_endpoint: str = ""


# ---------------------------------------------------------------------------
# Multi-agent configuration
# ---------------------------------------------------------------------------


class MultiAgentConfig(BaseModel):
    enabled: bool = False


# ---------------------------------------------------------------------------
# Cron configuration
# ---------------------------------------------------------------------------


class CronJobConfig(BaseModel):
    name: str
    schedule: str
    message: str
    channel: str = "gchat"
    enabled: bool = True


class CronConfig(BaseModel):
    jobs: list[CronJobConfig] = Field(default_factory=list)
    check_interval_seconds: int = 30


# ---------------------------------------------------------------------------
# Security configuration
# ---------------------------------------------------------------------------


class SecurityConfig(BaseModel):
    restrict_to_workspace: bool = True
    allowed_commands: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Gateway configuration
# ---------------------------------------------------------------------------


class GatewayConfig(BaseModel):
    port: int = 18790
    host: str = "0.0.0.0"
    api_keys: list[str] = Field(
        default_factory=list,
        description="API keys for gateway authentication. Empty list = no auth (open mode).",
    )
    rate_limit_rpm: int = 60
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])


# ---------------------------------------------------------------------------
# Agent configuration
# ---------------------------------------------------------------------------


class AgentConfig(BaseModel):
    max_iterations: int = 20
    max_session_messages: int = 50
    system_prompt: str = ""
    retry_attempts: int = 3
    retry_delay_seconds: float = 1.0
    tool_approval_required: list[str] = Field(
        default_factory=list,
        description="Tool names that require human approval before execution.",
    )


# ---------------------------------------------------------------------------
# Root configuration
# ---------------------------------------------------------------------------


class DatabotConfig(BaseModel):
    providers: ProvidersConfig = Field(default_factory=ProvidersConfig)
    channels: ChannelsConfig = Field(default_factory=ChannelsConfig)
    tools: ToolsConfig = Field(default_factory=ToolsConfig)
    connectors: ConnectorsConfig = Field(default_factory=ConnectorsConfig)
    rag: RAGConfig = Field(default_factory=RAGConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    multi_agent: MultiAgentConfig = Field(default_factory=MultiAgentConfig)
    cron: CronConfig = Field(default_factory=CronConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    agent: AgentConfig = Field(default_factory=AgentConfig)

    @classmethod
    def load(cls, path: Path | None = None) -> "DatabotConfig":
        """Load configuration from YAML file with env var resolution."""
        if path is None:
            path = Path.home() / ".databot" / "config.yaml"

        if not path.exists():
            return cls()

        with open(path) as f:
            raw = yaml.safe_load(f) or {}

        resolved = _resolve_env_vars(raw)
        return cls.model_validate(resolved)

    def save(self, path: Path | None = None) -> None:
        """Save configuration to YAML file."""
        if path is None:
            path = Path.home() / ".databot" / "config.yaml"

        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(
                self.model_dump(exclude_defaults=False),
                f,
                default_flow_style=False,
                sort_keys=False,
            )
