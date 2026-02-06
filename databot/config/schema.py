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
    openai: ProviderConfig = Field(
        default_factory=lambda: ProviderConfig(model="gpt-4o")
    )
    deepseek: ProviderConfig = Field(
        default_factory=lambda: ProviderConfig(model="deepseek-chat")
    )
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


class ChannelsConfig(BaseModel):
    gchat: GChatConfig = Field(default_factory=GChatConfig)


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


class ShellToolConfig(BaseModel):
    enabled: bool = True
    restrict_to_workspace: bool = True
    timeout: int = 30
    allowed_commands: list[str] = Field(default_factory=list)


class ToolsConfig(BaseModel):
    sql: SQLToolConfig = Field(default_factory=SQLToolConfig)
    airflow: AirflowToolConfig = Field(default_factory=AirflowToolConfig)
    lineage: LineageToolConfig = Field(default_factory=LineageToolConfig)
    shell: ShellToolConfig = Field(default_factory=ShellToolConfig)


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


# ---------------------------------------------------------------------------
# Security configuration
# ---------------------------------------------------------------------------

class SecurityConfig(BaseModel):
    restrict_to_workspace: bool = True
    allowed_commands: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Agent configuration
# ---------------------------------------------------------------------------

class AgentConfig(BaseModel):
    max_iterations: int = 20
    system_prompt: str = ""


# ---------------------------------------------------------------------------
# Root configuration
# ---------------------------------------------------------------------------

class DatabotConfig(BaseModel):
    providers: ProvidersConfig = Field(default_factory=ProvidersConfig)
    channels: ChannelsConfig = Field(default_factory=ChannelsConfig)
    tools: ToolsConfig = Field(default_factory=ToolsConfig)
    cron: CronConfig = Field(default_factory=CronConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
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
