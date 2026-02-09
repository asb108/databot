"""Tests for configuration schema."""

from __future__ import annotations

import yaml

from databot.config.schema import (
    AgentConfig,
    DatabotConfig,
    GatewayConfig,
    SecurityConfig,
    SlackConfig,
    _resolve_env_vars,
)


class TestEnvVarResolution:
    def test_resolve_string(self, monkeypatch):
        monkeypatch.setenv("TEST_KEY", "my_value")
        assert _resolve_env_vars("${TEST_KEY}") == "my_value"

    def test_unresolved_passthrough(self):
        result = _resolve_env_vars("${NONEXISTENT_KEY_12345}")
        assert result == "${NONEXISTENT_KEY_12345}"

    def test_resolve_dict(self, monkeypatch):
        monkeypatch.setenv("DB_HOST", "localhost")
        result = _resolve_env_vars({"host": "${DB_HOST}", "port": 3306})
        assert result == {"host": "localhost", "port": 3306}

    def test_resolve_list(self, monkeypatch):
        monkeypatch.setenv("ITEM", "value")
        result = _resolve_env_vars(["${ITEM}", "static"])
        assert result == ["value", "static"]


class TestDatabotConfig:
    def test_default_config(self):
        cfg = DatabotConfig()
        assert cfg.providers.default == "anthropic"
        assert cfg.agent.max_iterations == 20
        assert cfg.security.restrict_to_workspace is True
        assert cfg.gateway.port == 18790
        assert cfg.gateway.rate_limit_rpm == 60

    def test_load_nonexistent_path(self, tmp_path):
        cfg = DatabotConfig.load(tmp_path / "nonexistent.yaml")
        assert cfg.providers.default == "anthropic"

    def test_save_and_load(self, tmp_path):
        cfg = DatabotConfig()
        cfg.agent.max_iterations = 42
        cfg.gateway.api_keys = ["key1", "key2"]

        path = tmp_path / "config.yaml"
        cfg.save(path)

        loaded = DatabotConfig.load(path)
        assert loaded.agent.max_iterations == 42
        assert loaded.gateway.api_keys == ["key1", "key2"]

    def test_load_with_env_vars(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MY_API_KEY", "secret123")
        config_data = {
            "providers": {
                "default": "anthropic",
                "anthropic": {
                    "api_key": "${MY_API_KEY}",
                    "model": "claude-sonnet-4-5-20250929",
                },
            }
        }
        path = tmp_path / "config.yaml"
        with open(path, "w") as f:
            yaml.dump(config_data, f)

        cfg = DatabotConfig.load(path)
        assert cfg.providers.anthropic.api_key == "secret123"


class TestGatewayConfig:
    def test_defaults(self):
        cfg = GatewayConfig()
        assert cfg.port == 18790
        assert cfg.host == "0.0.0.0"
        assert cfg.api_keys == []
        assert cfg.rate_limit_rpm == 60
        assert cfg.cors_origins == ["*"]


class TestAgentConfig:
    def test_defaults(self):
        cfg = AgentConfig()
        assert cfg.max_iterations == 20
        assert cfg.max_session_messages == 50
        assert cfg.retry_attempts == 3
        assert cfg.tool_approval_required == []


class TestSecurityConfig:
    def test_defaults(self):
        cfg = SecurityConfig()
        assert cfg.restrict_to_workspace is True
        assert cfg.allowed_commands == []


class TestSlackConfig:
    def test_defaults(self):
        cfg = SlackConfig()
        assert cfg.enabled is False
        assert cfg.bot_token == ""
