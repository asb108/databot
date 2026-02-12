"""Tests for expanded provider configuration and litellm prefix mapping."""

from __future__ import annotations

import pytest

from databot.config.schema import DatabotConfig, ProviderConfig, ProvidersConfig


class TestProvidersConfig:
    """Verify all seven named providers have correct defaults."""

    def test_default_provider(self):
        cfg = ProvidersConfig()
        assert cfg.default == "anthropic"

    def test_anthropic_defaults(self):
        cfg = ProvidersConfig()
        assert cfg.anthropic.model == "claude-sonnet-4-5-20250929"
        assert cfg.anthropic.api_key == ""
        assert cfg.anthropic.api_base is None

    def test_openai_defaults(self):
        cfg = ProvidersConfig()
        assert cfg.openai.model == "gpt-4o"

    def test_deepseek_defaults(self):
        cfg = ProvidersConfig()
        assert cfg.deepseek.model == "deepseek-chat"

    def test_gemini_defaults(self):
        cfg = ProvidersConfig()
        assert cfg.gemini.model == "gemini-2.0-flash"

    def test_qwen_defaults(self):
        cfg = ProvidersConfig()
        assert cfg.qwen.model == "qwen-turbo"

    def test_mistral_defaults(self):
        cfg = ProvidersConfig()
        assert cfg.mistral.model == "mistral-large-latest"

    def test_groq_defaults(self):
        cfg = ProvidersConfig()
        assert cfg.groq.model == "llama-3.3-70b-versatile"

    def test_custom_empty_by_default(self):
        cfg = ProvidersConfig()
        assert cfg.custom == {}

    def test_set_default_to_each_provider(self):
        """Each named provider can be set as the default."""
        names = ["anthropic", "openai", "deepseek", "gemini", "qwen", "mistral", "groq"]
        for name in names:
            cfg = ProvidersConfig(default=name)
            assert cfg.default == name

    def test_custom_provider_entry(self):
        cfg = ProvidersConfig(
            custom={"deepbricks": ProviderConfig(api_key="k1", model="gpt-4o-mini")}
        )
        assert cfg.custom["deepbricks"].model == "gpt-4o-mini"
        assert cfg.custom["deepbricks"].api_key == "k1"

    def test_provider_api_base_override(self):
        cfg = ProvidersConfig()
        cfg.openai.api_base = "https://custom.openai.example.com/v1"
        assert cfg.openai.api_base == "https://custom.openai.example.com/v1"


class TestProviderConfigSaveLoad:
    """Test save/load round-trip for new provider fields."""

    def test_round_trip_all_providers(self, tmp_path):
        cfg = DatabotConfig()
        cfg.providers.default = "gemini"
        cfg.providers.gemini.api_key = "gemini-key-123"
        cfg.providers.qwen.api_key = "qwen-key-456"

        path = tmp_path / "config.yaml"
        cfg.save(path)

        loaded = DatabotConfig.load(path)
        assert loaded.providers.default == "gemini"
        assert loaded.providers.gemini.api_key == "gemini-key-123"
        assert loaded.providers.qwen.api_key == "qwen-key-456"
        # Untouched providers still have defaults
        assert loaded.providers.mistral.model == "mistral-large-latest"

    def test_env_var_resolution_new_providers(self, tmp_path, monkeypatch):
        import yaml

        monkeypatch.setenv("GROQ_API_KEY", "groq-secret")
        monkeypatch.setenv("MISTRAL_API_KEY", "mistral-secret")
        data = {
            "providers": {
                "default": "groq",
                "groq": {"api_key": "${GROQ_API_KEY}", "model": "llama-3.3-70b-versatile"},
                "mistral": {"api_key": "${MISTRAL_API_KEY}", "model": "mistral-large-latest"},
            }
        }
        path = tmp_path / "config.yaml"
        with open(path, "w") as f:
            yaml.dump(data, f)

        loaded = DatabotConfig.load(path)
        assert loaded.providers.groq.api_key == "groq-secret"
        assert loaded.providers.mistral.api_key == "mistral-secret"


class TestLiteLLMPrefixMapping:
    """Test the _LITELLM_PREFIX dict in _build_components."""

    def test_qwen_maps_to_dashscope(self):
        """Qwen provider name should map to dashscope/ prefix for litellm."""
        cfg = DatabotConfig()
        cfg.providers.default = "qwen"
        cfg.providers.qwen.api_key = "test-key"

        from databot.cli.commands import _build_components

        result = _build_components(cfg)
        provider = result[1]

        # Provider model should start with dashscope/
        model = provider.get_default_model()
        assert model.startswith("dashscope/"), f"Expected dashscope/ prefix, got {model}"
        assert "qwen-turbo" in model

    def test_gemini_uses_own_prefix(self):
        cfg = DatabotConfig()
        cfg.providers.default = "gemini"
        cfg.providers.gemini.api_key = "test-key"

        from databot.cli.commands import _build_components

        result = _build_components(cfg)
        provider = result[1]
        model = provider.get_default_model()
        assert model.startswith("gemini/"), f"Expected gemini/ prefix, got {model}"

    def test_groq_uses_own_prefix(self):
        cfg = DatabotConfig()
        cfg.providers.default = "groq"
        cfg.providers.groq.api_key = "test-key"

        from databot.cli.commands import _build_components

        result = _build_components(cfg)
        provider = result[1]
        model = provider.get_default_model()
        assert model.startswith("groq/"), f"Expected groq/ prefix, got {model}"

    def test_mistral_uses_own_prefix(self):
        cfg = DatabotConfig()
        cfg.providers.default = "mistral"
        cfg.providers.mistral.api_key = "test-key"

        from databot.cli.commands import _build_components

        result = _build_components(cfg)
        provider = result[1]
        model = provider.get_default_model()
        assert model.startswith("mistral/"), f"Expected mistral/ prefix, got {model}"

    def test_anthropic_uses_own_prefix(self):
        cfg = DatabotConfig()
        cfg.providers.default = "anthropic"

        from databot.cli.commands import _build_components

        result = _build_components(cfg)
        provider = result[1]
        model = provider.get_default_model()
        assert model.startswith("anthropic/"), f"Expected anthropic/ prefix, got {model}"

    def test_custom_provider_falls_back_to_default(self):
        """When provider is not in named dict, falls back to anthropic default model."""
        cfg = DatabotConfig()
        cfg.providers.default = "nonexistent"

        from databot.cli.commands import _build_components

        result = _build_components(cfg)
        provider = result[1]
        model = provider.get_default_model()
        # Should fall back to anthropic/claude-sonnet-4-5-20250929
        assert "anthropic/claude-sonnet-4-5-20250929" in model
