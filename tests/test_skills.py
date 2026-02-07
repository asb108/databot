"""Tests for the skills registry."""

from __future__ import annotations


class TestSkillRegistry:
    """Test the SkillRegistry class."""

    def test_builtin_skills_exist(self):
        from databot.skills import BUILTIN_SKILLS

        assert len(BUILTIN_SKILLS) >= 8
        assert "filesystem" in BUILTIN_SKILLS
        assert "shell" in BUILTIN_SKILLS
        assert "sql_analytics" in BUILTIN_SKILLS
        assert "web_search" in BUILTIN_SKILLS

    def test_from_config_defaults(self):
        from databot.skills import SkillRegistry

        reg = SkillRegistry.from_config([])
        # Default-enabled skills
        assert reg.is_enabled("filesystem")
        assert reg.is_enabled("shell")
        assert reg.is_enabled("web_search")
        # Not default-enabled
        assert not reg.is_enabled("sql_analytics")
        assert not reg.is_enabled("spark_jobs")

    def test_from_config_explicit(self):
        from databot.skills import SkillRegistry

        reg = SkillRegistry.from_config(["sql_analytics", "kafka_ops"])
        assert reg.is_enabled("sql_analytics")
        assert reg.is_enabled("kafka_ops")
        assert not reg.is_enabled("filesystem")

    def test_enable_disable(self):
        from databot.skills import SkillRegistry

        reg = SkillRegistry.from_config(["filesystem"])
        assert reg.is_enabled("filesystem")
        reg.disable("filesystem")
        assert not reg.is_enabled("filesystem")
        reg.enable("filesystem")
        assert reg.is_enabled("filesystem")

    def test_enabled_tool_names(self):
        from databot.skills import SkillRegistry

        reg = SkillRegistry.from_config(["filesystem", "shell"])
        names = reg.enabled_tool_names()
        assert "read_file" in names
        assert "write_file" in names
        assert "shell" in names
        assert "sql_query" not in names

    def test_summary(self):
        from databot.skills import SkillRegistry

        reg = SkillRegistry.from_config(["filesystem"])
        summary = reg.summary()
        assert isinstance(summary, list)
        assert len(summary) >= 8
        fs = next(s for s in summary if s["name"] == "filesystem")
        assert fs["enabled"] is True
        sql = next(s for s in summary if s["name"] == "sql_analytics")
        assert sql["enabled"] is False

    def test_custom_skill(self):
        from databot.skills import Skill, SkillRegistry

        reg = SkillRegistry()
        custom = Skill(
            name="custom_tool",
            label="Custom",
            description="A custom skill",
            tools=["my_tool"],
        )
        reg.register(custom)
        reg.enable("custom_tool")
        assert reg.is_enabled("custom_tool")
        assert "my_tool" in reg.enabled_tool_names()

    def test_skill_registry_filters_tools(self):
        """Integration: _register_tools respects skill_registry."""
        from pathlib import Path

        from databot.config.schema import DatabotConfig
        from databot.skills import SkillRegistry
        from databot.tools.base import ToolRegistry

        cfg = DatabotConfig()
        tools = ToolRegistry()
        # Only filesystem skill
        sr = SkillRegistry.from_config(["filesystem"])

        from databot.cli.commands import _register_tools

        _register_tools(tools, cfg, Path.cwd(), skill_registry=sr)

        tool_names = {d.get("function", d).get("name") for d in tools.get_definitions()}
        assert "read_file" in tool_names
        assert "list_dir" in tool_names
        # Shell and web should be filtered out
        assert "shell" not in tool_names
        assert "web_fetch" not in tool_names


class TestSkillsConfig:
    """Test skills config integration."""

    def test_default_skills_config(self):
        from databot.config.schema import DatabotConfig

        cfg = DatabotConfig()
        assert hasattr(cfg, "skills")
        assert isinstance(cfg.skills.enabled, list)

    def test_default_ui_config(self):
        from databot.config.schema import DatabotConfig

        cfg = DatabotConfig()
        assert hasattr(cfg, "ui")
        assert cfg.ui.enabled is True
        assert cfg.ui.theme == "dark"
