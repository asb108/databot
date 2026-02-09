from __future__ import annotations

"""Tests for plugin discovery."""

import pytest

from databot.plugins.loader import discover_channels, discover_providers, discover_tools
from databot.tools.base import ToolRegistry


class TestPluginDiscovery:
    """Test plugin discovery from entry points."""

    def test_discover_tools_returns_list(self):
        """Verify discover_tools returns a list (may be empty if no plugins)."""
        tools = discover_tools()
        assert isinstance(tools, list)

    def test_discover_channels_returns_list(self):
        """Verify discover_channels returns a list."""
        channels = discover_channels()
        assert isinstance(channels, list)

    def test_discover_providers_returns_list(self):
        """Verify discover_providers returns a list."""
        providers = discover_providers()
        assert isinstance(providers, list)


class TestToolRegistryPlugins:
    """Test ToolRegistry plugin loading."""

    def test_load_plugins_returns_count(self):
        """Verify load_plugins returns the count of loaded plugins."""
        registry = ToolRegistry()
        count = registry.load_plugins()
        # Without any plugins installed, should return 0
        assert isinstance(count, int)
        assert count >= 0

    def test_load_plugins_does_not_raise(self):
        """Verify load_plugins handles missing plugins gracefully."""
        registry = ToolRegistry()
        # Should not raise even with no plugins
        try:
            registry.load_plugins()
        except Exception as e:
            pytest.fail(f"load_plugins raised an exception: {e}")
