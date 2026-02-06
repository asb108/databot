"""Plugin discovery via entry points.
from __future__ import annotations

This module provides automatic discovery of plugins registered via Python
entry points. Third-party packages can register their tools, channels, and
providers by adding entry points in their pyproject.toml.

Example pyproject.toml for a plugin:

    [project.entry-points."databot.tools"]
    my_tool = "my_package.tools:MyCustomTool"

    [project.entry-points."databot.channels"]
    my_channel = "my_package.channels:MyCustomChannel"

    [project.entry-points."databot.providers"]
    my_provider = "my_package.providers:MyCustomProvider"

Each entry point should reference a class that inherits from the appropriate
base class (BaseTool, BaseChannel, or LLMProvider).
"""

from importlib.metadata import entry_points
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from databot.channels.base import BaseChannel
    from databot.providers.base import LLMProvider
    from databot.tools.base import BaseTool


def discover_tools() -> list[type["BaseTool"]]:
    """Discover tool classes from entry points.

    Returns:
        List of tool classes (not instances) that can be instantiated.
    """
    tools: list[type["BaseTool"]] = []

    try:
        eps = entry_points(group="databot.tools")
    except TypeError:
        # Python < 3.10 compatibility
        eps = entry_points().get("databot.tools", [])

    for ep in eps:
        try:
            tool_class = ep.load()
            tools.append(tool_class)
            logger.debug(f"Discovered tool plugin: {ep.name}")
        except Exception as e:
            logger.warning(f"Failed to load tool plugin '{ep.name}': {e}")

    return tools


def discover_channels() -> list[type["BaseChannel"]]:
    """Discover channel classes from entry points.

    Returns:
        List of channel classes (not instances) that can be instantiated.
    """
    channels: list[type["BaseChannel"]] = []

    try:
        eps = entry_points(group="databot.channels")
    except TypeError:
        eps = entry_points().get("databot.channels", [])

    for ep in eps:
        try:
            channel_class = ep.load()
            channels.append(channel_class)
            logger.debug(f"Discovered channel plugin: {ep.name}")
        except Exception as e:
            logger.warning(f"Failed to load channel plugin '{ep.name}': {e}")

    return channels


def discover_providers() -> list[type["LLMProvider"]]:
    """Discover LLM provider classes from entry points.

    Returns:
        List of provider classes (not instances) that can be instantiated.
    """
    providers: list[type["LLMProvider"]] = []

    try:
        eps = entry_points(group="databot.providers")
    except TypeError:
        eps = entry_points().get("databot.providers", [])

    for ep in eps:
        try:
            provider_class = ep.load()
            providers.append(provider_class)
            logger.debug(f"Discovered provider plugin: {ep.name}")
        except Exception as e:
            logger.warning(f"Failed to load provider plugin '{ep.name}': {e}")

    return providers
