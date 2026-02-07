"""Tests for MCP server, integration wiring, and connector-backed tools."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ===================================================================
# MCP server tests
# ===================================================================


class TestMCPServerBuild:
    """Test MCP server construction and tool/resource listing."""

    def test_import_guard(self):
        """If mcp is not installed, building the server raises ImportError."""
        import importlib
        import sys

        # Simulate mcp not installed
        with patch.dict(sys.modules, {"mcp": None, "mcp.server": None, "mcp.types": None}):
            from databot.mcp import _build_mcp_server

            with pytest.raises(ImportError, match="mcp"):
                _build_mcp_server()

    def test_build_succeeds_with_mcp(self):
        """When mcp is importable, build returns a server-like object."""
        try:
            import mcp  # noqa: F401
        except ImportError:
            pytest.skip("mcp not installed")

        from databot.mcp import _build_mcp_server
        from databot.config.schema import DatabotConfig

        cfg = DatabotConfig()
        server = _build_mcp_server(cfg)
        assert server is not None


class TestMCPEntryPoints:
    """Test the run_stdio / run_sse entry points exist."""

    def test_run_stdio_callable(self):
        from databot.mcp import run_stdio

        assert callable(run_stdio)

    def test_run_sse_callable(self):
        from databot.mcp import run_sse

        assert callable(run_sse)


# ===================================================================
# Integration wiring tests
# ===================================================================


class TestBuildComponents:
    """Test the _build_components function returns all expected items."""

    def test_returns_extended_tuple(self):
        """_build_components returns 10-element tuple with all subsystems."""
        from databot.cli.commands import _build_components
        from databot.config.schema import DatabotConfig

        cfg = DatabotConfig()
        result = _build_components(cfg)

        assert len(result) == 10, f"Expected 10-tuple, got {len(result)}-tuple"

        (
            bus, provider, tools, sessions, memory, workspace,
            connector_registry, rag_context, tracer, delegator,
        ) = result

        # Subsystems present
        assert bus is not None
        assert provider is not None
        assert tools is not None
        assert workspace == Path.cwd()
        # Subsystems disabled by default
        assert rag_context is None
        assert tracer is None
        assert delegator is None

    def test_connector_registry_always_present(self):
        """Even with no connectors configured, the registry is returned."""
        from databot.cli.commands import _build_components
        from databot.config.schema import DatabotConfig

        cfg = DatabotConfig()
        result = _build_components(cfg)
        connector_registry = result[6]
        assert connector_registry is not None
        assert len(connector_registry) == 0

    def test_observability_enabled(self):
        """When observability is enabled, a tracer is returned."""
        from databot.cli.commands import _build_components
        from databot.config.schema import DatabotConfig, ObservabilityConfig

        cfg = DatabotConfig(observability=ObservabilityConfig(enabled=True))
        result = _build_components(cfg)
        tracer = result[8]
        assert tracer is not None

    def test_multi_agent_enabled(self):
        """When multi_agent is enabled, a delegator is returned."""
        from databot.cli.commands import _build_components
        from databot.config.schema import DatabotConfig, MultiAgentConfig

        cfg = DatabotConfig(multi_agent=MultiAgentConfig(enabled=True))
        result = _build_components(cfg)
        delegator = result[9]
        assert delegator is not None

    def test_rag_skipped_without_chromadb(self):
        """RAG is gracefully skipped when chromadb is not installed."""
        from databot.cli.commands import _build_components
        from databot.config.schema import DatabotConfig, RAGConfig

        cfg = DatabotConfig(rag=RAGConfig(enabled=True))
        result = _build_components(cfg)
        # rag_context may or may not be None depending on whether chromadb is installed
        # Either way it should not raise
        assert len(result) == 10


class TestRegisterToolsWithConnectors:
    """Test that domain tools are registered when connectors are configured."""

    def test_no_domain_tools_without_connectors(self):
        """Without connectors, domain tools (spark, kafka, catalog) are not registered."""
        from databot.cli.commands import _register_tools
        from databot.config.schema import DatabotConfig
        from databot.connectors.registry import ConnectorRegistry
        from databot.tools.base import ToolRegistry

        cfg = DatabotConfig()
        tools = ToolRegistry()
        reg = ConnectorRegistry()

        _register_tools(tools, cfg, Path.cwd(), reg)

        # Should have filesystem tools + shell + web, but not spark/kafka/catalog
        tool_names = [d["function"]["name"] for d in tools.get_definitions()]
        assert "spark" not in tool_names
        assert "kafka" not in tool_names
        assert "catalog" not in tool_names
        # Filesystem tools are always registered
        assert "read_file" in tool_names

    def test_spark_tool_registered_with_processing_connector(self):
        """SparkTool is auto-registered when a PROCESSING connector exists."""
        from databot.cli.commands import _register_tools
        from databot.config.schema import DatabotConfig
        from databot.connectors.base import ConnectorType
        from databot.connectors.registry import ConnectorRegistry
        from databot.tools.base import ToolRegistry

        cfg = DatabotConfig()
        tools = ToolRegistry()
        reg = ConnectorRegistry()

        # Add a mock processing connector
        mock_conn = MagicMock()
        mock_conn.name = "my_spark"
        mock_conn.connector_type = ConnectorType.PROCESSING
        reg.register(mock_conn)

        _register_tools(tools, cfg, Path.cwd(), reg)

        tool_names = [d["function"]["name"] for d in tools.get_definitions()]
        assert "spark" in tool_names

    def test_plugin_tools_loaded(self):
        """Plugins are loaded during registration."""
        from databot.cli.commands import _register_tools
        from databot.config.schema import DatabotConfig
        from databot.connectors.registry import ConnectorRegistry
        from databot.tools.base import ToolRegistry

        cfg = DatabotConfig()
        tools = ToolRegistry()
        reg = ConnectorRegistry()

        # Shouldn't raise even if no plugins exist
        _register_tools(tools, cfg, Path.cwd(), reg)
        assert len(tools.get_definitions()) > 0  # at least filesystem tools


# ===================================================================
# SQLTool connector delegation tests
# ===================================================================


class TestSQLToolConnectorDelegation:
    """Test SQLTool connector-backed execution path."""

    def test_backward_compatible_without_registry(self):
        """SQLTool works without a connector registry (legacy mode)."""
        from databot.tools.sql import SQLTool

        tool = SQLTool(connections={"test": {"driver": "mysql", "host": "localhost"}})
        assert tool._connector_registry is None
        assert tool.name == "sql"

    @pytest.mark.asyncio
    async def test_delegates_to_connector(self):
        """When a matching SQL connector exists, queries go through it."""
        from databot.connectors.base import ConnectorResult, ConnectorType
        from databot.connectors.registry import ConnectorRegistry
        from databot.tools.sql import SQLTool

        # Create a mock SQL connector
        mock_connector = AsyncMock()
        mock_connector.name = "my_db"
        mock_connector.connector_type = ConnectorType.SQL
        mock_connector.execute.return_value = ConnectorResult(
            success=True,
            columns=["id", "name"],
            rows=[[1, "alice"], [2, "bob"]],
            row_count=2,
        )

        registry = ConnectorRegistry()
        registry.register(mock_connector)

        tool = SQLTool(
            connections={"my_db": {"driver": "mysql", "host": "localhost"}},
            connector_registry=registry,
        )

        result = await tool.execute(query="SELECT * FROM users", connection="my_db")

        mock_connector.execute.assert_called_once()
        assert "alice" in result
        assert "bob" in result

    @pytest.mark.asyncio
    async def test_falls_back_on_connector_error(self):
        """When the connector fails, execution falls through to legacy path."""
        from databot.connectors.base import ConnectorType
        from databot.connectors.registry import ConnectorRegistry
        from databot.tools.sql import SQLTool

        mock_connector = AsyncMock()
        mock_connector.name = "my_db"
        mock_connector.connector_type = ConnectorType.SQL
        mock_connector.execute.side_effect = Exception("Connector exploded")

        registry = ConnectorRegistry()
        registry.register(mock_connector)

        tool = SQLTool(
            connections={"my_db": {"driver": "mysql", "host": "localhost"}},
            connector_registry=registry,
        )

        # Legacy path will also fail (no real DB), but the point is
        # it doesn't short-circuit with the connector error
        result = await tool.execute(query="SELECT 1", connection="my_db")
        assert "Error" in result or "error" in result.lower()

    @pytest.mark.asyncio
    async def test_read_only_enforced_before_delegation(self):
        """Read-only checks happen before connector delegation."""
        from databot.connectors.registry import ConnectorRegistry
        from databot.tools.sql import SQLTool

        registry = ConnectorRegistry()
        tool = SQLTool(
            connections={"my_db": {"driver": "mysql"}},
            read_only=True,
            connector_registry=registry,
        )

        result = await tool.execute(query="DROP TABLE users", connection="my_db")
        assert "read_only" in result.lower() or "not allowed" in result.lower()


# ===================================================================
# AirflowTool connector delegation tests
# ===================================================================


class TestAirflowToolConnectorDelegation:
    """Test AirflowTool connector-backed execution path."""

    def test_backward_compatible_without_registry(self):
        """AirflowTool works without a connector registry."""
        from databot.tools.airflow import AirflowTool

        tool = AirflowTool(base_url="http://airflow:8080")
        assert tool._connector_registry is None
        assert tool._connector is None
        assert tool.name == "airflow"

    def test_picks_up_airflow_connector(self):
        """When registry has an 'airflow' connector, AirflowTool uses it."""
        from databot.connectors.base import ConnectorType
        from databot.connectors.registry import ConnectorRegistry
        from databot.tools.airflow import AirflowTool

        mock_connector = MagicMock()
        mock_connector.name = "airflow"
        mock_connector.connector_type = ConnectorType.REST_API

        registry = ConnectorRegistry()
        registry.register(mock_connector)

        tool = AirflowTool(
            base_url="http://airflow:8080",
            connector_registry=registry,
        )
        assert tool._connector is mock_connector


# ===================================================================
# AgentLoop RAG integration test
# ===================================================================


class TestAgentLoopRAGIntegration:
    """Test AgentLoop with rag_context parameter."""

    def test_init_with_rag_context(self):
        """AgentLoop accepts rag_context parameter."""
        from databot.core.bus import MessageBus
        from databot.core.loop import AgentLoop
        from databot.memory.manager import MemoryManager
        from databot.session.manager import SessionManager

        bus = MessageBus()
        mock_provider = MagicMock()
        mock_provider.get_default_model.return_value = "test-model"

        from databot.tools.base import ToolRegistry

        tools = ToolRegistry()
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            sessions = SessionManager(Path(td))
            memory = MemoryManager(Path(td) / "mem.db")

            mock_rag = MagicMock()
            loop = AgentLoop(
                bus=bus,
                provider=mock_provider,
                tools=tools,
                workspace=Path.cwd(),
                sessions=sessions,
                memory=memory,
                rag_context=mock_rag,
            )
            assert loop._rag_context is mock_rag

    def test_init_without_rag_context(self):
        """AgentLoop works fine without rag_context (default None)."""
        from databot.core.bus import MessageBus
        from databot.core.loop import AgentLoop
        from databot.memory.manager import MemoryManager
        from databot.session.manager import SessionManager
        from databot.tools.base import ToolRegistry

        bus = MessageBus()
        mock_provider = MagicMock()
        mock_provider.get_default_model.return_value = "test-model"
        tools = ToolRegistry()
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            sessions = SessionManager(Path(td))
            memory = MemoryManager(Path(td) / "mem.db")
            loop = AgentLoop(
                bus=bus,
                provider=mock_provider,
                tools=tools,
                workspace=Path.cwd(),
                sessions=sessions,
                memory=memory,
            )
            assert loop._rag_context is None


# ===================================================================
# CLI MCP command test
# ===================================================================


class TestMCPCommand:
    """Test the MCP CLI command exists."""

    def test_mcp_command_registered(self):
        from databot.cli.commands import app

        # Typer stores commands; check both registered_commands and
        # the click group underneath
        if hasattr(app, "registered_commands"):
            # Some names may be None since Typer derives them from function names
            command_names = []
            for cmd in app.registered_commands:
                name = cmd.name or (cmd.callback.__name__ if cmd.callback else None)
                if name:
                    command_names.append(name)
            assert "mcp" in command_names
