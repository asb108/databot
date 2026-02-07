"""MCP (Model Context Protocol) server — exposes databot tools and connectors.

This module implements an MCP server that allows external LLM clients
(Claude Desktop, Cursor, VS Code Copilot, etc.) to discover and invoke
databot's tools and query connectors over the standard MCP protocol.

Architecture
------------
- Uses the ``mcp`` Python SDK with **stdio** transport (default) or
  optional **SSE** transport for HTTP-based clients.
- Each registered ``BaseTool`` is exposed as an MCP tool with its JSON
  Schema parameters.
- Connector health and metadata are exposed as MCP *resources*.
- The server auto-loads tools and connectors from config, just like the
  CLI ``agent`` / ``gateway`` commands.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from loguru import logger


# ---------------------------------------------------------------------------
# MCP Server builder
# ---------------------------------------------------------------------------


def _build_mcp_server(cfg: Any | None = None):
    """Build and configure the MCP server.

    Returns an ``mcp.server.Server`` ready to be served over a transport.
    """
    try:
        from mcp.server import Server
        from mcp.types import Resource, TextContent, Tool
    except ImportError:
        raise ImportError(
            "The 'mcp' package is required for the MCP server. "
            "Install with: pip install databot[mcp]"
        )

    from databot.connectors.registry import ConnectorRegistry
    from databot.tools.base import ToolRegistry

    server = Server("databot")

    # ------------------------------------------------------------------
    # Internal state — populated by ``_init_components``
    # ------------------------------------------------------------------
    _tools: dict[str, Any] = {}  # name → BaseTool instance
    _tool_registry: ToolRegistry | None = None
    _connector_registry: ConnectorRegistry | None = None

    # ------------------------------------------------------------------
    # Initialise tools & connectors
    # ------------------------------------------------------------------

    async def _init_components():
        nonlocal _tool_registry, _connector_registry

        if cfg is None:
            from databot.config.schema import DatabotConfig

            config = DatabotConfig.load()
        else:
            config = cfg

        # --- Connectors ---
        _connector_registry = ConnectorRegistry()
        if config.connectors.instances:
            connector_cfgs = {
                name: c.to_dict() for name, c in config.connectors.instances.items()
            }
            _connector_registry.load_from_config(connector_cfgs)
            await _connector_registry.connect_all()

        # --- Tools ---
        _tool_registry = ToolRegistry()
        _register_all_tools(_tool_registry, config, _connector_registry)

        for defn in _tool_registry.get_definitions():
            func = defn["function"]
            _tools[func["name"]] = func

        logger.info(
            f"MCP server initialised: {len(_tools)} tools, "
            f"{len(_connector_registry)} connectors"
        )

    def _register_all_tools(
        tools: ToolRegistry,
        config: Any,
        connector_reg: ConnectorRegistry,
    ) -> None:
        """Register tools identically to the CLI/gateway startup path."""
        workspace = Path.cwd()

        from databot.tools.filesystem import (
            EditFileTool,
            ListDirTool,
            ReadFileTool,
            WriteFileTool,
        )
        from databot.tools.shell import ShellTool
        from databot.tools.web import WebFetchTool, WebSearchTool

        allowed_dir = workspace if config.security.restrict_to_workspace else None

        tools.register(ReadFileTool(allowed_dir=allowed_dir))
        tools.register(WriteFileTool(allowed_dir=allowed_dir))
        tools.register(EditFileTool(allowed_dir=allowed_dir))
        tools.register(ListDirTool(allowed_dir=allowed_dir))

        if config.tools.shell.enabled:
            tools.register(
                ShellTool(
                    working_dir=str(workspace),
                    timeout=config.tools.shell.timeout,
                    restrict_to_workspace=config.security.restrict_to_workspace,
                    allowed_commands=config.security.allowed_commands or None,
                    max_output_length=config.tools.shell.max_output_length,
                )
            )

        tools.register(WebFetchTool(max_length=config.tools.web.max_fetch_length))
        if config.tools.web.search_api_key:
            tools.register(
                WebSearchTool(
                    api_key=config.tools.web.search_api_key,
                    results_count=config.tools.web.search_results_count,
                )
            )

        # SQL tool (connector-backed when connectors exist)
        if config.tools.sql.connections:
            from databot.tools.sql import SQLTool

            conn_configs = {
                name: conn.model_dump()
                for name, conn in config.tools.sql.connections.items()
            }
            sql_tool = SQLTool(
                connections=conn_configs,
                read_only=config.tools.sql.read_only,
                max_rows=config.tools.sql.max_rows,
                connector_registry=connector_reg,
            )
            tools.register(sql_tool)

            from databot.tools.data_quality import DataQualityTool

            tools.register(DataQualityTool(sql_tool=sql_tool))

        # Airflow tool (connector-backed when connectors exist)
        if config.tools.airflow.base_url:
            from databot.tools.airflow import AirflowTool

            tools.register(
                AirflowTool(
                    base_url=config.tools.airflow.base_url,
                    username=config.tools.airflow.username,
                    password=config.tools.airflow.password,
                    connector_registry=connector_reg,
                )
            )

        # Lineage tool
        if config.tools.lineage.graph_path or config.tools.lineage.marquez_url:
            from databot.tools.lineage import LineageTool

            tools.register(
                LineageTool(
                    graph_path=config.tools.lineage.graph_path,
                    marquez_url=config.tools.lineage.marquez_url,
                )
            )

        # Domain tools from connectors
        from databot.connectors.base import ConnectorType

        if connector_reg.get_by_type(ConnectorType.PROCESSING):
            from databot.tools.spark import SparkTool

            tools.register(SparkTool(registry=connector_reg))

        if connector_reg.get_by_type(ConnectorType.STREAMING):
            from databot.tools.kafka import KafkaTool

            tools.register(KafkaTool(registry=connector_reg))

        if connector_reg.get_by_type(ConnectorType.CATALOG):
            from databot.tools.catalog import CatalogTool

            tools.register(CatalogTool(registry=connector_reg))

        tools.load_plugins(workspace=workspace)

    # ------------------------------------------------------------------
    # MCP handlers
    # ------------------------------------------------------------------

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        if not _tools:
            await _init_components()

        result = []
        for name, func in _tools.items():
            result.append(
                Tool(
                    name=func["name"],
                    description=func.get("description", ""),
                    inputSchema=func.get("parameters", {"type": "object", "properties": {}}),
                )
            )
        return result

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any] | None = None) -> list[TextContent]:
        if not _tool_registry:
            await _init_components()

        args = arguments or {}
        try:
            result = await _tool_registry.execute(name, args)
        except Exception as e:
            logger.error(f"MCP tool '{name}' error: {e}")
            result = f"Error: {e}"

        return [TextContent(type="text", text=str(result))]

    @server.list_resources()
    async def list_resources() -> list[Resource]:
        if not _connector_registry:
            await _init_components()

        resources = []
        # Expose each connector as a resource
        for conn in _connector_registry.list_all():
            resources.append(
                Resource(
                    uri=f"connector://{conn.name}",
                    name=conn.name,
                    description=f"{conn.connector_type.value} connector — capabilities: {', '.join(conn.capabilities())}",
                    mimeType="application/json",
                )
            )
        # Health overview resource
        resources.append(
            Resource(
                uri="databot://health",
                name="health",
                description="Health status of all connectors",
                mimeType="application/json",
            )
        )
        return resources

    @server.read_resource()
    async def read_resource(uri: str) -> str:
        if not _connector_registry:
            await _init_components()

        uri_str = str(uri)

        if uri_str == "databot://health":
            health = await _connector_registry.health_check_all()
            return json.dumps(
                {name: status.value for name, status in health.items()},
                indent=2,
            )

        # connector://<name>
        if uri_str.startswith("connector://"):
            conn_name = uri_str.replace("connector://", "")
            conn = _connector_registry.get(conn_name)
            if conn is None:
                return json.dumps({"error": f"Connector '{conn_name}' not found"})
            return json.dumps(
                {
                    "name": conn.name,
                    "type": conn.connector_type.value,
                    "connected": conn.is_connected,
                    "capabilities": conn.capabilities(),
                    "config_keys": list(conn.config.keys()),
                },
                indent=2,
            )

        return json.dumps({"error": f"Unknown resource: {uri_str}"})

    return server


# ---------------------------------------------------------------------------
# Entry-point helpers
# ---------------------------------------------------------------------------


async def run_stdio(cfg: Any | None = None) -> None:
    """Run the MCP server over stdio transport."""
    from mcp.server.stdio import stdio_server

    server = _build_mcp_server(cfg)

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


async def run_sse(cfg: Any | None = None, host: str = "0.0.0.0", port: int = 18791) -> None:
    """Run the MCP server over SSE transport (HTTP)."""
    from mcp.server.sse import SseServerTransport
    from starlette.applications import Starlette
    from starlette.routing import Mount, Route

    import uvicorn

    server = _build_mcp_server(cfg)
    sse = SseServerTransport("/messages/")

    async def handle_sse(request):
        async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
            await server.run(
                streams[0],
                streams[1],
                server.create_initialization_options(),
            )

    app = Starlette(
        routes=[
            Route("/sse", endpoint=handle_sse),
            Mount("/messages/", app=sse.handle_post_message),
        ],
    )

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    uv_server = uvicorn.Server(config)
    logger.info(f"MCP SSE server starting on {host}:{port}")
    await uv_server.serve()
