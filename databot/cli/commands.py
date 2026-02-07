"""CLI commands for databot."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

import typer
from rich.console import Console

app = typer.Typer(
    name="databot",
    help="A lightweight AI assistant for data platform operations.",
)
console = Console()


def _get_data_dir() -> Path:
    """Get the databot data directory."""
    return Path.home() / ".databot"


def _get_config_path() -> Path:
    return _get_data_dir() / "config.yaml"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@app.command()
def onboard():
    """Initialize databot — interactive setup wizard (like OpenClaw QuickStart)."""
    data_dir = _get_data_dir()
    config_path = _get_config_path()

    if config_path.exists():
        console.print(f"[yellow]Config already exists at {config_path}[/]")
        overwrite = typer.confirm("Overwrite?", default=False)
        if not overwrite:
            console.print("[dim]Keeping existing config.[/]")
            return

    data_dir.mkdir(parents=True, exist_ok=True)

    from databot.config.schema import DatabotConfig

    console.print("\n[bold cyan]🤖 Welcome to databot![/]")
    console.print("[dim]Let's set up your AI data assistant in under 2 minutes.\n[/]")

    # --- Mode selection ---
    console.print("[bold]Setup mode:[/]")
    console.print("  [cyan]1[/] QuickStart — sensible defaults, get chatting fast")
    console.print("  [cyan]2[/] Custom    — pick provider, skills, and channels")
    mode = typer.prompt("Choose", default="1")
    console.print()

    config = DatabotConfig()

    if mode == "1":
        # QuickStart: just ask for API key
        console.print("[bold]Provider:[/] Anthropic (Claude Sonnet)")
        api_key = typer.prompt(
            "Anthropic API key (or press Enter to set later)",
            default="",
            show_default=False,
        )
        if api_key:
            config.providers.anthropic.api_key = api_key
        config.skills.enabled = ["filesystem", "shell", "web_search"]
        config.ui.enabled = True
    else:
        # --- Provider ---
        console.print("[bold]Choose your LLM provider:[/]")
        console.print("  [cyan]1[/] Anthropic (Claude)")
        console.print("  [cyan]2[/] OpenAI (GPT-4)")
        console.print("  [cyan]3[/] DeepSeek")
        provider_choice = typer.prompt("Provider", default="1")
        provider_map = {"1": "anthropic", "2": "openai", "3": "deepseek"}
        provider_name = provider_map.get(provider_choice, "anthropic")
        config.providers.default = provider_name

        api_key = typer.prompt(
            f"{provider_name.title()} API key (or Enter to set later)",
            default="",
            show_default=False,
        )
        if api_key:
            provider_cfg = getattr(config.providers, provider_name)
            provider_cfg.api_key = api_key
        console.print()

        # --- Skills ---
        from databot.skills import BUILTIN_SKILLS

        console.print("[bold]Select skills to enable:[/]")
        for i, (name, skill) in enumerate(BUILTIN_SKILLS.items(), 1):
            default_mark = "[green]✓[/]" if skill.default_enabled else "[dim]○[/]"
            console.print(f"  {default_mark} [cyan]{i}[/] {skill.label} — {skill.description}")

        console.print()
        skill_input = typer.prompt(
            "Enter skill numbers (comma-separated, or 'all')",
            default="1,2,8",  # filesystem, shell, web_search
        )

        skill_names = list(BUILTIN_SKILLS.keys())
        if skill_input.strip().lower() == "all":
            selected_skills = skill_names
        else:
            try:
                indices = [int(x.strip()) - 1 for x in skill_input.split(",")]
                selected_skills = [skill_names[i] for i in indices if 0 <= i < len(skill_names)]
            except (ValueError, IndexError):
                selected_skills = ["filesystem", "shell", "web_search"]

        config.skills.enabled = selected_skills
        console.print(f"  Enabled: {', '.join(selected_skills)}\n")

        # --- UI ---
        ui_enabled = typer.confirm("Enable web dashboard UI?", default=True)
        config.ui.enabled = ui_enabled

        # --- Channels ---
        console.print("[bold]Chat channels (optional):[/]")
        if typer.confirm("  Enable Slack?", default=False):
            config.channels.slack.enabled = True
            token = typer.prompt("    Slack bot token", default="", show_default=False)
            if token:
                config.channels.slack.bot_token = token
        if typer.confirm("  Enable Discord?", default=False):
            config.channels.discord.enabled = True
            token = typer.prompt("    Discord bot token", default="", show_default=False)
            if token:
                config.channels.discord.bot_token = token

    # Save
    config.save(config_path)

    console.print(f"\n[green]✓ Config saved to {config_path}[/]")
    console.print(f"[green]✓ Data directory: {data_dir}[/]")
    console.print("\n[bold]Next steps:[/]")
    if not api_key:
        console.print(f"  1. Add your API key to [bold]{config_path}[/]")
    console.print("  2. [bold]databot agent -m 'Hello!'[/]  — quick test")
    console.print("  3. [bold]databot gateway[/]            — start full service")
    if config.ui.enabled:
        console.print(
            f"  4. Open [bold]http://localhost:{config.gateway.port}/ui[/]  — web dashboard"
        )


@app.command()
def agent(
    message: str = typer.Option(None, "-m", "--message", help="Single message to process."),
    config: str = typer.Option(None, "-c", "--config", help="Path to config file."),
):
    """Chat with the databot agent. Interactive mode if no message provided."""
    config_path = Path(config) if config else _get_config_path()

    from databot.config.schema import DatabotConfig

    cfg = DatabotConfig.load(config_path)

    if message:
        result = asyncio.run(_process_single(cfg, message))
        console.print(result)
    else:
        asyncio.run(_run_interactive(cfg))


@app.command()
def gateway(
    config: str = typer.Option(None, "-c", "--config", help="Path to config file."),
    port: int = typer.Option(18790, "-p", "--port", help="Gateway port."),
):
    """Start the databot gateway (always-on service with channels and cron)."""
    config_path = Path(config) if config else _get_config_path()

    from databot.config.schema import DatabotConfig

    cfg = DatabotConfig.load(config_path)

    asyncio.run(_run_gateway(cfg, port))


@app.command()
def mcp(
    transport: str = typer.Option("stdio", "--transport", "-t", help="Transport: stdio or sse."),
    config: str = typer.Option(None, "-c", "--config", help="Path to config file."),
    port: int = typer.Option(18791, "-p", "--port", help="SSE transport port."),
):
    """Start the MCP (Model Context Protocol) server."""
    config_path = Path(config) if config else _get_config_path()

    from databot.config.schema import DatabotConfig

    cfg = DatabotConfig.load(config_path)

    if transport == "sse":
        from databot.mcp import run_sse

        console.print(f"[bold green]databot MCP server (SSE)[/] starting on port {port}")
        asyncio.run(run_sse(cfg, port=port))
    else:
        from databot.mcp import run_stdio

        asyncio.run(run_stdio(cfg))


@app.command()
def status():
    """Show databot status and configuration."""
    config_path = _get_config_path()
    data_dir = _get_data_dir()

    console.print("[bold]databot status[/]\n")
    console.print(f"  Config: {config_path} ({'exists' if config_path.exists() else 'not found'})")
    console.print(f"  Data dir: {data_dir} ({'exists' if data_dir.exists() else 'not found'})")

    if config_path.exists():
        from databot.config.schema import DatabotConfig

        cfg = DatabotConfig.load(config_path)
        console.print(f"  Default provider: {cfg.providers.default}")
        console.print(f"  Skills: {', '.join(cfg.skills.enabled)}")
        console.print(f"  UI enabled: {cfg.ui.enabled}")
        console.print(f"  GChat enabled: {cfg.channels.gchat.enabled}")
        console.print(f"  Cron jobs: {len(cfg.cron.jobs)}")

        if cfg.tools.sql.connections:
            console.print(f"  SQL connections: {', '.join(cfg.tools.sql.connections.keys())}")

        provider = getattr(cfg.providers, cfg.providers.default, None)
        if provider and provider.api_key and not provider.api_key.startswith("${"):
            console.print("  API key: [green]configured[/]")
        else:
            console.print("  API key: [red]not set[/]")


@app.command(name="skills")
def skills_cmd(
    action: str = typer.Argument("list", help="Action: list, enable, disable"),
    name: str = typer.Option(None, "--name", "-n", help="Skill name."),
    config: str = typer.Option(None, "-c", "--config", help="Path to config file."),
):
    """Manage skills — list, enable, or disable skill bundles."""
    config_path = Path(config) if config else _get_config_path()

    from databot.config.schema import DatabotConfig
    from databot.skills import SkillRegistry

    cfg = DatabotConfig.load(config_path)
    registry = SkillRegistry.from_config(cfg.skills.enabled)

    if action == "list":
        console.print("[bold]Skills:[/]\n")
        for skill in registry.all_skills():
            status_icon = "[green]✓[/]" if registry.is_enabled(skill.name) else "[dim]○[/]"
            extra = (
                f" [dim](pip install databot[{skill.requires_extra}])[/]"
                if skill.requires_extra
                else ""
            )
            console.print(f"  {status_icon} [bold]{skill.label}[/] ({skill.name}){extra}")
            console.print(f"      {skill.description}")
            console.print(f"      Tools: {', '.join(skill.tools)}")

    elif action == "enable":
        if not name:
            console.print("[red]--name required[/]")
            raise typer.Exit(1)
        if name not in [s.name for s in registry.all_skills()]:
            console.print(f"[red]Unknown skill: {name}[/]")
            raise typer.Exit(1)
        if name not in cfg.skills.enabled:
            cfg.skills.enabled.append(name)
            cfg.save(config_path)
        console.print(f"[green]✓ Enabled skill: {name}[/]")

    elif action == "disable":
        if not name:
            console.print("[red]--name required[/]")
            raise typer.Exit(1)
        if name in cfg.skills.enabled:
            cfg.skills.enabled.remove(name)
            cfg.save(config_path)
        console.print(f"[yellow]Disabled skill: {name}[/]")

    else:
        console.print(f"[red]Unknown action: {action}[/]")


@app.command(name="cron")
def cron_cmd(
    action: str = typer.Argument(help="Action: add, remove, list"),
    name: str = typer.Option(None, "--name", help="Job name."),
    schedule: str = typer.Option(None, "--schedule", help="Cron expression."),
    message: str = typer.Option(None, "--message", "-m", help="Task message."),
    job_id: str = typer.Option(None, "--id", help="Job ID (for remove)."),
):
    """Manage cron jobs."""
    data_dir = _get_data_dir()

    from databot.cron.store import CronStore

    store = CronStore(data_dir / "cron.db")

    if action == "list":
        jobs = store.list_all()
        if not jobs:
            console.print("[dim]No cron jobs.[/]")
            return
        for job in jobs:
            status_str = "[green]enabled[/]" if job["enabled"] else "[red]disabled[/]"
            console.print(f"  {job['id']}  {job['name']}  {job['schedule']}  {status_str}")

    elif action == "add":
        if not all([name, schedule, message]):
            console.print("[red]Error: --name, --schedule, and --message are required.[/]")
            raise typer.Exit(1)

        from croniter import croniter

        if not croniter.is_valid(schedule):
            console.print(f"[red]Error: Invalid cron expression: {schedule}[/]")
            raise typer.Exit(1)

        new_id = str(uuid.uuid4())[:8]
        store.add(new_id, name, schedule, message)
        console.print(f"[green]Added job '{name}' (ID: {new_id})[/]")

    elif action == "remove":
        if not job_id:
            console.print("[red]Error: --id is required for remove.[/]")
            raise typer.Exit(1)
        if store.remove(job_id):
            console.print(f"[green]Removed job {job_id}[/]")
        else:
            console.print(f"[red]Job {job_id} not found.[/]")

    else:
        console.print(f"[red]Unknown action: {action}[/]")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_components(cfg):
    """Build all components from config.

    Returns a ``Components`` namedtuple containing the bus, provider, tools,
    sessions, memory, workspace, connector_registry, rag_context, tracer,
    and optional delegator (multi-agent).
    """
    from databot.connectors.registry import ConnectorRegistry
    from databot.core.bus import MessageBus
    from databot.memory.manager import MemoryManager
    from databot.providers.litellm_provider import LiteLLMProvider
    from databot.session.manager import SessionManager
    from databot.tools.base import ToolRegistry

    data_dir = _get_data_dir()
    workspace = Path.cwd()

    bus = MessageBus()

    # Provider
    provider_name = cfg.providers.default
    provider_cfg = getattr(cfg.providers, provider_name, None)
    if provider_cfg is None and provider_name in cfg.providers.custom:
        provider_cfg = cfg.providers.custom[provider_name]

    model = (
        f"{provider_name}/{provider_cfg.model}"
        if provider_cfg
        else "anthropic/claude-sonnet-4-5-20250929"
    )
    provider = LiteLLMProvider(
        default_model=model,
        api_key=provider_cfg.api_key if provider_cfg else None,
        api_base=provider_cfg.api_base if provider_cfg else None,
        retry_attempts=cfg.agent.retry_attempts,
        retry_delay=cfg.agent.retry_delay_seconds,
    )

    # Storage
    sessions = SessionManager(data_dir)
    memory = MemoryManager(data_dir / "memory.db")

    # ------------------------------------------------------------------
    # Connector registry
    # ------------------------------------------------------------------
    connector_registry = ConnectorRegistry()
    if cfg.connectors.instances:
        connector_cfgs = {name: c.to_dict() for name, c in cfg.connectors.instances.items()}
        connector_registry.load_from_config(connector_cfgs)

    # ------------------------------------------------------------------
    # Observability (tracing)
    # ------------------------------------------------------------------
    tracer = None
    if cfg.observability.enabled:
        from databot.observability import Tracer

        tracer = Tracer(
            service_name=cfg.observability.service_name,
            endpoint=cfg.observability.otlp_endpoint,
        )

    # ------------------------------------------------------------------
    # RAG context
    # ------------------------------------------------------------------
    rag_context = None
    if cfg.rag.enabled:
        try:
            from databot.rag import RAGContext, VectorStore

            store = VectorStore(
                persist_directory=cfg.rag.persist_directory,
                collection_name=cfg.rag.collection_name,
                embedding_model=cfg.rag.embedding_model,
                api_key=cfg.rag.api_key,
            )
            rag_context = RAGContext(
                store=store,
                max_context_docs=cfg.rag.max_context_docs,
                max_context_chars=cfg.rag.max_context_chars,
            )
        except ImportError:
            console.print("[yellow]RAG enabled but chromadb not installed. Skipping.[/]")

    # ------------------------------------------------------------------
    # Skills registry
    # ------------------------------------------------------------------
    from databot.skills import SkillRegistry

    skill_registry = SkillRegistry.from_config(cfg.skills.enabled)

    # ------------------------------------------------------------------
    # Tools
    # ------------------------------------------------------------------
    tools = ToolRegistry()
    _register_tools(tools, cfg, workspace, connector_registry, skill_registry)

    # ------------------------------------------------------------------
    # Multi-agent delegator
    # ------------------------------------------------------------------
    delegator = None
    if cfg.multi_agent.enabled:
        from databot.agents import build_default_agents

        _, _, delegator = build_default_agents(
            provider=provider,
            tools=tools,
            model=model,
        )

    return (
        bus,
        provider,
        tools,
        sessions,
        memory,
        workspace,
        connector_registry,
        rag_context,
        tracer,
        delegator,
        skill_registry,
    )


def _register_tools(tools, cfg, workspace: Path, connector_registry=None, skill_registry=None):
    """Register tools based on config and enabled skills."""
    from databot.tools.filesystem import EditFileTool, ListDirTool, ReadFileTool, WriteFileTool
    from databot.tools.shell import ShellTool
    from databot.tools.web import WebFetchTool, WebSearchTool

    allowed_dir = workspace if cfg.security.restrict_to_workspace else None

    # Determine which tools are allowed by skills
    enabled_tools = skill_registry.enabled_tool_names() if skill_registry else None  # None = all

    def _should_register(tool_name: str) -> bool:
        if enabled_tools is None:
            return True
        return tool_name in enabled_tools

    # Filesystem tools
    if _should_register("read_file"):
        tools.register(ReadFileTool(allowed_dir=allowed_dir))
    if _should_register("write_file"):
        tools.register(WriteFileTool(allowed_dir=allowed_dir))
    if _should_register("edit_file"):
        tools.register(EditFileTool(allowed_dir=allowed_dir))
    if _should_register("list_dir"):
        tools.register(ListDirTool(allowed_dir=allowed_dir))

    # Shell tool
    if cfg.tools.shell.enabled and _should_register("shell"):
        tools.register(
            ShellTool(
                working_dir=str(workspace),
                timeout=cfg.tools.shell.timeout,
                restrict_to_workspace=cfg.security.restrict_to_workspace,
                allowed_commands=cfg.security.allowed_commands or None,
                max_output_length=cfg.tools.shell.max_output_length,
            )
        )

    # Web tools
    if _should_register("web_fetch"):
        tools.register(WebFetchTool(max_length=cfg.tools.web.max_fetch_length))
    if cfg.tools.web.search_api_key and _should_register("web_search"):
        tools.register(
            WebSearchTool(
                api_key=cfg.tools.web.search_api_key,
                results_count=cfg.tools.web.search_results_count,
            )
        )

    # SQL tool (optionally connector-backed)
    if cfg.tools.sql.connections and _should_register("sql_query"):
        from databot.tools.sql import SQLTool

        conn_configs = {name: conn.model_dump() for name, conn in cfg.tools.sql.connections.items()}
        sql_tool = SQLTool(
            connections=conn_configs,
            read_only=cfg.tools.sql.read_only,
            max_rows=cfg.tools.sql.max_rows,
            connector_registry=connector_registry,
        )
        tools.register(sql_tool)

        # DQ tool (depends on SQL)
        if _should_register("data_quality"):
            from databot.tools.data_quality import DataQualityTool

            tools.register(DataQualityTool(sql_tool=sql_tool))

    # Airflow tool (optionally connector-backed)
    if cfg.tools.airflow.base_url and _should_register("airflow"):
        from databot.tools.airflow import AirflowTool

        tools.register(
            AirflowTool(
                base_url=cfg.tools.airflow.base_url,
                username=cfg.tools.airflow.username,
                password=cfg.tools.airflow.password,
                connector_registry=connector_registry,
            )
        )

    # Lineage tool (with optional Marquez backend)
    if (cfg.tools.lineage.graph_path or cfg.tools.lineage.marquez_url) and _should_register(
        "lineage"
    ):
        from databot.tools.lineage import LineageTool

        tools.register(
            LineageTool(
                graph_path=cfg.tools.lineage.graph_path,
                marquez_url=cfg.tools.lineage.marquez_url,
            )
        )

    # ------------------------------------------------------------------
    # Domain tools from connectors
    # ------------------------------------------------------------------
    if connector_registry:
        from databot.connectors.base import ConnectorType

        if connector_registry.get_by_type(ConnectorType.PROCESSING) and _should_register("spark"):
            from databot.tools.spark import SparkTool

            tools.register(SparkTool(registry=connector_registry))

        if connector_registry.get_by_type(ConnectorType.STREAMING) and _should_register("kafka"):
            from databot.tools.kafka import KafkaTool

            tools.register(KafkaTool(registry=connector_registry))

        if connector_registry.get_by_type(ConnectorType.CATALOG) and _should_register("catalog"):
            from databot.tools.catalog import CatalogTool

            tools.register(CatalogTool(registry=connector_registry))

    # Plugin tools
    tools.load_plugins(workspace=workspace)


async def _process_single(cfg, message: str) -> str:
    """Process a single message and return the response."""
    from databot.core.loop import AgentLoop

    (
        bus,
        provider,
        tools,
        sessions,
        memory,
        workspace,
        connector_registry,
        rag_context,
        tracer,
        delegator,
        _skill_registry,
    ) = _build_components(cfg)

    # Connect connectors
    if connector_registry:
        await connector_registry.connect_all()

    # If multi-agent is enabled, use the delegator directly
    if delegator:
        extra_context = ""
        if rag_context:
            extra_context = rag_context.enrich_prompt(message)
        result = await delegator.handle(message, extra_context=extra_context)
        if connector_registry:
            await connector_registry.disconnect_all()
        return result

    loop = AgentLoop(
        bus=bus,
        provider=provider,
        tools=tools,
        workspace=workspace,
        sessions=sessions,
        memory=memory,
        system_prompt=cfg.agent.system_prompt,
        max_iterations=cfg.agent.max_iterations,
        approval_required_tools=cfg.agent.tool_approval_required,
        rag_context=rag_context,
    )

    result = await loop.process_direct(message)

    if connector_registry:
        await connector_registry.disconnect_all()
    if tracer:
        tracer.shutdown()

    return result


async def _run_interactive(cfg):
    """Run interactive CLI mode."""
    from databot.channels.cli_channel import CLIChannel
    from databot.core.loop import AgentLoop

    (
        bus,
        provider,
        tools,
        sessions,
        memory,
        workspace,
        connector_registry,
        rag_context,
        tracer,
        delegator,
        _skill_registry,
    ) = _build_components(cfg)

    if connector_registry:
        await connector_registry.connect_all()

    loop = AgentLoop(
        bus=bus,
        provider=provider,
        tools=tools,
        workspace=workspace,
        sessions=sessions,
        memory=memory,
        system_prompt=cfg.agent.system_prompt,
        max_iterations=cfg.agent.max_iterations,
        approval_required_tools=cfg.agent.tool_approval_required,
        rag_context=rag_context,
    )

    cli = CLIChannel(bus)

    # Run agent loop and CLI concurrently
    agent_task = asyncio.create_task(loop.run())
    try:
        await cli.start()
    finally:
        loop.stop()
        agent_task.cancel()
        if connector_registry:
            await connector_registry.disconnect_all()
        if tracer:
            tracer.shutdown()


async def _run_gateway(cfg, port: int):
    """Run the gateway with all channels, cron, SSE streaming, and connectors."""
    import uvicorn
    from fastapi import FastAPI
    from loguru import logger
    from starlette.middleware.cors import CORSMiddleware

    from databot.core.loop import AgentLoop
    from databot.cron.service import CronService
    from databot.middleware.auth import APIKeyAuthMiddleware
    from databot.middleware.rate_limit import RateLimitMiddleware
    from databot.tools.cron import CronTool

    (
        bus,
        provider,
        tools,
        sessions,
        memory,
        workspace,
        connector_registry,
        rag_context,
        tracer,
        delegator,
        skill_registry,
    ) = _build_components(cfg)

    data_dir = _get_data_dir()

    # Connect connectors
    if connector_registry:
        health = await connector_registry.connect_all()
        for name, status in health.items():
            logger.info(f"  Connector '{name}': {status.value}")

    # Cron service
    cron_service = CronService(data_dir, bus)
    tools.register(CronTool(cron_service))

    # Load cron jobs from config
    for job in cfg.cron.jobs:
        if job.enabled:
            try:
                cron_service.add_job(job.name, job.schedule, job.message, job.channel)
            except Exception as e:
                logger.warning(f"Failed to load cron job '{job.name}': {e}")

    # Agent loop
    loop = AgentLoop(
        bus=bus,
        provider=provider,
        tools=tools,
        workspace=workspace,
        sessions=sessions,
        memory=memory,
        system_prompt=cfg.agent.system_prompt,
        max_iterations=cfg.agent.max_iterations,
        approval_required_tools=cfg.agent.tool_approval_required,
        rag_context=rag_context,
    )

    # FastAPI app
    api = FastAPI(title="databot", version="0.2.0")

    # Add middleware
    api.add_middleware(
        CORSMiddleware,
        allow_origins=cfg.gateway.cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    api.add_middleware(APIKeyAuthMiddleware, api_keys=cfg.gateway.api_keys)
    api.add_middleware(RateLimitMiddleware, requests_per_minute=cfg.gateway.rate_limit_rpm)

    @api.get("/health")
    async def health():
        connector_health = {}
        if connector_registry:
            raw = await connector_registry.health_check_all()
            connector_health = {n: s.value for n, s in raw.items()}
        return {
            "status": "ok",
            "version": "0.2.0",
            "connectors": connector_health,
            "skills": [s.name for s in skill_registry.enabled_skills()],
            "ui_enabled": cfg.ui.enabled,
        }

    @api.post("/api/v1/message")
    async def post_message(request: dict):
        from fastapi.responses import JSONResponse

        from databot.core.bus import InboundMessage

        content = request.get("message", "").strip()
        if not content:
            return JSONResponse({"error": "Empty message"}, status_code=400)

        msg = InboundMessage(
            channel="api",
            sender_id=request.get("sender", "api"),
            chat_id=request.get("chat_id", "api"),
            content=content,
        )

        try:
            # Multi-agent routing if enabled
            if delegator:
                extra_context = ""
                if rag_context:
                    extra_context = rag_context.enrich_prompt(msg.content)
                result = await delegator.handle_with_metadata(
                    msg.content,
                    extra_context=extra_context,
                )
                return {
                    "response": result["response"],
                    "agent": result.get("agent"),
                }

            response = await loop.process_message(msg)
            return {"response": response.content if response else ""}
        except Exception as e:
            logger.error(f"Message processing error: {e}")
            err_msg = str(e)
            if "API Key" in err_msg or "AuthenticationError" in err_msg:
                err_msg = (
                    "LLM API key not configured. Set ANTHROPIC_API_KEY "
                    "environment variable or run 'databot init'."
                )
            return JSONResponse({"error": err_msg}, status_code=500)

    @api.post("/api/v1/stream")
    async def post_message_stream(request: dict):
        """SSE streaming endpoint."""
        from fastapi.responses import JSONResponse

        from databot.core.bus import InboundMessage

        content = request.get("message", "").strip()
        if not content:
            return JSONResponse({"error": "Empty message"}, status_code=400)

        try:
            from sse_starlette.sse import EventSourceResponse
        except ImportError:
            return JSONResponse(
                {"error": "sse-starlette not installed. pip install databot[streaming]"},
                status_code=500,
            )

        import json as _json

        msg = InboundMessage(
            channel="api",
            sender_id=request.get("sender", "api"),
            chat_id=request.get("chat_id", "api"),
            content=content,
            stream=True,
        )

        async def _event_generator():
            try:
                async for event in loop.process_message_stream(msg):
                    yield {
                        "event": event.event_type,
                        "data": _json.dumps(
                            {
                                "type": event.event_type,
                                "data": event.data,
                                "tool_name": event.tool_name,
                            }
                        ),
                    }
            except Exception as e:
                err_msg = str(e)
                if "API Key" in err_msg or "AuthenticationError" in err_msg:
                    err_msg = (
                        "LLM API key not configured. Set ANTHROPIC_API_KEY "
                        "environment variable or run 'databot init'."
                    )
                yield {
                    "event": "error",
                    "data": _json.dumps({"type": "error", "data": err_msg}),
                }

        return EventSourceResponse(_event_generator())

    @api.get("/api/v1/connectors")
    async def list_connectors():
        """List all registered connectors and their status."""
        if not connector_registry:
            return {"connectors": []}
        result = []
        for conn in connector_registry.list_all():
            result.append(
                {
                    "name": conn.name,
                    "type": conn.connector_type.value,
                    "connected": conn.is_connected,
                    "capabilities": conn.capabilities(),
                }
            )
        return {"connectors": result}

    @api.get("/api/v1/sessions")
    async def list_sessions():
        """List all sessions with metadata."""
        keys = sessions.store.list_keys()
        result = []
        for key in keys:
            meta = sessions.store.get_metadata(key)
            result.append(meta)
        return {"sessions": result}

    @api.get("/api/v1/sessions/{key:path}")
    async def get_session(key: str):
        """Get session history by key."""
        history = sessions.store.get_history(key)
        if not history and key not in sessions.store.list_keys():
            from fastapi.responses import JSONResponse

            return JSONResponse({"error": "Session not found"}, status_code=404)
        return {"key": key, "messages": history}

    @api.delete("/api/v1/sessions/{key:path}")
    async def delete_session_endpoint(key: str):
        """Delete a session."""
        try:
            sessions.delete(key)
        except Exception:
            pass  # Silently ignore if session doesn't exist
        return {"deleted": key}

    @api.get("/api/v1/tools")
    async def list_tools():
        """List all registered tools and their schemas."""
        defs = tools.get_definitions()
        result = []
        for d in defs:
            fn = d.get("function", d)
            result.append(
                {
                    "name": fn.get("name", ""),
                    "description": fn.get("description", ""),
                    "parameters": fn.get("parameters", {}),
                }
            )
        return {"tools": result}

    # ------------------------------------------------------------------
    # Skills API
    # ------------------------------------------------------------------
    @api.get("/api/v1/skills")
    async def list_skills():
        """List all skills with enabled/disabled status."""
        return {"skills": skill_registry.summary()}

    @api.put("/api/v1/skills/{name}")
    async def update_skill(name: str, body: dict):
        """Enable or disable a skill at runtime."""
        enabled = body.get("enabled")
        if enabled is None:
            from fastapi.responses import JSONResponse

            return JSONResponse({"error": "missing 'enabled' field"}, status_code=400)
        if enabled:
            skill_registry.enable(name)
        else:
            skill_registry.disable(name)
        return {"name": name, "enabled": skill_registry.is_enabled(name)}

    # ------------------------------------------------------------------
    # Embedded UI
    # ------------------------------------------------------------------
    if cfg.ui.enabled:
        from fastapi.responses import FileResponse, HTMLResponse

        from databot.ui import STATIC_DIR

        @api.get("/ui")
        async def ui_index():
            index_path = STATIC_DIR / "index.html"
            if index_path.exists():
                return FileResponse(index_path, media_type="text/html")
            return HTMLResponse("<h1>databot UI not found</h1>", status_code=404)

    # Google Chat routes
    if cfg.channels.gchat.enabled:
        from databot.channels.gchat import GChatChannel

        gchat = GChatChannel(
            bus=bus,
            mode=cfg.channels.gchat.mode,
            webhook_url=cfg.channels.gchat.webhook_url,
        )
        await gchat.start()

        if cfg.channels.gchat.mode == "app":
            api.include_router(gchat.get_fastapi_routes())

    # Slack channel
    if cfg.channels.slack.enabled:
        from databot.channels.slack import SlackChannel

        slack = SlackChannel(
            bus=bus,
            bot_token=cfg.channels.slack.bot_token,
            app_token=cfg.channels.slack.app_token,
            signing_secret=cfg.channels.slack.signing_secret,
        )
        await slack.start()
        logger.info("Slack channel started")

    # Discord channel
    if cfg.channels.discord.enabled:
        from databot.channels.discord import DiscordChannel

        discord_channel = DiscordChannel(
            bus=bus,
            bot_token=cfg.channels.discord.bot_token,
            command_prefix=cfg.channels.discord.command_prefix,
        )
        asyncio.create_task(discord_channel.start())
        logger.info("Discord channel started")

    # Start services
    agent_task = asyncio.create_task(loop.run())
    cron_task = asyncio.create_task(cron_service.run())

    config = uvicorn.Config(api, host=cfg.gateway.host, port=port, log_level="info")
    server = uvicorn.Server(config)

    console.print(f"[bold green]databot gateway[/] starting on port {port}")
    console.print(f"  Health: http://0.0.0.0:{port}/health")
    console.print(f"  API:    http://0.0.0.0:{port}/api/v1/message")
    if cfg.ui.enabled:
        console.print(f"  UI:     http://0.0.0.0:{port}/ui")

    try:
        await server.serve()
    finally:
        loop.stop()
        cron_service.stop()
        agent_task.cancel()
        cron_task.cancel()
        if connector_registry:
            await connector_registry.disconnect_all()
        if tracer:
            tracer.shutdown()


if __name__ == "__main__":
    app()
