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
    """Initialize databot configuration and workspace."""
    data_dir = _get_data_dir()
    config_path = _get_config_path()

    if config_path.exists():
        console.print(f"[yellow]Config already exists at {config_path}[/]")
        overwrite = typer.confirm("Overwrite?", default=False)
        if not overwrite:
            console.print("[dim]Keeping existing config.[/]")
            return

    data_dir.mkdir(parents=True, exist_ok=True)

    # Write default config
    from databot.config.schema import DatabotConfig

    config = DatabotConfig()
    config.save(config_path)

    console.print(f"[green]Created config at {config_path}[/]")
    console.print(f"[green]Data directory: {data_dir}[/]")
    console.print("\n[bold]Next steps:[/]")
    console.print(f"  1. Edit {config_path} to add your API keys")
    console.print("  2. Run [bold]databot agent -m 'Hello!'[/] to test")
    console.print("  3. Run [bold]databot gateway[/] to start the always-on service")


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
        console.print(f"  GChat enabled: {cfg.channels.gchat.enabled}")
        console.print(f"  Cron jobs: {len(cfg.cron.jobs)}")

        if cfg.tools.sql.connections:
            console.print(f"  SQL connections: {', '.join(cfg.tools.sql.connections.keys())}")

        provider = getattr(cfg.providers, cfg.providers.default, None)
        if provider and provider.api_key and not provider.api_key.startswith("${"):
            console.print("  API key: [green]configured[/]")
        else:
            console.print("  API key: [red]not set[/]")


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
    """Build all components from config."""
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
    )

    # Storage
    sessions = SessionManager(data_dir)
    memory = MemoryManager(data_dir / "memory.db")

    # Tools
    tools = ToolRegistry()
    _register_tools(tools, cfg, workspace)

    return bus, provider, tools, sessions, memory, workspace


def _register_tools(tools, cfg, workspace: Path):
    """Register all tools based on config."""
    from databot.tools.filesystem import EditFileTool, ListDirTool, ReadFileTool, WriteFileTool
    from databot.tools.shell import ShellTool
    from databot.tools.web import WebFetchTool

    allowed_dir = workspace if cfg.security.restrict_to_workspace else None

    # Filesystem tools
    tools.register(ReadFileTool(allowed_dir=allowed_dir))
    tools.register(WriteFileTool(allowed_dir=allowed_dir))
    tools.register(EditFileTool(allowed_dir=allowed_dir))
    tools.register(ListDirTool(allowed_dir=allowed_dir))

    # Shell tool
    if cfg.tools.shell.enabled:
        tools.register(
            ShellTool(
                working_dir=str(workspace),
                timeout=cfg.tools.shell.timeout,
                restrict_to_workspace=cfg.security.restrict_to_workspace,
                allowed_commands=cfg.security.allowed_commands or None,
            )
        )

    # Web tools
    tools.register(WebFetchTool())

    # SQL tool
    if cfg.tools.sql.connections:
        from databot.tools.sql import SQLTool

        conn_configs = {name: conn.model_dump() for name, conn in cfg.tools.sql.connections.items()}
        sql_tool = SQLTool(
            connections=conn_configs,
            read_only=cfg.tools.sql.read_only,
            max_rows=cfg.tools.sql.max_rows,
        )
        tools.register(sql_tool)

        # DQ tool (depends on SQL)
        from databot.tools.data_quality import DataQualityTool

        tools.register(DataQualityTool(sql_tool=sql_tool))

    # Airflow tool
    if cfg.tools.airflow.base_url:
        from databot.tools.airflow import AirflowTool

        tools.register(
            AirflowTool(
                base_url=cfg.tools.airflow.base_url,
                username=cfg.tools.airflow.username,
                password=cfg.tools.airflow.password,
            )
        )

    # Lineage tool
    if cfg.tools.lineage.graph_path:
        from databot.tools.lineage import LineageTool

        tools.register(LineageTool(graph_path=cfg.tools.lineage.graph_path))


async def _process_single(cfg, message: str) -> str:
    """Process a single message and return the response."""
    from databot.core.loop import AgentLoop

    bus, provider, tools, sessions, memory, workspace = _build_components(cfg)

    loop = AgentLoop(
        bus=bus,
        provider=provider,
        tools=tools,
        workspace=workspace,
        sessions=sessions,
        memory=memory,
        system_prompt=cfg.agent.system_prompt,
        max_iterations=cfg.agent.max_iterations,
    )

    return await loop.process_direct(message)


async def _run_interactive(cfg):
    """Run interactive CLI mode."""
    from databot.channels.cli_channel import CLIChannel
    from databot.core.loop import AgentLoop

    bus, provider, tools, sessions, memory, workspace = _build_components(cfg)

    loop = AgentLoop(
        bus=bus,
        provider=provider,
        tools=tools,
        workspace=workspace,
        sessions=sessions,
        memory=memory,
        system_prompt=cfg.agent.system_prompt,
        max_iterations=cfg.agent.max_iterations,
    )

    cli = CLIChannel(bus)

    # Run agent loop and CLI concurrently
    agent_task = asyncio.create_task(loop.run())
    try:
        await cli.start()
    finally:
        loop.stop()
        agent_task.cancel()


async def _run_gateway(cfg, port: int):
    """Run the gateway with all channels and cron."""
    import uvicorn
    from fastapi import FastAPI

    from databot.core.loop import AgentLoop
    from databot.cron.service import CronService
    from databot.tools.cron import CronTool

    bus, provider, tools, sessions, memory, workspace = _build_components(cfg)
    data_dir = _get_data_dir()

    # Cron service
    cron_service = CronService(data_dir, bus)
    tools.register(CronTool(cron_service))

    # Load cron jobs from config
    for job in cfg.cron.jobs:
        if job.enabled:
            try:
                cron_service.add_job(job.name, job.schedule, job.message, job.channel)
            except Exception:
                pass

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
    )

    # FastAPI app
    api = FastAPI(title="databot", version="0.1.0")

    @api.get("/health")
    async def health():
        return {"status": "ok", "version": "0.1.0"}

    @api.post("/api/v1/message")
    async def post_message(request: dict):
        from databot.core.bus import InboundMessage

        msg = InboundMessage(
            channel="api",
            sender_id=request.get("sender", "api"),
            chat_id=request.get("chat_id", "api"),
            content=request.get("message", ""),
        )
        response = await loop.process_message(msg)
        return {"response": response.content if response else ""}

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

    # Start services
    agent_task = asyncio.create_task(loop.run())
    cron_task = asyncio.create_task(cron_service.run())

    config = uvicorn.Config(api, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)

    console.print(f"[bold green]databot gateway[/] starting on port {port}")
    console.print(f"  Health: http://0.0.0.0:{port}/health")
    console.print(f"  API:    http://0.0.0.0:{port}/api/v1/message")

    try:
        await server.serve()
    finally:
        loop.stop()
        cron_service.stop()
        agent_task.cancel()
        cron_task.cancel()


if __name__ == "__main__":
    app()
