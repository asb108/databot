"""FastAPI application factory for the databot gateway.

``build_api`` constructs the gateway's HTTP app from already-built components
and returns it. It performs NO lifecycle work — no connector connect, no channel
``.start()``, no uvicorn. The ``gateway()`` CLI command owns all of that. Keeping
app construction here makes the full HTTP surface testable via ``TestClient``.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger  # used by moved route closures (post_message, config PUT, ...)

from databot.middleware.auth import APIKeyAuthMiddleware
from databot.middleware.rate_limit import RateLimitMiddleware


def build_api(
    *,
    cfg: Any,
    tools: Any,
    sessions: Any,
    connector_registry: Any,
    rag_context: Any,
    delegator: Any,
    skill_registry: Any,
    usage_tracker: Any,
    monitor_service: Any,
    loop: Any,
    metrics: Any,
    audit_log: Any,
    channels: dict[str, Any] | None = None,
) -> FastAPI:
    """Build the gateway FastAPI app from constructed components.

    ``channels`` is a mutable name->channel dict shared with ``gateway()``; its
    webhook routers are registered here, and the PUT /api/v1/config route reads
    it (as ``_channels``) to start/stop channels at runtime. Pass ``None`` (the
    default) for an app with no channel webhook routes; the runtime start/stop
    logic in PUT /api/v1/config then becomes a no-op.
    """
    api = FastAPI(title="databot", version="0.3.0")

    api.add_middleware(
        CORSMiddleware,
        allow_origins=cfg.gateway.cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    api.add_middleware(APIKeyAuthMiddleware, api_keys=cfg.gateway.api_keys)
    api.add_middleware(RateLimitMiddleware, requests_per_minute=cfg.gateway.rate_limit_rpm)

    # Shared reference: the route closures below capture this handle, so runtime
    # mutations by gateway() (e.g. adding slack/discord) are visible to them.
    _channels = channels if channels is not None else {}

    @api.get("/health")
    async def health():
        connector_health = {}
        if connector_registry:
            raw = await connector_registry.health_check_all()
            connector_health = {n: s.value for n, s in raw.items()}
        return {
            "status": "ok",
            "version": "0.3.0",
            "connectors": connector_health,
            "skills": [s.name for s in skill_registry.enabled_skills()],
            "ui_enabled": cfg.ui.enabled,
        }

    @api.get("/livez")
    async def livez():
        """Liveness probe — the process is up and serving."""
        return {"status": "alive"}

    @api.get("/readyz")
    async def readyz():
        """Readiness probe — dependencies (connectors) are reachable.

        Returns 503 when any configured connector is unreachable so an
        orchestrator can hold traffic until the bot can actually do work.
        """
        from fastapi.responses import JSONResponse

        from databot.connectors.base import ConnectorStatus

        connector_health: dict[str, str] = {}
        ready = True
        if connector_registry:
            raw = await connector_registry.health_check_all()
            connector_health = {n: s.value for n, s in raw.items()}
            ready = all(s != ConnectorStatus.UNREACHABLE for s in raw.values())
        body = {"status": "ready" if ready else "not_ready", "connectors": connector_health}
        return JSONResponse(body, status_code=200 if ready else 503)

    @api.get("/metrics")
    async def get_prometheus_metrics():
        """Prometheus exposition endpoint."""
        from fastapi.responses import Response

        return Response(content=metrics.render(), media_type=metrics.content_type)

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
                    session_key=msg.session_key,
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
        """List all registered connectors and their status, plus configured ones."""
        result = []
        registered_names = set()

        # Active/registered connectors
        if connector_registry:
            for conn in connector_registry.list_all():
                registered_names.add(conn.name)
                health = await conn.health_check()
                result.append(
                    {
                        "name": conn.name,
                        "type": conn.connector_type.value,
                        "connected": conn.is_connected,
                        "status": health.value,
                        "capabilities": conn.capabilities(),
                        "source": "active",
                    }
                )

        # Config-defined connectors (not yet registered)
        if cfg.connectors and cfg.connectors.instances:
            for name, conn_cfg in cfg.connectors.instances.items():
                if name not in registered_names:
                    result.append(
                        {
                            "name": name,
                            "type": conn_cfg.type,
                            "connected": False,
                            "status": "not_configured",
                            "capabilities": [],
                            "source": "config",
                            "config_summary": {
                                "driver": conn_cfg.driver or None,
                                "host": conn_cfg.host or None,
                                "database": conn_cfg.database or None,
                                "read_only": conn_cfg.read_only,
                            },
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
    # Configuration API
    # ------------------------------------------------------------------
    @api.get("/api/v1/config")
    async def get_config():
        """Return current configuration (safe subset)."""
        return {
            "providers": {
                "default": cfg.providers.default,
                "available": list(
                    {
                        "anthropic": cfg.providers.anthropic,
                        "openai": cfg.providers.openai,
                        "deepseek": cfg.providers.deepseek,
                        "gemini": cfg.providers.gemini,
                        "qwen": cfg.providers.qwen,
                        "mistral": cfg.providers.mistral,
                        "groq": cfg.providers.groq,
                    }.keys()
                ),
                "models": {
                    "anthropic": cfg.providers.anthropic.model,
                    "openai": cfg.providers.openai.model,
                    "deepseek": cfg.providers.deepseek.model,
                    "gemini": cfg.providers.gemini.model,
                    "qwen": cfg.providers.qwen.model,
                    "mistral": cfg.providers.mistral.model,
                    "groq": cfg.providers.groq.model,
                },
            },
            "agent": {
                "max_iterations": cfg.agent.max_iterations,
                "max_session_messages": cfg.agent.max_session_messages,
                "system_prompt": cfg.agent.system_prompt,
                "retry_attempts": cfg.agent.retry_attempts,
                "tool_approval_required": cfg.agent.tool_approval_required,
            },
            "gateway": {
                "port": cfg.gateway.port,
                "rate_limit_rpm": cfg.gateway.rate_limit_rpm,
                "cors_origins": cfg.gateway.cors_origins,
            },
            "channels": {
                "gchat": {"enabled": cfg.channels.gchat.enabled, "mode": cfg.channels.gchat.mode},
                "slack": {"enabled": cfg.channels.slack.enabled},
                "discord": {"enabled": cfg.channels.discord.enabled},
                "whatsapp": {"enabled": cfg.channels.whatsapp.enabled},
                "telegram": {
                    "enabled": cfg.channels.telegram.enabled,
                    "mode": cfg.channels.telegram.mode,
                },
            },
            "ui": {"enabled": cfg.ui.enabled, "theme": cfg.ui.theme},
            "rag": {"enabled": cfg.rag.enabled},
            "observability": {
                "enabled": cfg.observability.enabled,
                "service_name": cfg.observability.service_name,
            },
            "cron": {
                "jobs_count": len(cfg.cron.jobs),
                "check_interval": cfg.cron.check_interval_seconds,
            },
        }

    @api.put("/api/v1/config")
    async def update_config(body: dict):
        """Update configuration fields at runtime."""
        from fastapi.responses import JSONResponse

        updated = []
        errors = []

        # Provider default
        if "default_provider" in body:
            valid = ["anthropic", "openai", "deepseek", "gemini", "qwen", "mistral", "groq"]
            if body["default_provider"] in valid:
                cfg.providers.default = body["default_provider"]
                updated.append("default_provider")
            else:
                errors.append(f"Invalid provider: {body['default_provider']}")

        # Agent settings
        if "max_iterations" in body:
            val = int(body["max_iterations"])
            if 1 <= val <= 100:
                cfg.agent.max_iterations = val
                loop.max_iterations = val
                updated.append("max_iterations")
            else:
                errors.append("max_iterations must be 1-100")

        if "system_prompt" in body:
            cfg.agent.system_prompt = str(body["system_prompt"])
            loop.system_prompt = cfg.agent.system_prompt
            updated.append("system_prompt")

        if "max_session_messages" in body:
            val = int(body["max_session_messages"])
            if 1 <= val <= 500:
                cfg.agent.max_session_messages = val
                updated.append("max_session_messages")
            else:
                errors.append("max_session_messages must be 1-500")

        if "rate_limit_rpm" in body:
            val = int(body["rate_limit_rpm"])
            if val >= 0:
                cfg.gateway.rate_limit_rpm = val
                updated.append("rate_limit_rpm")

        if "theme" in body:
            cfg.ui.theme = body["theme"]
            updated.append("theme")

        # Channel settings
        if "channels" in body and isinstance(body["channels"], dict):
            valid_channels = ["gchat", "slack", "discord", "whatsapp", "telegram"]
            for ch_name, ch_data in body["channels"].items():
                if ch_name not in valid_channels:
                    errors.append(f"Unknown channel: {ch_name}")
                    continue
                ch_cfg = getattr(cfg.channels, ch_name)
                was_enabled = getattr(ch_cfg, "enabled", False)
                if isinstance(ch_data, dict):
                    for k, v in ch_data.items():
                        if hasattr(ch_cfg, k):
                            setattr(ch_cfg, k, v)
                    updated.append(f"channels.{ch_name}")

                # Auto-start/stop channels that need background tasks
                now_enabled = getattr(ch_cfg, "enabled", False)
                if ch_name in _channels:
                    ch_obj = _channels[ch_name]
                    if now_enabled and not was_enabled:
                        await ch_obj.start()
                        logger.info(f"Channel '{ch_name}' started via config update")
                    elif not now_enabled and was_enabled:
                        await ch_obj.stop()
                        logger.info(f"Channel '{ch_name}' stopped via config update")

        if errors:
            return JSONResponse(
                {"updated": updated, "errors": errors},
                status_code=400 if not updated else 200,
            )
        return {"updated": updated, "errors": []}

    # ------------------------------------------------------------------
    # Connector Configuration API
    # ------------------------------------------------------------------
    @api.get("/api/v1/config/connectors")
    async def list_configured_connectors():
        """Return all connector instances from config."""
        result = {}
        for name, cc in cfg.connectors.instances.items():
            result[name] = {
                "type": cc.type,
                "driver": cc.driver,
                "host": cc.host,
                "port": cc.port,
                "database": cc.database,
                "username": cc.username,
                "base_url": cc.base_url,
                "read_only": cc.read_only,
                "timeout": cc.timeout,
                "max_rows": cc.max_rows,
            }
        return {"connectors": result}

    @api.put("/api/v1/config/connectors/{name}")
    async def upsert_connector(name: str, body: dict):
        """Add or update a connector configuration."""
        from databot.config.schema import ConnectorConfig
        from databot.connectors.factory import create_connector

        try:
            cc = ConnectorConfig(**body)
            cfg.connectors.instances[name] = cc

            # Load into live connector registry
            if connector_registry:
                try:
                    connector = create_connector(name, cc.to_dict())
                    connector_registry.register(connector)
                    await connector.connect()
                    logger.info(f"Connector '{name}' registered and connected")
                except Exception as e:
                    logger.warning(f"Connector '{name}' saved but connect failed: {e}")

            # Auto-register Airflow tool if this is the airflow connector
            if name == "airflow" and cc.type == "rest_api":
                if not tools.get("airflow"):
                    from databot.tools.airflow import AirflowTool

                    tools.register(
                        AirflowTool(
                            base_url=cc.base_url,
                            username=cc.username,
                            password=cc.password,
                            connector_registry=connector_registry,
                        )
                    )
                    logger.info("AirflowTool auto-registered from connector config")

            return {"status": "ok", "name": name, "action": "upserted"}
        except Exception as e:
            from fastapi.responses import JSONResponse

            return JSONResponse({"error": str(e)}, status_code=400)

    @api.delete("/api/v1/config/connectors/{name}")
    async def delete_connector(name: str):
        """Remove a connector configuration."""
        from fastapi.responses import JSONResponse

        if name in cfg.connectors.instances:
            del cfg.connectors.instances[name]
            # Deregister from live registry
            if connector_registry:
                connector_registry.deregister(name)
            return {"status": "ok", "name": name, "action": "deleted"}
        return JSONResponse({"error": f"Connector '{name}' not found"}, status_code=404)

    # ------------------------------------------------------------------
    # Channel Configuration API
    # ------------------------------------------------------------------
    @api.get("/api/v1/config/channels")
    async def get_channels_config():
        """Return full channel configurations (masking secrets)."""

        def _mask(v):
            if not v:
                return ""
            return v[:4] + "****" + v[-4:] if len(v) > 8 else "****"

        return {
            "channels": {
                "gchat": {
                    "enabled": cfg.channels.gchat.enabled,
                    "mode": cfg.channels.gchat.mode,
                    "webhook_url": _mask(cfg.channels.gchat.webhook_url),
                    "project_id": cfg.channels.gchat.project_id,
                },
                "slack": {
                    "enabled": cfg.channels.slack.enabled,
                    "bot_token": _mask(cfg.channels.slack.bot_token),
                    "default_channel": cfg.channels.slack.default_channel,
                },
                "discord": {
                    "enabled": cfg.channels.discord.enabled,
                    "bot_token": _mask(cfg.channels.discord.bot_token),
                    "command_prefix": cfg.channels.discord.command_prefix,
                },
                "whatsapp": {
                    "enabled": cfg.channels.whatsapp.enabled,
                    "phone_number_id": cfg.channels.whatsapp.phone_number_id,
                    "access_token": _mask(cfg.channels.whatsapp.access_token),
                    "verify_token": _mask(cfg.channels.whatsapp.verify_token),
                },
                "telegram": {
                    "enabled": cfg.channels.telegram.enabled,
                    "bot_token": _mask(cfg.channels.telegram.bot_token),
                    "mode": cfg.channels.telegram.mode,
                    "webhook_url": cfg.channels.telegram.webhook_url,
                },
            }
        }

    @api.get("/api/v1/stats")
    async def get_stats():
        """Return runtime statistics."""

        session_keys = sessions.store.list_keys()
        total_messages = 0
        for key in session_keys:
            h = sessions.store.get_history(key)
            total_messages += len(h)

        return {
            "sessions_count": len(session_keys),
            "total_messages": total_messages,
            "tools_count": len(tools.get_definitions()),
            "skills_total": len(skill_registry.summary()),
            "skills_enabled": len([s for s in skill_registry.summary() if s.get("enabled")]),
            "connectors_count": len(connector_registry.list_all()) if connector_registry else 0,
            "provider": cfg.providers.default,
            "model": getattr(cfg.providers, cfg.providers.default, cfg.providers.anthropic).model,
            "cron_jobs": len(cfg.cron.jobs),
            "usage": usage_tracker.global_stats(),
            "session_cost_budget_usd": usage_tracker.session_budget_usd,
        }

    @api.get("/api/v1/monitors")
    async def get_monitors():
        """List configured data monitors and their latest status."""
        return {"monitors": monitor_service.list_monitors()}

    @api.get("/api/v1/audit")
    async def get_audit(limit: int = 100, tool: str = ""):
        """Return recent tool-execution audit records."""
        return {
            "enabled": audit_log.enabled,
            "entries": audit_log.recent(limit=min(limit, 1000), tool=tool),
        }

    @api.get("/api/v1/queries")
    async def get_saved_queries():
        """List saved queries in the library."""
        sq = tools.get("saved_query")
        return {"queries": sq.store.list_all() if sq else []}

    @api.get("/api/v1/usage")
    async def get_usage():
        """Return LLM token usage and cost, globally and per session."""
        return {
            "global": usage_tracker.global_stats(),
            "session_budget_usd": usage_tracker.session_budget_usd,
            "sessions": usage_tracker.all_session_stats(),
        }

    @api.post("/api/v1/config/save")
    async def save_config():
        """Persist current config to disk."""
        try:
            cfg.save()
            return {"status": "saved"}
        except Exception as e:
            from fastapi.responses import JSONResponse

            return JSONResponse({"error": str(e)}, status_code=500)

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
                return FileResponse(
                    index_path,
                    media_type="text/html",
                    headers={
                        "Cache-Control": "no-cache, no-store, must-revalidate",
                        "Pragma": "no-cache",
                        "Expires": "0",
                    },
                )
            return HTMLResponse("<h1>databot UI not found</h1>", status_code=404)

    # Channel webhook routers — always registered so runtime-enable works.
    for _ch in _channels.values():
        api.include_router(_ch.get_fastapi_routes())

    return api
