# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Slack channel** integration via Slack Bolt (Socket Mode + HTTP)
- **Discord channel** integration via discord.py
- **Gateway authentication** with API key support (`Authorization: Bearer` and `X-API-Key` headers)
- **Rate limiting** middleware for the gateway (configurable RPM per client IP)
- **Human-in-the-loop tool approval** — configure sensitive tools that require user confirmation
- **Web search tool** now wired up in agent mode (previously only existed as code)
- **Token usage tracking** — cumulative prompt/completion token counts per provider
- **LLM retry with exponential backoff** — configurable retry attempts for transient failures
- **CORS support** for the gateway
- **SQL identifier validation** in Data Quality tool to prevent SQL injection
- **Multi-statement detection** in SQL read-only enforcement
- **CTE write detection** (e.g., `WITH cte AS (...) INSERT INTO ...`)
- New config sections: `gateway`, `tools.web`, `channels.slack`, `channels.discord`
- Configurable: `max_session_messages`, `shell.max_output_length`, `web.max_fetch_length`, `cron.check_interval_seconds`
- Comprehensive test suite: SQL tool, DQ tool, config, middleware, session/memory, context builder, web tools
- CHANGELOG.md, CODE_OF_CONDUCT.md, issue templates, PR template
- Pre-commit configuration with ruff and mypy
- Docker security hardening (non-root user, read-only filesystem)

### Changed
- SQL tool now runs blocking database I/O in `asyncio.to_thread()` to avoid blocking the event loop
- Error messages from the agent loop no longer expose raw exception details to users
- Message bus now logs outbound handler failures instead of silently swallowing them
- Plugin loader docstring syntax error fixed (`from __future__` was inside docstring)
- `WebFetchTool` and `WebSearchTool` constructors now accept configurable parameters
- `ShellTool` max output length is now configurable
- `Session.max_messages` is now configurable via `agent.max_session_messages`

### Security
- SQL injection prevention in Data Quality tool via identifier validation
- Enhanced SQL read-only enforcement: multi-statement blocking, CTE+write detection, expanded keyword list
- Gateway API endpoints protected with API key authentication
- Rate limiting prevents LLM cost explosion from abuse
- Docker container runs as non-root user
- Sanitized error messages prevent information leakage

## [0.1.0] - 2025-12-01

### Added
- Initial release
- Core agent loop with multi-turn tool calling
- SQL, Airflow, Data Quality, Lineage, Shell, Filesystem, Web, Cron tools
- Google Chat channel (webhook + app modes)
- CLI channel with Rich rendering
- LiteLLM multi-provider support
- SQLite-backed sessions, memory, and cron storage
- Plugin system via Python entry points
- Pydantic v2 configuration with YAML + env var substitution
- Docker + Kubernetes deployment manifests
- CI/CD pipeline with GitHub Actions
