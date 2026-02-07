# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-02-07

### Added
- **Connector framework** — pluggable SQL, REST, Spark, Kafka, and Data Catalog connectors with factory pattern
- **MCP (Model Context Protocol) server** — expose databot tools over stdio/SSE for IDE integration
- **Multi-agent architecture** — coordinator delegates to specialist sub-agents (SQL, pipeline, quality)
- **RAG pipeline** — ChromaDB-backed document retrieval for runbook-augmented answers
- **Observability** — OpenTelemetry tracing with span context, distributed trace propagation
- **SSE streaming** — real-time token streaming via Server-Sent Events endpoint
- **Connector config** in YAML — declare SQL/Spark/Kafka/Catalog connections with health checks
- Spark tool — job submission, status, logs, cluster info via Spark REST API
- Kafka tool — topic listing, consumer group lag, message peek via Kafka REST Proxy
- Catalog tool — dataset search, schema/lineage/quality lookup via data catalog APIs
- Integration bootstrap — `_build_components` wires connectors, RAG, multi-agent, tracing into CLI/gateway
- `databot mcp` CLI command to launch MCP server

### Changed
- `SQLTool` and `AirflowTool` now delegate to connectors instead of direct DB/HTTP calls
- Agent loop supports streaming token callbacks
- Gateway serves SSE stream at `/v1/stream`

### v0.1.1 (included)
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
- Comprehensive test suite: 188 tests covering all modules
- CHANGELOG.md, CODE_OF_CONDUCT.md, issue templates, PR template
- Docker security hardening (non-root user, read-only filesystem)

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
