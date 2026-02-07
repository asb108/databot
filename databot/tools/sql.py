"""SQL query tool for data platform databases."""

from __future__ import annotations

import asyncio
import re
from typing import Any

from loguru import logger

from databot.tools.base import BaseTool

# Forbidden SQL keywords for write operations
_FORBIDDEN_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "TRUNCATE", "GRANT", "REVOKE", "MERGE", "REPLACE",
    "RENAME", "CALL", "EXEC", "EXECUTE", "LOAD", "COPY",
}
# Pattern to detect multiple SQL statements (semicolons not inside quotes)
_MULTI_STATEMENT_RE = re.compile(
    r""";\ *(?=(?:[^'"]*['"][^'"]*['"])*[^'"]*$)""",
)


class SQLTool(BaseTool):
    """Execute SQL queries against configured databases.

    When a ``connector_registry`` is provided and contains SQL connectors
    whose names match the configured connection names, queries are delegated
    to the connector framework.  Otherwise the legacy direct-driver path is
    used (mysql-connector, clickzetta, SQLAlchemy).
    """

    def __init__(
        self,
        connections: dict[str, dict] | None = None,
        read_only: bool = True,
        max_rows: int = 1000,
        connector_registry: Any | None = None,
    ):
        self._connections = connections or {}
        self._read_only = read_only
        self._max_rows = max_rows
        self._connector_registry = connector_registry

    @property
    def name(self) -> str:
        return "sql"

    @property
    def description(self) -> str:
        conns = ", ".join(self._connections.keys()) if self._connections else "none configured"
        return (
            f"Execute a SQL query against a database. "
            f"Available connections: {conns}. Results limited to {self._max_rows} rows."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The SQL query to execute."},
                "connection": {
                    "type": "string",
                    "description": (
                        f"Database connection name. "
                        f"Available: {', '.join(self._connections.keys())}"
                    ),
                },
            },
            "required": ["query", "connection"],
        }

    async def execute(self, query: str, connection: str) -> str:
        if connection not in self._connections:
            available = ", ".join(self._connections.keys())
            return f"Error: Unknown connection '{connection}'. Available: {available}"

        # Read-only enforcement
        if self._read_only:
            violation = self._check_read_only(query)
            if violation:
                return violation

        # --- Try connector-backed execution first ---
        if self._connector_registry:
            connector = self._connector_registry.get(connection)
            if connector is not None:
                try:
                    from databot.connectors.base import ConnectorType

                    if connector.connector_type == ConnectorType.SQL:
                        result = await connector.execute(
                            "query", query=query, max_rows=self._max_rows
                        )
                        if result.success:
                            return result.to_markdown_table(max_rows=self._max_rows)
                        return f"Error: {result.error}"
                except Exception as e:
                    logger.warning(
                        f"Connector '{connection}' failed, falling back to legacy: {e}"
                    )

        # --- Legacy direct-driver path ---
        conn_config = self._connections[connection]
        driver = conn_config.get("driver", "mysql")

        try:
            if driver == "mysql":
                return await self._execute_mysql(query, conn_config)
            elif driver == "clickzetta":
                return await self._execute_clickzetta(query, conn_config)
            else:
                return await self._execute_sqlalchemy(query, conn_config)
        except ImportError as e:
            return (
                f"Error: Required database driver not installed: {e}. "
                f"Install with: pip install databot[sql]"
            )
        except Exception as e:
            logger.error(f"SQL error on {connection}: {e}")
            return f"Error executing query: {str(e)}"

    def _check_read_only(self, query: str) -> str | None:
        """Validate that a query is read-only. Returns error message or None."""
        normalized = query.strip()

        # Reject multiple statements (prevents "SELECT 1; DROP TABLE x")
        parts = [p.strip() for p in _MULTI_STATEMENT_RE.split(normalized) if p.strip()]
        if len(parts) > 1:
            return "Error: Multiple SQL statements are not allowed in read-only mode."

        # Check all keywords in the query, not just the first word
        upper = normalized.upper()
        # Strip comments before analysis
        upper = re.sub(r'--[^\n]*', '', upper)  # single-line comments
        upper = re.sub(r'/\*.*?\*/', '', upper, flags=re.DOTALL)  # block comments

        first_word = upper.split()[0] if upper.split() else ""
        if first_word in _FORBIDDEN_KEYWORDS:
            return (
                f"Error: Write operations are not allowed (read_only=true). "
                f"Blocked: {first_word}"
            )

        # Also scan for forbidden keywords after WITH (CTE wrapping writes)
        # e.g., WITH x AS (SELECT 1) INSERT INTO ...
        if first_word == "WITH":
            # Find content after the last closing paren of CTE
            # Look for forbidden words that appear as statements
            for keyword in _FORBIDDEN_KEYWORDS:
                pattern = rf'\)\s*{keyword}\b'
                if re.search(pattern, upper):
                    return (
                        f"Error: Write operations are not allowed (read_only=true). "
                        f"Blocked: CTE wrapping {keyword}"
                    )

        return None

    async def _execute_mysql(self, query: str, config: dict) -> str:
        """Execute query using mysql-connector-python."""
        import mysql.connector

        def _run() -> str:
            conn = mysql.connector.connect(
                host=config.get("host", "localhost"),
                port=config.get("port", 3306),
                database=config.get("database", ""),
                user=config.get("username", ""),
                password=config.get("password", ""),
            )
            try:
                cursor = conn.cursor()
                cursor.execute(query)

                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchmany(self._max_rows)
                    return self._format_table(columns, rows, cursor.rowcount)
                else:
                    return f"Query executed. Rows affected: {cursor.rowcount}"
            finally:
                conn.close()

        return await asyncio.to_thread(_run)

    async def _execute_clickzetta(self, query: str, config: dict) -> str:
        """Execute query using clickzetta connector."""
        from clickzetta import connector as cz_connector

        def _run() -> str:
            conn = cz_connector.connect(
                host=config.get("host", ""),
                schema=config.get("schema_name", ""),
                virtual_cluster=config.get("virtual_cluster", ""),
                username=config.get("username", ""),
                password=config.get("password", ""),
            )
            try:
                cursor = conn.cursor()
                cursor.execute(query)

                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchmany(self._max_rows)
                    return self._format_table(columns, rows, cursor.rowcount)
                else:
                    return f"Query executed. Rows affected: {cursor.rowcount}"
            finally:
                conn.close()

        return await asyncio.to_thread(_run)

    async def _execute_sqlalchemy(self, query: str, config: dict) -> str:
        """Execute query using SQLAlchemy (generic fallback)."""
        from sqlalchemy import create_engine, text

        def _run() -> str:
            driver = config.get("driver", "mysql")
            host = config.get("host", "localhost")
            port = config.get("port", 3306)
            database = config.get("database", "")
            username = config.get("username", "")
            password = config.get("password", "")

            url = f"{driver}://{username}:{password}@{host}:{port}/{database}"
            engine = create_engine(url)

            with engine.connect() as conn:
                result = conn.execute(text(query))
                if result.returns_rows:
                    columns = list(result.keys())
                    rows = result.fetchmany(self._max_rows)
                    return self._format_table(columns, rows, result.rowcount)
                else:
                    return f"Query executed. Rows affected: {result.rowcount}"

        return await asyncio.to_thread(_run)

    def _format_table(self, columns: list[str], rows: list, total_rows: int) -> str:
        """Format query results as a markdown table."""
        if not rows:
            return "Query returned 0 rows."

        # Header
        lines = ["| " + " | ".join(str(c) for c in columns) + " |"]
        lines.append("| " + " | ".join("---" for _ in columns) + " |")

        # Rows
        for row in rows:
            cells = []
            for val in row:
                s = str(val) if val is not None else "NULL"
                if len(s) > 50:
                    s = s[:47] + "..."
                cells.append(s)
            lines.append("| " + " | ".join(cells) + " |")

        result = "\n".join(lines)
        if total_rows > self._max_rows:
            result += f"\n\n*Showing {len(rows)} of {total_rows} total rows.*"
        else:
            result += f"\n\n*{len(rows)} row(s) returned.*"

        return result
