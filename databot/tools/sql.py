"""SQL query tool for data platform databases."""
from __future__ import annotations

from typing import Any

from loguru import logger

from databot.tools.base import BaseTool


class SQLTool(BaseTool):
    """Execute SQL queries against configured databases."""

    def __init__(
        self,
        connections: dict[str, dict] | None = None,
        read_only: bool = True,
        max_rows: int = 1000,
    ):
        self._connections = connections or {}
        self._read_only = read_only
        self._max_rows = max_rows

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
            normalized = query.strip().upper()
            forbidden = [
                "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
                "CREATE", "TRUNCATE", "GRANT", "REVOKE",
            ]
            first_word = normalized.split()[0] if normalized else ""
            if first_word in forbidden:
                return (
                    f"Error: Write operations are not allowed (read_only=true). "
                    f"Blocked: {first_word}"
                )

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

    async def _execute_mysql(self, query: str, config: dict) -> str:
        """Execute query using mysql-connector-python."""
        import mysql.connector

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

    async def _execute_clickzetta(self, query: str, config: dict) -> str:
        """Execute query using clickzetta connector."""
        from clickzetta import connector as cz_connector

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

    async def _execute_sqlalchemy(self, query: str, config: dict) -> str:
        """Execute query using SQLAlchemy (generic fallback)."""
        from sqlalchemy import create_engine, text

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
