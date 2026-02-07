"""SQL connector — unified interface for relational databases.

Supports MySQL, PostgreSQL, Trino, Presto, StarRocks, ClickHouse, Hive,
and any database with a SQLAlchemy-compatible driver.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

from loguru import logger

from databot.connectors.base import (
    BaseConnector,
    ConnectorResult,
    ConnectorStatus,
    ConnectorType,
)

# Forbidden SQL keywords for write operations
_FORBIDDEN_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "TRUNCATE", "GRANT", "REVOKE", "MERGE", "REPLACE",
    "RENAME", "CALL", "EXEC", "EXECUTE", "LOAD", "COPY",
}
_MULTI_STATEMENT_RE = re.compile(
    r""";\ *(?=(?:[^'"]*['"][^'"]*['"])*[^'"]*$)""",
)

# Maps driver names to SQLAlchemy URL schemes (for the generic path)
_DRIVER_SCHEMES: dict[str, str] = {
    "postgresql": "postgresql",
    "postgres": "postgresql",
    "trino": "trino",
    "presto": "presto",
    "starrocks": "starrocks",
    "clickhouse": "clickhouse",
    "hive": "hive",
    "sqlite": "sqlite",
    "duckdb": "duckdb",
    "mssql": "mssql+pymssql",
}


class SQLConnector(BaseConnector):
    """Connector for relational / analytical SQL databases."""

    def __init__(self, name: str, config: dict[str, Any] | None = None):
        super().__init__(name, config)
        self._driver = self._config.get("driver", "mysql")
        self._read_only = self._config.get("read_only", True)
        self._max_rows = self._config.get("max_rows", 1000)

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.SQL

    def capabilities(self) -> list[str]:
        caps = ["query", "list_databases", "list_tables", "get_schema", "health_check"]
        if not self._read_only:
            caps.append("execute_write")
        return caps

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Validate that required config is present."""
        driver = self._driver
        if driver in ("mysql", "clickzetta"):
            if not self._config.get("host"):
                logger.warning(f"SQL connector '{self._name}': no host configured")
        self._connected = True

    async def health_check(self) -> ConnectorStatus:
        if not self._config.get("host") and self._driver not in ("sqlite", "duckdb"):
            return ConnectorStatus.NOT_CONFIGURED
        try:
            result = await self.execute("query", query="SELECT 1")
            return ConnectorStatus.HEALTHY if result.success else ConnectorStatus.DEGRADED
        except Exception:
            return ConnectorStatus.UNREACHABLE

    # ------------------------------------------------------------------
    # Operations (dispatched by BaseConnector.execute)
    # ------------------------------------------------------------------

    async def _op_query(self, query: str, **kwargs: Any) -> ConnectorResult:
        """Execute a read query."""
        if self._read_only:
            violation = self._check_read_only(query)
            if violation:
                return ConnectorResult(success=False, error=violation)

        max_rows = kwargs.get("max_rows", self._max_rows)
        return await self._run_query(query, max_rows)

    async def _op_execute_write(self, query: str, **kwargs: Any) -> ConnectorResult:
        """Execute a write query (disabled when read_only=True)."""
        if self._read_only:
            return ConnectorResult(
                success=False, error="Write operations disabled (read_only=True)."
            )
        return await self._run_query(query, 0)

    async def _op_list_databases(self, **kwargs: Any) -> ConnectorResult:
        """List databases / catalogs available on this connection."""
        driver = self._driver
        if driver == "mysql":
            return await self._run_query("SHOW DATABASES", self._max_rows)
        elif driver in ("trino", "presto"):
            return await self._run_query("SHOW CATALOGS", self._max_rows)
        elif driver in ("starrocks", "clickhouse"):
            return await self._run_query("SHOW DATABASES", self._max_rows)
        elif driver == "hive":
            return await self._run_query("SHOW DATABASES", self._max_rows)
        else:
            # Generic — try information_schema
            return await self._run_query(
                "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
                self._max_rows,
            )

    async def _op_list_tables(self, database: str = "", schema: str = "", **kwargs: Any) -> ConnectorResult:
        """List tables in a database/schema."""
        driver = self._driver
        if driver in ("mysql", "starrocks", "clickhouse"):
            db = database or self._config.get("database", "")
            if db:
                return await self._run_query(f"SHOW TABLES FROM `{db}`", self._max_rows)
            return await self._run_query("SHOW TABLES", self._max_rows)
        elif driver in ("trino", "presto"):
            catalog = database or self._config.get("catalog", "")
            sch = schema or self._config.get("schema_name", "")
            target = f'"{catalog}"."{sch}"' if catalog and sch else ""
            q = f"SHOW TABLES FROM {target}" if target else "SHOW TABLES"
            return await self._run_query(q, self._max_rows)
        else:
            db = database or self._config.get("database", "")
            sch = schema or self._config.get("schema_name", "public")
            return await self._run_query(
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{sch}' ORDER BY table_name",
                self._max_rows,
            )

    async def _op_get_schema(
        self, table: str, database: str = "", schema: str = "", **kwargs: Any
    ) -> ConnectorResult:
        """Get column definitions for a table."""
        driver = self._driver
        if driver in ("mysql", "starrocks", "clickhouse"):
            db = database or self._config.get("database", "")
            fqn = f"`{db}`.`{table}`" if db else f"`{table}`"
            return await self._run_query(f"DESCRIBE {fqn}", self._max_rows)
        elif driver in ("trino", "presto"):
            catalog = database or self._config.get("catalog", "")
            sch = schema or self._config.get("schema_name", "")
            fqn = f'"{catalog}"."{sch}"."{table}"' if catalog and sch else f'"{table}"'
            return await self._run_query(f"DESCRIBE {fqn}", self._max_rows)
        else:
            db = database or self._config.get("database", "")
            sch = schema or self._config.get("schema_name", "public")
            return await self._run_query(
                f"SELECT column_name, data_type, is_nullable, column_default "
                f"FROM information_schema.columns "
                f"WHERE table_schema = '{sch}' AND table_name = '{table}' "
                f"ORDER BY ordinal_position",
                self._max_rows,
            )

    # ------------------------------------------------------------------
    # Internal — query execution per driver
    # ------------------------------------------------------------------

    async def _run_query(self, query: str, max_rows: int) -> ConnectorResult:
        """Route query to the appropriate driver."""
        driver = self._driver
        try:
            if driver == "mysql":
                return await self._exec_mysql(query, max_rows)
            elif driver == "clickzetta":
                return await self._exec_clickzetta(query, max_rows)
            else:
                return await self._exec_sqlalchemy(query, max_rows)
        except ImportError as e:
            return ConnectorResult(
                success=False,
                error=f"Required driver not installed: {e}. Install the appropriate package.",
            )
        except Exception as e:
            logger.error(f"SQL error on connector '{self._name}': {e}")
            return ConnectorResult(success=False, error=f"SQL error: {str(e)}")

    async def _exec_mysql(self, query: str, max_rows: int) -> ConnectorResult:
        import mysql.connector

        def _run() -> ConnectorResult:
            conn = mysql.connector.connect(
                host=self._config.get("host", "localhost"),
                port=self._config.get("port", 3306),
                database=self._config.get("database", ""),
                user=self._config.get("username", ""),
                password=self._config.get("password", ""),
            )
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchmany(max_rows) if max_rows > 0 else cursor.fetchall()
                    return ConnectorResult(
                        columns=columns,
                        rows=[list(r) for r in rows],
                        row_count=cursor.rowcount,
                    )
                return ConnectorResult(data=f"Rows affected: {cursor.rowcount}")
            finally:
                conn.close()

        return await asyncio.to_thread(_run)

    async def _exec_clickzetta(self, query: str, max_rows: int) -> ConnectorResult:
        from clickzetta import connector as cz_connector

        def _run() -> ConnectorResult:
            conn = cz_connector.connect(
                host=self._config.get("host", ""),
                schema=self._config.get("schema_name", ""),
                virtual_cluster=self._config.get("virtual_cluster", ""),
                username=self._config.get("username", ""),
                password=self._config.get("password", ""),
            )
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchmany(max_rows) if max_rows > 0 else cursor.fetchall()
                    return ConnectorResult(
                        columns=columns,
                        rows=[list(r) for r in rows],
                        row_count=cursor.rowcount,
                    )
                return ConnectorResult(data=f"Rows affected: {cursor.rowcount}")
            finally:
                conn.close()

        return await asyncio.to_thread(_run)

    async def _exec_sqlalchemy(self, query: str, max_rows: int) -> ConnectorResult:
        from sqlalchemy import create_engine, text

        def _run() -> ConnectorResult:
            url = self._build_sqlalchemy_url()
            engine = create_engine(url)
            with engine.connect() as conn:
                result = conn.execute(text(query))
                if result.returns_rows:
                    columns = list(result.keys())
                    rows = (
                        [list(r) for r in result.fetchmany(max_rows)]
                        if max_rows > 0
                        else [list(r) for r in result.fetchall()]
                    )
                    return ConnectorResult(
                        columns=columns,
                        rows=rows,
                        row_count=result.rowcount if result.rowcount >= 0 else len(rows),
                    )
                return ConnectorResult(data=f"Rows affected: {result.rowcount}")

        return await asyncio.to_thread(_run)

    def _build_sqlalchemy_url(self) -> str:
        driver = self._driver
        scheme = _DRIVER_SCHEMES.get(driver, driver)
        user = self._config.get("username", "")
        password = self._config.get("password", "")
        host = self._config.get("host", "localhost")
        port = self._config.get("port", "")
        database = self._config.get("database", "")
        catalog = self._config.get("catalog", "")
        schema_name = self._config.get("schema_name", "")

        creds = f"{user}:{password}@" if user else ""
        port_part = f":{port}" if port else ""

        # Trino / Presto use catalog/schema in the path
        if driver in ("trino", "presto"):
            db_part = f"/{catalog}" if catalog else ""
            if schema_name:
                db_part += f"/{schema_name}"
        else:
            db_part = f"/{database}" if database else ""

        url = f"{scheme}://{creds}{host}{port_part}{db_part}"

        # Append extra params
        extra = self._config.get("extra", {})
        if extra:
            params = "&".join(f"{k}={v}" for k, v in extra.items())
            url += f"?{params}"

        return url

    # ------------------------------------------------------------------
    # Safety
    # ------------------------------------------------------------------

    @staticmethod
    def _check_read_only(query: str) -> str | None:
        """Check that a query is safe for read-only mode."""
        normalized = query.strip()

        # Reject multiple statements
        parts = [p.strip() for p in _MULTI_STATEMENT_RE.split(normalized) if p.strip()]
        if len(parts) > 1:
            return "Multiple SQL statements are not allowed in read-only mode."

        upper = normalized.upper()
        upper = re.sub(r"--[^\n]*", "", upper)
        upper = re.sub(r"/\*.*?\*/", "", upper, flags=re.DOTALL)

        first_word = upper.split()[0] if upper.split() else ""
        if first_word in _FORBIDDEN_KEYWORDS:
            return f"Write operations are blocked (read_only=true). Blocked: {first_word}"

        if first_word == "WITH":
            for kw in _FORBIDDEN_KEYWORDS:
                if re.search(rf"\)\s*{kw}\b", upper):
                    return f"Write operations blocked: CTE wrapping {kw}"

        return None
