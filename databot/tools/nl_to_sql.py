"""Natural-language-to-SQL tool with schema grounding.

Generates a read-only SQL query from a natural-language question, grounded in
the connection's real schema (tables + columns fetched via the connector's
hardened introspection ops). The generated SQL is validated to be read-only
before it is ever returned or executed, and execution (optional) goes through
the SQL tool, which enforces the read-only policy and row limits.
"""

from __future__ import annotations

import re
from typing import Any

from loguru import logger

from databot.tools.base import BaseTool

# Write keywords that must never appear as the leading statement.
_FORBIDDEN_LEADING = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
    "MERGE",
    "REPLACE",
    "RENAME",
    "CALL",
    "EXEC",
    "EXECUTE",
    "LOAD",
    "COPY",
}

_SYSTEM_PROMPT = (
    "You are an expert SQL generator. Given a database schema and a question, "
    "write a single SQL query that answers it. Rules: (1) read-only SELECT only "
    "— never modify data; (2) use only tables and columns present in the schema; "
    "(3) prefer explicit column lists over SELECT *; (4) add a sensible LIMIT for "
    "exploratory queries. Respond with ONLY the SQL query, no prose, no markdown "
    "fences."
)


def _strip_sql(text: str) -> str:
    """Extract a bare SQL statement from an LLM response."""
    s = (text or "").strip()
    # Strip ```sql ... ``` fences if present.
    fence = re.search(r"```(?:sql)?\s*(.*?)```", s, re.DOTALL | re.IGNORECASE)
    if fence:
        s = fence.group(1).strip()
    # Drop a trailing semicolon for uniformity.
    return s.rstrip(";").strip()


def _is_read_only(sql: str) -> bool:
    s = re.sub(r"--[^\n]*", "", sql)
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL).strip().upper()
    if not s:
        return False
    # Reject multiple statements (a trailing ';' is stripped by _strip_sql, so
    # any remaining ';' indicates a second statement like "SELECT 1; DROP ...").
    if ";" in s.rstrip(";"):
        return False
    first = s.split()[0]
    if first in _FORBIDDEN_LEADING:
        return False
    # A CTE may wrap a write; reject if any forbidden keyword follows a ")".
    if first == "WITH":
        for kw in _FORBIDDEN_LEADING:
            if re.search(rf"\)\s*{kw}\b", s):
                return False
    return True


class NLToSQLTool(BaseTool):
    """Generate (and optionally run) read-only SQL from natural language."""

    timeout = 120

    def __init__(
        self,
        provider: Any,
        sql_tool: Any,
        connector_registry: Any | None = None,
        model: str | None = None,
        max_tables: int = 25,
        usage_tracker: Any | None = None,
    ):
        self._provider = provider
        self._sql = sql_tool
        self._registry = connector_registry
        self._model = model
        self._max_tables = max_tables
        self._usage = usage_tracker

    @property
    def name(self) -> str:
        return "text_to_sql"

    @property
    def description(self) -> str:
        return (
            "Generate a read-only SQL query from a natural-language question, "
            "grounded in the connection's real schema. Optionally execute it. Use "
            "when the user asks a data question in plain English."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "connection": {"type": "string", "description": "Database connection name."},
                "question": {"type": "string", "description": "The question in plain English."},
                "tables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional table names to scope the schema context.",
                },
                "execute": {
                    "type": "boolean",
                    "description": "If true, run the generated query and include results.",
                },
            },
            "required": ["connection", "question"],
        }

    async def execute(
        self,
        connection: str,
        question: str,
        tables: list[str] | None = None,
        execute: bool = False,
        **kwargs: Any,
    ) -> str:
        schema_text = await self._gather_schema(connection, tables)
        if not schema_text:
            return (
                f"Could not read the schema for connection '{connection}'. Make sure "
                "it is configured and reachable, or pass `tables` explicitly."
            )

        try:
            sql = await self._generate(question, schema_text)
        except Exception as e:
            logger.warning(f"NL-to-SQL generation failed: {e}")
            return f"Error generating SQL: {e}"

        if not sql:
            return "The model did not produce a query for that question."
        if not _is_read_only(sql):
            return (
                "Refusing to run the generated query because it is not read-only:\n\n"
                f"```sql\n{sql}\n```"
            )

        out = f"**Generated SQL:**\n```sql\n{sql}\n```"
        if execute:
            result = await self._sql.execute(query=sql, connection=connection)
            out += f"\n\n**Result:**\n{result}"
        return out

    async def _generate(self, question: str, schema_text: str) -> str:
        messages = [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {
                "role": "user",
                "content": f"Schema:\n{schema_text}\n\nQuestion: {question}\n\nSQL:",
            },
        ]
        model = self._model or self._provider.get_default_model()
        response = await self._provider.chat(messages=messages, tools=None, model=model)
        # Record the nested LLM call so cost accounting is never bypassed.
        from databot.core.cost import record_llm_usage

        record_llm_usage(self._usage, model, response)
        return _strip_sql(response.content or "")

    async def _gather_schema(self, connection: str, tables: list[str] | None) -> str:
        """Build a compact schema description: ``table(col type, ...)`` lines."""
        connector = self._sql_connector(connection)
        if connector is None:
            return ""

        table_names = tables or await self._list_tables(connector)
        if not table_names:
            return ""

        lines: list[str] = []
        for tbl in table_names[: self._max_tables]:
            cols = await self._table_columns(connector, tbl)
            if cols:
                lines.append(f"{tbl}({', '.join(cols)})")
        return "\n".join(lines)

    def _sql_connector(self, connection: str) -> Any | None:
        if not self._registry:
            return None
        connector = self._registry.get(connection)
        if connector is None:
            return None
        try:
            from databot.connectors.base import ConnectorType

            if connector.connector_type == ConnectorType.SQL:
                return connector
        except Exception:
            return None
        return None

    async def _list_tables(self, connector: Any) -> list[str]:
        try:
            result = await connector.execute("list_tables")
        except Exception:
            return []
        if not result.success or not result.rows:
            return []
        # The table name is the first column of each row.
        return [str(row[0]) for row in result.rows if row]

    async def _table_columns(self, connector: Any, table: str) -> list[str]:
        try:
            result = await connector.execute("get_schema", table=table)
        except Exception:
            return []
        if not result.success or not result.rows:
            return []
        cols: list[str] = []
        for row in result.rows:
            if not row:
                continue
            name = str(row[0])
            dtype = str(row[1]) if len(row) > 1 else ""
            cols.append(f"{name} {dtype}".strip())
        return cols
