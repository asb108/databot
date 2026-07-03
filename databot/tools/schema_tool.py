"""Schema introspection tool.

Exposes the SQL connector's (hardened) introspection operations — list
databases, list tables, describe a table's columns — directly to the agent so
it can ground its reasoning in the real schema instead of guessing table and
column names. Falls back to ``information_schema`` queries via the SQL tool for
connections that aren't connector-backed.
"""

from __future__ import annotations

from typing import Any

from databot.connectors.identifiers import IdentifierError, validate_identifier
from databot.tools.base import BaseTool


class SchemaTool(BaseTool):
    """Introspect database schemas: databases, tables, and columns."""

    def __init__(self, connector_registry: Any | None = None, sql_tool: Any | None = None):
        self._registry = connector_registry
        self._sql = sql_tool

    @property
    def name(self) -> str:
        return "describe_schema"

    @property
    def description(self) -> str:
        return (
            "Inspect a database's schema: list databases/catalogs, list tables in "
            "a schema, or describe a table's columns and types. Use this to ground "
            "queries in the real schema before writing SQL."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list_databases", "list_tables", "describe_table"],
                    "description": "Which introspection to run.",
                },
                "connection": {"type": "string", "description": "Connection name."},
                "table": {
                    "type": "string",
                    "description": "Table name (required for describe_table).",
                },
                "database": {"type": "string", "description": "Database/catalog (optional)."},
                "schema": {"type": "string", "description": "Schema name (optional)."},
            },
            "required": ["action", "connection"],
        }

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

    async def execute(
        self,
        action: str,
        connection: str,
        table: str = "",
        database: str = "",
        schema: str = "",
        **kwargs: Any,
    ) -> str:
        if action == "describe_table" and not table:
            return "Error: 'table' is required for describe_table."

        connector = self._sql_connector(connection)
        if connector is not None:
            return await self._via_connector(connector, action, table, database, schema)
        return await self._via_sql_fallback(connection, action, table, schema)

    async def _via_connector(
        self, connector: Any, action: str, table: str, database: str, schema: str
    ) -> str:
        op_map = {
            "list_databases": ("list_databases", {}),
            "list_tables": ("list_tables", {"database": database, "schema": schema}),
            "describe_table": (
                "get_schema",
                {"table": table, "database": database, "schema": schema},
            ),
        }
        op, params = op_map[action]
        result = await connector.execute(op, **{k: v for k, v in params.items() if v})
        if result.success:
            return result.to_markdown_table()
        return f"Error: {result.error}"

    async def _via_sql_fallback(self, connection: str, action: str, table: str, schema: str) -> str:
        if not self._sql:
            return (
                f"Error: connection '{connection}' is not connector-backed and no SQL "
                "tool is available for schema introspection."
            )
        try:
            if action == "list_databases":
                query = "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
            elif action == "list_tables":
                if schema:
                    sch = validate_identifier(schema, "schema", allow_qualified=False)
                    query = (
                        "SELECT table_name FROM information_schema.tables "
                        f"WHERE table_schema = '{sch}' ORDER BY table_name"
                    )
                else:
                    query = (
                        "SELECT table_schema, table_name FROM information_schema.tables "
                        "ORDER BY table_schema, table_name"
                    )
            else:  # describe_table
                tbl = validate_identifier(table, "table", allow_qualified=False)
                query = (
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    f"WHERE table_name = '{tbl}' ORDER BY ordinal_position"
                )
        except IdentifierError as e:
            return f"Error: {e}"
        return await self._sql.execute(query=query, connection=connection)
