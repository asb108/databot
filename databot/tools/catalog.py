"""Data Catalog tool — browse catalogs, schemas, tables across Iceberg/Glue/Unity."""

from __future__ import annotations

from typing import Any

from databot.connectors.catalog_connector import CatalogConnector
from databot.connectors.registry import ConnectorRegistry
from databot.tools.base import BaseTool


class CatalogTool(BaseTool):
    """Browse and search data catalogs (Iceberg REST, AWS Glue, Unity Catalog).

    Delegates to :class:`CatalogConnector` instances registered in the
    :class:`ConnectorRegistry`.
    """

    def __init__(self, registry: ConnectorRegistry):
        self._registry = registry

    @property
    def name(self) -> str:
        return "catalog"

    @property
    def description(self) -> str:
        cat_connectors = [
            c.name for c in self._registry.list_all()
            if isinstance(c, CatalogConnector)
        ]
        names = ", ".join(cat_connectors) if cat_connectors else "none configured"
        return (
            f"Browse data catalogs: list databases/namespaces, list tables, get table schemas, "
            f"search for tables. Supports Iceberg REST, AWS Glue, Databricks Unity Catalog. "
            f"Available catalogs: {names}."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [
                        "list_databases", "list_tables", "get_schema",
                        "search", "load_table",
                    ],
                    "description": "Catalog action to perform.",
                },
                "connector": {
                    "type": "string",
                    "description": "Name of the catalog connector to use.",
                },
                "database": {
                    "type": "string",
                    "description": "Database or namespace name.",
                },
                "namespace": {
                    "type": "string",
                    "description": "Namespace (alias for database).",
                },
                "table": {
                    "type": "string",
                    "description": "Table name.",
                },
                "query": {
                    "type": "string",
                    "description": "Search query for finding tables.",
                },
                "catalog_name": {
                    "type": "string",
                    "description": "Unity Catalog catalog name.",
                },
            },
            "required": ["action"],
        }

    async def execute(self, action: str, connector: str = "", **kwargs: Any) -> str:
        cat = self._resolve_connector(connector)
        if isinstance(cat, str):
            return cat  # Error message

        result = await cat.execute(action, **kwargs)
        if not result.success:
            return f"Error: {result.error}"
        if result.columns and result.rows:
            return result.to_markdown_table()
        if result.data is not None:
            if isinstance(result.data, str):
                return result.data
            import json
            return json.dumps(result.data, indent=2, default=str)
        return "OK"

    def _resolve_connector(self, name: str) -> CatalogConnector | str:
        """Resolve connector by name, or pick the only one if unambiguous."""
        if name:
            conn = self._registry.get(name)
            if conn is None:
                return f"Error: Catalog connector '{name}' not found."
            if not isinstance(conn, CatalogConnector):
                return f"Error: Connector '{name}' is not a Catalog connector."
            return conn

        cat_connectors = [
            c for c in self._registry.list_all()
            if isinstance(c, CatalogConnector)
        ]
        if len(cat_connectors) == 0:
            return "Error: No catalog connectors configured."
        if len(cat_connectors) == 1:
            return cat_connectors[0]
        names = ", ".join(c.name for c in cat_connectors)
        return f"Error: Multiple catalog connectors configured ({names}). Specify 'connector' parameter."
