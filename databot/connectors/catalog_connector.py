"""Catalog connector — interface for data catalog / metastore systems.

Provides uniform access to table metadata, schemas, namespaces, and search
across Iceberg REST Catalog, AWS Glue, Unity Catalog, Hive Metastore, Polaris,
and Nessie.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from databot.connectors.base import ConnectorResult, ConnectorStatus, ConnectorType
from databot.connectors.rest_connector import RESTConnector


class CatalogConnector(RESTConnector):
    """Base catalog connector built on top of RESTConnector.

    Subclasses or driver-specific modes implement the catalog protocol
    (Iceberg REST, Glue, Unity, etc.).  The ``driver`` config key selects
    which protocol is used.
    """

    def __init__(self, name: str, config: dict[str, Any] | None = None):
        super().__init__(name, config)
        self._catalog_driver = self._config.get("driver", "iceberg_rest")

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.CATALOG

    def capabilities(self) -> list[str]:
        return [
            "list_namespaces",
            "list_tables",
            "get_table_schema",
            "get_table_metadata",
            "search",
            "health_check",
        ]

    # ------------------------------------------------------------------
    # Operations — dispatched by BaseConnector.execute
    # ------------------------------------------------------------------

    async def _op_list_namespaces(self, parent: str = "", **kwargs: Any) -> ConnectorResult:
        """List namespaces / databases / schemas."""
        driver = self._catalog_driver
        if driver == "iceberg_rest":
            return await self._iceberg_list_namespaces(parent)
        elif driver == "glue":
            return await self._glue_list_databases()
        elif driver == "unity":
            return await self._unity_list_schemas()
        return ConnectorResult(success=False, error=f"Unsupported catalog driver: {driver}")

    async def _op_list_tables(self, namespace: str = "", **kwargs: Any) -> ConnectorResult:
        """List tables in a namespace."""
        driver = self._catalog_driver
        if driver == "iceberg_rest":
            return await self._iceberg_list_tables(namespace)
        elif driver == "glue":
            return await self._glue_list_tables(namespace)
        elif driver == "unity":
            return await self._unity_list_tables(namespace)
        return ConnectorResult(success=False, error=f"Unsupported catalog driver: {driver}")

    async def _op_get_table_schema(self, namespace: str, table: str, **kwargs: Any) -> ConnectorResult:
        """Get column definitions for a table."""
        driver = self._catalog_driver
        if driver == "iceberg_rest":
            return await self._iceberg_table_schema(namespace, table)
        elif driver == "glue":
            return await self._glue_table_schema(namespace, table)
        elif driver == "unity":
            return await self._unity_table_schema(namespace, table)
        return ConnectorResult(success=False, error=f"Unsupported catalog driver: {driver}")

    async def _op_get_table_metadata(self, namespace: str, table: str, **kwargs: Any) -> ConnectorResult:
        """Get full metadata for a table (format, location, properties, etc.)."""
        driver = self._catalog_driver
        if driver == "iceberg_rest":
            return await self._iceberg_table_metadata(namespace, table)
        elif driver == "glue":
            return await self._glue_table_metadata(namespace, table)
        elif driver == "unity":
            return await self._unity_table_metadata(namespace, table)
        return ConnectorResult(success=False, error=f"Unsupported catalog driver: {driver}")

    async def _op_search(self, query: str, **kwargs: Any) -> ConnectorResult:
        """Search tables by name or description."""
        # Not all catalogs support native search; fall back to list + filter
        driver = self._catalog_driver
        if driver == "glue":
            return await self._glue_search(query)
        elif driver == "unity":
            return await self._unity_search(query)
        # Iceberg REST Catalog doesn't have native search — list and filter
        return await self._catalog_search_fallback(query)

    # ------------------------------------------------------------------
    # Iceberg REST Catalog protocol
    # ------------------------------------------------------------------

    async def _iceberg_list_namespaces(self, parent: str = "") -> ConnectorResult:
        prefix = self._config.get("prefix", "")
        path = f"/v1/{prefix}/namespaces" if prefix else "/v1/namespaces"
        params: dict[str, str] = {}
        if parent:
            params["parent"] = parent
        result = await self.request("GET", path, params=params or None)
        if not result.success:
            return result
        namespaces = result.data.get("namespaces", []) if isinstance(result.data, dict) else []
        # Flatten namespace arrays to dotted names
        ns_names = [".".join(ns) if isinstance(ns, list) else str(ns) for ns in namespaces]
        return ConnectorResult(
            data=ns_names,
            columns=["namespace"],
            rows=[[n] for n in ns_names],
            row_count=len(ns_names),
        )

    async def _iceberg_list_tables(self, namespace: str) -> ConnectorResult:
        prefix = self._config.get("prefix", "")
        ns_path = namespace.replace(".", "\x1f")  # Iceberg uses \x1f separator
        path = f"/v1/{prefix}/namespaces/{ns_path}/tables" if prefix else f"/v1/namespaces/{ns_path}/tables"
        result = await self.request("GET", path)
        if not result.success:
            return result
        identifiers = result.data.get("identifiers", []) if isinstance(result.data, dict) else []
        table_names = [t.get("name", "") for t in identifiers]
        return ConnectorResult(
            data=table_names,
            columns=["table_name"],
            rows=[[t] for t in table_names],
            row_count=len(table_names),
        )

    async def _iceberg_table_schema(self, namespace: str, table: str) -> ConnectorResult:
        metadata = await self._iceberg_load_table(namespace, table)
        if not metadata.success:
            return metadata
        tbl = metadata.data
        schema = tbl.get("metadata", {}).get("current-schema", tbl.get("metadata", {}).get("schema", {}))
        fields = schema.get("fields", [])
        columns = ["name", "type", "required", "doc"]
        rows = [
            [f.get("name"), f.get("type"), f.get("required", False), f.get("doc", "")]
            for f in fields
        ]
        return ConnectorResult(columns=columns, rows=rows, row_count=len(rows))

    async def _iceberg_table_metadata(self, namespace: str, table: str) -> ConnectorResult:
        return await self._iceberg_load_table(namespace, table)

    async def _iceberg_load_table(self, namespace: str, table: str) -> ConnectorResult:
        prefix = self._config.get("prefix", "")
        ns_path = namespace.replace(".", "\x1f")
        base = f"/v1/{prefix}" if prefix else "/v1"
        path = f"{base}/namespaces/{ns_path}/tables/{table}"
        return await self.request("GET", path)

    # ------------------------------------------------------------------
    # AWS Glue protocol (via REST — requires boto3)
    # ------------------------------------------------------------------

    async def _glue_list_databases(self) -> ConnectorResult:
        try:
            import boto3
        except ImportError:
            return ConnectorResult(success=False, error="boto3 not installed for Glue catalog")

        import asyncio

        def _run() -> ConnectorResult:
            client = boto3.client("glue", region_name=self._config.get("region", "us-east-1"))
            paginator = client.get_paginator("get_databases")
            databases = []
            for page in paginator.paginate():
                for db in page.get("DatabaseList", []):
                    databases.append(db["Name"])
            return ConnectorResult(
                data=databases,
                columns=["database"],
                rows=[[d] for d in databases],
                row_count=len(databases),
            )

        return await asyncio.to_thread(_run)

    async def _glue_list_tables(self, database: str) -> ConnectorResult:
        try:
            import boto3
        except ImportError:
            return ConnectorResult(success=False, error="boto3 not installed for Glue catalog")

        import asyncio

        def _run() -> ConnectorResult:
            client = boto3.client("glue", region_name=self._config.get("region", "us-east-1"))
            paginator = client.get_paginator("get_tables")
            tables = []
            for page in paginator.paginate(DatabaseName=database):
                for t in page.get("TableList", []):
                    tables.append(t["Name"])
            return ConnectorResult(
                data=tables,
                columns=["table_name"],
                rows=[[t] for t in tables],
                row_count=len(tables),
            )

        return await asyncio.to_thread(_run)

    async def _glue_table_schema(self, database: str, table: str) -> ConnectorResult:
        meta = await self._glue_table_metadata(database, table)
        if not meta.success:
            return meta
        cols = meta.data.get("Table", {}).get("StorageDescriptor", {}).get("Columns", [])
        partition_keys = meta.data.get("Table", {}).get("PartitionKeys", [])
        all_cols = cols + partition_keys
        columns = ["name", "type", "comment", "is_partition"]
        rows = []
        for c in cols:
            rows.append([c.get("Name"), c.get("Type"), c.get("Comment", ""), False])
        for c in partition_keys:
            rows.append([c.get("Name"), c.get("Type"), c.get("Comment", ""), True])
        return ConnectorResult(columns=columns, rows=rows, row_count=len(rows))

    async def _glue_table_metadata(self, database: str, table: str) -> ConnectorResult:
        try:
            import boto3
        except ImportError:
            return ConnectorResult(success=False, error="boto3 not installed for Glue catalog")

        import asyncio

        def _run() -> ConnectorResult:
            client = boto3.client("glue", region_name=self._config.get("region", "us-east-1"))
            resp = client.get_table(DatabaseName=database, Name=table)
            return ConnectorResult(data=resp)

        return await asyncio.to_thread(_run)

    async def _glue_search(self, query: str) -> ConnectorResult:
        try:
            import boto3
        except ImportError:
            return ConnectorResult(success=False, error="boto3 not installed")

        import asyncio

        def _run() -> ConnectorResult:
            client = boto3.client("glue", region_name=self._config.get("region", "us-east-1"))
            resp = client.search_tables(SearchText=query, MaxResults=25)
            tables = resp.get("TableList", [])
            columns_out = ["database", "table_name", "description"]
            rows_out = [
                [t.get("DatabaseName"), t.get("Name"), t.get("Description", "")]
                for t in tables
            ]
            return ConnectorResult(columns=columns_out, rows=rows_out, row_count=len(rows_out))

        return await asyncio.to_thread(_run)

    # ------------------------------------------------------------------
    # Unity Catalog protocol (Databricks REST API)
    # ------------------------------------------------------------------

    async def _unity_list_schemas(self) -> ConnectorResult:
        catalog_name = self._config.get("catalog", "")
        result = await self.request("GET", f"/api/2.1/unity-catalog/schemas", params={"catalog_name": catalog_name})
        if not result.success:
            return result
        schemas = result.data.get("schemas", []) if isinstance(result.data, dict) else []
        names = [s.get("name", "") for s in schemas]
        return ConnectorResult(
            data=names,
            columns=["schema"],
            rows=[[n] for n in names],
            row_count=len(names),
        )

    async def _unity_list_tables(self, namespace: str) -> ConnectorResult:
        catalog = self._config.get("catalog", "")
        result = await self.request(
            "GET",
            "/api/2.1/unity-catalog/tables",
            params={"catalog_name": catalog, "schema_name": namespace},
        )
        if not result.success:
            return result
        tables = result.data.get("tables", []) if isinstance(result.data, dict) else []
        names = [t.get("name", "") for t in tables]
        return ConnectorResult(
            data=names,
            columns=["table_name"],
            rows=[[n] for n in names],
            row_count=len(names),
        )

    async def _unity_table_schema(self, namespace: str, table: str) -> ConnectorResult:
        meta = await self._unity_table_metadata(namespace, table)
        if not meta.success:
            return meta
        cols = meta.data.get("columns", []) if isinstance(meta.data, dict) else []
        columns = ["name", "type", "nullable", "comment"]
        rows = [
            [c.get("name"), c.get("type_text"), c.get("nullable", True), c.get("comment", "")]
            for c in cols
        ]
        return ConnectorResult(columns=columns, rows=rows, row_count=len(rows))

    async def _unity_table_metadata(self, namespace: str, table: str) -> ConnectorResult:
        catalog = self._config.get("catalog", "")
        full_name = f"{catalog}.{namespace}.{table}"
        return await self.request("GET", f"/api/2.1/unity-catalog/tables/{full_name}")

    async def _unity_search(self, query: str) -> ConnectorResult:
        # Unity Catalog doesn't have a native search endpoint — list and filter
        return await self._catalog_search_fallback(query)

    # ------------------------------------------------------------------
    # Fallback: list-and-filter search
    # ------------------------------------------------------------------

    async def _catalog_search_fallback(self, query: str) -> ConnectorResult:
        """Search by listing namespaces → tables and filtering by name."""
        ns_result = await self.execute("list_namespaces")
        if not ns_result.success:
            return ns_result

        matching_tables: list[list[str]] = []
        namespaces = ns_result.data if isinstance(ns_result.data, list) else []
        query_lower = query.lower()

        for ns in namespaces[:20]:  # Cap to avoid excessive API calls
            tables_result = await self.execute("list_tables", namespace=str(ns))
            if tables_result.success and isinstance(tables_result.data, list):
                for t in tables_result.data:
                    if query_lower in str(t).lower():
                        matching_tables.append([str(ns), str(t)])

        return ConnectorResult(
            data=matching_tables,
            columns=["namespace", "table_name"],
            rows=matching_tables,
            row_count=len(matching_tables),
        )
