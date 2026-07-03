"""Export tool — write query results to a CSV or JSON file in the workspace."""

from __future__ import annotations

import csv
import io
import json
import re
from pathlib import Path
from typing import Any

from databot.tools.base import BaseTool

_SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


class ExportTool(BaseTool):
    """Run a read-only query and write its rows to a CSV/JSON file."""

    timeout = 180

    def __init__(
        self,
        sql_tool: Any | None = None,
        connector_registry: Any | None = None,
        workspace: Path | None = None,
        max_rows: int = 10000,
    ):
        self._sql = sql_tool
        self._registry = connector_registry
        self._workspace = Path(workspace) if workspace else Path.cwd()
        self._max_rows = max_rows

    @property
    def name(self) -> str:
        return "export_query"

    @property
    def description(self) -> str:
        return (
            "Run a read-only SQL query and export the results to a CSV or JSON file "
            "in the workspace. Returns the file path and row count."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "connection": {"type": "string", "description": "Database connection name."},
                "query": {"type": "string", "description": "Read-only SQL query."},
                "filename": {
                    "type": "string",
                    "description": "Output filename (workspace-relative, e.g. orders.csv).",
                },
                "format": {
                    "type": "string",
                    "enum": ["csv", "json"],
                    "description": "Default csv.",
                },
            },
            "required": ["connection", "query", "filename"],
        }

    def _resolve_path(self, filename: str) -> Path | None:
        # Single path component, safe characters only, kept inside the workspace.
        name = Path(filename).name
        if name != filename or not _SAFE_NAME.match(name):
            return None
        target = (self._workspace / name).resolve()
        try:
            target.relative_to(self._workspace.resolve())
        except ValueError:
            return None
        return target

    async def _fetch_rows(self, connection: str, query: str):
        """Return (columns, rows) using the connector for structured data."""
        if self._registry is not None:
            connector = self._registry.get(connection)
            if connector is not None:
                from databot.connectors.base import ConnectorType

                if connector.connector_type == ConnectorType.SQL:
                    result = await connector.execute("query", query=query, max_rows=self._max_rows)
                    if not result.success:
                        return None, result.error
                    return (result.columns or [], result.rows or []), None
        return None, "Export requires a connector-backed SQL connection."

    async def execute(
        self,
        connection: str,
        query: str,
        filename: str,
        format: str = "csv",
        **kwargs: Any,
    ) -> str:
        target = self._resolve_path(filename)
        if target is None:
            return (
                f"Error: invalid filename '{filename}'. Use a simple name like "
                "report.csv (no paths or special characters)."
            )

        data, error = await self._fetch_rows(connection, query)
        if data is None:
            return f"Error: {error}"
        columns, rows = data

        try:
            if format == "json":
                payload = [dict(zip(columns, row)) for row in rows]
                target.write_text(json.dumps(payload, indent=2, default=str))
            else:
                buf = io.StringIO()
                writer = csv.writer(buf)
                if columns:
                    writer.writerow(columns)
                writer.writerows(rows)
                target.write_text(buf.getvalue())
        except OSError as e:
            return f"Error writing file: {e}"

        note = ""
        if len(rows) >= self._max_rows:
            note = f" (capped at {self._max_rows} rows)"
        return f"Exported {len(rows)} row(s) to `{target.name}` as {format}{note}."
