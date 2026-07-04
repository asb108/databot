"""Data quality check tool with SQL injection protection."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from databot.connectors.identifiers import validate_identifier as _validate_identifier
from databot.tools.base import BaseTool
from databot.tools.errors import ErrorCode, ToolError

if TYPE_CHECKING:
    from databot.tools.sql import SQLTool


class DataQualityTool(BaseTool):
    """Run data quality checks against databases."""

    def __init__(self, sql_tool: SQLTool | None = None):
        self._sql = sql_tool

    @property
    def name(self) -> str:
        return "data_quality"

    @property
    def description(self) -> str:
        return (
            "Run data quality checks: row counts, null rates, freshness, "
            "and source-target comparisons."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "check_type": {
                    "type": "string",
                    "enum": ["row_count", "null_check", "freshness", "compare"],
                    "description": "Type of DQ check.",
                },
                "connection": {
                    "type": "string",
                    "description": "Database connection name.",
                },
                "table": {
                    "type": "string",
                    "description": "Table name (schema.table format).",
                },
                "column": {
                    "type": "string",
                    "description": "Column name (for null_check).",
                },
                "timestamp_column": {
                    "type": "string",
                    "description": "Timestamp column (for freshness check).",
                },
                "source_connection": {
                    "type": "string",
                    "description": "Source connection (for compare).",
                },
                "source_table": {
                    "type": "string",
                    "description": "Source table (for compare).",
                },
                "threshold_hours": {
                    "type": "number",
                    "description": "Freshness threshold in hours. Default 24.",
                },
            },
            "required": ["check_type", "connection", "table"],
        }

    async def execute(self, check_type: str, connection: str, table: str, **kwargs: Any) -> str:
        if not self._sql:
            raise ToolError(
                ErrorCode.NOT_CONFIGURED, "SQL tool not configured. Cannot run DQ checks."
            )

        # Validate table name to prevent SQL injection
        try:
            _validate_identifier(table, "table")
        except ValueError as e:
            raise ToolError(ErrorCode.INVALID_ARGUMENT, str(e))

        try:
            if check_type == "row_count":
                return await self._row_count(connection, table)
            elif check_type == "null_check":
                column = kwargs.get("column", "")
                if not column:
                    raise ToolError(
                        ErrorCode.INVALID_ARGUMENT, "column is required for null_check."
                    )
                try:
                    _validate_identifier(column, "column")
                except ValueError as e:
                    raise ToolError(ErrorCode.INVALID_ARGUMENT, str(e))
                return await self._null_check(connection, table, column)
            elif check_type == "freshness":
                ts_col = kwargs.get("timestamp_column", "")
                if not ts_col:
                    raise ToolError(
                        ErrorCode.INVALID_ARGUMENT,
                        "timestamp_column is required for freshness check.",
                    )
                try:
                    _validate_identifier(ts_col, "timestamp_column")
                except ValueError as e:
                    raise ToolError(ErrorCode.INVALID_ARGUMENT, str(e))
                threshold = kwargs.get("threshold_hours", 24)
                return await self._freshness(connection, table, ts_col, threshold)
            elif check_type == "compare":
                src_conn = kwargs.get("source_connection", "")
                src_table = kwargs.get("source_table", "")
                if not src_conn or not src_table:
                    raise ToolError(
                        ErrorCode.INVALID_ARGUMENT,
                        "source_connection and source_table required for compare.",
                    )
                try:
                    _validate_identifier(src_table, "source_table")
                except ValueError as e:
                    raise ToolError(ErrorCode.INVALID_ARGUMENT, str(e))
                return await self._compare(connection, table, src_conn, src_table)
            else:
                raise ToolError(ErrorCode.INVALID_ARGUMENT, f"Unknown check type: {check_type}")
        except ToolError:
            raise
        except Exception as e:
            raise ToolError(ErrorCode.INTERNAL_ERROR, f"DQ check error: {e}")

    async def _row_count(self, connection: str, table: str) -> str:
        query = f"SELECT COUNT(*) as cnt FROM {table}"
        try:
            result = await self._sql.execute(query=query, connection=connection)
        except ToolError as e:
            result = e.to_text()
        return f"**Row count for `{table}`:**\n{result}"

    async def _null_check(self, connection: str, table: str, column: str) -> str:
        query = f"""
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as null_count,
                ROUND(
                    SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
                ) as null_pct
            FROM {table}
        """
        try:
            result = await self._sql.execute(query=query, connection=connection)
        except ToolError as e:
            result = e.to_text()
        return f"**Null check for `{table}.{column}`:**\n{result}"

    async def _freshness(
        self, connection: str, table: str, ts_column: str, threshold_hours: float
    ) -> str:
        query = f"""
            SELECT
                MAX({ts_column}) as latest_record,
                TIMESTAMPDIFF(HOUR, MAX({ts_column}), NOW()) as hours_stale
            FROM {table}
        """
        try:
            result = await self._sql.execute(query=query, connection=connection)
        except ToolError as e:
            result = e.to_text()
        return f"**Freshness check for `{table}` (threshold: {threshold_hours}h):**\n{result}"

    async def _compare(
        self,
        target_conn: str,
        target_table: str,
        source_conn: str,
        source_table: str,
    ) -> str:
        try:
            src_result = await self._sql.execute(
                query=f"SELECT COUNT(*) as cnt FROM {source_table}",
                connection=source_conn,
            )
        except ToolError as e:
            src_result = e.to_text()
        try:
            tgt_result = await self._sql.execute(
                query=f"SELECT COUNT(*) as cnt FROM {target_table}",
                connection=target_conn,
            )
        except ToolError as e:
            tgt_result = e.to_text()
        return (
            f"**Source-target comparison:**\n"
            f"Source (`{source_table}` on {source_conn}):\n{src_result}\n\n"
            f"Target (`{target_table}` on {target_conn}):\n{tgt_result}"
        )
