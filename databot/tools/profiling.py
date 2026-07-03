"""Data profiling tool — statistical column profiles for a table.

Profiling is a staple of serious data tooling (great_expectations, dbt, pandas
profiling): given a table, summarize each column's null rate, distinct count, and
value range so an engineer can spot data-quality problems at a glance. This tool
builds those summaries from safe aggregate SQL, reusing the centralized
identifier validation (so column/table names from the LLM can never inject SQL)
and the existing SQL tool for execution (so it works against every configured
engine).
"""

from __future__ import annotations

from typing import Any

from databot.connectors.identifiers import IdentifierError, validate_identifier
from databot.tools.base import BaseTool

_MAX_COLUMNS = 40


def _parse_single_row(markdown: str) -> dict[str, str] | None:
    """Parse a one-row markdown table (header + separator + one data row)."""
    lines = [ln for ln in markdown.splitlines() if ln.strip().startswith("|")]
    if len(lines) < 3:
        return None

    def cells(line: str) -> list[str]:
        return [c.strip() for c in line.strip().strip("|").split("|")]

    header = cells(lines[0])
    data = cells(lines[2])
    if len(data) != len(header):
        return None
    return dict(zip(header, data))


class DataProfileTool(BaseTool):
    """Profile a table's columns: nulls, distinct counts, and value ranges."""

    # Profiling runs one aggregate query per column, so allow extra time.
    timeout = 300

    def __init__(self, sql_tool: Any | None = None):
        self._sql = sql_tool

    @property
    def name(self) -> str:
        return "profile_data"

    @property
    def description(self) -> str:
        return (
            "Profile a database table: total rows, and per-column null rate, "
            "distinct count, and min/max values. Provide specific columns to "
            "profile, or omit to auto-discover them. Useful for spotting data "
            "quality issues (high null rates, low cardinality, unexpected ranges)."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "connection": {"type": "string", "description": "Database connection name."},
                "table": {"type": "string", "description": "Table to profile (schema.table)."},
                "columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Columns to profile. Omit to auto-discover.",
                },
                "max_columns": {
                    "type": "integer",
                    "description": f"Max columns to profile (default {_MAX_COLUMNS}).",
                },
            },
            "required": ["connection", "table"],
        }

    async def execute(
        self,
        connection: str,
        table: str,
        columns: list[str] | None = None,
        max_columns: int = _MAX_COLUMNS,
        **kwargs: Any,
    ) -> str:
        if not self._sql:
            return "Error: SQL tool not configured. Cannot profile data."

        try:
            validate_identifier(table, "table")
        except IdentifierError as e:
            return f"Error: {e}"

        # Total row count first (also validates the table is queryable).
        total_md = await self._sql.execute(
            query=f"SELECT COUNT(*) AS total FROM {table}", connection=connection
        )
        total_row = _parse_single_row(total_md)
        if total_row is None:
            # Surface the underlying error (e.g. unknown connection / table).
            return f"**Profile for `{table}`** — could not read row count:\n{total_md}"
        try:
            total = int(float(next(iter(total_row.values()))))
        except (ValueError, StopIteration):
            total = 0

        # Resolve the column set.
        if columns:
            try:
                target_cols = [validate_identifier(c, "column") for c in columns]
            except IdentifierError as e:
                return f"Error: {e}"
        else:
            discovered = await self._discover_columns(connection, table)
            if discovered is None:
                return (
                    f"**Profile for `{table}`**: {total:,} rows.\n\n"
                    "Could not auto-discover columns; pass an explicit `columns` list "
                    "to profile individual columns."
                )
            target_cols = discovered

        target_cols = target_cols[: max(1, min(max_columns, _MAX_COLUMNS))]

        rows = []
        for col in target_cols:
            rows.append(await self._profile_column(connection, table, col, total))

        return self._render(table, total, rows)

    async def _discover_columns(self, connection: str, table: str) -> list[str] | None:
        """Best-effort column discovery via a single-row sample."""
        sample = await self._sql.execute(
            query=f"SELECT * FROM {table} LIMIT 1", connection=connection
        )
        lines = [ln for ln in sample.splitlines() if ln.strip().startswith("|")]
        if not lines:
            return None
        header = [c.strip() for c in lines[0].strip().strip("|").split("|")]
        cols: list[str] = []
        for h in header:
            try:
                cols.append(validate_identifier(h, "column"))
            except IdentifierError:
                continue  # skip columns whose names aren't safe to interpolate
        return cols or None

    async def _profile_column(
        self, connection: str, table: str, col: str, total: int
    ) -> dict[str, Any]:
        query = (
            f"SELECT COUNT({col}) AS non_null, COUNT(DISTINCT {col}) AS distinct_count, "
            f"MIN({col}) AS min_val, MAX({col}) AS max_val FROM {table}"
        )
        md = await self._sql.execute(query=query, connection=connection)
        parsed = _parse_single_row(md)
        if parsed is None:
            return {"column": col, "error": True}

        def _int(key: str) -> int | None:
            try:
                return int(float(parsed.get(key, "")))
            except (ValueError, TypeError):
                return None

        non_null = _int("non_null")
        distinct = _int("distinct_count")
        null_pct = None
        if non_null is not None and total > 0:
            null_pct = round((total - non_null) * 100.0 / total, 2)
        return {
            "column": col,
            "non_null": non_null,
            "distinct": distinct,
            "null_pct": null_pct,
            "min": parsed.get("min_val", ""),
            "max": parsed.get("max_val", ""),
        }

    def _render(self, table: str, total: int, rows: list[dict[str, Any]]) -> str:
        lines = [
            f"**Data profile for `{table}`** — {total:,} total rows",
            "",
            "| column | null % | distinct | min | max |",
            "| --- | --- | --- | --- | --- |",
        ]
        flags: list[str] = []
        for r in rows:
            if r.get("error"):
                lines.append(f"| {r['column']} | _error_ | | | |")
                continue
            null_pct = r["null_pct"]
            null_disp = "?" if null_pct is None else f"{null_pct}%"
            lines.append(
                f"| {r['column']} | {null_disp} | {r['distinct'] if r['distinct'] is not None else '?'} "
                f"| {r['min']} | {r['max']} |"
            )
            # Surface notable data-quality signals.
            if null_pct is not None and null_pct >= 50:
                flags.append(f"- `{r['column']}` is {null_pct}% null")
            if r["distinct"] == 1 and total > 1:
                flags.append(f"- `{r['column']}` has a single distinct value (constant)")
            if r["distinct"] == 0:
                flags.append(f"- `{r['column']}` is entirely null/empty")

        if flags:
            lines.append("")
            lines.append("**Potential issues:**")
            lines.extend(flags)
        return "\n".join(lines)
