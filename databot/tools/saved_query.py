"""Saved query tool — save, list, run, and delete named SQL queries."""

from __future__ import annotations

from typing import Any

from databot.queries.store import QueryStore, placeholders, render_query
from databot.tools.base import BaseTool


class SavedQueryTool(BaseTool):
    """Manage and run a library of named, parameterized queries."""

    def __init__(self, store: QueryStore, sql_tool: Any | None = None):
        self._store = store
        self._sql = sql_tool

    @property
    def store(self) -> QueryStore:
        return self._store

    @property
    def name(self) -> str:
        return "saved_query"

    @property
    def description(self) -> str:
        return (
            "Manage a library of reusable named SQL queries. Save a query with "
            "{param} placeholders, then run it by name with parameters. Actions: "
            "save, list, get, run, delete. Parameters are substituted as escaped "
            "SQL literals (backslashes/null bytes are rejected)."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["save", "list", "get", "run", "delete"],
                },
                "name": {"type": "string", "description": "Query name."},
                "sql": {"type": "string", "description": "SQL with {param} placeholders (save)."},
                "connection": {"type": "string", "description": "Default connection."},
                "description": {"type": "string", "description": "Human description (save)."},
                "params": {
                    "type": "object",
                    "description": 'Parameter values for run, e.g. {"region": "APAC"}.',
                },
            },
            "required": ["action"],
        }

    async def execute(self, action: str, **kwargs: Any) -> str:
        if action == "list":
            return self._list()
        name = kwargs.get("name", "")
        if action == "save":
            return self._save(kwargs)
        if not name:
            return f"Error: 'name' is required for {action}."
        if action == "get":
            return self._get(name)
        if action == "delete":
            ok = self._store.delete(name)
            return f"Deleted query '{name}'." if ok else f"No saved query named '{name}'."
        if action == "run":
            return await self._run(name, kwargs.get("params"), kwargs.get("connection", ""))
        return f"Unknown action '{action}'."

    def _save(self, kw: dict[str, Any]) -> str:
        name = kw.get("name", "")
        sql = kw.get("sql", "")
        if not name or not sql:
            return "Error: 'name' and 'sql' are required to save a query."
        self._store.save(
            name, sql, connection=kw.get("connection", ""), description=kw.get("description", "")
        )
        params = placeholders(sql)
        param_note = f" Parameters: {', '.join(params)}." if params else ""
        return f"Saved query '{name}'.{param_note}"

    def _list(self) -> str:
        queries = self._store.list_all()
        if not queries:
            return "No saved queries."
        lines = [
            "**Saved queries:**",
            "",
            "| name | connection | params | description |",
            "| --- | --- | --- | --- |",
        ]
        for q in queries:
            params = ", ".join(placeholders(q["sql"])) or "-"
            lines.append(
                f"| {q['name']} | {q.get('connection') or '-'} | {params} "
                f"| {(q.get('description') or '')[:50]} |"
            )
        return "\n".join(lines)

    def _get(self, name: str) -> str:
        q = self._store.get(name)
        if not q:
            return f"No saved query named '{name}'."
        params = ", ".join(placeholders(q["sql"])) or "none"
        return (
            f"**{q['name']}**\n"
            f"- connection: {q.get('connection') or '(none)'}\n"
            f"- parameters: {params}\n"
            f"- description: {q.get('description') or '(none)'}\n\n"
            f"```sql\n{q['sql']}\n```"
        )

    async def _run(self, name: str, params: dict[str, Any] | None, connection: str) -> str:
        q = self._store.get(name)
        if not q:
            return f"No saved query named '{name}'."
        if not self._sql:
            return "Error: SQL tool not available to run queries."
        conn = connection or q.get("connection", "")
        if not conn:
            return f"Error: no connection for '{name}'. Pass 'connection' to run it."
        try:
            rendered = render_query(q["sql"], params)
        except ValueError as e:
            return f"Error: {e}"
        result = await self._sql.execute(query=rendered, connection=conn)
        return f"**{name}**\n```sql\n{rendered}\n```\n\n{result}"
