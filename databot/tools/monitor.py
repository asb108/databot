"""Monitor management tool — create and inspect scheduled data monitors."""

from __future__ import annotations

from typing import Any

from databot.monitors.evaluator import OPERATORS, MonitorError
from databot.tools.base import BaseTool
from databot.tools.errors import ErrorCode, ToolError


class MonitorTool(BaseTool):
    """Create, list, run, and remove scheduled data monitors."""

    def __init__(self, service: Any | None = None):
        self._service = service

    @property
    def name(self) -> str:
        return "monitor"

    @property
    def description(self) -> str:
        return (
            "Manage scheduled data monitors that alert a channel when a metric "
            "breaches a threshold. check_type: row_count, null_rate (needs column), "
            "or sql_metric (needs query selecting one number). operator: "
            f"{', '.join(sorted(OPERATORS))} (alert when value <operator> threshold). "
            "schedule is a cron expression."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["create", "list", "remove", "run_now", "history"],
                },
                "name": {"type": "string", "description": "Monitor name (for create)."},
                "connection": {"type": "string", "description": "Database connection."},
                "check_type": {
                    "type": "string",
                    "enum": ["row_count", "null_rate", "sql_metric"],
                },
                "table": {"type": "string", "description": "Table (row_count/null_rate)."},
                "column": {"type": "string", "description": "Column (null_rate)."},
                "query": {
                    "type": "string",
                    "description": "SQL selecting one number (sql_metric).",
                },
                "operator": {"type": "string", "enum": sorted(OPERATORS)},
                "threshold": {"type": "number", "description": "Threshold to compare against."},
                "schedule": {"type": "string", "description": "Cron schedule, e.g. '0 * * * *'."},
                "channel": {"type": "string", "description": "Alert channel (default gchat)."},
                "chat_id": {
                    "type": "string",
                    "description": (
                        "Destination room/thread ID for the alert. Required for real "
                        "channels (Slack/Discord/gchat) to route the message."
                    ),
                },
                "monitor_id": {
                    "type": "string",
                    "description": "Monitor ID (remove/run_now/history).",
                },
            },
            "required": ["action"],
        }

    async def execute(self, action: str, **kwargs: Any) -> str:
        if self._service is None:
            raise ToolError(
                ErrorCode.NOT_CONFIGURED,
                "monitor service not available (run inside the gateway).",
            )

        if action == "list":
            return self._format_list()
        if action == "create":
            return self._create(kwargs)
        if action == "remove":
            mid = kwargs.get("monitor_id", "")
            ok = self._service.remove_monitor(mid)
            return f"Removed monitor {mid}." if ok else f"No monitor with id '{mid}'."
        if action == "run_now":
            return await self._run_now(kwargs.get("monitor_id", ""))
        if action == "history":
            return self._history(kwargs.get("monitor_id", ""))
        raise ToolError(ErrorCode.INVALID_ARGUMENT, f"Unknown action '{action}'.")

    def _create(self, kw: dict[str, Any]) -> str:
        required = ["name", "connection", "check_type", "operator", "threshold", "schedule"]
        missing = [r for r in required if not kw.get(r) and kw.get(r) != 0]
        if missing:
            raise ToolError(
                ErrorCode.INVALID_ARGUMENT,
                f"missing required fields: {', '.join(missing)}.",
            )
        try:
            monitor_id = self._service.add_monitor(
                name=kw["name"],
                connection=kw["connection"],
                check_type=kw["check_type"],
                operator=kw["operator"],
                threshold=float(kw["threshold"]),
                schedule=kw["schedule"],
                table=kw.get("table", ""),
                column=kw.get("column", ""),
                query=kw.get("query", ""),
                channel=kw.get("channel", "gchat"),
                chat_id=kw.get("chat_id", ""),
            )
        except (MonitorError, ValueError) as e:
            raise ToolError(ErrorCode.INVALID_ARGUMENT, str(e))
        return f"Created monitor '{kw['name']}' (id: {monitor_id}), schedule '{kw['schedule']}'."

    def _format_list(self) -> str:
        monitors = self._service.list_monitors()
        if not monitors:
            return "No monitors configured."
        lines = [
            "**Monitors:**",
            "",
            "| id | name | check | schedule | last value | status |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for m in monitors:
            lines.append(
                f"| {m['id']} | {m['name']} | {m['check_type']} | {m['schedule']} "
                f"| {m.get('last_value', '')} | {m.get('last_status', '-')} |"
            )
        return "\n".join(lines)

    async def _run_now(self, monitor_id: str) -> str:
        monitor = self._service.store.get(monitor_id)
        if not monitor:
            return f"No monitor with id '{monitor_id}'."
        result = await self._service.run_monitor(monitor)
        if result["status"] == "error":
            return f"Monitor '{monitor['name']}' error: {result.get('detail', '')}"
        verdict = "BREACHED" if result["breached"] else "ok"
        return (
            f"Monitor '{monitor['name']}' ran: value={result['value']} → {verdict} "
            f"(value {monitor['operator']} {monitor['threshold']})."
        )

    def _history(self, monitor_id: str) -> str:
        monitor = self._service.store.get(monitor_id)
        if not monitor:
            return f"No monitor with id '{monitor_id}'."
        runs = self._service.store.history(monitor_id)
        if not runs:
            return f"No run history for monitor '{monitor['name']}'."
        lines = [
            f"**History for '{monitor['name']}':**",
            "",
            "| ran_at | value | status |",
            "| --- | --- | --- |",
        ]
        for r in runs:
            lines.append(f"| {r['ran_at']} | {r.get('value', '')} | {r['status']} |")
        return "\n".join(lines)
