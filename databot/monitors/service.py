"""Monitor scheduler: runs due checks and alerts channels on threshold breach."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from croniter import croniter
from loguru import logger

from databot.core.bus import MessageBus, OutboundMessage
from databot.monitors.evaluator import (
    OPERATORS,
    MonitorError,
    build_check_query,
    evaluate_breach,
    parse_metric_value,
)
from databot.monitors.store import MonitorStore


class MonitorService:
    """Manages and executes scheduled data monitors."""

    def __init__(self, data_dir: Path, bus: MessageBus, sql_tool: Any | None = None):
        self.store = MonitorStore(data_dir / "monitors.db")
        self.bus = bus
        self._sql = sql_tool
        self._running = False

    def set_sql_tool(self, sql_tool: Any) -> None:
        self._sql = sql_tool

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------
    def add_monitor(
        self,
        name: str,
        connection: str,
        check_type: str,
        operator: str,
        threshold: float,
        schedule: str,
        table: str = "",
        column: str = "",
        query: str = "",
        channel: str = "gchat",
        chat_id: str = "",
    ) -> str:
        """Validate and persist a monitor. Returns its ID."""
        if operator not in OPERATORS:
            raise MonitorError(f"Unknown operator '{operator}'. Use one of {sorted(OPERATORS)}.")
        if not croniter.is_valid(schedule):
            raise MonitorError(f"Invalid cron schedule: {schedule}")
        # Validate the check definition eagerly (raises MonitorError on bad input).
        build_check_query(check_type, table=table, column=column, query=query)

        monitor_id = str(uuid.uuid4())[:8]
        self.store.add(
            monitor_id,
            name=name,
            connection=connection,
            check_type=check_type,
            table_name=table,
            column_name=column,
            query=query,
            operator=operator,
            threshold=float(threshold),
            schedule=schedule,
            channel=channel,
            chat_id=chat_id or f"monitor:{name}",
        )
        logger.info(f"Added monitor '{name}' ({monitor_id}): {schedule}")
        return monitor_id

    def remove_monitor(self, monitor_id: str) -> bool:
        return self.store.remove(monitor_id)

    def list_monitors(self) -> list[dict]:
        return self.store.list_all()

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    async def run_monitor(self, monitor: dict[str, Any]) -> dict[str, Any]:
        """Run a single monitor check. Records the run and returns a result dict."""
        if self._sql is None:
            return {"status": "error", "detail": "no SQL tool configured", "breached": False}
        try:
            query = build_check_query(
                monitor["check_type"],
                table=monitor.get("table_name", ""),
                column=monitor.get("column_name", ""),
                query=monitor.get("query", ""),
            )
        except MonitorError as e:
            self.store.record_run(monitor["id"], None, "error", False)
            return {"status": "error", "detail": str(e), "breached": False}

        result_md = await self._sql.execute(query=query, connection=monitor["connection"])
        value = parse_metric_value(result_md)
        if value is None:
            self.store.record_run(monitor["id"], None, "error", False)
            return {"status": "error", "detail": result_md[:200], "breached": False}

        breached = evaluate_breach(value, monitor["operator"], float(monitor["threshold"]))
        status = "breached" if breached else "ok"
        self.store.record_run(monitor["id"], value, status, breached)
        return {"status": status, "value": value, "breached": breached}

    def format_alert(self, monitor: dict[str, Any], value: float) -> str:
        op = OPERATORS.get(monitor["operator"], monitor["operator"])
        return (
            f"🚨 *Monitor alert: {monitor['name']}*\n"
            f"Check `{monitor['check_type']}` on `{monitor['connection']}` returned "
            f"**{value}**, which breaches the threshold (value {op} {monitor['threshold']})."
        )

    async def _alert(self, monitor: dict[str, Any], value: float) -> None:
        await self.bus.publish_outbound(
            OutboundMessage(
                channel=monitor.get("channel", "gchat"),
                chat_id=monitor.get("chat_id") or f"monitor:{monitor['name']}",
                content=self.format_alert(monitor, value),
            )
        )

    async def run(self) -> None:
        """Run the monitor scheduler loop."""
        self._running = True
        logger.info("Monitor service started")
        while self._running:
            try:
                await self._check_and_execute()
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await asyncio.sleep(60)

    def stop(self) -> None:
        self._running = False
        logger.info("Monitor service stopping")

    async def _check_and_execute(self) -> None:
        now = datetime.now(timezone.utc)
        for monitor in self.store.get_enabled():
            try:
                prev = croniter(monitor["schedule"], now).get_prev(datetime)
                last_run = monitor.get("last_run")
                if last_run is None:
                    should_run = True
                elif isinstance(last_run, str):
                    last_dt = datetime.fromisoformat(last_run).replace(tzinfo=timezone.utc)
                    should_run = prev > last_dt
                else:
                    should_run = prev > last_run

                if should_run:
                    result = await self.run_monitor(monitor)
                    if result.get("breached"):
                        logger.warning(f"Monitor '{monitor['name']}' breached: {result['value']}")
                        await self._alert(monitor, result["value"])
            except Exception as e:
                logger.error(f"Error running monitor '{monitor.get('name')}': {e}")
