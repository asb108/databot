"""Airflow tool for DAG operations via REST API."""

from __future__ import annotations

from typing import Any

import httpx

from databot.tools.base import BaseTool


class AirflowTool(BaseTool):
    """Interact with Apache Airflow via REST API."""

    def __init__(self, base_url: str = "", username: str = "", password: str = ""):
        self._base_url = base_url.rstrip("/")
        self._auth = (username, password) if username else None

    @property
    def name(self) -> str:
        return "airflow"

    @property
    def description(self) -> str:
        return (
            "Interact with Apache Airflow. "
            "Check DAG status, view task logs, list recent runs, or trigger DAGs."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list_dags", "dag_runs", "task_status", "task_log", "trigger"],
                    "description": "Action to perform.",
                },
                "dag_id": {
                    "type": "string",
                    "description": "DAG ID (required for most actions).",
                },
                "run_id": {
                    "type": "string",
                    "description": "DAG run ID (for task_status, task_log).",
                },
                "task_id": {
                    "type": "string",
                    "description": "Task ID (for task_log).",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results to return. Default 10.",
                },
            },
            "required": ["action"],
        }

    async def execute(self, action: str, **kwargs: Any) -> str:
        if not self._base_url:
            return "Error: Airflow base_url not configured."

        try:
            if action == "list_dags":
                return await self._list_dags(kwargs.get("limit", 25))
            elif action == "dag_runs":
                dag_id = kwargs.get("dag_id", "")
                if not dag_id:
                    return "Error: dag_id is required for dag_runs."
                return await self._dag_runs(dag_id, kwargs.get("limit", 10))
            elif action == "task_status":
                dag_id = kwargs.get("dag_id", "")
                run_id = kwargs.get("run_id", "")
                if not dag_id or not run_id:
                    return "Error: dag_id and run_id are required for task_status."
                return await self._task_status(dag_id, run_id)
            elif action == "task_log":
                dag_id = kwargs.get("dag_id", "")
                run_id = kwargs.get("run_id", "")
                task_id = kwargs.get("task_id", "")
                if not all([dag_id, run_id, task_id]):
                    return "Error: dag_id, run_id, and task_id are required for task_log."
                return await self._task_log(dag_id, run_id, task_id)
            elif action == "trigger":
                dag_id = kwargs.get("dag_id", "")
                if not dag_id:
                    return "Error: dag_id is required for trigger."
                return await self._trigger(dag_id)
            else:
                return f"Unknown action: {action}"
        except Exception as e:
            return f"Airflow API error: {str(e)}"

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict:
        """Make an authenticated request to the Airflow API."""
        url = f"{self._base_url}/api/v1{path}"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.request(
                method,
                url,
                auth=self._auth,
                headers={"Content-Type": "application/json"},
                **kwargs,
            )
            resp.raise_for_status()
            return resp.json()

    async def _list_dags(self, limit: int) -> str:
        data = await self._request("GET", f"/dags?limit={limit}&only_active=true")
        dags = data.get("dags", [])
        if not dags:
            return "No active DAGs found."

        lines = ["| DAG ID | Paused | Schedule |", "|---|---|---|"]
        for dag in dags:
            paused = "Yes" if dag.get("is_paused") else "No"
            schedule = dag.get("schedule_interval", "None")
            lines.append(f"| {dag['dag_id']} | {paused} | {schedule} |")
        return "\n".join(lines)

    async def _dag_runs(self, dag_id: str, limit: int) -> str:
        data = await self._request(
            "GET", f"/dags/{dag_id}/dagRuns?limit={limit}&order_by=-execution_date"
        )
        runs = data.get("dag_runs", [])
        if not runs:
            return f"No runs found for DAG '{dag_id}'."

        lines = ["| Run ID | State | Execution Date | Duration |", "|---|---|---|---|"]
        for run in runs:
            duration = ""
            if run.get("start_date") and run.get("end_date"):
                duration = "completed"
            elif run.get("start_date"):
                duration = "running"
            lines.append(
                f"| {run['dag_run_id'][:40]} | {run['state']} | "
                f"{run.get('execution_date', '')[:19]} | {duration} |"
            )
        return "\n".join(lines)

    async def _task_status(self, dag_id: str, run_id: str) -> str:
        data = await self._request("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        tasks = data.get("task_instances", [])
        if not tasks:
            return "No task instances found."

        lines = ["| Task ID | State | Duration (s) |", "|---|---|---|"]
        for task in tasks:
            duration = task.get("duration", "")
            if duration:
                duration = f"{float(duration):.1f}"
            lines.append(f"| {task['task_id']} | {task['state']} | {duration} |")
        return "\n".join(lines)

    async def _task_log(self, dag_id: str, run_id: str, task_id: str) -> str:
        url = (
            f"{self._base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1"
        )
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                url,
                auth=self._auth,
                headers={"Accept": "text/plain"},
            )
            resp.raise_for_status()
            content = resp.text
            if len(content) > 5000:
                content = "... (truncated)\n" + content[-5000:]
            return content

    async def _trigger(self, dag_id: str) -> str:
        data = await self._request("POST", f"/dags/{dag_id}/dagRuns", json={"conf": {}})
        return f"Triggered DAG '{dag_id}'. Run ID: {data.get('dag_run_id', 'unknown')}"
