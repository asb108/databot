"""Cron tool for managing scheduled tasks from within agent conversations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from databot.tools.base import BaseTool

if TYPE_CHECKING:
    from databot.cron.service import CronService


class CronTool(BaseTool):
    """Tool for managing scheduled tasks."""

    def __init__(self, cron_service: CronService):
        self._cron = cron_service

    @property
    def name(self) -> str:
        return "cron"

    @property
    def description(self) -> str:
        return "Manage scheduled tasks. Can add, remove, or list cron jobs."

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["add", "remove", "list"],
                    "description": "Action to perform.",
                },
                "name": {"type": "string", "description": "Job name (for 'add')."},
                "schedule": {
                    "type": "string",
                    "description": "Cron expression (for 'add'). E.g. '0 9 * * *' for daily at 9am.",
                },
                "message": {
                    "type": "string",
                    "description": "Message/task to execute (for 'add').",
                },
                "job_id": {"type": "string", "description": "Job ID (for 'remove')."},
            },
            "required": ["action"],
        }

    async def execute(self, action: str, **kwargs: Any) -> str:
        if action == "list":
            jobs = self._cron.list_jobs()
            if not jobs:
                return "No scheduled jobs."
            lines = ["| ID | Name | Schedule | Enabled |", "|---|---|---|---|"]
            for j in jobs:
                enabled = "Yes" if j["enabled"] else "No"
                lines.append(f"| {j['id']} | {j['name']} | {j['schedule']} | {enabled} |")
            return "\n".join(lines)

        elif action == "add":
            name = kwargs.get("name", "")
            schedule = kwargs.get("schedule", "")
            message = kwargs.get("message", "")
            if not all([name, schedule, message]):
                return "Error: 'add' requires name, schedule, and message."
            try:
                job_id = self._cron.add_job(name, schedule, message)
                return f"Created job '{name}' (ID: {job_id}) with schedule '{schedule}'"
            except ValueError as e:
                return f"Error: {e}"

        elif action == "remove":
            job_id = kwargs.get("job_id", "")
            if not job_id:
                return "Error: 'remove' requires job_id."
            if self._cron.remove_job(job_id):
                return f"Removed job {job_id}"
            return f"Job {job_id} not found."

        return f"Unknown action: {action}"
