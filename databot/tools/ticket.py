"""Support ticket management tool for interacting with ticketing systems (e.g., Jira, Zendesk).

This tool provides a generic interface to fetch, create, and update support tickets.
It can be configured to point to a specific REST API or use mock data for testing.
"""

import json
import os
from typing import Any, Dict

import httpx
from loguru import logger

from databot.tools.base import BaseTool


class TicketTool(BaseTool):
    """Tool for managing support tickets (fetch, update, create)."""

    def __init__(self, api_url: str = "", api_token: str = "", system_type: str = "mock"):
        """Initialize the Ticket Tool.

        Args:
            api_url: Base URL of the ticketing system REST API.
            api_token: API token for authentication.
            system_type: Type of ticketing system ("jira", "zendesk", "mock").
        """
        self.api_url = api_url or os.getenv("TICKET_API_URL", "")
        self.api_token = api_token or os.getenv("TICKET_API_TOKEN", "")
        self.system_type = system_type.lower() or os.getenv("TICKET_SYSTEM_TYPE", "mock").lower()

        # Simple mock storage for testing without a real backend
        self._mock_tickets: Dict[str, Dict[str, Any]] = {
            "TKT-101": {
                "id": "TKT-101",
                "status": "Open",
                "summary": "Pipeline XYZ is failing at transform step",
                "description": "The daily pipeline XYZ failed with an OOM error.",
                "assignee": "unassigned",
            },
            "TKT-102": {
                "id": "TKT-102",
                "status": "In Progress",
                "summary": "Data quality check failed on users table",
                "description": "Null values found in the non-nullable email column.",
                "assignee": "jane.doe",
            },
        }
        self._mock_counter = 103

    @property
    def name(self) -> str:
        return "support_ticket"

    @property
    def description(self) -> str:
        return (
            "Managing support tickets. Allows you to 'get_ticket' to fetch details, "
            "'update_ticket' to change status/assignee, or 'create_ticket' for new issues."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["get_ticket", "update_ticket", "create_ticket"],
                    "description": "The action to perform.",
                },
                "ticket_id": {
                    "type": "string",
                    "description": "The ticket ID (e.g., TKT-123). Required for get/update.",
                },
                "status": {
                    "type": "string",
                    "description": "New status for the ticket (e.g., Open, Closed). Used in update.",
                },
                "summary": {
                    "type": "string",
                    "description": "Title/Summary of the new ticket. Required for create.",
                },
                "description": {
                    "type": "string",
                    "description": "Detailed description. Used in create/update.",
                },
                "assignee": {"type": "string", "description": "Person assigned to the ticket."},
            },
            "required": ["action"],
        }

    async def execute(self, **kwargs: Any) -> str:
        action = kwargs.get("action")

        if self.system_type == "mock":
            return self._execute_mock(action, kwargs)

        # Real HTTP Implementation (Generic)
        if not self.api_url or not self.api_token:
            return "Error: Ticket API URL or Token not configured in environment (TICKET_API_URL, TICKET_API_TOKEN)."

        try:
            return await self._execute_http(action, kwargs)
        except Exception as e:
            logger.error(f"Ticket HTTP Error: {e}")
            return f"Error communicating with ticket system: {e}"

    def _execute_mock(self, action: str, kwargs: Dict[str, Any]) -> str:
        if action == "get_ticket":
            ticket_id = kwargs.get("ticket_id")
            if not ticket_id:
                return "Error: ticket_id is required for get_ticket."
            ticket = self._mock_tickets.get(ticket_id)
            if not ticket:
                return f"Ticket {ticket_id} not found."
            return json.dumps(ticket, indent=2)

        elif action == "update_ticket":
            ticket_id = kwargs.get("ticket_id")
            if not ticket_id:
                return "Error: ticket_id is required for update_ticket."
            ticket = self._mock_tickets.get(ticket_id)
            if not ticket:
                return f"Ticket {ticket_id} not found."

            if "status" in kwargs:
                ticket["status"] = kwargs["status"]
            if "description" in kwargs:
                ticket["description"] = kwargs["description"]
            if "assignee" in kwargs:
                ticket["assignee"] = kwargs["assignee"]

            self._mock_tickets[ticket_id] = ticket
            return f"Ticket {ticket_id} updated successfully: {json.dumps(ticket)}"

        elif action == "create_ticket":
            summary = kwargs.get("summary")
            if not summary:
                return "Error: summary is required for create_ticket."

            ticket_id = f"TKT-{self._mock_counter}"
            self._mock_counter += 1

            new_ticket = {
                "id": ticket_id,
                "status": kwargs.get("status", "Open"),
                "summary": summary,
                "description": kwargs.get("description", ""),
                "assignee": kwargs.get("assignee", "unassigned"),
            }
            self._mock_tickets[ticket_id] = new_ticket
            return f"Ticket created successfully: {json.dumps(new_ticket)}"

        return f"Error: Unknown action '{action}'"

    async def _execute_http(self, action: str, kwargs: Dict[str, Any]) -> str:
        """Generic HTTP implementation. Can be tailored for Jira/Zendesk."""
        headers = {"Authorization": f"Bearer {self.api_token}", "Content-Type": "application/json"}

        async with httpx.AsyncClient(timeout=10) as client:
            if action == "get_ticket":
                ticket_id = kwargs.get("ticket_id")
                url = f"{self.api_url}/tickets/{ticket_id}"  # generic endpoint
                res = await client.get(url, headers=headers)
                res.raise_for_status()
                return json.dumps(res.json(), indent=2)

            elif action == "update_ticket":
                ticket_id = kwargs.get("ticket_id")
                url = f"{self.api_url}/tickets/{ticket_id}"

                payload = {}
                if "status" in kwargs:
                    payload["status"] = kwargs["status"]
                if "description" in kwargs:
                    payload["description"] = kwargs["description"]
                if "assignee" in kwargs:
                    payload["assignee"] = kwargs["assignee"]

                res = await client.patch(url, json=payload, headers=headers)
                res.raise_for_status()
                return "Ticket updated successfully."

            elif action == "create_ticket":
                url = f"{self.api_url}/tickets"
                payload = {
                    "summary": kwargs.get("summary"),
                    "description": kwargs.get("description", ""),
                }
                res = await client.post(url, json=payload, headers=headers)
                res.raise_for_status()
                return f"Ticket created successfully: {res.json().get('id', 'Unknown ID')}"

        return "Unknown action"
