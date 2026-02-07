"""Spark management tool — submit jobs, check status, manage sessions via connectors."""

from __future__ import annotations

from typing import Any

from databot.connectors.registry import ConnectorRegistry
from databot.connectors.spark_connector import SparkConnector
from databot.tools.base import BaseTool


class SparkTool(BaseTool):
    """Manage Apache Spark jobs and interactive sessions.

    Delegates to :class:`SparkConnector` instances registered in the
    :class:`ConnectorRegistry`.
    """

    def __init__(self, registry: ConnectorRegistry):
        self._registry = registry

    @property
    def name(self) -> str:
        return "spark"

    @property
    def description(self) -> str:
        spark_connectors = [
            c.name for c in self._registry.list_all() if isinstance(c, SparkConnector)
        ]
        names = ", ".join(spark_connectors) if spark_connectors else "none configured"
        return (
            f"Manage Apache Spark jobs: submit batch jobs, check status, view logs, "
            f"manage interactive sessions. Available clusters: {names}."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [
                        "submit_batch",
                        "batch_status",
                        "batch_logs",
                        "kill_batch",
                        "list_batches",
                        "create_session",
                        "run_statement",
                        "session_status",
                        "list_sessions",
                    ],
                    "description": "Spark action to perform.",
                },
                "connector": {
                    "type": "string",
                    "description": "Name of the Spark connector to use.",
                },
                "file": {
                    "type": "string",
                    "description": "Application file path (for submit_batch).",
                },
                "class_name": {
                    "type": "string",
                    "description": "Main class name (for Scala/Java batch jobs).",
                },
                "args": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Arguments for the batch job.",
                },
                "conf": {
                    "type": "object",
                    "description": "Spark configuration overrides.",
                },
                "batch_id": {
                    "type": "string",
                    "description": "Batch ID (for status/logs/kill).",
                },
                "app_id": {
                    "type": "string",
                    "description": "YARN/K8s application ID.",
                },
                "session_id": {
                    "type": "string",
                    "description": "Interactive session ID.",
                },
                "code": {
                    "type": "string",
                    "description": "Code to execute in interactive session.",
                },
                "kind": {
                    "type": "string",
                    "description": "Session kind: pyspark, spark, sql.",
                },
                "name": {
                    "type": "string",
                    "description": "Job or session name.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results for list operations.",
                },
            },
            "required": ["action"],
        }

    async def execute(self, action: str, connector: str = "", **kwargs: Any) -> str:
        spark = self._resolve_connector(connector)
        if isinstance(spark, str):
            return spark  # Error message

        result = await spark.execute(action, **kwargs)
        if not result.success:
            return f"Error: {result.error}"
        if result.columns and result.rows:
            return result.to_markdown_table()
        if result.data is not None:
            if isinstance(result.data, str):
                return result.data
            import json

            return json.dumps(result.data, indent=2, default=str)
        return "OK"

    def _resolve_connector(self, name: str) -> SparkConnector | str:
        """Resolve connector by name, or pick the only one if unambiguous."""
        if name:
            conn = self._registry.get(name)
            if conn is None:
                return f"Error: Spark connector '{name}' not found."
            if not isinstance(conn, SparkConnector):
                return f"Error: Connector '{name}' is not a Spark connector."
            return conn

        # Auto-select if only one spark connector exists
        spark_connectors = [c for c in self._registry.list_all() if isinstance(c, SparkConnector)]
        if len(spark_connectors) == 0:
            return "Error: No Spark connectors configured."
        if len(spark_connectors) == 1:
            return spark_connectors[0]
        names = ", ".join(c.name for c in spark_connectors)
        return (
            f"Error: Multiple Spark connectors configured ({names}). Specify 'connector' parameter."
        )
