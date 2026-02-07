"""Kafka ecosystem tool — topics, consumer groups, schemas, connectors."""

from __future__ import annotations

from typing import Any

from databot.connectors.kafka_connector import KafkaConnector
from databot.connectors.registry import ConnectorRegistry
from databot.tools.base import BaseTool


class KafkaTool(BaseTool):
    """Manage Apache Kafka ecosystem: topics, consumers, schemas, connectors.

    Delegates to :class:`KafkaConnector` instances registered in the
    :class:`ConnectorRegistry`.
    """

    def __init__(self, registry: ConnectorRegistry):
        self._registry = registry

    @property
    def name(self) -> str:
        return "kafka"

    @property
    def description(self) -> str:
        kafka_connectors = [
            c.name for c in self._registry.list_all() if isinstance(c, KafkaConnector)
        ]
        names = ", ".join(kafka_connectors) if kafka_connectors else "none configured"
        return (
            f"Manage Apache Kafka: list topics, describe topics, check consumer group lag, "
            f"query Schema Registry, manage Kafka Connect connectors. Clusters: {names}."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [
                        "list_topics",
                        "describe_topic",
                        "list_consumer_groups",
                        "consumer_group_lag",
                        "list_subjects",
                        "get_schema",
                        "list_connectors",
                        "connector_status",
                    ],
                    "description": "Kafka action to perform.",
                },
                "connector": {
                    "type": "string",
                    "description": "Name of the Kafka connector to use.",
                },
                "topic": {
                    "type": "string",
                    "description": "Topic name (for describe_topic).",
                },
                "group": {
                    "type": "string",
                    "description": "Consumer group ID (for consumer_group_lag).",
                },
                "subject": {
                    "type": "string",
                    "description": "Schema Registry subject name.",
                },
                "version": {
                    "type": "string",
                    "description": "Schema version (default: latest).",
                },
                "connector_name": {
                    "type": "string",
                    "description": "Kafka Connect connector name.",
                },
            },
            "required": ["action"],
        }

    async def execute(self, action: str, connector: str = "", **kwargs: Any) -> str:
        kafka = self._resolve_connector(connector)
        if isinstance(kafka, str):
            return kafka  # Error message

        result = await kafka.execute(action, **kwargs)
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

    def _resolve_connector(self, name: str) -> KafkaConnector | str:
        """Resolve connector by name, or pick the only one if unambiguous."""
        if name:
            conn = self._registry.get(name)
            if conn is None:
                return f"Error: Kafka connector '{name}' not found."
            if not isinstance(conn, KafkaConnector):
                return f"Error: Connector '{name}' is not a Kafka connector."
            return conn

        kafka_connectors = [c for c in self._registry.list_all() if isinstance(c, KafkaConnector)]
        if len(kafka_connectors) == 0:
            return "Error: No Kafka connectors configured."
        if len(kafka_connectors) == 1:
            return kafka_connectors[0]
        names = ", ".join(c.name for c in kafka_connectors)
        return (
            f"Error: Multiple Kafka connectors configured ({names}). Specify 'connector' parameter."
        )
