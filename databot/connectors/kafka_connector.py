"""Kafka connector — interface for Apache Kafka ecosystem.

Covers:
  * Kafka Admin REST API (or Confluent Admin REST) — topics, consumer groups
  * Schema Registry — Avro/Protobuf/JSON schema management
  * Kafka Connect — connector lifecycle management
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from databot.connectors.base import ConnectorResult, ConnectorStatus, ConnectorType
from databot.connectors.rest_connector import RESTConnector


class KafkaConnector(RESTConnector):
    """Connector for the Apache Kafka ecosystem via REST APIs."""

    def __init__(self, name: str, config: dict[str, Any] | None = None):
        super().__init__(name, config)
        self._schema_registry_url = self._config.get("schema_registry_url", "")
        self._connect_url = self._config.get("connect_url", "")
        # Override base_url to the Kafka REST Proxy / Admin URL
        self._kafka_rest_url = self._config.get("base_url", "")

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.STREAMING

    def capabilities(self) -> list[str]:
        caps = ["list_topics", "describe_topic", "list_consumer_groups", "consumer_group_lag"]
        if self._schema_registry_url:
            caps += ["list_subjects", "get_schema"]
        if self._connect_url:
            caps += ["list_connectors", "connector_status"]
        return caps

    async def health_check(self) -> ConnectorStatus:
        if not self._kafka_rest_url:
            return ConnectorStatus.NOT_CONFIGURED
        try:
            result = await self.request("GET", "/v3/clusters")
            if result.success:
                return ConnectorStatus.HEALTHY
            # Fallback: try Confluent Admin v2
            result = await self.request("GET", "/")
            return ConnectorStatus.HEALTHY if result.success else ConnectorStatus.DEGRADED
        except Exception:
            return ConnectorStatus.UNREACHABLE

    # ------------------------------------------------------------------
    # Topic operations
    # ------------------------------------------------------------------

    async def _op_list_topics(self, **kwargs: Any) -> ConnectorResult:
        """List Kafka topics."""
        # Try Confluent REST Proxy v3
        result = await self.request("GET", "/v3/clusters")
        if result.success and isinstance(result.data, dict):
            clusters = result.data.get("data", [])
            if clusters:
                cluster_id = clusters[0].get("cluster_id", "")
                topics_result = await self.request("GET", f"/v3/clusters/{cluster_id}/topics")
                if topics_result.success and isinstance(topics_result.data, dict):
                    topics = topics_result.data.get("data", [])
                    columns = ["topic_name", "partitions_count", "is_internal"]
                    rows = [
                        [t.get("topic_name"), t.get("partitions_count", ""), t.get("is_internal", False)]
                        for t in topics
                    ]
                    return ConnectorResult(columns=columns, rows=rows, row_count=len(rows))

        # Fallback: basic /topics endpoint (Confluent REST Proxy v2 or Karapace)
        result = await self.request("GET", "/topics")
        if result.success:
            topics = result.data if isinstance(result.data, list) else []
            return ConnectorResult(
                data=topics,
                columns=["topic_name"],
                rows=[[t] for t in topics],
                row_count=len(topics),
            )
        return result

    async def _op_describe_topic(self, topic: str, **kwargs: Any) -> ConnectorResult:
        """Get topic details including partitions and configs."""
        result = await self.request("GET", f"/topics/{topic}")
        if result.success:
            return result  # Returns full topic config
        return result

    async def _op_list_consumer_groups(self, **kwargs: Any) -> ConnectorResult:
        """List consumer groups."""
        # Try v3 API
        result = await self.request("GET", "/v3/clusters")
        if result.success and isinstance(result.data, dict):
            clusters = result.data.get("data", [])
            if clusters:
                cluster_id = clusters[0].get("cluster_id", "")
                groups_result = await self.request("GET", f"/v3/clusters/{cluster_id}/consumer-groups")
                if groups_result.success and isinstance(groups_result.data, dict):
                    groups = groups_result.data.get("data", [])
                    columns = ["consumer_group_id", "state", "coordinator"]
                    rows = [
                        [g.get("consumer_group_id"), g.get("state", ""), g.get("coordinator", {}).get("host", "")]
                        for g in groups
                    ]
                    return ConnectorResult(columns=columns, rows=rows, row_count=len(rows))
        return ConnectorResult(success=False, error="Consumer group listing requires Confluent REST Proxy v3")

    async def _op_consumer_group_lag(self, group: str, **kwargs: Any) -> ConnectorResult:
        """Get consumer lag for a consumer group."""
        # Try v3 API
        result = await self.request("GET", "/v3/clusters")
        if result.success and isinstance(result.data, dict):
            clusters = result.data.get("data", [])
            if clusters:
                cluster_id = clusters[0].get("cluster_id", "")
                lag_result = await self.request(
                    "GET", f"/v3/clusters/{cluster_id}/consumer-groups/{group}/lags"
                )
                if lag_result.success and isinstance(lag_result.data, dict):
                    lags = lag_result.data.get("data", [])
                    columns = ["topic_name", "partition_id", "current_offset", "log_end_offset", "lag"]
                    rows = [
                        [
                            l.get("topic_name"),
                            l.get("partition_id"),
                            l.get("current_offset"),
                            l.get("log_end_offset"),
                            l.get("lag"),
                        ]
                        for l in lags
                    ]
                    return ConnectorResult(columns=columns, rows=rows, row_count=len(rows))
                return lag_result
        return ConnectorResult(success=False, error="Consumer lag requires Confluent REST Proxy v3")

    # ------------------------------------------------------------------
    # Schema Registry operations
    # ------------------------------------------------------------------

    async def _op_list_subjects(self, **kwargs: Any) -> ConnectorResult:
        """List subjects in Schema Registry."""
        if not self._schema_registry_url:
            return ConnectorResult(success=False, error="schema_registry_url not configured")

        # Use a temporary client for the schema registry
        import httpx
        async with httpx.AsyncClient(base_url=self._schema_registry_url, timeout=self._timeout) as client:
            resp = await client.get("/subjects")
            resp.raise_for_status()
            subjects = resp.json()
            return ConnectorResult(
                data=subjects,
                columns=["subject"],
                rows=[[s] for s in subjects],
                row_count=len(subjects),
            )

    async def _op_get_schema(self, subject: str, version: str = "latest", **kwargs: Any) -> ConnectorResult:
        """Get schema for a subject from Schema Registry."""
        if not self._schema_registry_url:
            return ConnectorResult(success=False, error="schema_registry_url not configured")

        import httpx
        async with httpx.AsyncClient(base_url=self._schema_registry_url, timeout=self._timeout) as client:
            resp = await client.get(f"/subjects/{subject}/versions/{version}")
            resp.raise_for_status()
            return ConnectorResult(data=resp.json())

    # ------------------------------------------------------------------
    # Kafka Connect operations
    # ------------------------------------------------------------------

    async def _op_list_connectors(self, **kwargs: Any) -> ConnectorResult:
        """List Kafka Connect connectors."""
        if not self._connect_url:
            return ConnectorResult(success=False, error="connect_url not configured")

        import httpx
        async with httpx.AsyncClient(base_url=self._connect_url, timeout=self._timeout) as client:
            resp = await client.get("/connectors?expand=status")
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, dict):
                # Expanded format
                columns = ["connector", "state", "worker_id", "type"]
                rows = []
                for name, info in data.items():
                    status = info.get("status", {}).get("connector", {})
                    rows.append([
                        name,
                        status.get("state", ""),
                        status.get("worker_id", ""),
                        info.get("type", ""),
                    ])
                return ConnectorResult(columns=columns, rows=rows, row_count=len(rows))
            else:
                # Simple list
                return ConnectorResult(
                    data=data,
                    columns=["connector"],
                    rows=[[c] for c in data],
                    row_count=len(data),
                )

    async def _op_connector_status(self, connector_name: str, **kwargs: Any) -> ConnectorResult:
        """Get status of a specific Kafka Connect connector."""
        if not self._connect_url:
            return ConnectorResult(success=False, error="connect_url not configured")

        import httpx
        async with httpx.AsyncClient(base_url=self._connect_url, timeout=self._timeout) as client:
            resp = await client.get(f"/connectors/{connector_name}/status")
            resp.raise_for_status()
            return ConnectorResult(data=resp.json())
