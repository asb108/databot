"""Factory for creating connector instances from config."""

from __future__ import annotations

from typing import Any

from databot.connectors.base import BaseConnector


def create_connector(name: str, config: dict[str, Any]) -> BaseConnector:
    """Create a connector instance from a config dict.

    The ``type`` key selects the connector class:

    * ``sql`` → :class:`SQLConnector`
    * ``rest_api`` → :class:`RESTConnector`
    * ``catalog`` → :class:`CatalogConnector`
    * ``spark`` → :class:`SparkConnector`
    * ``kafka`` → :class:`KafkaConnector`
    * ``streaming`` → :class:`KafkaConnector` (alias)
    * ``processing`` → :class:`SparkConnector` (alias)

    Raises ``ValueError`` for unknown types.
    """
    connector_type = config.get("type", "sql")

    if connector_type == "sql":
        from databot.connectors.sql_connector import SQLConnector

        return SQLConnector(name, config)

    elif connector_type == "rest_api":
        from databot.connectors.rest_connector import RESTConnector

        return RESTConnector(name, config)

    elif connector_type == "catalog":
        from databot.connectors.catalog_connector import CatalogConnector

        return CatalogConnector(name, config)

    elif connector_type in ("spark", "processing"):
        from databot.connectors.spark_connector import SparkConnector

        return SparkConnector(name, config)

    elif connector_type in ("kafka", "streaming"):
        from databot.connectors.kafka_connector import KafkaConnector

        return KafkaConnector(name, config)

    else:
        raise ValueError(
            f"Unknown connector type '{connector_type}' for connector '{name}'. "
            f"Supported: sql, rest_api, catalog, spark, kafka"
        )
