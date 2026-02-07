"""Connector registry — manages connector lifecycle and discovery."""

from __future__ import annotations

from typing import Any

from loguru import logger

from databot.connectors.base import (
    BaseConnector,
    ConnectorResult,
    ConnectorStatus,
    ConnectorType,
)


class ConnectorRegistry:
    """Central registry for all connector instances.

    Responsibilities:
    * Register / deregister connectors
    * Discover connectors from config and entry-points
    * Look up connectors by name or type
    * Orchestrate connect / disconnect lifecycle
    * Aggregate health-check results
    """

    def __init__(self) -> None:
        self._connectors: dict[str, BaseConnector] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, connector: BaseConnector) -> None:
        """Register a connector instance."""
        self._connectors[connector.name] = connector
        logger.debug(f"Registered connector: {connector.name} ({connector.connector_type.value})")

    def deregister(self, name: str) -> None:
        """Remove a connector from the registry."""
        if name in self._connectors:
            del self._connectors[name]
            logger.debug(f"Deregistered connector: {name}")

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get(self, name: str) -> BaseConnector | None:
        """Get a connector by name."""
        return self._connectors.get(name)

    def get_by_type(self, connector_type: ConnectorType) -> list[BaseConnector]:
        """Get all connectors of a given type."""
        return [c for c in self._connectors.values() if c.connector_type == connector_type]

    def list_names(self) -> list[str]:
        """List all registered connector names."""
        return list(self._connectors.keys())

    def list_all(self) -> list[BaseConnector]:
        """List all registered connectors."""
        return list(self._connectors.values())

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect_all(self) -> dict[str, ConnectorStatus]:
        """Connect all registered connectors and return health map."""
        results: dict[str, ConnectorStatus] = {}
        for name, connector in self._connectors.items():
            try:
                await connector.connect()
                status = await connector.health_check()
                results[name] = status
                logger.info(f"Connector '{name}' → {status.value}")
            except Exception as e:
                results[name] = ConnectorStatus.UNREACHABLE
                logger.warning(f"Connector '{name}' failed to connect: {e}")
        return results

    async def disconnect_all(self) -> None:
        """Disconnect all registered connectors."""
        for name, connector in self._connectors.items():
            try:
                await connector.disconnect()
            except Exception as e:
                logger.warning(f"Connector '{name}' disconnect error: {e}")

    async def health_check_all(self) -> dict[str, ConnectorStatus]:
        """Run health checks on all connectors."""
        results: dict[str, ConnectorStatus] = {}
        for name, connector in self._connectors.items():
            try:
                results[name] = await connector.health_check()
            except Exception:
                results[name] = ConnectorStatus.UNREACHABLE
        return results

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute(self, connector_name: str, operation: str, **params: Any) -> ConnectorResult:
        """Execute an operation on a named connector."""
        connector = self._connectors.get(connector_name)
        if connector is None:
            return ConnectorResult(
                success=False,
                error=(
                    f"Unknown connector '{connector_name}'. "
                    f"Available: {', '.join(self._connectors.keys())}"
                ),
            )
        return await connector.execute(operation, **params)

    # ------------------------------------------------------------------
    # Discovery from config
    # ------------------------------------------------------------------

    def load_from_config(self, connectors_config: dict[str, dict[str, Any]]) -> int:
        """Instantiate and register connectors from a config dict.

        Expected format::

            {
              "my_warehouse": {"type": "sql", "driver": "trino", "host": ...},
              "airflow":      {"type": "rest_api", "base_url": ..., "auth": ...},
              "iceberg_cat":  {"type": "catalog", "driver": "iceberg_rest", ...},
            }

        Returns the number of connectors loaded.
        """
        from databot.connectors.factory import create_connector

        loaded = 0
        for name, cfg in connectors_config.items():
            try:
                connector = create_connector(name, cfg)
                self.register(connector)
                loaded += 1
            except Exception as e:
                logger.warning(f"Failed to create connector '{name}': {e}")
        if loaded:
            logger.info(f"Loaded {loaded} connector(s) from config")
        return loaded

    def load_plugins(self) -> int:
        """Discover connector classes from ``databot.connectors`` entry-points."""
        try:
            from importlib.metadata import entry_points
        except ImportError:
            from importlib_metadata import entry_points  # type: ignore[no-redef]

        eps = entry_points()
        group = "databot.connectors"

        # Python 3.9/3.10 compat
        if hasattr(eps, "select"):
            connector_eps = eps.select(group=group)
        else:
            connector_eps = eps.get(group, [])  # type: ignore[assignment]

        loaded = 0
        for ep in connector_eps:
            try:
                ep.load()
                loaded += 1
                logger.debug(f"Loaded connector plugin: {ep.name}")
            except Exception as e:
                logger.warning(f"Failed to load connector plugin '{ep.name}': {e}")
        return loaded

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._connectors)

    def __repr__(self) -> str:
        return f"<ConnectorRegistry connectors={list(self._connectors.keys())}>"
