"""Base connector abstraction for external data systems."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ConnectorType(str, Enum):
    """Categories of connectors."""

    SQL = "sql"
    REST_API = "rest_api"
    CATALOG = "catalog"
    STREAMING = "streaming"
    PROCESSING = "processing"


class ConnectorStatus(str, Enum):
    """Health status of a connector."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNREACHABLE = "unreachable"
    NOT_CONFIGURED = "not_configured"


@dataclass
class ConnectorResult:
    """Generic result from a connector operation.

    Handles tabular data, scalar values, metadata, and errors uniformly.
    """

    success: bool = True
    data: Any = None
    columns: list[str] | None = None
    rows: list[list[Any]] | None = None
    row_count: int = 0
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_markdown_table(self, max_rows: int = 100) -> str:
        """Format tabular results as a markdown table."""
        if not self.columns or not self.rows:
            if self.data is not None:
                return str(self.data)
            return "No data returned."

        lines = [
            "| " + " | ".join(str(c) for c in self.columns) + " |",
            "| " + " | ".join("---" for _ in self.columns) + " |",
        ]
        display_rows = self.rows[:max_rows]
        for row in display_rows:
            cells = [str(v) if v is not None else "NULL" for v in row]
            lines.append("| " + " | ".join(cells) + " |")

        total = len(self.rows)
        if total > max_rows:
            lines.append(f"\n*Showing {max_rows} of {total} rows.*")
        elif self.row_count > total:
            lines.append(f"\n*Showing {total} of {self.row_count} total rows.*")

        return "\n".join(lines)


class BaseConnector(ABC):
    """Abstract base class for all connectors.

    A connector encapsulates communication with a single external system
    (database, REST API, catalog, streaming platform, etc.).  It exposes
    a uniform interface for health-checking, capability discovery, and
    generic operation dispatch so that higher-level tools can compose
    connectors without knowing the underlying protocol.
    """

    def __init__(self, name: str, config: dict[str, Any] | None = None):
        self._name = name
        self._config = config or {}
        self._connected = False

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """Unique identifier for this connector instance."""
        return self._name

    @property
    @abstractmethod
    def connector_type(self) -> ConnectorType:
        """Category of this connector (sql, rest_api, catalog, …)."""
        ...

    @property
    def config(self) -> dict[str, Any]:
        """Raw configuration dict."""
        return self._config

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Establish connection / validate configuration.

        Subclasses should override to perform actual connection setup
        (e.g. creating connection pools, verifying credentials).
        """
        self._connected = True

    async def disconnect(self) -> None:
        """Release resources held by this connector."""
        self._connected = False

    # ------------------------------------------------------------------
    # Capabilities
    # ------------------------------------------------------------------

    @abstractmethod
    def capabilities(self) -> list[str]:
        """Declare the operations this connector supports.

        Examples: ``["query", "list_tables", "get_schema"]``
        """
        ...

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    async def health_check(self) -> ConnectorStatus:
        """Check whether the external system is reachable.

        Default implementation returns HEALTHY if ``connect()`` has been
        called and NOT_CONFIGURED otherwise.  Subclasses should override
        with a real probe (e.g. ``SELECT 1``).
        """
        if not self._config:
            return ConnectorStatus.NOT_CONFIGURED
        return ConnectorStatus.HEALTHY if self._connected else ConnectorStatus.UNREACHABLE

    # ------------------------------------------------------------------
    # Generic dispatch
    # ------------------------------------------------------------------

    async def execute(self, operation: str, **params: Any) -> ConnectorResult:
        """Dispatch a named operation.

        Subclasses implement specific operations and can use the default
        pattern of mapping *operation* to ``_op_{operation}`` methods.
        """
        method_name = f"_op_{operation}"
        method = getattr(self, method_name, None)
        if method is None:
            return ConnectorResult(
                success=False,
                error=(
                    f"Connector '{self._name}' does not support operation '{operation}'. "
                    f"Supported: {', '.join(self.capabilities())}"
                ),
            )
        return await method(**params)

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self._name!r} type={self.connector_type.value}>"
