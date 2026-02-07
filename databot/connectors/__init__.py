"""Connector framework for external data systems."""

from __future__ import annotations

from databot.connectors.base import (
    BaseConnector,
    ConnectorResult,
    ConnectorStatus,
)
from databot.connectors.registry import ConnectorRegistry

__all__ = [
    "BaseConnector",
    "ConnectorRegistry",
    "ConnectorResult",
    "ConnectorStatus",
]
