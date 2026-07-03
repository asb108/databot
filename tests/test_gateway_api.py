"""HTTP-level tests for the gateway API, driven through the build_api factory."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from databot.audit import configure_audit_log
from databot.cli.api import build_api
from databot.config.schema import DatabotConfig
from databot.core.bus import MessageBus
from databot.core.cost import UsageTracker
from databot.monitors.service import MonitorService
from databot.observability.metrics import configure_metrics
from databot.session.manager import SessionManager
from databot.tools.base import ToolRegistry


class _FakeRegistry:
    """Minimal connector registry stub with controllable health."""

    def __init__(self, health: dict | None = None):
        self._health = health or {}

    async def health_check_all(self):
        return self._health

    def list_all(self):
        return []


def _build(tmp_path: Path, *, connector_registry=None, rate_limit_rpm=0):
    """Construct a minimal-but-real gateway app for testing."""
    cfg = DatabotConfig()
    cfg.gateway.api_keys = []              # open mode -> auth middleware no-ops
    cfg.gateway.rate_limit_rpm = rate_limit_rpm  # 0 -> rate limiter disabled
    cfg.ui.enabled = False                 # skip the /ui mount

    bus = MessageBus()
    tools = ToolRegistry()
    sessions = SessionManager(tmp_path)
    usage_tracker = UsageTracker()
    monitor_service = MonitorService(tmp_path, bus)
    metrics = configure_metrics(enabled=False)
    audit_log = configure_audit_log(tmp_path / "audit.db")

    skill_registry = MagicMock()
    skill_registry.enabled_skills.return_value = []
    skill_registry.summary.return_value = []

    return build_api(
        cfg=cfg,
        tools=tools,
        sessions=sessions,
        connector_registry=connector_registry,
        rag_context=None,
        delegator=None,
        skill_registry=skill_registry,
        usage_tracker=usage_tracker,
        monitor_service=monitor_service,
        loop=MagicMock(),
        metrics=metrics,
        audit_log=audit_log,
        channels=None,
    )
    # note: `bus` is built above for MonitorService but is NOT a build_api param
    # (no moved route references it).


@pytest.fixture
def client(tmp_path):
    return TestClient(_build(tmp_path))


def test_livez(client):
    r = client.get("/livez")
    assert r.status_code == 200
    assert r.json() == {"status": "alive"}


def test_readyz_healthy_when_no_connectors(client):
    r = client.get("/readyz")
    assert r.status_code == 200
    assert r.json()["status"] == "ready"


def test_readyz_503_when_connector_unreachable(tmp_path):
    from databot.connectors.base import ConnectorStatus

    reg = _FakeRegistry(health={"warehouse": ConnectorStatus.UNREACHABLE})
    client = TestClient(_build(tmp_path, connector_registry=reg))
    r = client.get("/readyz")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "not_ready"
    assert body["connectors"]["warehouse"] == "unreachable"


def test_metrics_returns_exposition_payload(client):
    r = client.get("/metrics")
    assert r.status_code == 200
    # metrics configured disabled -> graceful placeholder, no prometheus_client needed
    assert b"databot metrics disabled" in r.content


def test_usage(client):
    r = client.get("/api/v1/usage")
    assert r.status_code == 200
    body = r.json()
    assert "global" in body
    assert "sessions" in body
    assert "session_budget_usd" in body


def test_audit(client):
    r = client.get("/api/v1/audit")
    assert r.status_code == 200
    body = r.json()
    assert "enabled" in body
    assert "entries" in body
    assert isinstance(body["entries"], list)


def test_monitors_empty(client):
    r = client.get("/api/v1/monitors")
    assert r.status_code == 200
    assert r.json() == {"monitors": []}


def test_queries_empty_without_saved_query_tool(client):
    r = client.get("/api/v1/queries")
    assert r.status_code == 200
    assert r.json() == {"queries": []}
