"""Slash-command error rendering."""

from __future__ import annotations

import pytest

from databot.core.slash import SlashCommandHandler


class _BoomConnectors:
    async def health_check_all(self):
        raise RuntimeError("psycopg2 OperationalError: connection refused (password=secret123)")


@pytest.mark.asyncio
async def test_status_report_hides_raw_error():
    handler = SlashCommandHandler(connector_registry=_BoomConnectors())
    out = await handler._status_report()
    assert "secret123" not in out
    assert "reaching a service" in out.lower()  # network class
