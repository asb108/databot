"""CLI single-shot error UX: a forced failure prints the friendly line, no traceback."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

from databot.cli.commands import _process_single
from databot.config.schema import DatabotConfig


class TestProcessSingleErrorUX:
    def test_forced_failure_renders_friendly_message(self):
        cfg = DatabotConfig()
        with patch(
            "databot.core.loop.AgentLoop.process_direct",
            side_effect=ValueError("boom with secret-token-123"),
        ):
            result = asyncio.run(_process_single(cfg, "hello"))

        assert "Traceback" not in result
        assert "secret-token-123" not in result
        assert "internal error" in result.lower()
