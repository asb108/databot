"""Embedded lightweight UI for databot.

Serves a single-page dashboard directly from the gateway —
no npm, no node_modules, no separate build step.
"""

from __future__ import annotations

from pathlib import Path

STATIC_DIR = Path(__file__).parent / "static"
