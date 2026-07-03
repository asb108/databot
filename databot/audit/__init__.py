"""Append-only audit log of tool executions.

Every tool call the agent makes — especially SQL queries and shell commands —
is recorded to a tamper-evident, append-only SQLite log: what tool, the
arguments (secrets redacted, large values truncated), the outcome, and when.
This gives data teams the governance trail they need to answer "who ran what,
when, and did it succeed" without trawling application logs.

Like the metrics facade, this exposes a process-global sink configured by the
gateway, so ``ToolRegistry.execute`` can record without threading an instance
through every call site. When unconfigured it is a no-op.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from loguru import logger

# Argument keys whose values are redacted regardless of nesting.
_SECRET_KEYS = {
    "password",
    "token",
    "api_key",
    "apikey",
    "secret",
    "access_token",
    "bot_token",
    "authorization",
}
_MAX_ARG_CHARS = 2000


def _redact(args: dict[str, Any]) -> dict[str, Any]:
    """Redact secret-looking keys and bound the serialized size."""
    redacted: dict[str, Any] = {}
    for key, value in (args or {}).items():
        if key.lower() in _SECRET_KEYS:
            redacted[key] = "***redacted***"
        else:
            redacted[key] = value
    return redacted


class AuditLog:
    """Append-only SQLite audit log of tool executions."""

    def __init__(self, db_path: Path | None):
        self._enabled = db_path is not None
        self._db_path = db_path
        self._local = threading.local()
        if self._enabled:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._init_db()

    @property
    def enabled(self) -> bool:
        return self._enabled

    def _get_conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tool TEXT NOT NULL,
                arguments TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL,
                session_key TEXT NOT NULL DEFAULT '',
                duration_ms INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.commit()

    def record(
        self,
        tool: str,
        arguments: dict[str, Any] | None,
        status: str,
        session_key: str = "",
        duration_ms: int = 0,
    ) -> None:
        """Append one tool-execution record. Never raises into the caller."""
        if not self._enabled:
            return
        try:
            args_json = json.dumps(_redact(arguments or {}), default=str)[:_MAX_ARG_CHARS]
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO audit_log (tool, arguments, status, session_key, duration_ms) "
                "VALUES (?, ?, ?, ?, ?)",
                (tool, args_json, status, session_key, int(duration_ms)),
            )
            conn.commit()
        except Exception as e:  # auditing must never break tool execution
            logger.warning(f"Audit log write failed: {e}")

    def recent(self, limit: int = 100, tool: str = "") -> list[dict[str, Any]]:
        if not self._enabled:
            return []
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        if tool:
            rows = conn.execute(
                "SELECT * FROM audit_log WHERE tool = ? ORDER BY id DESC LIMIT ?",
                (tool, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        conn.row_factory = None
        return [dict(r) for r in rows]


# Process-global sink (disabled no-op until configured).
_global_audit: AuditLog | None = None


def get_audit_log() -> AuditLog:
    global _global_audit
    if _global_audit is None:
        _global_audit = AuditLog(None)
    return _global_audit


def configure_audit_log(db_path: Path | None) -> AuditLog:
    """(Re)configure and return the global audit log."""
    global _global_audit
    _global_audit = AuditLog(db_path)
    return _global_audit
