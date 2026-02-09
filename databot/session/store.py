"""SQLite-backed session store with WAL mode and connection reuse."""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


class SessionStore:
    """Persistent session storage using SQLite.

    Uses WAL journal mode for better concurrent read performance and
    keeps a thread-local connection to avoid reconnecting on every call.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Return a thread-local connection (created on first access)."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                key TEXT PRIMARY KEY,
                history TEXT NOT NULL DEFAULT '[]',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

    def get_history(self, key: str) -> list[dict[str, Any]]:
        conn = self._get_conn()
        row = conn.execute("SELECT history FROM sessions WHERE key = ?", (key,)).fetchone()
        if row:
            return json.loads(row[0])
        return []

    def save_history(self, key: str, history: list[dict[str, Any]]) -> None:
        conn = self._get_conn()
        conn.execute(
            """
            INSERT INTO sessions (key, history, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET
                history = excluded.history,
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, json.dumps(history)),
        )
        conn.commit()

    def delete(self, key: str) -> None:
        conn = self._get_conn()
        conn.execute("DELETE FROM sessions WHERE key = ?", (key,))
        conn.commit()

    def list_keys(self) -> list[str]:
        conn = self._get_conn()
        rows = conn.execute("SELECT key FROM sessions ORDER BY updated_at DESC").fetchall()
        return [row[0] for row in rows]

    def get_metadata(self, key: str) -> dict[str, Any]:
        """Get session metadata (key, timestamps, message count)."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT key, history, created_at, updated_at FROM sessions WHERE key = ?",
            (key,),
        ).fetchone()
        if not row:
            return {"key": key, "created_at": "", "updated_at": "", "message_count": 0}
        try:
            messages = json.loads(row[1])
            count = len(messages)
        except Exception:
            count = 0
        return {
            "key": row[0],
            "created_at": row[2] or "",
            "updated_at": row[3] or "",
            "message_count": count,
        }
