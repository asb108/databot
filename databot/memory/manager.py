"""Persistent memory manager using SQLite with WAL mode."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path


class MemoryManager:
    """Persistent key-value memory stored in SQLite.

    Uses WAL journal mode and a thread-local connection for better
    concurrent-read performance and reduced connection overhead.
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
            CREATE TABLE IF NOT EXISTS memory (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

    def get(self, key: str) -> str | None:
        conn = self._get_conn()
        row = conn.execute("SELECT value FROM memory WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set(self, key: str, value: str) -> None:
        conn = self._get_conn()
        conn.execute(
            """
            INSERT INTO memory (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, value),
        )
        conn.commit()

    def delete(self, key: str) -> None:
        conn = self._get_conn()
        conn.execute("DELETE FROM memory WHERE key = ?", (key,))
        conn.commit()

    def get_all(self) -> dict[str, str]:
        conn = self._get_conn()
        rows = conn.execute("SELECT key, value FROM memory ORDER BY updated_at DESC").fetchall()
        return {row[0]: row[1] for row in rows}

    def clear(self) -> None:
        conn = self._get_conn()
        conn.execute("DELETE FROM memory")
        conn.commit()
