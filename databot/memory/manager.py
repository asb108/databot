"""Persistent memory manager using SQLite."""

from __future__ import annotations

import sqlite3
from pathlib import Path


class MemoryManager:
    """Persistent key-value memory stored in SQLite."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT value FROM memory WHERE key = ?", (key,)).fetchone()
            return row[0] if row else None

    def set(self, key: str, value: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM memory WHERE key = ?", (key,))
            conn.commit()

    def get_all(self) -> dict[str, str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT key, value FROM memory ORDER BY updated_at DESC").fetchall()
            return {row[0]: row[1] for row in rows}

    def clear(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM memory")
            conn.commit()
