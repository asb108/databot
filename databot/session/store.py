"""SQLite-backed session store."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class SessionStore:
    """Persistent session storage using SQLite."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT history FROM sessions WHERE key = ?", (key,)
            ).fetchone()
            if row:
                return json.loads(row[0])
            return []

    def save_history(self, key: str, history: list[dict[str, Any]]) -> None:
        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM sessions WHERE key = ?", (key,))
            conn.commit()

    def list_keys(self) -> list[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT key FROM sessions ORDER BY updated_at DESC"
            ).fetchall()
            return [row[0] for row in rows]
