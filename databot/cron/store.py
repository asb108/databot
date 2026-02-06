"""SQLite-backed cron job store."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


class CronStore:
    """Persistent storage for cron jobs."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cron_jobs (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    schedule TEXT NOT NULL,
                    message TEXT NOT NULL,
                    channel TEXT NOT NULL DEFAULT 'gchat',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    last_run TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def add(
        self, job_id: str, name: str, schedule: str, message: str, channel: str = "gchat"
    ) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO cron_jobs (id, name, schedule, message, channel, enabled)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (job_id, name, schedule, message, channel),
            )
            conn.commit()

    def remove(self, job_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM cron_jobs WHERE id = ?", (job_id,))
            conn.commit()
            return cursor.rowcount > 0

    def list_all(self) -> list[dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM cron_jobs ORDER BY name").fetchall()
            return [dict(row) for row in rows]

    def get_enabled(self) -> list[dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM cron_jobs WHERE enabled = 1 ORDER BY name"
            ).fetchall()
            return [dict(row) for row in rows]

    def update_last_run(self, job_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE cron_jobs SET last_run = CURRENT_TIMESTAMP WHERE id = ?",
                (job_id,),
            )
            conn.commit()
