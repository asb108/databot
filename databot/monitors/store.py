"""SQLite-backed monitor store with WAL mode and run history."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any


class MonitorStore:
    """Persistent storage for data monitors and their run history."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
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
            CREATE TABLE IF NOT EXISTS monitors (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                connection TEXT NOT NULL,
                check_type TEXT NOT NULL,
                table_name TEXT NOT NULL DEFAULT '',
                column_name TEXT NOT NULL DEFAULT '',
                query TEXT NOT NULL DEFAULT '',
                operator TEXT NOT NULL,
                threshold REAL NOT NULL,
                schedule TEXT NOT NULL,
                channel TEXT NOT NULL DEFAULT 'gchat',
                chat_id TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                last_run TIMESTAMP,
                last_value REAL,
                last_status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS monitor_runs (
                monitor_id TEXT NOT NULL,
                value REAL,
                status TEXT NOT NULL,
                breached INTEGER NOT NULL DEFAULT 0,
                ran_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

    def add(self, monitor_id: str, **fields: Any) -> None:
        conn = self._get_conn()
        conn.execute(
            """
            INSERT OR REPLACE INTO monitors
            (id, name, connection, check_type, table_name, column_name, query,
             operator, threshold, schedule, channel, chat_id, enabled)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                monitor_id,
                fields.get("name", ""),
                fields.get("connection", ""),
                fields.get("check_type", ""),
                fields.get("table_name", ""),
                fields.get("column_name", ""),
                fields.get("query", ""),
                fields.get("operator", ""),
                float(fields.get("threshold", 0.0)),
                fields.get("schedule", ""),
                fields.get("channel", "gchat"),
                fields.get("chat_id", ""),
            ),
        )
        conn.commit()

    def remove(self, monitor_id: str) -> bool:
        conn = self._get_conn()
        cursor = conn.execute("DELETE FROM monitors WHERE id = ?", (monitor_id,))
        conn.execute("DELETE FROM monitor_runs WHERE monitor_id = ?", (monitor_id,))
        conn.commit()
        return cursor.rowcount > 0

    def get(self, monitor_id: str) -> dict[str, Any] | None:
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM monitors WHERE id = ?", (monitor_id,)).fetchone()
        conn.row_factory = None
        return dict(row) if row else None

    def list_all(self) -> list[dict[str, Any]]:
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM monitors ORDER BY name").fetchall()
        conn.row_factory = None
        return [dict(r) for r in rows]

    def get_enabled(self) -> list[dict[str, Any]]:
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM monitors WHERE enabled = 1 ORDER BY name").fetchall()
        conn.row_factory = None
        return [dict(r) for r in rows]

    def record_run(self, monitor_id: str, value: float | None, status: str, breached: bool) -> None:
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO monitor_runs (monitor_id, value, status, breached) VALUES (?, ?, ?, ?)",
            (monitor_id, value, status, 1 if breached else 0),
        )
        conn.execute(
            "UPDATE monitors SET last_run = CURRENT_TIMESTAMP, last_value = ?, "
            "last_status = ? WHERE id = ?",
            (value, status, monitor_id),
        )
        conn.commit()

    def history(self, monitor_id: str, limit: int = 20) -> list[dict[str, Any]]:
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM monitor_runs WHERE monitor_id = ? ORDER BY ran_at DESC LIMIT ?",
            (monitor_id, limit),
        ).fetchall()
        conn.row_factory = None
        return [dict(r) for r in rows]
