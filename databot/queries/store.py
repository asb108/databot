"""SQLite-backed saved-query store with safe parameter rendering."""

from __future__ import annotations

import re
import sqlite3
import threading
from pathlib import Path
from typing import Any

# ``{name}`` style placeholders. Names are restricted to identifiers so the
# regex can't match SQL braces or JSON.
_PLACEHOLDER = re.compile(r"\{([A-Za-z_]\w*)\}")


def _render_value(value: Any) -> str:
    """Render a parameter value as an escaped SQL literal.

    Numbers/bools become bare literals; strings become single-quoted with
    embedded quotes doubled. Backslashes and null bytes are rejected: on MySQL
    (and ClickHouse/Hive) a backslash escapes the following character inside a
    string literal, so ``\\'`` would smuggle a closing quote past quote-doubling.
    Forbidding backslashes keeps the escaping safe across every supported engine
    without per-driver branching. (For arbitrary text, save it in the query
    itself rather than passing it as a parameter.)
    """
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if "\\" in text or "\x00" in text:
        raise ValueError(
            "string parameters may not contain backslashes or null bytes "
            "(rejected to keep value substitution injection-safe across SQL engines)"
        )
    return "'" + text.replace("'", "''") + "'"


def render_query(sql: str, params: dict[str, Any] | None) -> str:
    """Substitute ``{param}`` placeholders with safe literals.

    Raises ValueError listing any placeholders without a provided value.
    """
    params = params or {}
    missing: list[str] = []

    def _sub(match: re.Match) -> str:
        key = match.group(1)
        if key not in params:
            missing.append(key)
            return match.group(0)
        return _render_value(params[key])

    rendered = _PLACEHOLDER.sub(_sub, sql)
    if missing:
        raise ValueError(f"Missing parameter(s): {', '.join(sorted(set(missing)))}")
    return rendered


def placeholders(sql: str) -> list[str]:
    """Return the distinct placeholder names used in a query."""
    return sorted(set(_PLACEHOLDER.findall(sql)))


class QueryStore:
    """Persistent storage for named, parameterized queries."""

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
            CREATE TABLE IF NOT EXISTS saved_queries (
                name TEXT PRIMARY KEY,
                sql TEXT NOT NULL,
                connection TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

    def save(self, name: str, sql: str, connection: str = "", description: str = "") -> None:
        conn = self._get_conn()
        conn.execute(
            """
            INSERT INTO saved_queries (name, sql, connection, description)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                sql = excluded.sql,
                connection = excluded.connection,
                description = excluded.description
            """,
            (name, sql, connection, description),
        )
        conn.commit()

    def get(self, name: str) -> dict[str, Any] | None:
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM saved_queries WHERE name = ?", (name,)).fetchone()
        conn.row_factory = None
        return dict(row) if row else None

    def list_all(self) -> list[dict[str, Any]]:
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM saved_queries ORDER BY name").fetchall()
        conn.row_factory = None
        return [dict(r) for r in rows]

    def delete(self, name: str) -> bool:
        conn = self._get_conn()
        cursor = conn.execute("DELETE FROM saved_queries WHERE name = ?", (name,))
        conn.commit()
        return cursor.rowcount > 0
