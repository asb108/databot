"""Session manager for conversation history."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from databot.session.store import SessionStore


class Session:
    """A conversation session with message history."""

    def __init__(self, key: str, history: list[dict[str, Any]] | None = None):
        self.key = key
        self._history: list[dict[str, Any]] = history or []

    def add_message(self, role: str, content: str) -> None:
        self._history.append({"role": role, "content": content})
        # Keep history bounded to avoid context overflow
        max_messages = 50
        if len(self._history) > max_messages:
            self._history = self._history[-max_messages:]

    def get_history(self) -> list[dict[str, Any]]:
        return list(self._history)

    def clear(self) -> None:
        self._history = []


class SessionManager:
    """Manages conversation sessions with SQLite persistence."""

    def __init__(self, data_dir: Path):
        self.store = SessionStore(data_dir / "sessions.db")
        self._cache: dict[str, Session] = {}

    def get_or_create(self, key: str) -> Session:
        if key in self._cache:
            return self._cache[key]

        history = self.store.get_history(key)
        session = Session(key, history)
        self._cache[key] = session
        return session

    def save(self, session: Session) -> None:
        self.store.save_history(session.key, session.get_history())

    def delete(self, key: str) -> None:
        self.store.delete(key)
        self._cache.pop(key, None)

    def list_sessions(self) -> list[str]:
        return self.store.list_keys()
