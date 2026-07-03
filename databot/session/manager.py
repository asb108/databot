"""Session manager for conversation history."""

from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
from typing import Any

from loguru import logger

from databot.session.store import SessionStore


class Session:
    """A conversation session with message history."""

    def __init__(
        self,
        key: str,
        history: list[dict[str, Any]] | None = None,
        max_messages: int = 50,
    ):
        self.key = key
        raw = list(history or [])
        # A leading system entry is the persisted running compaction summary;
        # carry it separately so it survives history trimming and never breaks
        # user/assistant alternation.
        self.summary: str = ""
        if raw and raw[0].get("role") == "system":
            self.summary = raw[0].get("content", "")
            raw = raw[1:]
        self._history: list[dict[str, Any]] = raw
        self._max_messages = max_messages

    def add_message(self, role: str, content: str) -> None:
        self._history.append({"role": role, "content": content})
        # Keep history bounded to avoid context overflow
        if len(self._history) > self._max_messages:
            self._history = self._history[-self._max_messages :]

    def get_history(self) -> list[dict[str, Any]]:
        return list(self._history)

    def set_summary(self, summary: str) -> None:
        self.summary = summary or ""

    def replace_history(self, history: list[dict[str, Any]]) -> None:
        """Replace the user/assistant history (used after compaction)."""
        self._history = list(history)

    def to_storage(self) -> list[dict[str, Any]]:
        """History as persisted: summary (if any) as a leading system entry."""
        if self.summary:
            return [{"role": "system", "content": self.summary}, *self._history]
        return list(self._history)

    def clear(self) -> None:
        self._history = []
        self.summary = ""


class SessionManager:
    """Manages conversation sessions with SQLite persistence and LRU cache eviction.

    The in-memory cache is bounded by ``max_cached_sessions``.  When the limit
    is reached the least-recently-used session is evicted (and persisted).
    """

    DEFAULT_MAX_CACHED = 256

    def __init__(
        self,
        data_dir: Path,
        max_session_messages: int = 50,
        max_cached_sessions: int = DEFAULT_MAX_CACHED,
    ):
        self.store = SessionStore(data_dir / "sessions.db")
        self._max_session_messages = max_session_messages
        self._max_cached = max(1, max_cached_sessions)
        # OrderedDict gives us O(1) LRU eviction
        self._cache: OrderedDict[str, Session] = OrderedDict()

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _touch(self, key: str) -> None:
        """Move *key* to end of LRU order (most-recently used)."""
        self._cache.move_to_end(key)

    def _evict_if_needed(self) -> None:
        """Evict the least-recently-used session if cache is full."""
        while len(self._cache) > self._max_cached:
            evicted_key, evicted_session = self._cache.popitem(last=False)
            # Persist before evicting so no data is lost
            self.store.save_history(evicted_key, evicted_session.to_storage())
            logger.debug(f"Evicted session '{evicted_key}' from cache ({len(self._cache)} remain)")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_or_create(self, key: str) -> Session:
        if key in self._cache:
            self._touch(key)
            return self._cache[key]

        history = self.store.get_history(key)
        session = Session(key, history, max_messages=self._max_session_messages)
        self._cache[key] = session
        self._evict_if_needed()
        return session

    def save(self, session: Session) -> None:
        self.store.save_history(session.key, session.to_storage())

    def delete(self, key: str) -> None:
        self.store.delete(key)
        self._cache.pop(key, None)

    def list_sessions(self) -> list[str]:
        return self.store.list_keys()

    @property
    def cache_size(self) -> int:
        """Current number of sessions held in memory."""
        return len(self._cache)
