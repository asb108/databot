"""Tests for session and memory managers."""

from __future__ import annotations

import pytest

from databot.memory.manager import MemoryManager
from databot.session.manager import Session, SessionManager


class TestMemoryManager:
    @pytest.fixture
    def memory(self, tmp_path):
        return MemoryManager(tmp_path / "memory.db")

    def test_set_and_get(self, memory):
        memory.set("key1", "value1")
        assert memory.get("key1") == "value1"

    def test_get_nonexistent(self, memory):
        assert memory.get("nonexistent") is None

    def test_update(self, memory):
        memory.set("key1", "value1")
        memory.set("key1", "updated")
        assert memory.get("key1") == "updated"

    def test_delete(self, memory):
        memory.set("key1", "value1")
        memory.delete("key1")
        assert memory.get("key1") is None

    def test_get_all(self, memory):
        memory.set("a", "1")
        memory.set("b", "2")
        all_mem = memory.get_all()
        assert "a" in all_mem
        assert "b" in all_mem

    def test_clear(self, memory):
        memory.set("a", "1")
        memory.set("b", "2")
        memory.clear()
        assert memory.get_all() == {}


class TestSession:
    def test_add_message(self):
        session = Session("test")
        session.add_message("user", "hello")
        assert len(session.get_history()) == 1
        assert session.get_history()[0]["role"] == "user"

    def test_max_messages(self):
        session = Session("test", max_messages=5)
        for i in range(10):
            session.add_message("user", f"msg {i}")
        assert len(session.get_history()) == 5
        assert session.get_history()[0]["content"] == "msg 5"

    def test_clear(self):
        session = Session("test")
        session.add_message("user", "hello")
        session.clear()
        assert session.get_history() == []


class TestSessionManager:
    @pytest.fixture
    def manager(self, tmp_path):
        return SessionManager(tmp_path)

    def test_get_or_create(self, manager):
        session = manager.get_or_create("test:1")
        assert session.key == "test:1"
        assert session.get_history() == []

    def test_save_and_load(self, manager):
        session = manager.get_or_create("test:1")
        session.add_message("user", "hello")
        manager.save(session)

        # Clear cache and reload
        manager._cache.clear()
        loaded = manager.get_or_create("test:1")
        assert len(loaded.get_history()) == 1
        assert loaded.get_history()[0]["content"] == "hello"

    def test_delete(self, manager):
        session = manager.get_or_create("test:1")
        session.add_message("user", "hello")
        manager.save(session)
        manager.delete("test:1")
        assert "test:1" not in manager._cache

    def test_list_sessions(self, manager):
        s1 = manager.get_or_create("test:1")
        s1.add_message("user", "hello")
        manager.save(s1)

        s2 = manager.get_or_create("test:2")
        s2.add_message("user", "world")
        manager.save(s2)

        keys = manager.list_sessions()
        assert "test:1" in keys
        assert "test:2" in keys

    def test_lru_eviction(self, tmp_path):
        """Session cache evicts LRU entries when max_cached_sessions is exceeded."""
        manager = SessionManager(tmp_path, max_cached_sessions=3)

        # Create 4 sessions — only 3 should remain in cache
        for i in range(4):
            s = manager.get_or_create(f"s{i}")
            s.add_message("user", f"msg {i}")
            manager.save(s)

        assert manager.cache_size == 3
        # s0 should have been evicted (least recently used)
        assert "s0" not in manager._cache
        # s1, s2, s3 should still be cached
        assert "s1" in manager._cache
        assert "s3" in manager._cache

    def test_evicted_session_persisted(self, tmp_path):
        """Evicted sessions are persisted and can be reloaded."""
        manager = SessionManager(tmp_path, max_cached_sessions=2)

        s = manager.get_or_create("old")
        s.add_message("user", "remember me")
        manager.save(s)

        # Create 2 more to push "old" out of cache
        manager.get_or_create("new1")
        manager.get_or_create("new2")

        assert "old" not in manager._cache

        # Reloading should restore from DB
        reloaded = manager.get_or_create("old")
        assert reloaded.get_history()[0]["content"] == "remember me"

    def test_configurable_max_session_messages(self, tmp_path):
        """max_session_messages is passed through to new sessions."""
        manager = SessionManager(tmp_path, max_session_messages=5)
        s = manager.get_or_create("test")
        for i in range(10):
            s.add_message("user", f"msg {i}")
        assert len(s.get_history()) == 5
        assert s.get_history()[0]["content"] == "msg 5"
