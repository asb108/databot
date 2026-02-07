"""Tests for the context builder."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from databot.core.context import ContextBuilder


class TestContextBuilder:
    @pytest.fixture
    def builder(self, tmp_path):
        memory = MagicMock()
        memory.get_all.return_value = {"team": "data-platform", "env": "production"}
        return ContextBuilder(workspace=tmp_path, memory=memory)

    @pytest.fixture
    def builder_no_memory(self, tmp_path):
        return ContextBuilder(workspace=tmp_path)

    def test_build_basic_messages(self, builder):
        messages = builder.build_messages(
            history=[],
            current_message="Hello",
        )
        assert len(messages) == 2  # system + user
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"
        assert messages[1]["content"] == "Hello"

    def test_system_prompt_includes_memory(self, builder):
        messages = builder.build_messages(history=[], current_message="test")
        system = messages[0]["content"]
        assert "Persistent Memory" in system
        assert "team: data-platform" in system
        assert "env: production" in system

    def test_system_prompt_includes_workspace(self, builder, tmp_path):
        messages = builder.build_messages(history=[], current_message="test")
        system = messages[0]["content"]
        assert str(tmp_path) in system

    def test_history_preserved(self, builder):
        history = [
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "response"},
        ]
        messages = builder.build_messages(history=history, current_message="second")
        assert len(messages) == 4  # system + 2 history + current
        assert messages[1]["content"] == "first"
        assert messages[2]["content"] == "response"

    def test_media_handling(self, builder):
        media = [{"type": "image_url", "image_url": {"url": "https://example.com/img.png"}}]
        messages = builder.build_messages(
            history=[], current_message="look at this", media=media
        )
        user_msg = messages[1]
        # Should be multipart
        assert isinstance(user_msg["content"], list)
        assert user_msg["content"][0]["type"] == "text"
        assert user_msg["content"][1]["type"] == "image_url"

    def test_no_memory(self, builder_no_memory):
        messages = builder_no_memory.build_messages(history=[], current_message="test")
        system = messages[0]["content"]
        assert "Persistent Memory" not in system

    def test_custom_system_prompt(self, tmp_path):
        builder = ContextBuilder(
            workspace=tmp_path,
            system_prompt="You are a custom bot.",
        )
        messages = builder.build_messages(history=[], current_message="test")
        assert "custom bot" in messages[0]["content"]

    def test_add_assistant_message(self):
        messages = [{"role": "user", "content": "hi"}]
        result = ContextBuilder.add_assistant_message(messages, "hello")
        assert len(result) == 2
        assert result[1]["role"] == "assistant"
        assert result[1]["content"] == "hello"

    def test_add_assistant_with_tool_calls(self):
        messages = []
        tool_calls = [{"id": "tc1", "type": "function", "function": {"name": "test"}}]
        result = ContextBuilder.add_assistant_message(messages, None, tool_calls)
        assert result[0]["tool_calls"] == tool_calls

    def test_add_tool_result(self):
        messages = []
        result = ContextBuilder.add_tool_result(messages, "tc1", "sql", "result text")
        assert len(result) == 1
        assert result[0]["role"] == "tool"
        assert result[0]["tool_call_id"] == "tc1"
        assert result[0]["content"] == "result text"
