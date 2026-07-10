"""Tests for the shared user-facing error renderer."""

from __future__ import annotations

import asyncio

import pytest

from databot.core.error_ux import render_user_error
from databot.tools.errors import ErrorCode, ToolError

RETRY_SUFFIX = "You can try again in a moment."


class TestClassifier:
    def test_auth_by_type(self):
        out = render_user_error(PermissionError("nope"))
        assert "authentication failed" in out.lower()
        assert "administrator" in out.lower()
        assert RETRY_SUFFIX not in out

    def test_auth_by_message(self):
        out = render_user_error(RuntimeError("Invalid API Key provided"))
        assert "authentication failed" in out.lower()

    def test_rate_limit(self):
        out = render_user_error(RuntimeError("429 Too Many Requests"))
        assert "busy" in out.lower()
        assert RETRY_SUFFIX in out

    def test_timeout_by_type(self):
        out = render_user_error(asyncio.TimeoutError())
        assert "too long" in out.lower()
        assert RETRY_SUFFIX in out

    def test_network(self):
        out = render_user_error(ConnectionError("connection refused"))
        assert "reaching a service" in out.lower()
        assert RETRY_SUFFIX in out

    def test_generic_default(self):
        out = render_user_error(ValueError("something weird"))
        assert "internal error" in out.lower()
        assert RETRY_SUFFIX not in out

    def test_order_rate_limit_beats_network(self):
        # "429 ... connection reset" contains a network substring but must classify as rate-limit
        out = render_user_error(RuntimeError("HTTP 429 on connection to api"))
        assert "busy" in out.lower()

    def test_order_auth_beats_network(self):
        # auth message that also contains "connection" must classify as auth
        out = render_user_error(RuntimeError("AuthenticationError: connection to api failed"))
        assert "authentication failed" in out.lower()


class TestToolErrorRendering:
    def test_plain(self):
        out = render_user_error(ToolError(ErrorCode.NOT_FOUND, "Table 'x' was not found."))
        assert "Table 'x' was not found." in out
        assert "Error [" not in out  # never the LLM-facing prefix
        assert RETRY_SUFFIX not in out

    def test_with_hint(self):
        out = render_user_error(
            ToolError(ErrorCode.NOT_FOUND, "Unknown connection 'db'.", hint="Available: main")
        )
        assert "Unknown connection 'db'." in out
        assert "Available: main" in out

    def test_retryable_appends_nudge(self):
        out = render_user_error(
            ToolError(ErrorCode.CONNECTION_ERROR, "Database unavailable.", retryable=True)
        )
        assert RETRY_SUFFIX in out

    def test_not_retryable_no_nudge(self):
        out = render_user_error(ToolError(ErrorCode.INVALID_ARGUMENT, "Bad column name."))
        assert RETRY_SUFFIX not in out


class TestSecretSafety:
    def test_generic_path_never_leaks_str(self):
        secret = "sk-super-secret-token-98765"
        out = render_user_error(RuntimeError(f"boom with {secret}"))
        assert secret not in out

    def test_no_traceback_words(self):
        out = render_user_error(ValueError("x"))
        assert "Traceback" not in out
        assert "ValueError" not in out
