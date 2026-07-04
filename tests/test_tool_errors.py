"""Tests for the structured ToolError type and its registry integration."""

from __future__ import annotations

from databot.tools.errors import ErrorCode, ToolError


def test_to_text_minimal():
    err = ToolError(ErrorCode.NOT_FOUND, "Table 'x' does not exist.")
    assert err.to_text() == "Error [not_found]: Table 'x' does not exist."


def test_to_text_with_hint():
    err = ToolError("read_only_violation", "Only SELECT is allowed.", hint="Remove the INSERT.")
    assert err.to_text() == (
        "Error [read_only_violation]: Only SELECT is allowed. Hint: Remove the INSERT."
    )


def test_to_text_retryable_note():
    err = ToolError(ErrorCode.CONNECTION_ERROR, "Airflow unreachable.", retryable=True)
    text = err.to_text()
    assert text.startswith("Error [connection_error]: Airflow unreachable.")
    assert "transient" in text


def test_is_exception_and_carries_fields():
    err = ToolError(ErrorCode.INVALID_ARGUMENT, "bad arg", hint="fix it", retryable=False)
    assert isinstance(err, Exception)
    assert err.code == "invalid_argument"
    assert err.message == "bad arg"
    assert err.hint == "fix it"
    assert err.retryable is False
    assert str(err) == "bad arg"


def test_error_code_constants_are_slugs():
    assert ErrorCode.INTERNAL_ERROR == "internal_error"
    assert ErrorCode.UNKNOWN_TOOL == "unknown_tool"
    assert ErrorCode.TIMEOUT == "timeout"
