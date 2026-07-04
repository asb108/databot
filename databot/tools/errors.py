"""Structured tool errors.

Tools raise ``ToolError`` instead of returning ad-hoc ``"Error: …"`` strings.
``ToolRegistry.execute`` catches it, renders one consistent LLM-facing string via
``to_text()``, and records ``code`` as the audit/metrics status. ``execute`` still
returns ``str``, so callers (loop, agents, MCP) are unchanged.
"""

from __future__ import annotations


class ErrorCode:
    """Standard error-code slugs. ``code`` is a plain string; tools may pass a more
    specific slug, but prefer these for consistent audit/metrics grouping."""

    INVALID_ARGUMENT = "invalid_argument"
    NOT_FOUND = "not_found"
    READ_ONLY_VIOLATION = "read_only_violation"
    PERMISSION_DENIED = "permission_denied"
    CONNECTION_ERROR = "connection_error"
    NOT_CONFIGURED = "not_configured"
    DEPENDENCY_MISSING = "dependency_missing"
    UNSUPPORTED = "unsupported"
    # registry-owned:
    UNKNOWN_TOOL = "unknown_tool"
    TIMEOUT = "timeout"
    INTERNAL_ERROR = "internal_error"


class ToolError(Exception):
    """A structured, LLM-facing tool failure."""

    def __init__(
        self, code: str, message: str, *, hint: str | None = None, retryable: bool = False
    ) -> None:
        self.code = code
        self.message = message
        self.hint = hint
        self.retryable = retryable
        super().__init__(message)

    def to_text(self) -> str:
        """Render a consistent, LLM-readable error string (starts with 'Error')."""
        parts = [f"Error [{self.code}]: {self.message}"]
        if self.hint:
            parts.append(f"Hint: {self.hint}")
        if self.retryable:
            parts.append("This looks transient; a retry may succeed.")
        return " ".join(parts)
