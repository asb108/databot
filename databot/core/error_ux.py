"""Shared user-facing error rendering.

``render_user_error`` is the ONLY place a user-facing error string is produced.
It is pure and deterministic: given an exception it returns a clean, actionable,
traceback-free message. Raw exception internals (``str(exc)``) never appear in the
output for non-``ToolError`` exceptions — callers log the raw exception themselves.

It consumes the structured :class:`~databot.tools.errors.ToolError` fields
(``message`` / ``hint`` / ``retryable``) when the exception carries them.
"""

from __future__ import annotations

from databot.tools.errors import ToolError

# Reused for every retryable class and for ToolError(retryable=True).
_RETRY_NUDGE = "You can try again in a moment."

_GENERIC = "Sorry, I encountered an internal error. Please try again or contact an administrator."

# (substrings, type-names, message, retryable) — checked in order; first match wins.
# `substrings` and `type_names` are matched case-insensitively.
_CLASSES: list[tuple[tuple[str, ...], tuple[str, ...], str, bool]] = [
    # 1. auth / config
    (
        ("authenticationerror", "api key", "unauthorized", "permission denied", "forbidden"),
        ("permissionerror", "authenticationerror"),
        "I'm not configured correctly to do that (authentication failed). "
        "Please contact an administrator.",
        False,
    ),
    # 2. rate-limit
    (
        ("rate limit", "too many requests", "429"),
        (),
        "The service is busy at the moment. Please wait a few seconds and try again.",
        True,
    ),
    # 3. timeout
    (
        ("timed out", "timeout"),
        ("timeouterror",),
        "That took too long to complete. Please try again.",
        True,
    ),
    # 4. network / connection
    (
        ("connection", "network", "unreachable", "refused"),
        ("connectionerror", "connecterror", "connecttimeout", "readtimeout", "transporterror"),
        "I'm having trouble reaching a service right now. Please try again shortly.",
        True,
    ),
]


def _with_nudge(message: str, retryable: bool) -> str:
    return f"{message} {_RETRY_NUDGE}" if retryable else message


def render_user_error(exc: BaseException) -> str:
    """Render a clean, traceback-free, user-facing string for any exception."""
    if isinstance(exc, ToolError):
        parts = [exc.message]
        if exc.hint:
            parts.append(exc.hint)
        text = " ".join(parts)
        return _with_nudge(text, exc.retryable)

    type_name = type(exc).__name__.lower()
    text = str(exc).lower()
    for substrings, type_names, message, retryable in _CLASSES:
        if any(t in type_name for t in type_names) or any(s in text for s in substrings):
            return _with_nudge(message, retryable)
    return _GENERIC
