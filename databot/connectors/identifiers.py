"""Safe SQL identifier validation and quoting.

Centralized utilities to prevent SQL injection through table / schema / column
names. These values frequently originate from LLM tool calls or end-user input,
so any code that interpolates an identifier into a SQL string MUST route it
through :func:`validate_identifier` (to reject unsafe characters) or
:func:`quote_identifier` (to validate *and* wrap it in the driver's quoting
characters).

The validation rule is deliberately strict: an identifier component may contain
only ASCII letters, digits and underscores, and must start with a letter or
underscore. Dotted (``schema.table``) names are supported by validating each
component independently. Because a validated component can never contain a quote
character, quoting is injection-safe by construction.
"""

from __future__ import annotations

import re

# A single identifier component: letter/underscore, then letters/digits/underscores.
_SIMPLE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Most engines cap identifiers around 128-256 chars; 128 is a safe, strict bound.
_MAX_IDENT_LEN = 128

# Per-driver identifier quoting characters. Anything not listed uses ANSI
# double-quotes, which is correct for Trino, Presto, PostgreSQL, and most others.
_QUOTE_CHARS: dict[str, tuple[str, str]] = {
    "mysql": ("`", "`"),
    "clickzetta": ("`", "`"),
    "starrocks": ("`", "`"),
    "clickhouse": ("`", "`"),
    "hive": ("`", "`"),
}
_ANSI_QUOTES = ('"', '"')


class IdentifierError(ValueError):
    """Raised when a SQL identifier fails validation."""


def validate_identifier(
    value: str,
    label: str = "identifier",
    *,
    allow_qualified: bool = True,
) -> str:
    """Validate a SQL identifier and return its cleaned form.

    Surrounding quote characters (`` ` ``, ``"``, ``[``/``]``) are stripped from
    each component before validation; the returned value contains no quoting.

    Args:
        value: The raw identifier (optionally ``schema.table`` when
            ``allow_qualified`` is True).
        label: Human-readable name used in error messages.
        allow_qualified: When True, dots split the value into components that are
            each validated. When False, dots are rejected.

    Returns:
        The cleaned, dot-joined identifier (no quote characters).

    Raises:
        IdentifierError: If the identifier is empty or contains unsafe characters.
    """
    if value is None:
        raise IdentifierError(f"{label} is required")
    raw = str(value).strip()
    if not raw:
        raise IdentifierError(f"{label} must not be empty")

    parts = raw.split(".") if allow_qualified else [raw]
    cleaned: list[str] = []
    for part in parts:
        component = part.strip().strip('`"[]')
        if not component:
            raise IdentifierError(f"Invalid {label}: {value!r} (contains an empty component)")
        if len(component) > _MAX_IDENT_LEN:
            raise IdentifierError(
                f"Invalid {label}: {value!r} exceeds the maximum length of "
                f"{_MAX_IDENT_LEN} characters"
            )
        if not _SIMPLE_IDENT_RE.match(component):
            raise IdentifierError(
                f"Invalid {label}: {value!r}. Identifiers may contain only ASCII "
                f"letters, digits and underscores (optionally dotted as "
                f"schema.table) and must start with a letter or underscore."
            )
        cleaned.append(component)
    return ".".join(cleaned)


def quote_identifier(
    value: str,
    driver: str = "",
    label: str = "identifier",
    *,
    allow_qualified: bool = True,
) -> str:
    """Validate ``value`` and wrap each component in the driver's quote chars.

    Because validation guarantees no quote characters appear inside a component,
    the result is safe to interpolate directly into a SQL string.

    Example::

        quote_identifier("sales.orders", "mysql")  -> "`sales`.`orders`"
        quote_identifier("sales.orders", "trino")  -> '"sales"."orders"'
    """
    cleaned = validate_identifier(value, label, allow_qualified=allow_qualified)
    open_q, close_q = _QUOTE_CHARS.get((driver or "").lower(), _ANSI_QUOTES)
    return ".".join(f"{open_q}{component}{close_q}" for component in cleaned.split("."))
