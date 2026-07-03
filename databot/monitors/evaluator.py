"""Pure monitor-check logic: build check SQL, parse the value, evaluate breach.

Kept dependency-free and side-effect-free so the threshold semantics are easy to
test without a database or scheduler.
"""

from __future__ import annotations

from databot.connectors.identifiers import IdentifierError, validate_identifier

# Comparison operators. A monitor alerts when ``value <operator> threshold`` is
# True — e.g. row_count lt 1000 alerts when the count drops below 1000.
OPERATORS: dict[str, str] = {
    "lt": "<",
    "le": "<=",
    "gt": ">",
    "ge": ">=",
    "eq": "==",
    "ne": "!=",
}

CHECK_TYPES = ("row_count", "null_rate", "sql_metric")


class MonitorError(ValueError):
    """Raised when a monitor definition is invalid."""


def build_check_query(
    check_type: str,
    table: str = "",
    column: str = "",
    query: str = "",
) -> str:
    """Build the SQL that produces the monitor's single numeric value.

    For ``sql_metric`` the user-supplied ``query`` is returned as-is (executed
    read-only downstream). For the built-in types the identifiers are validated
    and interpolated safely.
    """
    if check_type == "sql_metric":
        if not query.strip():
            raise MonitorError("sql_metric requires a 'query' that selects a single number.")
        return query.strip()

    try:
        tbl = validate_identifier(table, "table")
    except IdentifierError as e:
        raise MonitorError(str(e)) from e

    if check_type == "row_count":
        return f"SELECT COUNT(*) AS value FROM {tbl}"

    if check_type == "null_rate":
        if not column:
            raise MonitorError("null_rate requires a 'column'.")
        try:
            col = validate_identifier(column, "column")
        except IdentifierError as e:
            raise MonitorError(str(e)) from e
        return (
            f"SELECT ROUND(SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) * 100.0 "
            f"/ NULLIF(COUNT(*), 0), 2) AS value FROM {tbl}"
        )

    raise MonitorError(f"Unknown check_type '{check_type}'. Use one of {CHECK_TYPES}.")


def parse_metric_value(markdown: str) -> float | None:
    """Extract the first numeric cell from a one-row markdown result table."""
    rows = [ln for ln in markdown.splitlines() if ln.strip().startswith("|")]
    if len(rows) < 3:
        return None
    data = [c.strip() for c in rows[2].strip().strip("|").split("|")]
    for cell in data:
        try:
            return float(cell)
        except (ValueError, TypeError):
            continue
    return None


def evaluate_breach(value: float, operator: str, threshold: float) -> bool:
    """Return True when the monitor's alert condition is met."""
    if operator not in OPERATORS:
        raise MonitorError(f"Unknown operator '{operator}'. Use one of {sorted(OPERATORS)}.")
    if operator == "lt":
        return value < threshold
    if operator == "le":
        return value <= threshold
    if operator == "gt":
        return value > threshold
    if operator == "ge":
        return value >= threshold
    if operator == "eq":
        return value == threshold
    return value != threshold  # ne
