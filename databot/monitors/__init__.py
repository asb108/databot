"""Scheduled data monitors with threshold alerting.

A monitor runs a numeric check (row count, null rate, or a custom SQL metric)
against a connection on a cron schedule, compares the result to a threshold, and
pushes an alert to a channel when the threshold is breached. Run history is
persisted so trends and recent breaches can be inspected. This is the
data-observability layer: freshness/volume/quality guardrails that page a human
(or a chat channel) when data goes wrong.
"""

from databot.monitors.evaluator import (
    OPERATORS,
    MonitorError,
    build_check_query,
    evaluate_breach,
    parse_metric_value,
)
from databot.monitors.service import MonitorService
from databot.monitors.store import MonitorStore

__all__ = [
    "OPERATORS",
    "MonitorError",
    "MonitorService",
    "MonitorStore",
    "build_check_query",
    "evaluate_breach",
    "parse_metric_value",
]
