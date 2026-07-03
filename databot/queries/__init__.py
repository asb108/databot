"""Saved query library — store and run named, parameterized SQL queries.

Turns ad-hoc SQL into a reusable, shareable catalog: save a query once (with
``{param}`` placeholders), then run it by name with parameters. Parameter values
are substituted as escaped SQL literals — numbers as numeric literals, strings
single-quoted with quotes doubled and backslashes/null bytes rejected (so the
escaping holds on MySQL/ClickHouse/Hive as well as ANSI engines). The query
itself runs through the read-only SQL path.
"""

from databot.queries.store import QueryStore, render_query

__all__ = ["QueryStore", "render_query"]
