"""Chart tool — render data as text charts in chat.

Most databot responses land in a terminal or a chat client, so visualization
has to survive plain text. This tool renders horizontal bar charts, line charts,
and inline sparklines using Unicode block characters — no plotting library, no
image hosting, works in every channel. It turns a column of numbers from a query
into something an engineer can read at a glance.
"""

from __future__ import annotations

from typing import Any

from databot.tools.base import BaseTool

# Eighth-block characters for sub-cell resolution.
_BLOCKS = " ▏▎▍▌▋▊▉█"
_SPARK = "▁▂▃▄▅▆▇█"


def _coerce_floats(values: list[Any]) -> list[float] | None:
    out: list[float] = []
    for v in values:
        try:
            out.append(float(v))
        except (TypeError, ValueError):
            return None
    return out


def render_bar_chart(
    labels: list[str], values: list[float], width: int = 40, title: str = ""
) -> str:
    if not values:
        return "(no data to chart)"
    label_w = min(24, max((len(str(lbl)) for lbl in labels), default=1))
    vmax = max(values)
    vmin = min(values)
    # Bars are scaled against the max; negatives are clamped to 0-length.
    span = vmax if vmax > 0 else 1.0
    lines: list[str] = []
    if title:
        lines.append(title)
    for lbl, val in zip(labels, values):
        frac = max(0.0, val / span) if span else 0.0
        full = int(frac * width)
        rem = frac * width - full
        bar = "█" * full
        if rem > 0 and full < width:
            bar += _BLOCKS[int(rem * 8)]
        lines.append(f"{str(lbl)[:label_w].ljust(label_w)} │ {bar} {_fmt(val)}")
    lines.append(f"{'':<{label_w}} └ range: {_fmt(vmin)} … {_fmt(vmax)}")
    return "```\n" + "\n".join(lines) + "\n```"


def render_sparkline(values: list[float], title: str = "") -> str:
    if not values:
        return "(no data)"
    vmin, vmax = min(values), max(values)
    span = (vmax - vmin) or 1.0
    spark = "".join(_SPARK[min(7, int((v - vmin) / span * 7))] for v in values)
    head = f"{title}: " if title else ""
    return f"`{head}{spark}` (min {_fmt(vmin)}, max {_fmt(vmax)})"


def render_line_chart(
    values: list[float], width: int = 60, height: int = 8, title: str = ""
) -> str:
    if not values:
        return "(no data)"
    # Downsample to width buckets.
    if len(values) > width:
        bucket = len(values) / width
        sampled = [values[int(i * bucket)] for i in range(width)]
    else:
        sampled = values
    vmin, vmax = min(sampled), max(sampled)
    span = (vmax - vmin) or 1.0
    grid = [[" "] * len(sampled) for _ in range(height)]
    for x, v in enumerate(sampled):
        y = int((v - vmin) / span * (height - 1))
        grid[height - 1 - y][x] = "•"
    lines = [title] if title else []
    for row in grid:
        lines.append("".join(row))
    lines.append(f"min {_fmt(vmin)}  max {_fmt(vmax)}  n={len(values)}")
    return "```\n" + "\n".join(lines) + "\n```"


def _fmt(v: float) -> str:
    if v == int(v):
        return f"{int(v):,}"
    return f"{v:,.2f}"


class ChartTool(BaseTool):
    """Render numeric data as a text chart (bar, line, or sparkline)."""

    def __init__(self, max_points: int = 200):
        self._max_points = max_points

    @property
    def name(self) -> str:
        return "chart"

    @property
    def description(self) -> str:
        return (
            "Render numeric data as a text chart that displays in any chat or "
            "terminal. Use to visualize query results — e.g. counts per category "
            "(bar), a metric over time (line/sparkline)."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "chart_type": {
                    "type": "string",
                    "enum": ["bar", "line", "sparkline"],
                    "description": "Chart style.",
                },
                "values": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "Numeric values to plot.",
                },
                "labels": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Category labels (bar charts).",
                },
                "title": {"type": "string", "description": "Optional chart title."},
            },
            "required": ["chart_type", "values"],
        }

    async def execute(
        self,
        chart_type: str,
        values: list[Any],
        labels: list[str] | None = None,
        title: str = "",
        **kwargs: Any,
    ) -> str:
        nums = _coerce_floats(values or [])
        if nums is None:
            return "Error: all values must be numbers."
        if not nums:
            return "Error: no values to chart."
        nums = nums[: self._max_points]

        if chart_type == "bar":
            labels = labels or [str(i + 1) for i in range(len(nums))]
            if len(labels) < len(nums):
                labels = labels + [str(i + 1) for i in range(len(labels), len(nums))]
            return render_bar_chart(labels[: len(nums)], nums, title=title)
        if chart_type == "line":
            return render_line_chart(nums, title=title)
        if chart_type == "sparkline":
            return render_sparkline(nums, title=title)
        return f"Error: unknown chart_type '{chart_type}'."
