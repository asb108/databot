"""Data lineage tool using NetworkX graph."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loguru import logger

from databot.tools.base import BaseTool


class LineageTool(BaseTool):
    """Query data lineage information."""

    def __init__(self, graph_path: str = ""):
        self._graph_path = graph_path
        self._graph = None

    def _load_graph(self):
        """Lazy-load the lineage graph."""
        if self._graph is not None:
            return

        try:
            import networkx as nx
        except ImportError:
            raise ImportError(
                "networkx not installed. Install with: pip install databot[lineage]"
            )

        if not self._graph_path or not Path(self._graph_path).exists():
            self._graph = nx.DiGraph()
            return

        with open(self._graph_path) as f:
            data = json.load(f)

        self._graph = nx.DiGraph()
        for node in data.get("nodes", []):
            self._graph.add_node(
                node["id"], **{k: v for k, v in node.items() if k != "id"}
            )
        for edge in data.get("edges", []):
            self._graph.add_edge(
                edge["source"],
                edge["target"],
                **{k: v for k, v in edge.items() if k not in ("source", "target")},
            )

    @property
    def name(self) -> str:
        return "lineage"

    @property
    def description(self) -> str:
        return (
            "Query data lineage. Find upstream/downstream tables, "
            "impact analysis, and dependency paths."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["upstream", "downstream", "path", "search", "info"],
                    "description": "Lineage action to perform.",
                },
                "table": {"type": "string", "description": "Table name to query."},
                "target_table": {
                    "type": "string",
                    "description": "Target table (for path action).",
                },
                "depth": {
                    "type": "integer",
                    "description": "Max traversal depth. Default 3.",
                },
            },
            "required": ["action", "table"],
        }

    async def execute(self, action: str, table: str, **kwargs: Any) -> str:
        try:
            import networkx as nx
        except ImportError:
            return "Error: networkx not installed. Install with: pip install databot[lineage]"

        try:
            self._load_graph()
        except Exception as e:
            return f"Error loading lineage graph: {e}"

        if action == "search":
            matches = [n for n in self._graph.nodes if table.lower() in n.lower()]
            if not matches:
                return f"No tables matching '{table}'."
            return (
                f"**Tables matching '{table}':**\n"
                + "\n".join(f"- `{m}`" for m in matches[:20])
            )

        if table not in self._graph:
            matches = [n for n in self._graph.nodes if table.lower() in n.lower()]
            if matches:
                return f"Table '{table}' not found. Did you mean: {', '.join(matches[:10])}"
            return f"Table '{table}' not found in lineage graph."

        depth = kwargs.get("depth", 3)

        if action == "upstream":
            ancestors = nx.ancestors(self._graph, table)
            result = set()
            for node in ancestors:
                try:
                    path_len = nx.shortest_path_length(self._graph, node, table)
                    if path_len <= depth:
                        result.add((node, path_len))
                except nx.NetworkXNoPath:
                    pass

            if not result:
                return f"No upstream dependencies found for `{table}`."

            lines = [f"**Upstream dependencies for `{table}` (depth={depth}):**\n"]
            for node, dist in sorted(result, key=lambda x: x[1]):
                lines.append(f"- `{node}` (distance: {dist})")
            return "\n".join(lines)

        elif action == "downstream":
            descendants = nx.descendants(self._graph, table)
            result = set()
            for node in descendants:
                try:
                    path_len = nx.shortest_path_length(self._graph, table, node)
                    if path_len <= depth:
                        result.add((node, path_len))
                except nx.NetworkXNoPath:
                    pass

            if not result:
                return f"No downstream dependencies found for `{table}`."

            lines = [f"**Downstream dependencies for `{table}` (depth={depth}):**\n"]
            for node, dist in sorted(result, key=lambda x: x[1]):
                lines.append(f"- `{node}` (distance: {dist})")
            return "\n".join(lines)

        elif action == "path":
            target = kwargs.get("target_table", "")
            if not target:
                return "Error: target_table is required for path action."
            if target not in self._graph:
                return f"Target table '{target}' not found in lineage graph."

            try:
                path = nx.shortest_path(self._graph, table, target)
                return (
                    f"**Path from `{table}` to `{target}`:**\n"
                    + " -> ".join(f"`{p}`" for p in path)
                )
            except nx.NetworkXNoPath:
                return f"No path found from `{table}` to `{target}`."

        elif action == "info":
            in_degree = self._graph.in_degree(table)
            out_degree = self._graph.out_degree(table)
            attrs = self._graph.nodes[table]
            info = [
                f"**Table info: `{table}`**\n",
                f"- Upstream count: {in_degree}",
                f"- Downstream count: {out_degree}",
            ]
            for k, v in attrs.items():
                info.append(f"- {k}: {v}")
            return "\n".join(info)

        return f"Unknown action: {action}"
