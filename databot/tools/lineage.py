"""Data lineage tool using NetworkX graph and/or Marquez (OpenLineage) backend."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from databot.tools.base import BaseTool


class LineageTool(BaseTool):
    """Query data lineage information.

    Supports two backends:
    * **graph** — local NetworkX graph loaded from JSON (original)
    * **marquez** — live lineage from a Marquez REST API (OpenLineage standard)

    When ``marquez_url`` is configured it takes priority; the local graph
    is used as fallback or when Marquez is unreachable.
    """

    def __init__(self, graph_path: str = "", marquez_url: str = ""):
        self._graph_path = graph_path
        self._marquez_url = marquez_url.rstrip("/") if marquez_url else ""
        self._graph = None

    def _load_graph(self):
        """Lazy-load the lineage graph."""
        if self._graph is not None:
            return

        try:
            import networkx as nx
        except ImportError:
            raise ImportError("networkx not installed. Install with: pip install databot[lineage]")

        if not self._graph_path or not Path(self._graph_path).exists():
            self._graph = nx.DiGraph()
            return

        with open(self._graph_path) as f:
            data = json.load(f)

        self._graph = nx.DiGraph()
        for node in data.get("nodes", []):
            self._graph.add_node(node["id"], **{k: v for k, v in node.items() if k != "id"})
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
        backend = "Marquez" if self._marquez_url else "local graph"
        return (
            f"Query data lineage ({backend}). Find upstream/downstream tables, "
            "impact analysis, and dependency paths."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["upstream", "downstream", "path", "search", "info", "jobs", "runs"],
                    "description": "Lineage action to perform.",
                },
                "table": {"type": "string", "description": "Table/dataset name to query."},
                "target_table": {
                    "type": "string",
                    "description": "Target table (for path action).",
                },
                "depth": {
                    "type": "integer",
                    "description": "Max traversal depth. Default 3.",
                },
                "namespace": {
                    "type": "string",
                    "description": "Marquez namespace (default: auto-detect).",
                },
            },
            "required": ["action", "table"],
        }

    async def execute(self, action: str, table: str, **kwargs: Any) -> str:
        # Try Marquez first if configured
        if self._marquez_url:
            try:
                return await self._execute_marquez(action, table, **kwargs)
            except Exception as e:
                from loguru import logger

                logger.warning(f"Marquez query failed, falling back to local graph: {e}")

        # Fallback to local NetworkX graph
        return await self._execute_graph(action, table, **kwargs)

    async def _execute_marquez(self, action: str, table: str, **kwargs: Any) -> str:
        """Execute lineage query against Marquez REST API."""
        import httpx

        namespace = kwargs.get("namespace", "")
        depth = kwargs.get("depth", 3)

        async with httpx.AsyncClient(base_url=self._marquez_url, timeout=30) as client:
            if action == "search":
                resp = await client.get("/api/v1/search/datasets", params={"q": table, "limit": 20})
                resp.raise_for_status()
                results = resp.json().get("results", [])
                if not results:
                    return f"No datasets matching '{table}' in Marquez."
                lines = [f"**Datasets matching '{table}':**\n"]
                for r in results:
                    ns = r.get("namespace", "")
                    name = r.get("name", "")
                    lines.append(f"- `{ns}.{name}`")
                return "\n".join(lines)

            elif action in ("upstream", "downstream"):
                # Get dataset lineage
                ns = namespace or "default"
                resp = await client.get(
                    "/api/v1/lineage",
                    params={"nodeId": f"dataset:{ns}:{table}", "depth": depth},
                )
                resp.raise_for_status()
                graph = resp.json().get("graph", [])

                # Filter for datasets only
                datasets = [
                    n
                    for n in graph
                    if n.get("type") == "DATASET" and n.get("id", {}).get("name") != table
                ]
                if not datasets:
                    return f"No {action} dependencies found for `{table}` in Marquez."

                lines = [f"**{action.title()} dependencies for `{table}` (depth={depth}):**\n"]
                for ds in datasets:
                    ds_name = ds.get("id", {}).get("name", "unknown")
                    ds_ns = ds.get("id", {}).get("namespace", "")
                    lines.append(f"- `{ds_ns}.{ds_name}`")
                return "\n".join(lines)

            elif action == "info":
                ns = namespace or "default"
                resp = await client.get(f"/api/v1/namespaces/{ns}/datasets/{table}")
                resp.raise_for_status()
                ds = resp.json()
                fields_info = ds.get("fields", [])
                info = [
                    f"**Dataset info: `{table}`**\n",
                    f"- Namespace: {ds.get('namespace', '')}",
                    f"- Source: {ds.get('sourceName', '')}",
                    f"- Created: {ds.get('createdAt', '')}",
                    f"- Updated: {ds.get('updatedAt', '')}",
                    f"- Fields: {len(fields_info)}",
                ]
                if fields_info:
                    info.append("\n**Schema:**")
                    for f_item in fields_info[:30]:
                        info.append(f"- `{f_item.get('name')}` ({f_item.get('type', '?')})")
                return "\n".join(info)

            elif action == "jobs":
                # List jobs related to a dataset
                ns = namespace or "default"
                resp = await client.get(
                    "/api/v1/lineage",
                    params={"nodeId": f"dataset:{ns}:{table}", "depth": 1},
                )
                resp.raise_for_status()
                graph = resp.json().get("graph", [])
                jobs = [n for n in graph if n.get("type") == "JOB"]
                if not jobs:
                    return f"No jobs found for dataset `{table}`."
                lines = [f"**Jobs related to `{table}`:**\n"]
                for j in jobs:
                    j_name = j.get("id", {}).get("name", "unknown")
                    j_ns = j.get("id", {}).get("namespace", "")
                    lines.append(f"- `{j_ns}.{j_name}`")
                return "\n".join(lines)

            elif action == "runs":
                # Get recent runs for a job (treat table as job name)
                ns = namespace or "default"
                resp = await client.get(
                    f"/api/v1/namespaces/{ns}/jobs/{table}/runs", params={"limit": 10}
                )
                resp.raise_for_status()
                runs = resp.json().get("runs", [])
                if not runs:
                    return f"No runs found for job `{table}`."
                lines = [f"**Recent runs for `{table}`:**\n"]
                for r in runs:
                    state = r.get("state", "")
                    started = r.get("startedAt", "N/A")
                    ended = r.get("endedAt", "N/A")
                    run_id = r.get("id", "")[:8]
                    lines.append(f"- `{run_id}` {state} ({started} → {ended})")
                return "\n".join(lines)

            elif action == "path":
                target = kwargs.get("target_table", "")
                if not target:
                    return "Error: target_table is required for path action."
                # Use lineage graph to find path
                ns = namespace or "default"
                resp = await client.get(
                    "/api/v1/lineage",
                    params={"nodeId": f"dataset:{ns}:{table}", "depth": 10},
                )
                resp.raise_for_status()
                # Return the full graph — path finding in Marquez requires client-side traversal
                graph = resp.json().get("graph", [])
                dataset_names = [
                    n.get("id", {}).get("name", "unknown")
                    for n in graph
                    if n.get("type") == "DATASET"
                ]
                if target in dataset_names:
                    return f"**Path exists** between `{table}` and `{target}` within depth 10."
                return f"No path found from `{table}` to `{target}` in Marquez."

        return f"Unknown action: {action}"

    async def _execute_graph(self, action: str, table: str, **kwargs: Any) -> str:
        """Execute lineage query against local NetworkX graph (original implementation)."""
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
            return f"**Tables matching '{table}':**\n" + "\n".join(f"- `{m}`" for m in matches[:20])

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
                return f"**Path from `{table}` to `{target}`:**\n" + " -> ".join(
                    f"`{p}`" for p in path
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
