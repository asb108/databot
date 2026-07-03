"""dbt integration tool — query a dbt project's metadata.

Reads a dbt ``manifest.json`` (produced by ``dbt compile``/``dbt run``) to answer
questions about the modern data stack without needing dbt installed or a
warehouse connection: list and describe models, trace model lineage
(upstream/downstream), and inspect tests and sources. This grounds the agent in
the project's *modeled* metadata — the layer between raw tables and dashboards.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loguru import logger

from databot.tools.base import BaseTool


class DbtTool(BaseTool):
    """Inspect dbt models, lineage, tests, and sources from manifest.json."""

    def __init__(self, manifest_path: str = "", project_dir: str = ""):
        if manifest_path:
            self._manifest_path = Path(manifest_path)
        elif project_dir:
            self._manifest_path = Path(project_dir) / "target" / "manifest.json"
        else:
            self._manifest_path = Path("target/manifest.json")
        self._manifest: dict[str, Any] | None = None
        self._model_index: dict[str, str] = {}  # model name -> unique_id

    @property
    def name(self) -> str:
        return "dbt"

    @property
    def description(self) -> str:
        return (
            "Query a dbt project's metadata: list/describe models, trace a model's "
            "upstream and downstream lineage, and inspect tests and sources. Use to "
            "answer 'what feeds model X', 'what depends on X', or 'describe model X'."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [
                        "list_models",
                        "get_model",
                        "lineage",
                        "list_tests",
                        "list_sources",
                        "search",
                    ],
                    "description": "Which metadata query to run.",
                },
                "name": {
                    "type": "string",
                    "description": "Model name (for get_model / lineage) or search term.",
                },
            },
            "required": ["action"],
        }

    # ------------------------------------------------------------------
    # Manifest loading
    # ------------------------------------------------------------------
    def _load(self) -> str | None:
        """Lazy-load and index the manifest. Returns an error string or None."""
        if self._manifest is not None:
            return None
        if not self._manifest_path.exists():
            return f"dbt manifest not found at {self._manifest_path}. Run `dbt compile` first."
        try:
            with open(self._manifest_path) as f:
                self._manifest = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to read dbt manifest: {e}")
            return f"Could not read dbt manifest: {e}"

        for uid, node in self._nodes().items():
            if node.get("resource_type") == "model":
                self._model_index[node.get("name", "")] = uid
        return None

    def _nodes(self) -> dict[str, Any]:
        return (self._manifest or {}).get("nodes", {})

    def _sources(self) -> dict[str, Any]:
        return (self._manifest or {}).get("sources", {})

    def _resolve_model(self, name: str) -> tuple[str, dict[str, Any]] | None:
        uid = self._model_index.get(name)
        if uid:
            return uid, self._nodes()[uid]
        # Accept a fully-qualified unique_id too.
        if name in self._nodes():
            return name, self._nodes()[name]
        return None

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------
    async def execute(self, action: str, name: str = "", **kwargs: Any) -> str:
        err = self._load()
        if err:
            return f"Error: {err}"

        if action == "list_models":
            return self._list_models()
        if action == "list_sources":
            return self._list_sources()
        if action == "list_tests":
            return self._list_tests()
        if action == "search":
            return self._search(name)
        if action in ("get_model", "lineage"):
            if not name:
                return f"Error: 'name' is required for {action}."
            resolved = self._resolve_model(name)
            if resolved is None:
                return f"No dbt model named '{name}'. Try `list_models` or `search`."
            uid, node = resolved
            return self._get_model(uid, node) if action == "get_model" else self._lineage(uid, node)
        return f"Error: unknown action '{action}'."

    def _list_models(self) -> str:
        models = [n for n in self._nodes().values() if n.get("resource_type") == "model"]
        if not models:
            return "No models found in the dbt manifest."
        lines = [
            f"**{len(models)} dbt models:**",
            "",
            "| model | schema | description |",
            "| --- | --- | --- |",
        ]
        for n in sorted(models, key=lambda m: m.get("name", "")):
            desc = (n.get("description") or "").replace("\n", " ")[:60]
            lines.append(f"| {n.get('name', '?')} | {n.get('schema', '')} | {desc} |")
        return "\n".join(lines)

    def _get_model(self, uid: str, node: dict[str, Any]) -> str:
        cols = node.get("columns", {})
        lines = [
            f"**Model `{node.get('name')}`**",
            f"- schema: {node.get('schema', '')}",
            f"- materialized: {node.get('config', {}).get('materialized', '?')}",
        ]
        if node.get("description"):
            lines.append(f"- description: {node['description']}")
        tags = node.get("tags") or []
        if tags:
            lines.append(f"- tags: {', '.join(tags)}")
        deps = node.get("depends_on", {}).get("nodes", [])
        if deps:
            lines.append(f"- depends on: {', '.join(self._short(d) for d in deps)}")
        if cols:
            lines.append("\n**Columns:**")
            lines.append("| column | type | description |")
            lines.append("| --- | --- | --- |")
            for cname, c in cols.items():
                cdesc = (c.get("description") or "").replace("\n", " ")[:50]
                lines.append(f"| {cname} | {c.get('data_type') or ''} | {cdesc} |")
        return "\n".join(lines)

    def _lineage(self, uid: str, node: dict[str, Any]) -> str:
        upstream = node.get("depends_on", {}).get("nodes", [])
        child_map = (self._manifest or {}).get("child_map", {})
        downstream = child_map.get(uid, [])
        lines = [f"**Lineage for model `{node.get('name')}`**", ""]
        lines.append("**Upstream (feeds this model):**")
        lines.extend(f"- {self._short(u)}" for u in upstream) if upstream else lines.append(
            "- (none)"
        )
        lines.append("")
        lines.append("**Downstream (depends on this model):**")
        lines.extend(f"- {self._short(d)}" for d in downstream) if downstream else lines.append(
            "- (none)"
        )
        return "\n".join(lines)

    def _list_tests(self) -> str:
        tests = [n for n in self._nodes().values() if n.get("resource_type") == "test"]
        if not tests:
            return "No tests found in the dbt manifest."
        lines = [f"**{len(tests)} dbt tests:**", "", "| test | tests |", "| --- | --- |"]
        for t in sorted(tests, key=lambda m: m.get("name", "")):
            tested = ", ".join(self._short(d) for d in t.get("depends_on", {}).get("nodes", []))
            lines.append(f"| {t.get('name', '?')} | {tested} |")
        return "\n".join(lines)

    def _list_sources(self) -> str:
        sources = self._sources().values()
        if not sources:
            return "No sources found in the dbt manifest."
        lines = [
            f"**{len(self._sources())} dbt sources:**",
            "",
            "| source | table | schema |",
            "| --- | --- | --- |",
        ]
        for s in sorted(sources, key=lambda m: (m.get("source_name", ""), m.get("name", ""))):
            lines.append(
                f"| {s.get('source_name', '?')} | {s.get('name', '?')} | {s.get('schema', '')} |"
            )
        return "\n".join(lines)

    def _search(self, term: str) -> str:
        if not term:
            return "Error: 'name' (search term) is required for search."
        t = term.lower()
        hits = []
        for n in self._nodes().values():
            if n.get("resource_type") != "model":
                continue
            if t in n.get("name", "").lower() or t in (n.get("description") or "").lower():
                hits.append(n)
        if not hits:
            return f"No models matching '{term}'."
        lines = [f"**Models matching '{term}':**"]
        for n in sorted(hits, key=lambda m: m.get("name", "")):
            desc = (n.get("description") or "").replace("\n", " ")[:60]
            lines.append(f"- `{n.get('name')}` — {desc}" if desc else f"- `{n.get('name')}`")
        return "\n".join(lines)

    @staticmethod
    def _short(unique_id: str) -> str:
        """Render a unique_id like 'model.proj.stg_orders' as 'stg_orders (model)'."""
        parts = unique_id.split(".")
        if len(parts) >= 2:
            return f"{parts[-1]} ({parts[0]})"
        return unique_id
