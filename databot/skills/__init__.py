"""Skills registry for databot.

A *skill* is a named bundle of tools, connector types, and config sections.
Users enable/disable skills to control which capabilities databot exposes.
Think of skills like feature-flags for tool groups.

Built-in skills:
  - sql_analytics   — SQL query, data quality, lineage
  - pipeline_ops    — Airflow DAG management
  - spark_jobs      — Spark job submission/monitoring
  - kafka_ops       — Kafka topic/consumer management
  - catalog         — Data catalog search/schema
  - web_search      — Web fetch and search
  - shell           — Shell command execution
  - filesystem      — File read/write/edit/list
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Skill:
    """Definition of a single skill."""

    name: str
    label: str
    description: str
    tools: list[str] = field(default_factory=list)
    connector_types: list[str] = field(default_factory=list)
    config_sections: list[str] = field(default_factory=list)
    requires_extra: str = ""  # pip extra needed, e.g. "sql"
    default_enabled: bool = True


# ---------------------------------------------------------------------------
# Built-in skill definitions
# ---------------------------------------------------------------------------

BUILTIN_SKILLS: dict[str, Skill] = {
    "filesystem": Skill(
        name="filesystem",
        label="Filesystem",
        description="Read, write, edit, and list files in the workspace",
        tools=["read_file", "write_file", "edit_file", "list_dir"],
        default_enabled=True,
    ),
    "shell": Skill(
        name="shell",
        label="Shell",
        description="Execute shell commands with safety restrictions",
        tools=["shell"],
        config_sections=["tools.shell"],
        default_enabled=True,
    ),
    "sql_analytics": Skill(
        name="sql_analytics",
        label="SQL Analytics",
        description="Query databases, check data quality, trace lineage",
        tools=["sql_query", "data_quality", "lineage"],
        connector_types=["sql"],
        config_sections=["tools.sql", "tools.data_quality", "tools.lineage"],
        requires_extra="sql",
        default_enabled=False,
    ),
    "pipeline_ops": Skill(
        name="pipeline_ops",
        label="Pipeline Ops",
        description="Manage Airflow DAGs — list, trigger, check status",
        tools=["airflow"],
        config_sections=["tools.airflow"],
        default_enabled=False,
    ),
    "spark_jobs": Skill(
        name="spark_jobs",
        label="Spark Jobs",
        description="Submit and monitor Spark jobs via Livy/YARN/K8s",
        tools=["spark"],
        connector_types=["spark", "processing"],
        config_sections=[],
        requires_extra="spark",
        default_enabled=False,
    ),
    "kafka_ops": Skill(
        name="kafka_ops",
        label="Kafka Ops",
        description="List topics, check consumer lag, peek messages",
        tools=["kafka"],
        connector_types=["kafka", "streaming"],
        config_sections=[],
        requires_extra="kafka",
        default_enabled=False,
    ),
    "catalog": Skill(
        name="catalog",
        label="Data Catalog",
        description="Search datasets, view schema, lineage, and quality",
        tools=["catalog"],
        connector_types=["catalog"],
        config_sections=[],
        requires_extra="catalog",
        default_enabled=False,
    ),
    "web_search": Skill(
        name="web_search",
        label="Web Search",
        description="Fetch web pages and search the internet",
        tools=["web_fetch", "web_search"],
        config_sections=["tools.web"],
        default_enabled=True,
    ),
}


class SkillRegistry:
    """Registry of available and enabled skills."""

    def __init__(self) -> None:
        self._skills: dict[str, Skill] = dict(BUILTIN_SKILLS)
        self._enabled: set[str] = set()

    # -- registration -------------------------------------------------------

    def register(self, skill: Skill) -> None:
        """Register a custom skill."""
        self._skills[skill.name] = skill

    def enable(self, name: str) -> None:
        if name in self._skills:
            self._enabled.add(name)

    def disable(self, name: str) -> None:
        self._enabled.discard(name)

    def set_enabled(self, names: list[str]) -> None:
        """Set the full list of enabled skills."""
        self._enabled = {n for n in names if n in self._skills}

    # -- queries ------------------------------------------------------------

    def all_skills(self) -> list[Skill]:
        return list(self._skills.values())

    def enabled_skills(self) -> list[Skill]:
        return [s for s in self._skills.values() if s.name in self._enabled]

    def is_enabled(self, name: str) -> bool:
        return name in self._enabled

    def enabled_tool_names(self) -> set[str]:
        """Return the set of tool names that should be registered."""
        names: set[str] = set()
        for skill in self.enabled_skills():
            names.update(skill.tools)
        return names

    def enabled_connector_types(self) -> set[str]:
        types: set[str] = set()
        for skill in self.enabled_skills():
            types.update(skill.connector_types)
        return types

    def summary(self) -> list[dict[str, Any]]:
        """Return a summary suitable for API / UI display."""
        return [
            {
                "name": s.name,
                "label": s.label,
                "description": s.description,
                "enabled": s.name in self._enabled,
                "tools": s.tools,
                "requires_extra": s.requires_extra,
            }
            for s in self._skills.values()
        ]

    # -- config helpers -----------------------------------------------------

    @classmethod
    def from_config(cls, enabled_skills: list[str]) -> "SkillRegistry":
        """Create a registry with the given skills enabled."""
        reg = cls()
        if not enabled_skills:
            # Default: enable skills that are default_enabled
            for skill in reg._skills.values():
                if skill.default_enabled:
                    reg.enable(skill.name)
        else:
            reg.set_enabled(enabled_skills)
        return reg
