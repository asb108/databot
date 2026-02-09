"""Tests for databot tools."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from databot.tools.base import BaseTool, ToolRegistry
from databot.tools.filesystem import EditFileTool, ListDirTool, ReadFileTool, WriteFileTool
from databot.tools.shell import ShellTool

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class SlowTool(BaseTool):
    """A tool that sleeps forever, for timeout testing."""

    @property
    def name(self) -> str:
        return "slow_tool"

    @property
    def description(self) -> str:
        return "Sleeps to test timeout"

    def parameters(self) -> dict[str, Any]:
        return {"type": "object", "properties": {}}

    async def execute(self, **kwargs: Any) -> str:
        await asyncio.sleep(999)
        return "done"


class FastToolWithCustomTimeout(BaseTool):
    """A tool that completes quickly but has a short custom timeout."""

    timeout = 5  # custom per-tool timeout

    @property
    def name(self) -> str:
        return "fast_tool"

    @property
    def description(self) -> str:
        return "Fast tool"

    def parameters(self) -> dict[str, Any]:
        return {"type": "object", "properties": {}}

    async def execute(self, **kwargs: Any) -> str:
        return "fast result"


@pytest.fixture
def tmp_workspace(tmp_path):
    """Create a temporary workspace directory."""
    (tmp_path / "test.txt").write_text("hello world\nline two\nline three\n")
    (tmp_path / "subdir").mkdir()
    (tmp_path / "subdir" / "nested.txt").write_text("nested content")
    return tmp_path


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------


class TestToolRegistry:
    def test_register_and_get(self, tmp_workspace):
        registry = ToolRegistry()
        tool = ReadFileTool(allowed_dir=tmp_workspace)
        registry.register(tool)
        assert registry.get("read_file") is tool
        assert "read_file" in registry.tool_names

    def test_get_definitions(self, tmp_workspace):
        registry = ToolRegistry()
        registry.register(ReadFileTool(allowed_dir=tmp_workspace))
        defs = registry.get_definitions()
        assert len(defs) == 1
        assert defs[0]["type"] == "function"
        assert defs[0]["function"]["name"] == "read_file"

    @pytest.mark.asyncio
    async def test_execute_unknown_tool(self):
        registry = ToolRegistry()
        result = await registry.execute("nonexistent", {})
        assert "Unknown tool" in result

    @pytest.mark.asyncio
    async def test_tool_execution_timeout(self):
        """Tools that exceed timeout are killed and return an error."""
        registry = ToolRegistry(default_timeout=1)  # 1 second timeout
        registry.register(SlowTool())
        result = await registry.execute("slow_tool", {})
        assert "timed out" in result

    @pytest.mark.asyncio
    async def test_custom_per_tool_timeout(self):
        """Tools can have a custom per-tool timeout attribute."""
        registry = ToolRegistry(default_timeout=120)
        tool = FastToolWithCustomTimeout()
        registry.register(tool)
        assert tool.timeout == 5
        result = await registry.execute("fast_tool", {})
        assert result == "fast result"


# ---------------------------------------------------------------------------
# Filesystem tools
# ---------------------------------------------------------------------------


class TestReadFileTool:
    @pytest.mark.asyncio
    async def test_read_file(self, tmp_workspace):
        tool = ReadFileTool(allowed_dir=tmp_workspace)
        result = await tool.execute(path=str(tmp_workspace / "test.txt"))
        assert "hello world" in result

    @pytest.mark.asyncio
    async def test_read_file_with_offset(self, tmp_workspace):
        tool = ReadFileTool(allowed_dir=tmp_workspace)
        result = await tool.execute(path=str(tmp_workspace / "test.txt"), offset=2, limit=1)
        assert "line two" in result

    @pytest.mark.asyncio
    async def test_read_nonexistent(self, tmp_workspace):
        tool = ReadFileTool(allowed_dir=tmp_workspace)
        result = await tool.execute(path=str(tmp_workspace / "nope.txt"))
        assert "does not exist" in result

    @pytest.mark.asyncio
    async def test_read_outside_workspace(self, tmp_workspace):
        tool = ReadFileTool(allowed_dir=tmp_workspace)
        result = await tool.execute(path="/etc/passwd")
        assert "outside" in result


class TestWriteFileTool:
    @pytest.mark.asyncio
    async def test_write_file(self, tmp_workspace):
        tool = WriteFileTool(allowed_dir=tmp_workspace)
        path = str(tmp_workspace / "new.txt")
        result = await tool.execute(path=path, content="new content")
        assert "Successfully" in result
        assert Path(path).read_text() == "new content"


class TestEditFileTool:
    @pytest.mark.asyncio
    async def test_edit_file(self, tmp_workspace):
        tool = EditFileTool(allowed_dir=tmp_workspace)
        path = str(tmp_workspace / "test.txt")
        result = await tool.execute(path=path, old_string="hello world", new_string="goodbye world")
        assert "Successfully" in result
        assert "goodbye world" in Path(path).read_text()

    @pytest.mark.asyncio
    async def test_edit_not_found(self, tmp_workspace):
        tool = EditFileTool(allowed_dir=tmp_workspace)
        path = str(tmp_workspace / "test.txt")
        result = await tool.execute(path=path, old_string="nonexistent", new_string="replacement")
        assert "not found" in result


class TestListDirTool:
    @pytest.mark.asyncio
    async def test_list_dir(self, tmp_workspace):
        tool = ListDirTool(allowed_dir=tmp_workspace)
        result = await tool.execute(path=str(tmp_workspace))
        assert "subdir" in result
        assert "test.txt" in result


# ---------------------------------------------------------------------------
# Shell tool
# ---------------------------------------------------------------------------


class TestShellTool:
    @pytest.mark.asyncio
    async def test_echo(self, tmp_workspace):
        tool = ShellTool(working_dir=str(tmp_workspace), timeout=10)
        result = await tool.execute(command="echo hello")
        assert "hello" in result
        assert "exit code: 0" in result

    @pytest.mark.asyncio
    async def test_allowed_commands(self, tmp_workspace):
        tool = ShellTool(
            working_dir=str(tmp_workspace),
            timeout=10,
            allowed_commands=["ls"],
        )
        result = await tool.execute(command="echo hello")
        assert "not in the allowed list" in result

    @pytest.mark.asyncio
    async def test_timeout(self, tmp_workspace):
        tool = ShellTool(working_dir=str(tmp_workspace), timeout=1)
        result = await tool.execute(command="sleep 10")
        assert "timed out" in result
