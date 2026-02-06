"""Filesystem tools: read, write, edit, list."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from databot.tools.base import BaseTool


class _FSBase(BaseTool):
    """Base for filesystem tools with optional workspace restriction."""

    def __init__(self, allowed_dir: Path | None = None):
        self._allowed_dir = allowed_dir

    def _validate_path(self, path: str) -> Path:
        p = Path(path).resolve()
        if self._allowed_dir and not str(p).startswith(str(self._allowed_dir.resolve())):
            raise PermissionError(f"Path '{path}' is outside the allowed workspace")
        return p


class ReadFileTool(_FSBase):
    @property
    def name(self) -> str:
        return "read_file"

    @property
    def description(self) -> str:
        return "Read the contents of a file."

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the file to read."},
                "offset": {
                    "type": "integer",
                    "description": "Line number to start reading from (1-indexed). Optional.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of lines to read. Optional.",
                },
            },
            "required": ["path"],
        }

    async def execute(
        self, path: str, offset: int | None = None, limit: int | None = None
    ) -> str:
        try:
            p = self._validate_path(path)
            if not p.exists():
                return f"Error: File '{path}' does not exist"

            with open(p) as f:
                lines = f.readlines()

            if offset is not None:
                start = max(0, offset - 1)
                if limit is not None:
                    lines = lines[start : start + limit]
                else:
                    lines = lines[start:]
            elif limit is not None:
                lines = lines[:limit]

            if not lines:
                return "(empty file)"

            return "".join(lines)
        except PermissionError as e:
            return str(e)
        except Exception as e:
            return f"Error reading file: {str(e)}"


class WriteFileTool(_FSBase):
    @property
    def name(self) -> str:
        return "write_file"

    @property
    def description(self) -> str:
        return "Write content to a file. Creates the file if it doesn't exist."

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the file to write."},
                "content": {"type": "string", "description": "Content to write to the file."},
            },
            "required": ["path", "content"],
        }

    async def execute(self, path: str, content: str) -> str:
        try:
            p = self._validate_path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "w") as f:
                f.write(content)
            return f"Successfully wrote {len(content)} bytes to {path}"
        except PermissionError as e:
            return str(e)
        except Exception as e:
            return f"Error writing file: {str(e)}"


class EditFileTool(_FSBase):
    @property
    def name(self) -> str:
        return "edit_file"

    @property
    def description(self) -> str:
        return "Replace a specific string in a file with new content."

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the file to edit."},
                "old_string": {
                    "type": "string",
                    "description": "The exact string to find and replace.",
                },
                "new_string": {"type": "string", "description": "The replacement string."},
            },
            "required": ["path", "old_string", "new_string"],
        }

    async def execute(self, path: str, old_string: str, new_string: str) -> str:
        try:
            p = self._validate_path(path)
            if not p.exists():
                return f"Error: File '{path}' does not exist"

            content = p.read_text()
            count = content.count(old_string)
            if count == 0:
                return "Error: old_string not found in file"
            if count > 1:
                return f"Error: old_string found {count} times; must be unique"

            new_content = content.replace(old_string, new_string, 1)
            p.write_text(new_content)
            return f"Successfully edited {path}"
        except PermissionError as e:
            return str(e)
        except Exception as e:
            return f"Error editing file: {str(e)}"


class ListDirTool(_FSBase):
    @property
    def name(self) -> str:
        return "list_dir"

    @property
    def description(self) -> str:
        return "List files and directories in a given path."

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Directory path to list."},
            },
            "required": ["path"],
        }

    async def execute(self, path: str) -> str:
        try:
            p = self._validate_path(path)
            if not p.is_dir():
                return f"Error: '{path}' is not a directory"

            entries = sorted(p.iterdir())
            lines = []
            for entry in entries:
                if entry.name.startswith("."):
                    continue
                prefix = "d " if entry.is_dir() else "f "
                lines.append(f"{prefix}{entry.name}")

            if not lines:
                return "(empty directory)"
            return "\n".join(lines)
        except PermissionError as e:
            return str(e)
        except Exception as e:
            return f"Error listing directory: {str(e)}"
