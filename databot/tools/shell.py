"""Shell command execution tool."""

from __future__ import annotations

import asyncio
import os
from typing import Any

from databot.tools.base import BaseTool


class ShellTool(BaseTool):
    """Execute shell commands with optional workspace sandboxing."""

    def __init__(
        self,
        working_dir: str = ".",
        timeout: int = 30,
        restrict_to_workspace: bool = True,
        allowed_commands: list[str] | None = None,
        max_output_length: int = 10000,
    ):
        self._working_dir = working_dir
        self._timeout = timeout
        self._restrict_to_workspace = restrict_to_workspace
        self._allowed_commands = allowed_commands or []
        self._max_output_length = max_output_length

    @property
    def name(self) -> str:
        return "shell"

    @property
    def description(self) -> str:
        return (
            "Execute a shell command and return its output. "
            "Use for system operations, kubectl, git, etc."
        )

    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The shell command to execute.",
                },
            },
            "required": ["command"],
        }

    async def execute(self, command: str) -> str:
        # Validate allowed commands if configured
        if self._allowed_commands:
            base_cmd = command.strip().split()[0] if command.strip() else ""
            if base_cmd not in self._allowed_commands:
                return (
                    f"Error: Command '{base_cmd}' is not in the allowed list: "
                    f"{self._allowed_commands}"
                )

        try:
            env = os.environ.copy()
            if self._restrict_to_workspace:
                env["HOME"] = self._working_dir

            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self._working_dir,
                env=env,
            )

            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=self._timeout)

            output = ""
            if stdout:
                output += stdout.decode("utf-8", errors="replace")
            if stderr:
                output += "\n[stderr]\n" + stderr.decode("utf-8", errors="replace")

            output += f"\n[exit code: {proc.returncode}]"

            # Truncate very long output
            max_len = self._max_output_length
            if len(output) > max_len:
                output = output[:max_len] + f"\n... (truncated, {len(output)} total chars)"

            return output.strip()

        except asyncio.TimeoutError:
            return f"Error: Command timed out after {self._timeout} seconds"
        except Exception as e:
            return f"Error executing command: {str(e)}"
