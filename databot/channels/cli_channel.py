"""CLI channel for interactive terminal usage."""

from __future__ import annotations

import asyncio

from rich.console import Console
from rich.markdown import Markdown

from databot.channels.base import BaseChannel
from databot.core.bus import InboundMessage, MessageBus


class CLIChannel(BaseChannel):
    """Interactive CLI channel using Rich for formatted output."""

    def __init__(self, bus: MessageBus):
        super().__init__(bus)
        self.console = Console()
        self._running = False

    @property
    def name(self) -> str:
        return "cli"

    async def start(self) -> None:
        """Start the interactive CLI loop."""
        self._running = True
        self.console.print("[bold green]databot[/] interactive mode. Type 'exit' to quit.\n")

        while self._running:
            try:
                # Read input in a thread to not block the event loop
                user_input = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: input("you > ")
                )

                if user_input.strip().lower() in ("exit", "quit", "/quit", "/exit"):
                    self.console.print("[dim]Goodbye![/]")
                    break

                if not user_input.strip():
                    continue

                await self.bus.publish_inbound(
                    InboundMessage(
                        channel="cli",
                        sender_id="user",
                        chat_id="direct",
                        content=user_input.strip(),
                    )
                )

                # Wait for response
                response = await self.bus.consume_outbound()
                self._print_response(response.content)

            except (EOFError, KeyboardInterrupt):
                self.console.print("\n[dim]Goodbye![/]")
                break

    async def stop(self) -> None:
        self._running = False

    async def send(self, chat_id: str, content: str, thread_id: str | None = None) -> None:
        self._print_response(content)

    def _print_response(self, content: str) -> None:
        self.console.print()
        try:
            self.console.print(Markdown(content))
        except Exception:
            self.console.print(content)
        self.console.print()
