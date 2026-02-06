"""Agent loop: the core processing engine."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from loguru import logger

from databot.core.bus import InboundMessage, MessageBus, OutboundMessage
from databot.core.context import ContextBuilder
from databot.memory.manager import MemoryManager
from databot.providers.base import LLMProvider
from databot.session.manager import SessionManager
from databot.tools.base import ToolRegistry


class AgentLoop:
    """
    Core agent loop: receive messages, reason with LLM, execute tools, respond.

    Flow:
    1. Receive message from bus
    2. Build context (system prompt + history + current message)
    3. Call LLM
    4. If tool calls: execute tools, feed results back, repeat
    5. If text response: send it back
    """

    def __init__(
        self,
        bus: MessageBus,
        provider: LLMProvider,
        tools: ToolRegistry,
        workspace: Path,
        sessions: SessionManager,
        memory: MemoryManager,
        model: str | None = None,
        max_iterations: int = 20,
        system_prompt: str = "",
    ):
        self.bus = bus
        self.provider = provider
        self.tools = tools
        self.workspace = workspace
        self.sessions = sessions
        self.memory = memory
        self.model = model or provider.get_default_model()
        self.max_iterations = max_iterations
        self.context = ContextBuilder(workspace, memory, system_prompt)
        self._running = False

    async def run(self) -> None:
        """Run the agent loop, processing messages from the bus."""
        self._running = True
        logger.info("Agent loop started")

        while self._running:
            try:
                msg = await asyncio.wait_for(self.bus.consume_inbound(), timeout=1.0)
                try:
                    response = await self.process_message(msg)
                    if response:
                        await self.bus.publish_outbound(response)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    await self.bus.publish_outbound(
                        OutboundMessage(
                            channel=msg.channel,
                            chat_id=msg.chat_id,
                            content=f"Sorry, I encountered an error: {str(e)}",
                        )
                    )
            except asyncio.TimeoutError:
                continue

    def stop(self) -> None:
        """Stop the agent loop."""
        self._running = False
        logger.info("Agent loop stopping")

    async def process_message(self, msg: InboundMessage) -> OutboundMessage | None:
        """Process a single inbound message through the LLM agent loop."""
        logger.info(f"Processing message from {msg.channel}:{msg.sender_id}")

        # Get or create session
        session = self.sessions.get_or_create(msg.session_key)

        # Build initial messages
        messages = self.context.build_messages(
            history=session.get_history(),
            current_message=msg.content,
            channel=msg.channel,
            chat_id=msg.chat_id,
            media=msg.media if msg.media else None,
        )

        # Agent loop: LLM <-> tool execution
        iteration = 0
        final_content = None

        while iteration < self.max_iterations:
            iteration += 1

            # Call LLM
            response = await self.provider.chat(
                messages=messages,
                tools=self.tools.get_definitions() or None,
                model=self.model,
            )

            if response.has_tool_calls:
                # Add assistant message with tool calls
                tool_call_dicts = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.name,
                            "arguments": json.dumps(tc.arguments),
                        },
                    }
                    for tc in response.tool_calls
                ]
                messages = self.context.add_assistant_message(
                    messages, response.content, tool_call_dicts
                )

                # Execute each tool
                for tc in response.tool_calls:
                    logger.debug(f"Executing tool: {tc.name}")
                    result = await self.tools.execute(tc.name, tc.arguments)
                    messages = self.context.add_tool_result(
                        messages, tc.id, tc.name, result
                    )
            else:
                final_content = response.content
                break

        if final_content is None:
            final_content = "I've completed processing but have no final response."

        # Save to session history
        session.add_message("user", msg.content)
        session.add_message("assistant", final_content)
        self.sessions.save(session)

        return OutboundMessage(
            channel=msg.channel,
            chat_id=msg.chat_id,
            content=final_content,
        )

    async def process_direct(self, content: str, session_key: str = "cli:direct") -> str:
        """Process a message directly (for CLI usage). Returns the response text."""
        msg = InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="direct",
            content=content,
        )
        response = await self.process_message(msg)
        return response.content if response else ""
