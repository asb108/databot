"""Agent loop: the core processing engine."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, AsyncIterator

from loguru import logger

from databot.core.bus import InboundMessage, MessageBus, OutboundMessage, StreamEvent
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

    Supports optional human-in-the-loop approval for sensitive tools.
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
        approval_required_tools: list[str] | None = None,
        rag_context: Any | None = None,
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
        self._approval_required = set(approval_required_tools or [])
        self._rag_context = rag_context
        # Callback for tool approval — set by channel/CLI for interactive use
        self._approval_callback: callable | None = None

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
                    # Sanitize error message to avoid leaking internals
                    safe_msg = (
                        "Sorry, I encountered an internal error. "
                        "Please try again or contact an administrator."
                    )
                    await self.bus.publish_outbound(
                        OutboundMessage(
                            channel=msg.channel,
                            chat_id=msg.chat_id,
                            content=safe_msg,
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

        # Enrich with RAG context if available
        enriched_content = msg.content
        if self._rag_context:
            try:
                extra = self._rag_context.enrich_prompt(msg.content)
                if extra:
                    enriched_content = f"{extra}\n\n{msg.content}"
            except Exception as e:
                logger.warning(f"RAG enrichment failed: {e}")

        # Build initial messages
        messages = self.context.build_messages(
            history=session.get_history(),
            current_message=enriched_content,
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

                    # Human-in-the-loop approval check
                    if tc.name in self._approval_required:
                        approved = await self._request_approval(tc.name, tc.arguments)
                        if not approved:
                            result = f"Tool '{tc.name}' was rejected by the user."
                            messages = self.context.add_tool_result(
                                messages, tc.id, tc.name, result
                            )
                            continue

                    result = await self.tools.execute(tc.name, tc.arguments)
                    messages = self.context.add_tool_result(messages, tc.id, tc.name, result)
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

    async def process_message_stream(self, msg: InboundMessage) -> AsyncIterator[StreamEvent]:
        """Process a message with streaming — yields StreamEvent for SSE delivery."""
        logger.info(f"Processing streaming message from {msg.channel}:{msg.sender_id}")

        session = self.sessions.get_or_create(msg.session_key)
        messages = self.context.build_messages(
            history=session.get_history(),
            current_message=msg.content,
            channel=msg.channel,
            chat_id=msg.chat_id,
            media=msg.media if msg.media else None,
        )

        iteration = 0
        full_content = ""

        while iteration < self.max_iterations:
            iteration += 1

            # Collect streaming chunks
            text_buffer = ""
            tool_calls_collected: list[dict[str, str]] = []

            async for chunk in self.provider.chat_stream(
                messages=messages,
                tools=self.tools.get_definitions() or None,
                model=self.model,
            ):
                if chunk.is_tool_call:
                    args = chunk.tool_arguments_delta
                    try:
                        parsed_args = json.loads(args)
                    except json.JSONDecodeError:
                        parsed_args = {"raw": args}
                    tool_calls_collected.append(
                        {
                            "id": chunk.tool_call_id,
                            "name": chunk.tool_name,
                            "arguments": args,
                            "parsed": parsed_args,
                        }
                    )
                elif chunk.delta:
                    text_buffer += chunk.delta
                    full_content += chunk.delta
                    yield StreamEvent(
                        channel=msg.channel,
                        chat_id=msg.chat_id,
                        event_type="delta",
                        data=chunk.delta,
                    )

            if tool_calls_collected:
                # Add assistant message with tool calls to context
                tc_dicts = [
                    {
                        "id": tc["id"],
                        "type": "function",
                        "function": {
                            "name": tc["name"],
                            "arguments": tc["arguments"],
                        },
                    }
                    for tc in tool_calls_collected
                ]
                messages = self.context.add_assistant_message(
                    messages, text_buffer or None, tc_dicts
                )

                # Execute each tool call
                for tc in tool_calls_collected:
                    yield StreamEvent(
                        channel=msg.channel,
                        chat_id=msg.chat_id,
                        event_type="tool_start",
                        tool_name=tc["name"],
                        data=json.dumps(tc["parsed"]),
                    )

                    # Approval check
                    if tc["name"] in self._approval_required:
                        approved = await self._request_approval(tc["name"], tc["parsed"])
                        if not approved:
                            result = f"Tool '{tc['name']}' was rejected by the user."
                            messages = self.context.add_tool_result(
                                messages, tc["id"], tc["name"], result
                            )
                            yield StreamEvent(
                                channel=msg.channel,
                                chat_id=msg.chat_id,
                                event_type="tool_result",
                                tool_name=tc["name"],
                                data=result,
                            )
                            continue

                    result = await self.tools.execute(tc["name"], tc["parsed"])
                    messages = self.context.add_tool_result(messages, tc["id"], tc["name"], result)
                    yield StreamEvent(
                        channel=msg.channel,
                        chat_id=msg.chat_id,
                        event_type="tool_result",
                        tool_name=tc["name"],
                        data=result,
                    )
            else:
                # No tool calls — done
                break

        # Save to history
        session.add_message("user", msg.content)
        session.add_message("assistant", full_content or "Processing complete.")
        self.sessions.save(session)

        yield StreamEvent(
            channel=msg.channel,
            chat_id=msg.chat_id,
            event_type="done",
            data=full_content or "Processing complete.",
        )

    async def _request_approval(self, tool_name: str, arguments: dict) -> bool:
        """Request human approval for a tool execution.

        Returns True if approved, False if rejected.
        Falls back to auto-approve if no callback is registered (non-interactive mode).
        """
        if self._approval_callback:
            try:
                return await self._approval_callback(tool_name, arguments)
            except Exception as e:
                logger.warning(f"Approval callback failed, auto-approving: {e}")
                return True
        # Non-interactive mode: auto-approve (log warning)
        logger.warning(
            f"Tool '{tool_name}' requires approval but no approval callback registered. "
            f"Auto-approving in non-interactive mode."
        )
        return True

    def set_approval_callback(self, callback: callable) -> None:
        """Set a callback for tool approval requests.

        The callback should be an async function with signature:
            async def callback(tool_name: str, arguments: dict) -> bool
        """
        self._approval_callback = callback
