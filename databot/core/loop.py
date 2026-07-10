"""Agent loop: the core processing engine."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, AsyncIterator

from loguru import logger

from databot.core.bus import InboundMessage, MessageBus, OutboundMessage, StreamEvent
from databot.core.context import ContextBuilder
from databot.core.error_ux import render_user_error
from databot.core.tokens import fit_messages_to_budget, get_context_window
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
        usage_tracker: Any | None = None,
        compactor: Any | None = None,
        context_budget_ratio: float = 0.75,
        slash_handler: Any | None = None,
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
        self._usage = usage_tracker
        self._compactor = compactor
        self._slash = slash_handler
        # Max input tokens to allow per LLM call (fraction of the model window).
        self._input_budget = int(get_context_window(self.model) * context_budget_ratio)
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
                    safe_msg = render_user_error(e)
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

    async def _maybe_compact(self, session: Any) -> None:
        """Summarize old turns into the session summary if history is large."""
        if not self._compactor:
            return
        try:
            result = await self._compactor.compact(
                session.get_history(), session.summary, self.model
            )
        except Exception as e:
            logger.warning(f"Compaction error (ignored): {e}")
            return
        if result:
            recent, summary = result
            session.replace_history(recent)
            session.set_summary(summary)
            self.sessions.save(session)

    def _budget_response(self, msg: InboundMessage) -> OutboundMessage | None:
        """Return a budget-exceeded reply if the session is over budget, else None."""
        if self._usage and self._usage.over_budget(msg.session_key):
            return OutboundMessage(
                channel=msg.channel,
                chat_id=msg.chat_id,
                content=(
                    "This conversation has reached its configured LLM cost budget "
                    f"(${self._usage.session_budget_usd:.2f}). Start a new session "
                    "or raise `agent.session_cost_budget_usd` to continue."
                ),
            )
        return None

    def _record_usage(self, response: Any, session_key: str) -> None:
        from databot.core.cost import record_llm_usage

        record_llm_usage(self._usage, self.model, response, session_key)

    async def process_message(self, msg: InboundMessage) -> OutboundMessage | None:
        """Process a single inbound message through the LLM agent loop."""
        logger.info(f"Processing message from {msg.channel}:{msg.sender_id}")

        from databot.observability.metrics import get_metrics

        metrics = get_metrics()
        started = asyncio.get_event_loop().time()

        # Deterministic slash commands bypass the LLM entirely.
        if self._slash:
            handled = await self._slash.handle(msg.content, msg.session_key)
            if handled is not None:
                metrics.message(msg.channel, asyncio.get_event_loop().time() - started)
                return OutboundMessage(channel=msg.channel, chat_id=msg.chat_id, content=handled)

        # Get or create session
        session = self.sessions.get_or_create(msg.session_key)

        # Enforce per-session cost budget before spending more.
        budget_reply = self._budget_response(msg)
        if budget_reply:
            metrics.message(msg.channel, asyncio.get_event_loop().time() - started)
            return budget_reply

        # Compact old history into a running summary if needed.
        await self._maybe_compact(session)

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
            summary=session.summary,
        )

        # Agent loop: LLM <-> tool execution
        iteration = 0
        final_content = None

        while iteration < self.max_iterations:
            iteration += 1

            # Keep the request within the model's context window.
            messages, dropped = fit_messages_to_budget(messages, self.model, self._input_budget)
            if dropped:
                logger.debug(f"Trimmed {dropped} message(s) to fit context budget")

            # Call LLM
            response = await self.provider.chat(
                messages=messages,
                tools=self.tools.get_definitions() or None,
                model=self.model,
            )
            self._record_usage(response, msg.session_key)

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

        metrics.message(msg.channel, asyncio.get_event_loop().time() - started)

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
        """Process a message with streaming — yields StreamEvent for SSE delivery.

        Never raises to the caller: on failure it yields a single sanitized
        ``error`` event (via ``render_user_error``) and stops.
        """
        try:
            async for event in self._process_message_stream_inner(msg):
                yield event
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            yield StreamEvent(
                channel=msg.channel,
                chat_id=msg.chat_id,
                event_type="error",
                data=render_user_error(e),
            )

    async def _process_message_stream_inner(
        self, msg: InboundMessage
    ) -> AsyncIterator[StreamEvent]:
        logger.info(f"Processing streaming message from {msg.channel}:{msg.sender_id}")

        # Deterministic slash commands bypass the LLM entirely.
        if self._slash:
            handled = await self._slash.handle(msg.content, msg.session_key)
            if handled is not None:
                yield StreamEvent(
                    channel=msg.channel,
                    chat_id=msg.chat_id,
                    event_type="done",
                    data=handled,
                )
                return

        session = self.sessions.get_or_create(msg.session_key)

        # Enforce per-session cost budget before spending more.
        budget_reply = self._budget_response(msg)
        if budget_reply:
            yield StreamEvent(
                channel=msg.channel,
                chat_id=msg.chat_id,
                event_type="done",
                data=budget_reply.content,
            )
            return

        await self._maybe_compact(session)
        messages = self.context.build_messages(
            history=session.get_history(),
            current_message=msg.content,
            channel=msg.channel,
            chat_id=msg.chat_id,
            media=msg.media if msg.media else None,
            summary=session.summary,
        )

        iteration = 0
        full_content = ""

        while iteration < self.max_iterations:
            iteration += 1

            # Keep the request within the model's context window.
            messages, _ = fit_messages_to_budget(messages, self.model, self._input_budget)

            # Collect streaming chunks
            text_buffer = ""
            tool_calls_collected: list[dict[str, str]] = []

            async for chunk in self.provider.chat_stream(
                messages=messages,
                tools=self.tools.get_definitions() or None,
                model=self.model,
            ):
                if getattr(chunk, "usage", None):
                    self._record_usage(chunk, msg.session_key)
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
