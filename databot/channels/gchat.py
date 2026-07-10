"""Google Chat channel: webhook and app modes."""

import asyncio
from typing import Any

import httpx
from fastapi import APIRouter, Request
from loguru import logger

from databot.channels.base import BaseChannel
from databot.channels.chunking import split_message
from databot.core.bus import InboundMessage, MessageBus, OutboundMessage
from databot.core.error_ux import render_user_error


class GChatChannel(BaseChannel):
    """Google Chat integration supporting webhook (send-only) and app (bidirectional) modes."""

    MAX_MESSAGE_LENGTH = 4096

    def __init__(
        self,
        bus: MessageBus,
        mode: str = "webhook",
        webhook_url: str = "",
        cfg_ref: Any | None = None,
        loop_ref: Any | None = None,
    ):
        super().__init__(bus)
        self._mode = mode
        self._webhook_url = webhook_url
        self._running = False
        self._cfg_ref = cfg_ref  # live reference to config for runtime updates
        self._loop_ref = loop_ref  # AgentLoop ref for synchronous processing

        # Register outbound handler
        self.bus.on_outbound(self._handle_outbound)

    @property
    def name(self) -> str:
        return "gchat"

    @property
    def webhook_url(self) -> str:
        """Return live webhook URL — prefer runtime config if available."""
        if self._cfg_ref:
            return self._cfg_ref.channels.gchat.webhook_url or self._webhook_url
        return self._webhook_url

    @property
    def is_enabled(self) -> bool:
        """Check if channel is currently enabled (supports runtime toggle)."""
        if self._cfg_ref:
            return self._cfg_ref.channels.gchat.enabled
        return self._running

    async def start(self) -> None:
        self._running = True
        logger.info(f"Google Chat channel started (mode={self._mode})")

    async def stop(self) -> None:
        self._running = False
        logger.info("Google Chat channel stopped")

    async def send(self, chat_id: str, content: str, thread_id: str | None = None) -> None:
        """Send a message to Google Chat via webhook."""
        url = self.webhook_url
        if not url:
            logger.warning("Google Chat webhook URL not configured")
            return

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                for chunk in split_message(content, self.MAX_MESSAGE_LENGTH):
                    payload: dict[str, Any] = {"text": chunk}
                    if thread_id:
                        payload["thread"] = {"threadKey": thread_id}
                        payload["messageReplyOption"] = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"
                    resp = await client.post(
                        url,
                        json=payload,
                        headers={"Content-Type": "application/json"},
                    )
                    resp.raise_for_status()
            logger.debug("Message sent to Google Chat")
        except Exception as e:
            logger.error(f"Failed to send to Google Chat: {e}")

    async def _handle_outbound(self, msg: OutboundMessage) -> None:
        """Handle outbound messages destined for Google Chat."""
        if msg.channel == "gchat" and self.is_enabled:
            await self.send(msg.chat_id, msg.content, msg.thread_id)

    def get_fastapi_routes(self):
        """Return FastAPI routes for Google Chat webhook/app events."""
        router = APIRouter()

        @router.post("/webhooks/gchat")
        async def gchat_webhook(request: Request):
            """Handle incoming Google Chat events.

            Google Chat expects a synchronous JSON response within 30 seconds.
            We process the message through the agent loop inline and return
            the LLM response directly.
            """
            if not self.is_enabled:
                from fastapi.responses import JSONResponse

                return JSONResponse(
                    {"error": "Google Chat channel is not enabled"},
                    status_code=403,
                )

            body = await request.json()
            event_type = body.get("type", "")

            if event_type == "MESSAGE":
                message = body.get("message", {})
                sender = message.get("sender", {})
                space = body.get("space", {})

                text = message.get("argumentText", "") or message.get("text", "")
                text = text.strip()

                if not text:
                    return {"text": "I didn't receive any message content."}

                inbound = InboundMessage(
                    channel="gchat",
                    sender_id=sender.get("name", "unknown"),
                    chat_id=space.get("name", "unknown"),
                    content=text,
                )

                # Process synchronously via agent loop (GChat allows 30s)
                if self._loop_ref:
                    try:
                        outbound = await asyncio.wait_for(
                            self._loop_ref.process_message(inbound),
                            timeout=28.0,  # leave 2s margin for GChat's 30s limit
                        )
                        if outbound and outbound.content:
                            return {"text": outbound.content}
                        return {"text": "I processed your request but had no output."}
                    except asyncio.TimeoutError:
                        logger.warning("GChat: agent loop timed out (28s)")
                        return {
                            "text": "Sorry, your request is taking too long. Please try a simpler query."
                        }
                    except Exception as e:
                        logger.error(f"GChat: error processing message: {e}")
                        return {"text": render_user_error(e)}
                else:
                    # Fallback: publish to bus (async, response via webhook)
                    await self.bus.publish_inbound(inbound)
                    return {"text": "Processing your request..."}

            elif event_type == "ADDED_TO_SPACE":
                return {
                    "text": (
                        "Thanks for adding me! I'm databot, your data platform assistant. "
                        "Ask me anything about your pipelines, tables, or data quality."
                    )
                }

            return {}

        return router
