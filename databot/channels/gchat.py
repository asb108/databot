"""Google Chat channel: webhook and app modes."""

from __future__ import annotations

from typing import Any

import httpx
from loguru import logger

from databot.channels.base import BaseChannel
from databot.core.bus import InboundMessage, MessageBus, OutboundMessage


class GChatChannel(BaseChannel):
    """Google Chat integration supporting webhook (send-only) and app (bidirectional) modes."""

    def __init__(
        self,
        bus: MessageBus,
        mode: str = "webhook",
        webhook_url: str = "",
    ):
        super().__init__(bus)
        self._mode = mode
        self._webhook_url = webhook_url
        self._running = False

        # Register outbound handler
        self.bus.on_outbound(self._handle_outbound)

    @property
    def name(self) -> str:
        return "gchat"

    async def start(self) -> None:
        self._running = True
        logger.info(f"Google Chat channel started (mode={self._mode})")

    async def stop(self) -> None:
        self._running = False
        logger.info("Google Chat channel stopped")

    async def send(self, chat_id: str, content: str, thread_id: str | None = None) -> None:
        """Send a message to Google Chat via webhook."""
        if not self._webhook_url:
            logger.warning("Google Chat webhook URL not configured")
            return

        payload: dict[str, Any] = {"text": content}
        if thread_id:
            payload["thread"] = {"threadKey": thread_id}
            payload["messageReplyOption"] = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    self._webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
                logger.debug("Message sent to Google Chat")
        except Exception as e:
            logger.error(f"Failed to send to Google Chat: {e}")

    async def _handle_outbound(self, msg: OutboundMessage) -> None:
        """Handle outbound messages destined for Google Chat."""
        if msg.channel == "gchat":
            await self.send(msg.chat_id, msg.content, msg.thread_id)

    def get_fastapi_routes(self):
        """Return FastAPI routes for Google Chat App mode (bidirectional)."""
        from fastapi import APIRouter, Request

        router = APIRouter()

        @router.post("/webhooks/gchat")
        async def gchat_webhook(request: Request):
            """Handle incoming Google Chat events."""
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

                # Publish inbound message to the bus
                inbound = InboundMessage(
                    channel="gchat",
                    sender_id=sender.get("name", "unknown"),
                    chat_id=space.get("name", "unknown"),
                    content=text,
                )
                await self.bus.publish_inbound(inbound)

                return {"text": "Processing your request... I'll post the response shortly."}

            elif event_type == "ADDED_TO_SPACE":
                return {
                    "text": (
                        "Thanks for adding me! I'm databot, your data platform assistant. "
                        "Ask me anything about your pipelines, tables, or data quality."
                    )
                }

            return {}

        return router
