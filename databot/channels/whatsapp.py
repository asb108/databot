"""WhatsApp channel integration via the WhatsApp Business Cloud API.

Supports **webhook** mode (receive inbound messages via a Facebook/Meta
webhook) and **notify** mode (send-only – push messages through the API).

Requires an HTTP client (``httpx`` – already a databot dependency).

Configuration example in ``config.yaml``::

    channels:
      whatsapp:
        enabled: true
        phone_number_id: "123456789"
        access_token: ${WHATSAPP_ACCESS_TOKEN}
        verify_token: ${WHATSAPP_VERIFY_TOKEN}   # For webhook verification
"""

from __future__ import annotations

import hmac
import hashlib
from typing import Any

import httpx
from fastapi import APIRouter, Request
from loguru import logger

from databot.channels.base import BaseChannel
from databot.core.bus import InboundMessage, MessageBus, OutboundMessage

# WhatsApp Cloud API base URL
_WA_API_BASE = "https://graph.facebook.com/v21.0"


class WhatsAppChannel(BaseChannel):
    """WhatsApp Business Cloud API integration.

    * Inbound messages arrive via a Meta webhook (``/webhooks/whatsapp``).
    * Outbound messages are sent through the Cloud API ``messages`` endpoint.
    """

    def __init__(
        self,
        bus: MessageBus,
        phone_number_id: str = "",
        access_token: str = "",
        verify_token: str = "",
        app_secret: str = "",
    ):
        super().__init__(bus)
        self._phone_number_id = phone_number_id
        self._access_token = access_token
        self._verify_token = verify_token
        self._app_secret = app_secret
        self._running = False

        # Register outbound handler
        self.bus.on_outbound(self._handle_outbound)

    @property
    def name(self) -> str:
        return "whatsapp"

    async def start(self) -> None:
        self._running = True
        logger.info("WhatsApp channel started")

    async def stop(self) -> None:
        self._running = False
        logger.info("WhatsApp channel stopped")

    async def send(self, chat_id: str, content: str, thread_id: str | None = None) -> None:
        """Send a text message to a WhatsApp number via the Cloud API."""
        if not self._phone_number_id or not self._access_token:
            logger.warning("WhatsApp phone_number_id or access_token not configured")
            return

        url = f"{_WA_API_BASE}/{self._phone_number_id}/messages"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }
        payload: dict[str, Any] = {
            "messaging_product": "whatsapp",
            "to": chat_id,
            "type": "text",
            "text": {"body": content},
        }

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(url, json=payload, headers=headers)
                resp.raise_for_status()
                logger.debug(f"Message sent to WhatsApp user {chat_id}")
        except Exception as e:
            logger.error(f"Failed to send WhatsApp message: {e}")

    async def _handle_outbound(self, msg: OutboundMessage) -> None:
        """Handle outbound messages destined for WhatsApp."""
        if msg.channel == "whatsapp":
            await self.send(msg.chat_id, msg.content, msg.thread_id)

    # ------------------------------------------------------------------
    # FastAPI webhook routes
    # ------------------------------------------------------------------

    def get_fastapi_routes(self) -> APIRouter:
        """Return FastAPI routes for the WhatsApp webhook."""
        router = APIRouter()

        @router.get("/webhooks/whatsapp")
        async def whatsapp_verify(request: Request):
            """Webhook verification challenge (required by Meta)."""
            params = request.query_params
            mode = params.get("hub.mode", "")
            token = params.get("hub.verify_token", "")
            challenge = params.get("hub.challenge", "")

            if mode == "subscribe" and token == self._verify_token:
                logger.info("WhatsApp webhook verified")
                from fastapi.responses import PlainTextResponse

                return PlainTextResponse(challenge)

            from fastapi.responses import JSONResponse

            return JSONResponse({"error": "Verification failed"}, status_code=403)

        @router.post("/webhooks/whatsapp")
        async def whatsapp_webhook(request: Request):
            """Handle incoming WhatsApp messages."""
            body = await request.json()

            # Process each entry / change
            for entry in body.get("entry", []):
                for change in entry.get("changes", []):
                    value = change.get("value", {})
                    messages = value.get("messages", [])

                    for msg in messages:
                        msg_type = msg.get("type", "")
                        sender = msg.get("from", "unknown")

                        if msg_type == "text":
                            text = msg.get("text", {}).get("body", "").strip()
                        else:
                            # For non-text messages, note the type
                            text = f"[Received {msg_type} message]"

                        if not text:
                            continue

                        inbound = InboundMessage(
                            channel="whatsapp",
                            sender_id=sender,
                            chat_id=sender,  # In WhatsApp, chat_id is the phone number
                            content=text,
                        )
                        await self.bus.publish_inbound(inbound)

            return {"status": "ok"}

        return router
