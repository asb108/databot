"""Telegram channel integration via the Bot API.

Supports two modes:

* **webhook** – Telegram pushes updates to ``/webhooks/telegram``
  (recommended for production behind HTTPS).
* **polling** – The bot long-polls ``getUpdates``
  (convenient for local development, no HTTPS required).

Requires ``httpx`` (already a databot dependency).

Configuration example in ``config.yaml``::

    channels:
      telegram:
        enabled: true
        bot_token: ${TELEGRAM_BOT_TOKEN}
        mode: polling            # "polling" or "webhook"
        webhook_url: ""          # Public URL for webhook mode
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
from fastapi import APIRouter, Request
from loguru import logger

from databot.channels.base import BaseChannel
from databot.core.bus import InboundMessage, MessageBus, OutboundMessage

_TG_API_BASE = "https://api.telegram.org"


class TelegramChannel(BaseChannel):
    """Telegram Bot API integration with webhook and polling modes."""

    def __init__(
        self,
        bus: MessageBus,
        bot_token: str = "",
        mode: str = "polling",
        webhook_url: str = "",
    ):
        super().__init__(bus)
        self._bot_token = bot_token
        self._mode = mode
        self._webhook_url = webhook_url
        self._running = False
        self._poll_task: asyncio.Task | None = None

        # Register outbound handler
        self.bus.on_outbound(self._handle_outbound)

    @property
    def name(self) -> str:
        return "telegram"

    @property
    def _api_url(self) -> str:
        return f"{_TG_API_BASE}/bot{self._bot_token}"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if not self._bot_token:
            logger.warning("Telegram bot_token not configured — channel inactive")
            return

        self._running = True

        if self._mode == "webhook":
            await self._set_webhook()
            logger.info("Telegram channel started (webhook mode)")
        else:
            # Start long-polling in a background task
            self._poll_task = asyncio.create_task(self._poll_loop())
            logger.info("Telegram channel started (polling mode)")

    async def stop(self) -> None:
        self._running = False
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        logger.info("Telegram channel stopped")

    # ------------------------------------------------------------------
    # Sending
    # ------------------------------------------------------------------

    async def send(self, chat_id: str, content: str, thread_id: str | None = None) -> None:
        """Send a text message via the Telegram Bot API."""
        if not self._bot_token:
            logger.warning("Telegram bot_token not configured")
            return

        url = f"{self._api_url}/sendMessage"
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": content,
            "parse_mode": "Markdown",
        }
        if thread_id:
            payload["reply_to_message_id"] = thread_id

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()
                logger.debug(f"Message sent to Telegram chat {chat_id}")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")

    async def _handle_outbound(self, msg: OutboundMessage) -> None:
        if msg.channel == "telegram":
            await self.send(msg.chat_id, msg.content, msg.thread_id)

    # ------------------------------------------------------------------
    # Webhook helpers
    # ------------------------------------------------------------------

    async def _set_webhook(self) -> None:
        """Register the webhook URL with Telegram."""
        if not self._webhook_url:
            logger.warning("Telegram webhook_url not configured — cannot register webhook")
            return

        url = f"{self._api_url}/setWebhook"
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(url, json={"url": self._webhook_url})
                resp.raise_for_status()
                logger.info(f"Telegram webhook set to {self._webhook_url}")
        except Exception as e:
            logger.error(f"Failed to set Telegram webhook: {e}")

    # ------------------------------------------------------------------
    # Long-polling
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        """Long-poll Telegram for updates."""
        offset = 0

        while self._running:
            try:
                url = f"{self._api_url}/getUpdates"
                params: dict[str, Any] = {"timeout": 30, "offset": offset}

                async with httpx.AsyncClient(timeout=45) as client:
                    resp = await client.get(url, params=params)
                    resp.raise_for_status()
                    data = resp.json()

                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    await self._process_update(update)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Telegram polling error: {e}")
                await asyncio.sleep(5)

    async def _process_update(self, update: dict) -> None:
        """Extract text from a Telegram update and publish to the bus."""
        message = update.get("message") or update.get("edited_message")
        if not message:
            return

        text = message.get("text", "").strip()
        if not text:
            return

        chat = message.get("chat", {})
        sender = message.get("from", {})

        inbound = InboundMessage(
            channel="telegram",
            sender_id=str(sender.get("id", "unknown")),
            chat_id=str(chat.get("id", "unknown")),
            content=text,
        )
        await self.bus.publish_inbound(inbound)

    # ------------------------------------------------------------------
    # FastAPI webhook routes
    # ------------------------------------------------------------------

    def get_fastapi_routes(self) -> APIRouter:
        """Return FastAPI routes for Telegram webhook mode."""
        router = APIRouter()

        @router.post("/webhooks/telegram")
        async def telegram_webhook(request: Request):
            """Handle incoming Telegram updates via webhook."""
            update = await request.json()
            await self._process_update(update)
            return {"ok": True}

        return router
