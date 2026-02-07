"""Slack channel integration using the Slack Bolt framework."""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from databot.channels.base import BaseChannel
from databot.core.bus import InboundMessage, MessageBus, OutboundMessage


class SlackChannel(BaseChannel):
    """Slack integration using Slack Bolt for Python.

    Supports both Socket Mode (recommended for development) and HTTP mode.
    Requires ``slack_bolt`` and ``slack_sdk`` packages::

        pip install slack-bolt slack-sdk

    Configuration example in ``config.yaml``::

        channels:
          slack:
            enabled: true
            bot_token: "xoxb-..."
            app_token: "xapp-..."  # Required for Socket Mode
            signing_secret: "..."
    """

    def __init__(
        self,
        bus: MessageBus,
        bot_token: str = "",
        app_token: str = "",
        signing_secret: str = "",
    ):
        super().__init__(bus)
        self._bot_token = bot_token
        self._app_token = app_token
        self._signing_secret = signing_secret
        self._app = None
        self._client = None
        self._running = False

        # Register outbound handler
        self.bus.on_outbound(self._handle_outbound)

    @property
    def name(self) -> str:
        return "slack"

    async def start(self) -> None:
        """Start the Slack channel using Socket Mode."""
        try:
            from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
            from slack_bolt.async_app import AsyncApp
            from slack_sdk.web.async_client import AsyncWebClient
        except ImportError:
            logger.error(
                "slack-bolt and slack-sdk are required for Slack integration. "
                "Install with: pip install slack-bolt slack-sdk"
            )
            return

        self._client = AsyncWebClient(token=self._bot_token)
        self._app = AsyncApp(token=self._bot_token, signing_secret=self._signing_secret)

        # Register message handler
        @self._app.event("message")
        async def handle_message(event: dict, say: Any) -> None:
            text = event.get("text", "").strip()
            if not text:
                return

            # Ignore bot messages
            if event.get("bot_id"):
                return

            user = event.get("user", "unknown")
            channel_id = event.get("channel", "unknown")

            inbound = InboundMessage(
                channel="slack",
                sender_id=user,
                chat_id=channel_id,
                content=text,
            )
            await self.bus.publish_inbound(inbound)

        # Register app_mention handler (when bot is @mentioned)
        @self._app.event("app_mention")
        async def handle_mention(event: dict, say: Any) -> None:
            text = event.get("text", "").strip()
            # Remove the bot mention from the text
            # Typically looks like "<@U1234> some message"
            import re

            text = re.sub(r"<@[A-Z0-9]+>\s*", "", text).strip()
            if not text:
                return

            user = event.get("user", "unknown")
            channel_id = event.get("channel", "unknown")

            inbound = InboundMessage(
                channel="slack",
                sender_id=user,
                chat_id=channel_id,
                content=text,
            )
            await self.bus.publish_inbound(inbound)

        self._running = True

        if self._app_token:
            # Socket Mode (recommended)
            handler = AsyncSocketModeHandler(self._app, self._app_token)
            asyncio.create_task(handler.start_async())
            logger.info("Slack channel started (Socket Mode)")
        else:
            logger.info("Slack channel registered (HTTP mode — mount routes in gateway)")

    async def stop(self) -> None:
        """Stop the Slack channel."""
        self._running = False
        logger.info("Slack channel stopped")

    async def send(self, chat_id: str, content: str, thread_id: str | None = None) -> None:
        """Send a message to a Slack channel or thread."""
        if not self._client:
            logger.warning("Slack client not initialized")
            return

        try:
            kwargs: dict[str, Any] = {
                "channel": chat_id,
                "text": content,
                "mrkdwn": True,
            }
            if thread_id:
                kwargs["thread_ts"] = thread_id

            await self._client.chat_postMessage(**kwargs)
            logger.debug(f"Message sent to Slack channel {chat_id}")
        except Exception as e:
            logger.error(f"Failed to send to Slack: {e}")

    async def _handle_outbound(self, msg: OutboundMessage) -> None:
        """Handle outbound messages destined for Slack."""
        if msg.channel == "slack":
            await self.send(msg.chat_id, msg.content, msg.thread_id)
