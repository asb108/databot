"""Discord channel integration using discord.py."""

from __future__ import annotations

from loguru import logger

from databot.channels.base import BaseChannel
from databot.core.bus import InboundMessage, MessageBus, OutboundMessage


class DiscordChannel(BaseChannel):
    """Discord integration using discord.py.

    Responds to messages that start with a command prefix (default ``!``)
    or when the bot is mentioned.

    Requires ``discord.py``::

        pip install discord.py

    Configuration example in ``config.yaml``::

        channels:
          discord:
            enabled: true
            bot_token: "..."
            command_prefix: "!"
    """

    # Discord has a 2000 char message limit
    MAX_MESSAGE_LENGTH = 2000

    def __init__(
        self,
        bus: MessageBus,
        bot_token: str = "",
        command_prefix: str = "!",
    ):
        super().__init__(bus)
        self._bot_token = bot_token
        self._command_prefix = command_prefix
        self._client = None
        self._running = False

        # Register outbound handler
        self.bus.on_outbound(self._handle_outbound)

    @property
    def name(self) -> str:
        return "discord"

    async def start(self) -> None:
        """Start the Discord bot."""
        try:
            import discord
        except ImportError:
            logger.error(
                "discord.py is required for Discord integration. "
                "Install with: pip install discord.py"
            )
            return

        intents = discord.Intents.default()
        intents.message_content = True

        self._client = discord.Client(intents=intents)
        channel_ref = self  # Avoid closure issues

        @self._client.event
        async def on_ready():
            logger.info(f"Discord bot connected as {self._client.user}")

        @self._client.event
        async def on_message(message: discord.Message):
            # Ignore own messages
            if message.author == self._client.user:
                return

            content = message.content.strip()

            # Respond to command prefix or mentions
            is_command = content.startswith(channel_ref._command_prefix)
            is_mention = self._client.user in message.mentions if self._client.user else False

            if not is_command and not is_mention:
                return

            # Strip prefix or mention
            if is_command:
                text = content[len(channel_ref._command_prefix) :].strip()
            else:
                text = content.replace(f"<@{self._client.user.id}>", "").strip()

            if not text:
                return

            inbound = InboundMessage(
                channel="discord",
                sender_id=str(message.author.id),
                chat_id=str(message.channel.id),
                content=text,
            )
            await channel_ref.bus.publish_inbound(inbound)

        self._running = True
        # Run in background — discord.py manages its own event loop
        await self._client.start(self._bot_token)

    async def stop(self) -> None:
        """Stop the Discord bot."""
        self._running = False
        if self._client:
            await self._client.close()
        logger.info("Discord channel stopped")

    async def send(self, chat_id: str, content: str, thread_id: str | None = None) -> None:
        """Send a message to a Discord channel."""
        if not self._client:
            logger.warning("Discord client not initialized")
            return

        try:
            channel = self._client.get_channel(int(chat_id))
            if not channel:
                channel = await self._client.fetch_channel(int(chat_id))

            # Split long messages (Discord 2000 char limit)
            chunks = self._split_message(content)
            for chunk in chunks:
                await channel.send(chunk)

            logger.debug(f"Message sent to Discord channel {chat_id}")
        except Exception as e:
            logger.error(f"Failed to send to Discord: {e}")

    async def _handle_outbound(self, msg: OutboundMessage) -> None:
        """Handle outbound messages destined for Discord."""
        if msg.channel == "discord":
            await self.send(msg.chat_id, msg.content, msg.thread_id)

    @classmethod
    def _split_message(cls, content: str) -> list[str]:
        """Split a message into chunks that fit Discord's limit."""
        if len(content) <= cls.MAX_MESSAGE_LENGTH:
            return [content]

        chunks = []
        while content:
            if len(content) <= cls.MAX_MESSAGE_LENGTH:
                chunks.append(content)
                break

            # Try to split at a newline
            split_at = content.rfind("\n", 0, cls.MAX_MESSAGE_LENGTH)
            if split_at == -1:
                split_at = cls.MAX_MESSAGE_LENGTH

            chunks.append(content[:split_at])
            content = content[split_at:].lstrip("\n")

        return chunks
