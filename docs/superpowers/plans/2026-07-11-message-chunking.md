# Message Chunking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split outbound replies that exceed a channel's length limit into multiple in-limit messages (capped, with a truncation notice), so Telegram/WhatsApp/Slack/Google Chat stop silently dropping long replies.

**Architecture:** One pure `split_message(content, limit, *, max_chunks)` in a new `databot/channels/chunking.py` (newline-preferred + hard-cut, cap with truncation notice). Each channel's `send()` loops over it; Discord's private `_split_message` is replaced by the shared function.

**Tech Stack:** Python 3, `httpx` (Telegram/WhatsApp/GChat), `slack_sdk` async client (Slack), `discord.py` (Discord). pytest (`python3 -m pytest`), ruff. Interpreter is `python3` (NOT `python`).

## Global Constraints

- Interpreter is `python3`. Tests: `python3 -m pytest`. Lint: `ruff check databot/` and `ruff format --check databot/` (CI lints `databot/` only).
- `split_message` guarantees: every returned chunk is non-empty and `len(chunk) <= limit`; at most `max_chunks` chunks; when capped, the last chunk ends with the truncation notice. Precondition: `limit >= len(_TRUNCATION_NOTICE)` (all real channel limits are ≥2000).
- Per-channel `MAX_MESSAGE_LENGTH`: Discord 2000 (existing), Telegram 4096, WhatsApp 4096, Google Chat 4096, Slack 40000.
- Each channel's existing `send()` `try/except` stays wrapped around the whole chunk loop — a mid-chunk failure logs and aborts remaining chunks (same as today's single-send handling). `thread_id`/reply params apply to each chunk uniformly.
- Do NOT change `BaseChannel`'s abstract interface. Do NOT touch the GChat synchronous webhook-return path (only its async `send()`).
- Baseline: **512** tests passing. Keep every existing test green.
- Local-only: branch `feat/message-chunking` off `main`; ff-merge to `main` at the end; push to `origin` only on explicit request.

---

### Task 1: The `split_message` chunking module

**Files:**
- Create: `databot/channels/chunking.py`
- Test: `tests/test_chunking.py`

**Interfaces:**
- Produces: `split_message(content: str, limit: int, *, max_chunks: int = MAX_CHUNKS) -> list[str]` and `MAX_CHUNKS: int` in `databot/channels/chunking.py`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_chunking.py`:

```python
"""Unit tests for message chunking."""

from __future__ import annotations

from databot.channels.chunking import MAX_CHUNKS, split_message


def test_short_content_one_chunk():
    assert split_message("hello", 100) == ["hello"]


def test_exact_limit_one_chunk():
    s = "a" * 100
    assert split_message(s, 100) == [s]


def test_empty_content():
    assert split_message("", 100) == [""]


def test_over_limit_splits_each_within_limit():
    s = "a" * 250  # no newlines
    chunks = split_message(s, 100)
    assert len(chunks) == 3
    assert all(len(c) <= 100 for c in chunks)
    assert all(c for c in chunks)  # none empty
    assert "".join(chunks) == s  # nothing lost when there are no newlines


def test_prefers_newline_boundary():
    content = "\n".join(["x" * 40, "y" * 40, "z" * 40])  # 122 chars, limit 50
    chunks = split_message(content, 50)
    assert chunks == ["x" * 40, "y" * 40, "z" * 40]


def test_hard_cut_long_line_without_newline():
    chunks = split_message("a" * 120, 50)
    assert chunks == ["a" * 50, "a" * 50, "a" * 20]


def test_leading_newline_no_empty_chunk():
    chunks = split_message("\n" + "a" * 120, 50)
    assert all(c for c in chunks)  # no empty chunk from the index-0 newline
    assert all(len(c) <= 50 for c in chunks)


def test_cap_truncates_with_notice():
    content = "\n".join("x" * 40 for _ in range(20))  # ~20 chunks at limit 50
    chunks = split_message(content, 50, max_chunks=3)
    assert len(chunks) == 3
    assert chunks[-1].endswith("(response truncated)")
    assert all(len(c) <= 50 for c in chunks)


def test_default_max_chunks_is_ten():
    content = "\n".join("x" * 40 for _ in range(50))
    chunks = split_message(content, 50)
    assert len(chunks) == MAX_CHUNKS
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_chunking.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'databot.channels.chunking'`

- [ ] **Step 3: Write the implementation**

Create `databot/channels/chunking.py`:

```python
"""Message chunking for channel outbound sends.

Channels have per-platform length limits; a reply over the limit is rejected by the
platform API and (inside each channel's ``send()`` try/except) silently dropped.
``split_message`` splits an over-limit reply into in-limit chunks, preferring newline
boundaries, with a cap so a runaway reply can't flood a channel.
"""

from __future__ import annotations

MAX_CHUNKS = 10
_TRUNCATION_NOTICE = "\n\n…(response truncated)"


def split_message(content: str, limit: int, *, max_chunks: int = MAX_CHUNKS) -> list[str]:
    """Split ``content`` into chunks no longer than ``limit`` characters.

    Prefers splitting on newlines within the limit; hard-cuts a line that has no
    newline within the limit. Returns at most ``max_chunks`` chunks; if more would be
    needed, the last kept chunk is marked with a truncation notice. Every returned
    chunk is non-empty and ``len(chunk) <= limit``. Precondition: ``limit`` is at least
    ``len(_TRUNCATION_NOTICE)`` (true for every real channel limit).
    """
    if len(content) <= limit:
        return [content]

    chunks: list[str] = []
    remaining = content
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        split_at = remaining.rfind("\n", 0, limit)
        if split_at <= 0:  # no newline, or one at index 0 (would yield an empty chunk)
            split_at = limit
        chunks.append(remaining[:split_at])
        remaining = remaining[split_at:].lstrip("\n")

    if len(chunks) <= max_chunks:
        return chunks

    kept = chunks[:max_chunks]
    last = kept[-1]
    if len(last) + len(_TRUNCATION_NOTICE) > limit:
        last = last[: limit - len(_TRUNCATION_NOTICE)]
    kept[-1] = last + _TRUNCATION_NOTICE
    return kept
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_chunking.py -q`
Expected: PASS (9 tests)

- [ ] **Step 5: Lint**

Run: `ruff check databot/ && ruff format --check databot/`
Expected: no errors. If format flags the new file, run `ruff format databot/channels/chunking.py`.

- [ ] **Step 6: Commit**

```bash
git add databot/channels/chunking.py tests/test_chunking.py
git commit -m "feat: add split_message chunking helper for channels"
```

---

### Task 2: Wire chunking into all five channels

**Files:**
- Modify: `databot/channels/discord.py`, `databot/channels/telegram.py`, `databot/channels/whatsapp.py`, `databot/channels/gchat.py`, `databot/channels/slack.py`
- Test: `tests/test_channels.py` (append send-chunking tests)

**Interfaces:**
- Consumes: `split_message` from Task 1 (`from databot.channels.chunking import split_message`).

**Before writing tests:** each channel constructor differs. READ each channel's `__init__` and confirm the kwargs used to instantiate it in the tests below (e.g. `SlackChannel`, `DiscordChannel`). If a constructor kwarg differs from what a test uses, adjust the instantiation to match the real signature — keep the assertions unchanged. The tests only need the channel object plus a mocked client/transport.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_channels.py`. First add this shared recording fake near the top of the file (after the imports):

```python
class _RecordingHTTPClient:
    """Fake httpx.AsyncClient context manager that records POST kwargs."""

    def __init__(self, calls):
        self._calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, **kwargs):
        self._calls.append(kwargs)

        class _Resp:
            def raise_for_status(self):
                return None

        return _Resp()
```

Then append this test class at the end of the file:

```python
class TestChannelChunking:
    @pytest.mark.asyncio
    async def test_telegram_send_chunks_long_message(self, monkeypatch):
        from databot.channels import telegram as tg_mod
        from databot.channels.telegram import TelegramChannel

        calls: list = []
        monkeypatch.setattr(
            tg_mod.httpx, "AsyncClient", lambda *a, **k: _RecordingHTTPClient(calls)
        )
        channel = TelegramChannel(bus=MessageBus(), bot_token="fake:token")
        long_content = "\n".join("x" * 4000 for _ in range(3))  # ~12000 chars > 4096
        await channel.send("123", long_content)

        assert len(calls) >= 2
        for c in calls:
            assert len(c["json"]["text"]) <= TelegramChannel.MAX_MESSAGE_LENGTH

    @pytest.mark.asyncio
    async def test_whatsapp_send_chunks_long_message(self, monkeypatch):
        from databot.channels import whatsapp as wa_mod
        from databot.channels.whatsapp import WhatsAppChannel

        calls: list = []
        monkeypatch.setattr(
            wa_mod.httpx, "AsyncClient", lambda *a, **k: _RecordingHTTPClient(calls)
        )
        channel = WhatsAppChannel(
            bus=MessageBus(), phone_number_id="123", access_token="tok"
        )
        long_content = "\n".join("x" * 4000 for _ in range(3))
        await channel.send("628123", long_content)

        assert len(calls) >= 2
        for c in calls:
            assert len(c["json"]["text"]["body"]) <= WhatsAppChannel.MAX_MESSAGE_LENGTH

    @pytest.mark.asyncio
    async def test_gchat_send_chunks_long_message(self, monkeypatch):
        from databot.channels import gchat as gc_mod
        from databot.channels.gchat import GChatChannel

        calls: list = []
        monkeypatch.setattr(
            gc_mod.httpx, "AsyncClient", lambda *a, **k: _RecordingHTTPClient(calls)
        )
        channel = GChatChannel(bus=MessageBus(), webhook_url="https://example.com/hook")
        long_content = "\n".join("x" * 4000 for _ in range(3))
        await channel.send("space", long_content)

        assert len(calls) >= 2
        for c in calls:
            assert len(c["json"]["text"]) <= GChatChannel.MAX_MESSAGE_LENGTH

    @pytest.mark.asyncio
    async def test_slack_send_chunks_long_message(self):
        from unittest.mock import AsyncMock

        from databot.channels.slack import SlackChannel

        channel = SlackChannel(bus=MessageBus(), bot_token="xoxb-fake")
        channel._client = AsyncMock()
        long_content = "y" * 90000  # > 40000, no newlines -> hard-cut
        await channel.send("C123", long_content)

        posts = channel._client.chat_postMessage.call_args_list
        assert len(posts) >= 2
        for call in posts:
            assert len(call.kwargs["text"]) <= SlackChannel.MAX_MESSAGE_LENGTH

    @pytest.mark.asyncio
    async def test_discord_send_chunks_long_message(self):
        pytest.importorskip("discord")
        from unittest.mock import AsyncMock, MagicMock

        from databot.channels.discord import DiscordChannel

        channel = DiscordChannel(bus=MessageBus(), bot_token="fake")
        fake_channel = MagicMock()
        fake_channel.send = AsyncMock()
        channel._client = MagicMock()
        channel._client.get_channel = MagicMock(return_value=fake_channel)

        long_content = "a" * 5000  # > 2000 -> 3 chunks
        await channel.send("123456", long_content)

        posts = fake_channel.send.call_args_list
        assert len(posts) == 3
        for call in posts:
            assert len(call.args[0]) <= DiscordChannel.MAX_MESSAGE_LENGTH
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m pytest tests/test_channels.py::TestChannelChunking -q`
Expected: FAIL — channels have no `MAX_MESSAGE_LENGTH` (except Discord) and don't chunk yet (Telegram/WhatsApp/GChat currently make exactly one POST regardless of length, so `len(calls) >= 2` fails; `AttributeError` on the missing constant for others).

- [ ] **Step 3: Discord — swap to the shared splitter**

In `databot/channels/discord.py`, add the import near the other `databot` imports:

```python
from databot.channels.chunking import split_message
```

Replace the send body's split section (currently `chunks = self._split_message(content)` then the loop) with:

```python
            for chunk in split_message(content, self.MAX_MESSAGE_LENGTH):
                await channel.send(chunk)
```

Then DELETE the `_split_message` classmethod entirely (the `@classmethod def _split_message(cls, content): ...` block). Keep `MAX_MESSAGE_LENGTH = 2000`.

- [ ] **Step 4: Telegram — loop over chunks with one client**

In `databot/channels/telegram.py`, add `from databot.channels.chunking import split_message` with the other imports, and add `MAX_MESSAGE_LENGTH = 4096` as a class attribute on `TelegramChannel` (next to other class-level constants). Replace the body of `send()` (after the `if not self.bot_token:` guard) with:

```python
        url = f"{self._api_url}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                for chunk in split_message(content, self.MAX_MESSAGE_LENGTH):
                    payload: dict[str, Any] = {
                        "chat_id": chat_id,
                        "text": chunk,
                        "parse_mode": "Markdown",
                    }
                    if thread_id:
                        payload["reply_to_message_id"] = thread_id
                    resp = await client.post(url, json=payload)
                    resp.raise_for_status()
            logger.debug(f"Message sent to Telegram chat {chat_id}")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
```

- [ ] **Step 5: WhatsApp — loop over chunks with one client**

In `databot/channels/whatsapp.py`, add the `split_message` import and `MAX_MESSAGE_LENGTH = 4096` as a `WhatsAppChannel` class attribute. Replace the body of `send()` (after the config guard) with:

```python
        url = f"{_WA_API_BASE}/{self._phone_number_id}/messages"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                for chunk in split_message(content, self.MAX_MESSAGE_LENGTH):
                    payload: dict[str, Any] = {
                        "messaging_product": "whatsapp",
                        "to": chat_id,
                        "type": "text",
                        "text": {"body": chunk},
                    }
                    resp = await client.post(url, json=payload, headers=headers)
                    resp.raise_for_status()
            logger.debug(f"Message sent to WhatsApp user {chat_id}")
        except Exception as e:
            logger.error(f"Failed to send WhatsApp message: {e}")
```

- [ ] **Step 6: Google Chat — loop over chunks with one client**

In `databot/channels/gchat.py`, add the `split_message` import and `MAX_MESSAGE_LENGTH = 4096` as a `GChatChannel` class attribute. Replace the body of `send()` (after the `if not url:` guard) with:

```python
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
```

- [ ] **Step 7: Slack — loop over chunks**

In `databot/channels/slack.py`, add the `split_message` import and `MAX_MESSAGE_LENGTH = 40000` as a `SlackChannel` class attribute. Replace the body of `send()` (after the `if not self._client:` guard) with:

```python
        try:
            for chunk in split_message(content, self.MAX_MESSAGE_LENGTH):
                kwargs: dict[str, Any] = {
                    "channel": chat_id,
                    "text": chunk,
                    "mrkdwn": True,
                }
                if thread_id:
                    kwargs["thread_ts"] = thread_id
                await self._client.chat_postMessage(**kwargs)
            logger.debug(f"Message sent to Slack channel {chat_id}")
        except Exception as e:
            logger.error(f"Failed to send to Slack: {e}")
```

(If `Any` is not already imported in a modified file, add `from typing import Any`. Check each file's existing imports first — most already import it.)

- [ ] **Step 8: Run the chunking tests to verify they pass**

Run: `python3 -m pytest tests/test_channels.py::TestChannelChunking -q`
Expected: PASS (5 tests; the Discord one skips if `discord` isn't installed).

- [ ] **Step 9: Full regression + lint**

Run: `python3 -m pytest -q && ruff check databot/ && ruff format --check databot/`
Expected: all pass (~529 tests). If format flags a channel file, run `ruff format` on it.

- [ ] **Step 10: Commit**

```bash
git add databot/channels/ tests/test_channels.py
git commit -m "feat: chunk long outbound messages across all channels"
```

---

### Task 3: CHANGELOG

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add the changelog entry**

Under the existing `### Changed — Reliability` section in `CHANGELOG.md`, add:

```markdown
- **Message chunking across channels:** outbound replies longer than a channel's
  length limit are now split into multiple in-limit messages instead of being silently
  dropped. A shared `split_message` helper (`databot/channels/chunking.py`) splits on
  newline boundaries, hard-cuts overlong lines, and caps output at 10 chunks with a
  "…(response truncated)" notice. Applied to Telegram (4096), WhatsApp (4096), Google
  Chat (4096), and Slack (40000); Discord's existing splitter now uses the shared helper.
```

- [ ] **Step 2: Full regression + lint (final gate)**

Run: `python3 -m pytest -q && ruff check databot/ && ruff format --check databot/`
Expected: all pass; ~529 tests (512 baseline + ~14).

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: changelog for message chunking"
```

---

## Self-Review

**Spec coverage:**
- `split_message` + `MAX_CHUNKS` + truncation notice, newline-preferred/hard-cut/cap → Task 1. ✅
- Non-empty & `<= limit` guarantees; leading-newline-no-empty-chunk guard → Task 1 impl + `test_leading_newline_no_empty_chunk`. ✅
- Cap with truncation notice → Task 1 `test_cap_truncates_with_notice` + `test_default_max_chunks_is_ten`. ✅
- Per-channel `MAX_MESSAGE_LENGTH` (Discord 2000, Telegram/WhatsApp/GChat 4096, Slack 40000) → Task 2 Steps 3–7. ✅
- Discord swap + `_split_message` deletion → Task 2 Step 3. ✅
- httpx channels: one client, loop → Task 2 Steps 4–6. ✅
- Slack loop → Task 2 Step 7. ✅
- Existing `try/except` around the whole loop; thread params per chunk → Task 2 Steps 4–7. ✅
- Per-channel send-chunking tests → Task 2 Step 1 (5 tests). ✅
- No BaseChannel change; GChat sync webhook path untouched → not modified by any task. ✅
- CHANGELOG → Task 3. ✅

**Placeholder scan:** No TBD/TODO. Task 2 explicitly instructs verifying each channel's real constructor signature before instantiating in tests (constructors vary and the plan author cannot guarantee every kwarg) — behavior and assertions are fully specified; only the instantiation kwargs may need adjustment.

**Type consistency:** `split_message(content, limit, *, max_chunks=MAX_CHUNKS) -> list[str]` used identically in Task 1 (def + tests) and Task 2 (all five channels). `MAX_MESSAGE_LENGTH` is the attribute name read in every channel `send()` and asserted in every channel test. Truncation-notice suffix `"(response truncated)"` matches between the impl and `test_cap_truncates_with_notice`. ✅
