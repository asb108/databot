# Message Chunking Across Channels

**Date:** 2026-07-11
**Status:** Design approved
**Origin:** Deferred non-goal from sub-spec #3 (error UX in chat).

## Problem

`BaseChannel.send(chat_id, content, thread_id)` is abstract; each channel posts
`content` directly to its platform API. Only **Discord** splits long messages today
(private `_split_message`, 2000-char limit). Telegram/WhatsApp/Google Chat cap at
~4096 chars and Slack at 40000; a reply over the limit raises inside the channel's
`send()` `try/except`, gets logged, and the user **silently receives nothing**. Long
replies (tables, exports, profiling output) are exactly what a data bot produces.

## Goal

Split every outbound reply that exceeds a channel's length limit into multiple
in-limit messages, with a safety cap so a runaway reply can't flood a channel.
Centralize the splitting logic so it isn't re-implemented per channel.

## Decisions (from brainstorming)

- **Platform hard limits** per channel (fewest messages; strictly prevents the
  "message too long" API errors): Discord 2000 *(existing)*, Telegram 4096,
  WhatsApp 4096, Google Chat 4096, Slack 40000.
- **Chunk-count cap** with a truncation notice (default `max_chunks=10`): send the
  first N chunks and mark the last as truncated, rather than posting dozens of messages.
- **Newline-preferred + hard-cut** splitter (predictable; matches Discord). No
  code-fence or space-aware cleverness (YAGNI).

## Design

### New module: `databot/channels/chunking.py`

One pure function plus its constants — the single home for splitting logic.

```python
MAX_CHUNKS = 10
_TRUNCATION_NOTICE = "\n\n…(response truncated)"


def split_message(content: str, limit: int, *, max_chunks: int = MAX_CHUNKS) -> list[str]:
    ...
```

Algorithm:

1. If `len(content) <= limit`: return `[content]`. (Empty content → `[""]`, which
   preserves today's behavior of posting whatever `content` is.)
2. Otherwise, loop building chunks: find the last `"\n"` within `content[:limit]`;
   if found **at an index > 0**, split there; otherwise (no newline, or a newline at
   index 0 that would yield an empty leading chunk) hard-cut at `limit`. Append the
   chunk; set `content = content[split_at:].lstrip("\n")`; repeat until `content` fits.
3. **Cap:** if the loop would produce more than `max_chunks` chunks, keep the first
   `max_chunks`. Ensure the last kept chunk has room for the notice: if
   `len(last) + len(_TRUNCATION_NOTICE) > limit`, trim `last` to
   `limit - len(_TRUNCATION_NOTICE)`; then append `_TRUNCATION_NOTICE`.

Guarantees: every returned chunk is non-empty and `len(chunk) <= limit`; the list has
at most `max_chunks` elements; when truncation occurs the final chunk ends with the
notice.

### Per-channel wiring

Add a `MAX_MESSAGE_LENGTH` class constant to each channel with the limit from the
table above, then change each channel's `send()` to loop over the shared splitter.

- **Discord** (`databot/channels/discord.py`): replace the private `_split_message`
  with `split_message(content, self.MAX_MESSAGE_LENGTH)`; delete `_split_message`.
  Keep `MAX_MESSAGE_LENGTH = 2000`. (DRY; proves parity with the shared function.)
- **Telegram / WhatsApp / Google Chat** (httpx-based): each currently builds an
  `httpx.AsyncClient` inline and posts once. Change to build the client **once** and
  post each chunk in a loop:
  ```python
  async with httpx.AsyncClient(timeout=...) as client:
      for chunk in split_message(content, self.MAX_MESSAGE_LENGTH):
          payload = {... chunk ...}
          resp = await client.post(url, json=payload, ...)
          resp.raise_for_status()
  ```
- **Slack** (`self._client.chat_postMessage`): loop over the splitter, one
  `chat_postMessage` per chunk.

`thread_id` (and Slack's `thread_ts`, Telegram's `reply_to_message_id`, GChat's
`thread`) is applied to each chunk uniformly — no per-channel first-chunk-only
threading logic (minimal change, matches current params).

The existing per-`send()` `try/except` stays wrapped around the whole loop: a
mid-chunk failure is logged and aborts the remaining chunks, exactly as a single-send
failure is handled today.

### Out of scope

- **Google Chat synchronous webhook-return path** — the 30s webhook handler returns a
  single `{"text": outbound.content}` message and cannot split a reply into follow-ups
  without switching to the async API. Only the async `send()` path is chunked. Known
  limitation, not addressed here.
- No change to `BaseChannel`'s abstract interface (splitting is done inside each
  concrete `send()`, not via a new template method).

## Testing

**`tests/test_chunking.py`** (new, pure unit tests):

- `content` shorter than `limit` → one chunk equal to `content`.
- `content` exactly `limit` long → one chunk.
- `content` over `limit` → multiple chunks, each `len <= limit`, none empty.
- Newline-preferred: a boundary newline is used as the split point (no chunk exceeds
  limit and splits land on newlines when available).
- A single line longer than `limit` with no newline → hard-cut into `limit`-sized
  chunks.
- Cap: input that would exceed `max_chunks` (use a small `max_chunks`, e.g. 3) → exactly
  `max_chunks` chunks; the last ends with the truncation notice and is `<= limit`.
- Empty content → `[""]`.

**Per-channel `send()` tests** (added to `tests/test_channels.py`, following that file's
existing mocking seam — to be confirmed when writing the plan):

- Telegram / WhatsApp / Google Chat: inject a fake transport / patch the httpx client so
  posts are captured; a `content` longer than `MAX_MESSAGE_LENGTH` produces multiple
  posts, each with a body `<= MAX_MESSAGE_LENGTH`.
- Slack: a fake `_client` capturing `chat_postMessage` calls; long `content` → multiple
  calls, each `text` `<= MAX_MESSAGE_LENGTH`.
- Discord: assert `send()` uses `split_message` (multiple `channel.send` calls for long
  content, each `<= 2000`) after the `_split_message` removal.

Baseline is **512** passing; expect roughly **+16**. `ruff check databot/` and
`ruff format --check databot/` must stay clean.

## Execution & integration

- Subagent-driven (fresh subagent per task, two-stage review per task, broad final
  review), consistent with the prior sub-specs.
- Branch `feat/message-chunking` off `main`; on completion **merge to `main` locally**
  with `--ff-only`, then (per the established preference) it can be pushed to
  `origin`/asb108 on request.
