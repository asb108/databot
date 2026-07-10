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
