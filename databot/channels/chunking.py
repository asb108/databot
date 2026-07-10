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
    newline within the limit. Consumes exactly one delimiter newline per split, so
    blank lines between paragraphs are preserved. Returns at most ``max_chunks``
    chunks; if more would be needed, the last kept chunk is marked with a truncation
    notice. Every returned chunk is non-empty and ``len(chunk) <= limit`` (the empty
    string is returned verbatim as ``[""]``). Raises ``ValueError`` if ``limit < 1``.
    """
    if limit < 1:
        raise ValueError("limit must be a positive integer")
    if len(content) <= limit:
        return [content]

    chunks: list[str] = []
    remaining = content
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        split_at = remaining.rfind("\n", 0, limit)
        if split_at <= 0:
            # No newline within the window (or one at index 0): hard-cut.
            chunks.append(remaining[:limit])
            remaining = remaining[limit:]
        else:
            # Split on the newline, consuming exactly that one delimiter newline
            # so adjacent blank lines are preserved.
            chunks.append(remaining[:split_at])
            remaining = remaining[split_at + 1 :]

    if len(chunks) <= max_chunks:
        return chunks

    kept = chunks[:max_chunks]
    last = kept[-1]
    if len(last) + len(_TRUNCATION_NOTICE) > limit:
        last = last[: max(0, limit - len(_TRUNCATION_NOTICE))]
    kept[-1] = (last + _TRUNCATION_NOTICE)[:limit]
    return kept
