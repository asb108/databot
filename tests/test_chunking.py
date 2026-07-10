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
