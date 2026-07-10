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


def test_consecutive_newlines_preserve_text_and_a_blank_line():
    content = "a" * 44 + "\n\n\n" + "b" * 44
    chunks = split_message(content, 45)
    assert sum(c.count("a") for c in chunks) == 44  # no text lost
    assert sum(c.count("b") for c in chunks) == 44
    assert (
        sum(c.count("\n") for c in chunks) >= 1
    )  # a blank-line newline survives (old code dropped all)


def test_all_newlines_terminates_and_within_limit():
    chunks = split_message("\n" * 100, 50)  # must not hang
    assert len(chunks) >= 1
    assert all(len(c) <= 50 for c in chunks)


def test_truncated_chunk_never_exceeds_tiny_limit():
    chunks = split_message("x" * 500, 10, max_chunks=2)
    assert len(chunks) == 2
    assert all(len(c) <= 10 for c in chunks)  # notice can't push last chunk over limit


def test_nonpositive_limit_raises():
    import pytest

    with pytest.raises(ValueError):
        split_message("ab", 0)
