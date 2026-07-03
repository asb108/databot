"""Tests for context budgeting, cost tracking, prompt caching, and compaction."""

from __future__ import annotations

import pytest

from databot.core.compaction import ConversationCompactor
from databot.core.cost import UsageTracker, estimate_cost
from databot.core.tokens import (
    estimate_tokens,
    fit_messages_to_budget,
    get_context_window,
)
from databot.providers.base import LLMResponse, TokenUsage
from databot.providers.caching import (
    apply_anthropic_cache_control,
    is_anthropic_model,
)


# --------------------------------------------------------------------------
# Token estimation & budgeting
# --------------------------------------------------------------------------
class TestTokens:
    def test_estimate_nonzero(self):
        msgs = [{"role": "user", "content": "hello world " * 100}]
        assert estimate_tokens(msgs) > 0

    def test_empty(self):
        assert estimate_tokens([]) == 0

    def test_context_window_known_and_default(self):
        assert get_context_window("anthropic/claude-sonnet-4-5") >= 200_000
        assert get_context_window("totally-unknown-model") == 128_000

    def test_fit_under_budget_is_noop(self):
        msgs = [
            {"role": "system", "content": "sys"},
            {"role": "user", "content": "hi"},
        ]
        fitted, dropped = fit_messages_to_budget(msgs, "gpt-4o", 100_000)
        assert dropped == 0
        assert fitted == msgs

    def test_fit_drops_oldest_keeps_system(self):
        msgs = [{"role": "system", "content": "sys"}]
        for i in range(40):
            role = "user" if i % 2 == 0 else "assistant"
            msgs.append({"role": role, "content": f"message number {i} " * 50})
        fitted, dropped = fit_messages_to_budget(msgs, "gpt-4o", 500)
        assert dropped > 0
        assert fitted[0]["role"] == "system"  # system preserved
        assert fitted[-1] == msgs[-1]  # most recent preserved
        assert len(fitted) < len(msgs)

    def test_fit_does_not_orphan_tool_result(self):
        # If trimming would leave a leading tool/assistant-with-tool_calls, drop it.
        msgs = [
            {"role": "system", "content": "sys"},
            {"role": "assistant", "tool_calls": [{"id": "1"}], "content": None},
            {"role": "tool", "tool_call_id": "1", "content": "x" * 5000},
            {"role": "user", "content": "latest"},
        ]
        fitted, _ = fit_messages_to_budget(msgs, "gpt-4o", 50, min_recent=1)
        # No leading orphan tool result after the system message.
        body = [m for m in fitted if m["role"] != "system"]
        if body:
            assert body[0]["role"] != "tool"


# --------------------------------------------------------------------------
# Cost tracking
# --------------------------------------------------------------------------
class TestCost:
    def test_estimate_cost_known_model(self):
        # 1M input tokens of a frontier model should price to a sane positive
        # dollar amount (exact value comes from litellm's pricing DB).
        cost = estimate_cost("anthropic/claude-sonnet-4-5", 1_000_000, 0)
        assert 0.5 < cost < 50.0

    def test_estimate_cost_fallback_table(self):
        # When litellm cannot price a model name, the static fallback applies.
        cost = estimate_cost("claude-3-5-sonnet", 1_000_000, 0)
        assert cost == pytest.approx(3.0, rel=0.2)

    def test_estimate_cost_unknown_zero(self):
        assert estimate_cost("nonexistent-model-xyz", 1000, 1000) == 0.0

    def test_tracker_accumulates(self):
        t = UsageTracker()
        t.record("gpt-4o", 1000, 500, session_key="s1")
        t.record("gpt-4o", 2000, 1000, session_key="s1")
        stats = t.session_stats("s1")
        assert stats["prompt_tokens"] == 3000
        assert stats["completion_tokens"] == 1500
        assert stats["calls"] == 2
        assert stats["cost_usd"] > 0
        assert t.global_stats()["total_tokens"] == 4500

    def test_budget_enforcement(self):
        t = UsageTracker(session_budget_usd=0.001)
        assert t.over_budget("s1") is False
        # gpt-4o input is $2.50/M -> 1M tokens = $2.50, well over $0.001.
        t.record("gpt-4o", 1_000_000, 0, session_key="s1")
        assert t.over_budget("s1") is True
        assert t.over_budget("s2") is False  # other session unaffected

    def test_no_budget_never_over(self):
        t = UsageTracker(session_budget_usd=0.0)
        t.record("gpt-4o", 10_000_000, 10_000_000, session_key="s1")
        assert t.over_budget("s1") is False


# --------------------------------------------------------------------------
# Prompt caching
# --------------------------------------------------------------------------
class TestPromptCaching:
    def test_detects_anthropic(self):
        assert is_anthropic_model("anthropic/claude-sonnet-4-5")
        assert is_anthropic_model("claude-3-5-haiku")
        assert not is_anthropic_model("gpt-4o")

    def test_noop_for_non_anthropic(self):
        msgs = [{"role": "system", "content": "sys"}]
        tools = [{"type": "function", "function": {"name": "x"}}]
        out_msgs, out_tools = apply_anthropic_cache_control("gpt-4o", msgs, tools)
        assert out_msgs == msgs
        assert out_tools == tools

    def test_marks_system_and_tools_for_anthropic(self):
        msgs = [
            {"role": "system", "content": "you are databot"},
            {"role": "user", "content": "hi"},
        ]
        tools = [
            {"type": "function", "function": {"name": "a"}},
            {"type": "function", "function": {"name": "b"}},
        ]
        out_msgs, out_tools = apply_anthropic_cache_control(
            "anthropic/claude-sonnet-4-5", msgs, tools
        )
        # System content became a block list with a cache_control breakpoint.
        sys_content = out_msgs[0]["content"]
        assert isinstance(sys_content, list)
        assert sys_content[0]["cache_control"] == {"type": "ephemeral"}
        # Last tool carries the cache breakpoint.
        assert out_tools[-1]["cache_control"] == {"type": "ephemeral"}
        # Original inputs are not mutated.
        assert msgs[0]["content"] == "you are databot"
        assert "cache_control" not in tools[-1]


class TestPromptCacheFailSafe:
    """A cached request that errors must fall back to an uncached request."""

    async def test_chat_falls_back_to_uncached(self, monkeypatch):
        import databot.providers.litellm_provider as mod

        provider = mod.LiteLLMProvider(
            default_model="anthropic/claude-sonnet-4-5", enable_prompt_caching=True
        )

        calls = []

        async def fake_acompletion(**kwargs):
            calls.append(kwargs)
            # First (cached) call: the last tool carries cache_control -> reject it.
            tools = kwargs.get("tools") or []
            cached = any("cache_control" in t for t in tools)
            sys_blocks = kwargs["messages"][0].get("content")
            cached = cached or isinstance(sys_blocks, list)
            if cached:
                raise RuntimeError("unsupported cache_control field")

            class _Msg:
                content = "ok"
                tool_calls = None

            class _Choice:
                message = _Msg()

            class _Resp:
                choices = [_Choice()]
                usage = type("U", (), {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2})()

            return _Resp()

        fake_litellm = type("L", (), {"acompletion": staticmethod(fake_acompletion)})
        monkeypatch.setitem(__import__("sys").modules, "litellm", fake_litellm)

        messages = [
            {"role": "system", "content": "sys"},
            {"role": "user", "content": "hi"},
        ]
        tools = [{"type": "function", "function": {"name": "x"}}]
        result = await provider.chat(messages, tools)
        assert result.content == "ok"
        # First call was cached (and failed), second was uncached (and succeeded).
        assert len(calls) == 2
        assert isinstance(calls[0]["messages"][0]["content"], list)  # cached form
        assert calls[1]["messages"][0]["content"] == "sys"  # uncached form

    async def test_auth_error_not_retried(self, monkeypatch):
        import databot.providers.litellm_provider as mod

        provider = mod.LiteLLMProvider(
            default_model="anthropic/claude-sonnet-4-5", enable_prompt_caching=True
        )
        calls = []

        async def fake_acompletion(**kwargs):
            calls.append(kwargs)
            raise RuntimeError("invalid api key")

        fake_litellm = type("L", (), {"acompletion": staticmethod(fake_acompletion)})
        monkeypatch.setitem(__import__("sys").modules, "litellm", fake_litellm)

        with pytest.raises(RuntimeError, match="invalid api key"):
            await provider.chat([{"role": "system", "content": "s"}], None)
        # Auth errors must not trigger the uncached fallback or retries.
        assert len(calls) == 1


# --------------------------------------------------------------------------
# Conversation compaction
# --------------------------------------------------------------------------
class _FakeProvider:
    def __init__(self, summary="- user wants pipeline status\n- discussed table orders"):
        self._summary = summary
        self.calls = 0

    async def chat(self, messages, tools=None, model=None):
        self.calls += 1
        return LLMResponse(content=self._summary, usage=TokenUsage())

    def get_default_model(self):
        return "fake/model"


class TestCompaction:
    def _long_history(self, n=40):
        history = []
        for i in range(n):
            role = "user" if i % 2 == 0 else "assistant"
            history.append({"role": role, "content": f"turn {i} content " * 60})
        return history

    async def test_no_compaction_when_small(self):
        comp = ConversationCompactor(_FakeProvider(), trigger_tokens=10_000)
        history = [{"role": "user", "content": "hi"}]
        assert await comp.compact(history) is None

    async def test_compacts_long_history(self):
        provider = _FakeProvider()
        comp = ConversationCompactor(provider, trigger_tokens=500, keep_recent_messages=8)
        history = self._long_history(40)
        result = await comp.compact(history)
        assert result is not None
        recent, summary = result
        assert provider.calls == 1
        assert summary  # got a summary
        assert len(recent) == 8  # kept the recent window
        assert recent == history[-8:]

    async def test_compaction_failure_is_safe(self):
        class _Boom(_FakeProvider):
            async def chat(self, messages, tools=None, model=None):
                raise RuntimeError("provider down")

        comp = ConversationCompactor(_Boom(), trigger_tokens=500, keep_recent_messages=8)
        # Returns None (caller keeps full history) rather than raising.
        assert await comp.compact(self._long_history(40)) is None
