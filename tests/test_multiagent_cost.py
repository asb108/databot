"""Tests for multi-agent cost/budget wiring and RAG relevance threshold."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from databot.agents import build_default_agents
from databot.core.cost import UsageTracker
from databot.providers.base import LLMResponse, TokenUsage
from databot.tools.base import ToolRegistry


def _provider(content="done"):
    p = MagicMock()
    p.get_default_model.return_value = "gpt-4o"
    p.chat = AsyncMock(
        return_value=LLMResponse(
            content=content,
            tool_calls=[],
            usage=TokenUsage(prompt_tokens=100, completion_tokens=20, total_tokens=120),
        )
    )
    return p


class TestMultiAgentUsage:
    async def test_specialist_records_usage(self):
        tracker = UsageTracker()
        provider = _provider()
        _, _, delegator = build_default_agents(
            provider=provider, tools=ToolRegistry(), model="gpt-4o", usage_tracker=tracker
        )
        # Router returns JSON picking 'general'; force it deterministically.
        provider.chat = AsyncMock(
            side_effect=[
                LLMResponse(content='{"agent": "general"}', usage=TokenUsage(10, 5, 15)),
                LLMResponse(content="answer", usage=TokenUsage(100, 20, 120)),
            ]
        )
        out = await delegator.handle("hello", session_key="s1")
        assert out == "answer"
        stats = tracker.session_stats("s1")
        # Router call + specialist call both recorded.
        assert stats["calls"] == 2
        assert stats["prompt_tokens"] == 110

    async def test_budget_blocks_before_routing(self):
        tracker = UsageTracker(session_budget_usd=0.01)
        tracker.record("gpt-4o", 1_000_000, 0, session_key="s1")  # over budget
        provider = _provider()
        _, _, delegator = build_default_agents(
            provider=provider, tools=ToolRegistry(), model="gpt-4o", usage_tracker=tracker
        )
        out = await delegator.handle("hello", session_key="s1")
        assert "budget" in out.lower()
        provider.chat.assert_not_called()  # never routed or ran a specialist

    async def test_metadata_budget_block(self):
        tracker = UsageTracker(session_budget_usd=0.01)
        tracker.record("gpt-4o", 1_000_000, 0, session_key="s1")
        provider = _provider()
        _, _, delegator = build_default_agents(
            provider=provider, tools=ToolRegistry(), model="gpt-4o", usage_tracker=tracker
        )
        result = await delegator.handle_with_metadata("hi", session_key="s1")
        assert result["agent"] == "none"
        assert "budget" in result["response"].lower()


class _ScriptedProvider:
    """Records every chat() call's messages and returns scripted contents."""

    def __init__(self, contents):
        self._contents = list(contents)
        self.calls: list[list[dict]] = []

    async def chat(self, messages, tools=None, model=None):
        self.calls.append(messages)
        content = self._contents.pop(0) if self._contents else "ok"
        return LLMResponse(content=content, usage=TokenUsage(10, 5, 15))

    def get_default_model(self):
        return "gpt-4o"


class TestMultiAgentSession:
    def _delegator(self, provider, tmp_path, compactor=None):
        from databot.session.manager import SessionManager

        _, _, delegator = build_default_agents(
            provider=provider,
            tools=ToolRegistry(),
            model="gpt-4o",
            sessions=SessionManager(tmp_path),
            compactor=compactor,
        )
        return delegator

    async def test_persists_and_replays_history(self, tmp_path):
        # Two turns: router+answer each. The 2nd turn must see turn 1 in history.
        provider = _ScriptedProvider(
            ['{"agent": "general"}', "first answer", '{"agent": "general"}', "second answer"]
        )
        delegator = self._delegator(provider, tmp_path)

        out1 = await delegator.handle("what is X?", session_key="s1")
        assert out1 == "first answer"

        out2 = await delegator.handle("and Y?", session_key="s1")
        assert out2 == "second answer"

        # The specialist call in turn 2 is the last recorded chat; its messages
        # must contain the prior user/assistant turn.
        specialist_msgs = provider.calls[-1]
        joined = " ".join(str(m.get("content")) for m in specialist_msgs)
        assert "what is X?" in joined
        assert "first answer" in joined

    async def test_compaction_injects_summary(self, tmp_path):
        from databot.core.compaction import ConversationCompactor
        from databot.session.manager import SessionManager

        # Seed a long history so compaction triggers on the next turn.
        sessions = SessionManager(tmp_path)
        session = sessions.get_or_create("s1")
        for i in range(20):
            session.add_message("user" if i % 2 == 0 else "assistant", f"turn {i} " * 30)
        sessions.save(session)

        provider = _ScriptedProvider(
            ["[summary of earlier turns]", '{"agent": "general"}', "answer"]
        )
        compactor = ConversationCompactor(provider, trigger_tokens=50, keep_recent_messages=4)
        _, _, delegator = build_default_agents(
            provider=provider,
            tools=ToolRegistry(),
            model="gpt-4o",
            sessions=sessions,
            compactor=compactor,
        )

        out = await delegator.handle("new question", session_key="s1")
        assert out == "answer"

        # Session now carries a summary, and the specialist saw it as a system msg.
        reloaded = sessions.get_or_create("s1")
        assert reloaded.summary == "[summary of earlier turns]"
        specialist_msgs = provider.calls[-1]
        system_text = " ".join(
            str(m.get("content")) for m in specialist_msgs if m.get("role") == "system"
        )
        assert "Summary of earlier conversation" in system_text


class TestRAGThreshold:
    def _ctx(self, results, max_distance):
        from databot.rag import RAGContext

        store = MagicMock()
        store.query = MagicMock(return_value=results)
        return RAGContext(store=store, max_distance=max_distance)

    def test_filters_distant_docs(self):
        results = [
            {"document": "relevant doc", "metadata": {"source": "a"}, "distance": 0.3},
            {"document": "unrelated doc", "metadata": {"source": "b"}, "distance": 0.9},
        ]
        out = self._ctx(results, max_distance=0.6).enrich_prompt("q")
        assert "relevant doc" in out
        assert "unrelated doc" not in out

    def test_threshold_disabled_keeps_all(self):
        results = [
            {"document": "doc one", "metadata": {"source": "a"}, "distance": 0.3},
            {"document": "doc two", "metadata": {"source": "b"}, "distance": 1.5},
        ]
        out = self._ctx(results, max_distance=0.0).enrich_prompt("q")
        assert "doc one" in out and "doc two" in out

    def test_all_filtered_returns_empty(self):
        results = [{"document": "far", "metadata": {}, "distance": 1.9}]
        assert self._ctx(results, max_distance=0.5).enrich_prompt("q") == ""
