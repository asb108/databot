"""LLM usage and cost tracking with per-session budgets.

Data teams care about LLM spend. The provider already accumulates raw token
counts, but that is global and dollar-blind. This module turns token usage into
dollars (via litellm pricing when available, with a static fallback table) and
attributes it per session so the gateway can expose cost stats and optionally
enforce a per-session budget.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

# Fallback per-1M-token prices (USD) for common models: (input, output).
# Used only when litellm cannot price the model. Approximate list prices.
_FALLBACK_PRICES: dict[str, tuple[float, float]] = {
    "claude-3-5-sonnet": (3.0, 15.0),
    "claude-sonnet-4": (3.0, 15.0),
    "claude-opus-4": (15.0, 75.0),
    "claude-3-5-haiku": (0.80, 4.0),
    "claude-haiku-4": (1.0, 5.0),
    "gpt-4o": (2.50, 10.0),
    "gpt-4o-mini": (0.15, 0.60),
    "o1": (15.0, 60.0),
    "o3": (10.0, 40.0),
    "deepseek-chat": (0.27, 1.10),
    "gemini-2.0-flash": (0.10, 0.40),
    "gemini-1.5-pro": (1.25, 5.0),
    "mistral-large": (2.0, 6.0),
    "llama-3.3-70b": (0.59, 0.79),
}


def estimate_cost(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    """Estimate the USD cost of a call. Best effort; returns 0.0 if unknown."""
    if prompt_tokens <= 0 and completion_tokens <= 0:
        return 0.0
    try:
        import litellm

        prompt_cost, completion_cost = litellm.cost_per_token(
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )
        total = (prompt_cost or 0.0) + (completion_cost or 0.0)
        if total > 0:
            return float(total)
    except Exception:
        pass

    name = (model or "").split("/")[-1].lower()
    for key, (in_price, out_price) in _FALLBACK_PRICES.items():
        if name.startswith(key) or key in name:
            return (prompt_tokens * in_price + completion_tokens * out_price) / 1_000_000
    return 0.0


def record_llm_usage(
    usage_tracker: "UsageTracker | None",
    model: str,
    response: object,
    session_key: str = "",
) -> float:
    """Record an LLM response's usage to the tracker and Prometheus metrics.

    Shared by every code path that calls an LLM directly (agent loop, multi-agent
    specialists, and LLM-backed tools like ``text_to_sql``) so that token and
    dollar accounting is never silently bypassed. Returns the call's cost.
    """
    usage = getattr(response, "usage", None)
    if not usage:
        return 0.0
    prompt = getattr(usage, "prompt_tokens", 0) or 0
    completion = getattr(usage, "completion_tokens", 0) or 0
    cost = 0.0
    if usage_tracker is not None:
        cost = usage_tracker.record(model, prompt, completion, session_key)
    from databot.observability.metrics import get_metrics

    get_metrics().llm_call(prompt, completion, cost)
    return cost


@dataclass
class UsageStats:
    """Accumulated usage for one scope (a session or the global total)."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0
    calls: int = 0

    def as_dict(self) -> dict[str, float]:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "cost_usd": round(self.cost_usd, 6),
            "calls": self.calls,
        }


class UsageTracker:
    """Tracks token usage and dollar cost globally and per session."""

    def __init__(self, session_budget_usd: float = 0.0):
        # session_budget_usd <= 0 means "no budget enforced".
        self._session_budget = max(0.0, session_budget_usd)
        self._global = UsageStats()
        self._by_session: dict[str, UsageStats] = {}
        self._lock = threading.Lock()

    def record(
        self,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        session_key: str = "",
    ) -> float:
        """Record a call's usage. Returns the call's estimated cost in USD."""
        total = (prompt_tokens or 0) + (completion_tokens or 0)
        cost = estimate_cost(model, prompt_tokens or 0, completion_tokens or 0)
        with self._lock:
            for stats in (self._global, self._scope(session_key)):
                stats.prompt_tokens += prompt_tokens or 0
                stats.completion_tokens += completion_tokens or 0
                stats.total_tokens += total
                stats.cost_usd += cost
                stats.calls += 1
        return cost

    def _scope(self, session_key: str) -> UsageStats:
        if not session_key:
            session_key = "_global"
        stats = self._by_session.get(session_key)
        if stats is None:
            stats = UsageStats()
            self._by_session[session_key] = stats
        return stats

    def session_cost(self, session_key: str) -> float:
        with self._lock:
            stats = self._by_session.get(session_key)
            return stats.cost_usd if stats else 0.0

    def over_budget(self, session_key: str) -> bool:
        """True if the session has exceeded its configured budget."""
        if self._session_budget <= 0:
            return False
        return self.session_cost(session_key) >= self._session_budget

    @property
    def session_budget_usd(self) -> float:
        return self._session_budget

    def global_stats(self) -> dict[str, float]:
        with self._lock:
            return self._global.as_dict()

    def session_stats(self, session_key: str) -> dict[str, float]:
        with self._lock:
            stats = self._by_session.get(session_key)
            return stats.as_dict() if stats else UsageStats().as_dict()

    def all_session_stats(self) -> dict[str, dict[str, float]]:
        with self._lock:
            return {k: v.as_dict() for k, v in self._by_session.items()}
