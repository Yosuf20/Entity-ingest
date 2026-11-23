"""
Tests for llm/orchestrator.py

Scenarios covered
-----------------
- Provider 1 rate-limits forever → falls through to provider 2.
- All providers rate-limit → record saved to pending_extractions, AllProvidersFailed raised.
- 413 → input shrunk and retried on the SAME provider (up to MAX_SHRINKS).
- Grounding check → made-up value not in source text is rejected.
- Successful extraction stamps extracted_by and source_url.
- Breaker-open provider is skipped entirely.
- Counters: summary() reflects 429s, shrinks, fallbacks, etc.
"""

from __future__ import annotations

import asyncio
import sqlite3
from typing import Any

import pytest

from src.llm.breaker import CircuitBreaker
from src.llm.errors import (
    AllProvidersFailed,
    BadOutput,
    PayloadTooLarge,
    ProviderDown,
    RateLimited,
)
from src.llm.orchestrator import Orchestrator, OrchestratorConfig
from src.llm.providers import MockProvider
from src.llm.ratelimit import TokenBucket
from src.storage.db import init_db


# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

SOURCE_TEXT = (
    "Acme Corp was founded in 2020 by Jane Doe. "
    "The company raised $5000000 in seed funding. "
    "Headquartered in San Francisco."
)


class _FakeClock:
    def __init__(self, t=0.0):
        self._t = t

    def __call__(self):
        return self._t

    def advance(self, s):
        self._t += s


async def _no_sleep(_: float) -> None:
    """Instant sleep stub."""
    await asyncio.sleep(0)


def _make_bucket(rpm=1000, clock=None, sleep=None) -> TokenBucket:
    return TokenBucket(
        rpm=rpm,
        clock=clock or (lambda: 0.0),
        sleep=sleep or _no_sleep,
    )


def _make_breaker(name="p", threshold=5, cooldown=60, clock=None) -> CircuitBreaker:
    return CircuitBreaker(name, threshold=threshold, cooldown=cooldown, clock=clock or (lambda: 0.0))


def _make_orchestrator(chain, *, db=None) -> Orchestrator:
    buckets = {p.name: _make_bucket() for p in chain}
    breakers = {p.name: _make_breaker(p.name) for p in chain}
    cfg = OrchestratorConfig(retry_base=0.0, retry_cap=0.0, retry_max_attempts=1)
    return Orchestrator(chain, buckets, breakers, cfg, db_conn=db, sleep=_no_sleep)


# Simple schema for tests
from pydantic import BaseModel
from typing import Optional


class _StartupSchema(BaseModel):
    source_url: str = ""
    extracted_by: Optional[str] = None
    name: Optional[str] = None
    founded_year: Optional[int] = None
    funding_total_usd: Optional[float] = None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fallthrough_to_provider2_on_rate_limit():
    """Provider 1 always rate-limits → provider 2 should succeed."""
    p1 = MockProvider("p1", [RateLimited()])
    p2 = MockProvider("p2", [{"name": "Acme Corp", "source_url": "", "founded_year": None, "funding_total_usd": None}])

    orch = _make_orchestrator([p1, p2])
    result = await orch.extract(SOURCE_TEXT, _StartupSchema, source_url="http://example.com")

    assert result["name"] == "Acme Corp"
    assert result["extracted_by"] == "p2"
    assert p1.call_count == 1
    assert p2.call_count == 1


@pytest.mark.asyncio
async def test_all_providers_fail_saves_pending_and_raises():
    """All providers rate-limit → record saved to DB, AllProvidersFailed raised."""
    db = init_db(db_path=":memory:")  # in-memory SQLite

    # Patch save_pending to use the in-memory conn
    p1 = MockProvider("p1", [RateLimited()])
    p2 = MockProvider("p2", [RateLimited()])

    buckets = {"p1": _make_bucket(), "p2": _make_bucket()}
    breakers = {"p1": _make_breaker("p1"), "p2": _make_breaker("p2")}
    cfg = OrchestratorConfig(retry_base=0.0, retry_cap=0.0, retry_max_attempts=1)
    orch = Orchestrator([p1, p2], buckets, breakers, cfg, db_conn=db, sleep=_no_sleep)

    with pytest.raises(AllProvidersFailed):
        await orch.extract(SOURCE_TEXT, _StartupSchema, source_url="http://fail.com")

    rows = db.execute("SELECT * FROM pending_extractions").fetchall()
    assert len(rows) == 1
    assert rows[0]["source_url"] == "http://fail.com"
    assert rows[0]["status"] == "pending"


@pytest.mark.asyncio
async def test_413_triggers_shrink_and_retry_same_provider():
    """413 → orchestrator shrinks input and retries the SAME provider."""
    # First call: PayloadTooLarge; second: success
    p1 = MockProvider(
        "p1",
        [
            PayloadTooLarge("too big"),
            {"name": "Acme Corp", "source_url": "", "founded_year": None, "funding_total_usd": None},
        ],
    )
    orch = _make_orchestrator([p1])
    result = await orch.extract(SOURCE_TEXT, _StartupSchema, source_url="http://example.com")

    assert result["name"] == "Acme Corp"
    assert p1.call_count == 2
    assert orch.summary().get("p1.shrinks", 0) == 1


@pytest.mark.asyncio
async def test_413_max_shrinks_then_next_provider():
    """After MAX_SHRINKS (3) all fail → move to provider 2."""
    p1 = MockProvider("p1", [PayloadTooLarge()] * 10)
    p2 = MockProvider("p2", [{"name": "Acme Corp", "source_url": "", "founded_year": None, "funding_total_usd": None}])

    orch = _make_orchestrator([p1, p2])
    result = await orch.extract(SOURCE_TEXT, _StartupSchema, source_url="http://example.com")

    assert result["extracted_by"] == "p2"
    assert p1.call_count == 4  # initial + 3 shrinks
    assert p2.call_count == 1


@pytest.mark.asyncio
async def test_grounding_check_rejects_hallucination():
    """Hallucinated value not in source text must be rejected."""
    p1 = MockProvider(
        "p1",
        [
            # 'name' value "FakeCompany" is NOT in SOURCE_TEXT → grounding fails
            {"name": "FakeCompany", "source_url": "", "founded_year": None, "funding_total_usd": None},
        ],
    )
    p2 = MockProvider(
        "p2",
        [
            {"name": "Acme Corp", "source_url": "", "founded_year": None, "funding_total_usd": None},
        ],
    )

    orch = _make_orchestrator([p1, p2])
    result = await orch.extract(SOURCE_TEXT, _StartupSchema, source_url="http://example.com")

    assert result["name"] == "Acme Corp"
    assert result["extracted_by"] == "p2"
    assert orch.summary().get("p1.grounding_failed", 0) == 1


@pytest.mark.asyncio
async def test_grounding_check_passes_for_real_values():
    """Values actually present in the source text should pass grounding."""
    p1 = MockProvider(
        "p1",
        [
            {
                "name": "Acme Corp",
                "source_url": "",
                "founded_year": 2020,
                "funding_total_usd": 5000000,
            }
        ],
    )
    orch = _make_orchestrator([p1])
    result = await orch.extract(SOURCE_TEXT, _StartupSchema, source_url="http://example.com")

    assert result["name"] == "Acme Corp"
    assert result["founded_year"] == 2020


@pytest.mark.asyncio
async def test_open_breaker_provider_skipped():
    """A provider whose circuit breaker is OPEN must be skipped."""
    clock = _FakeClock(0.0)
    p1 = MockProvider("p1", [{"name": "Acme Corp", "source_url": "", "founded_year": None, "funding_total_usd": None}])
    p2 = MockProvider("p2", [{"name": "Acme Corp", "source_url": "", "founded_year": None, "funding_total_usd": None}])

    breaker_p1 = CircuitBreaker("p1", threshold=5, cooldown=60, clock=clock)
    # Manually open the breaker
    for _ in range(5):
        breaker_p1.record_failure()

    breakers = {"p1": breaker_p1, "p2": _make_breaker("p2")}
    buckets = {"p1": _make_bucket(), "p2": _make_bucket()}
    cfg = OrchestratorConfig(retry_base=0.0, retry_cap=0.0, retry_max_attempts=1)
    orch = Orchestrator([p1, p2], buckets, breakers, cfg, sleep=_no_sleep)

    result = await orch.extract(SOURCE_TEXT, _StartupSchema, source_url="http://example.com")

    # p1 should not have been called
    assert p1.call_count == 0
    assert result["extracted_by"] == "p2"
    assert orch.summary().get("p1.skipped", 0) == 1


@pytest.mark.asyncio
async def test_extracted_by_and_source_url_stamped():
    """Result must include extracted_by and source_url."""
    p1 = MockProvider(
        "p1",
        [{"name": "Acme Corp", "source_url": "", "founded_year": None, "funding_total_usd": None}],
    )
    orch = _make_orchestrator([p1])
    result = await orch.extract(SOURCE_TEXT, _StartupSchema, source_url="http://example.com")

    assert result["extracted_by"] == "p1"
    assert result["source_url"] == "http://example.com"


@pytest.mark.asyncio
async def test_summary_counters():
    """summary() should track 429s, shrinks, grounding failures."""
    p1 = MockProvider(
        "p1",
        [
            RateLimited(),       # 1 rate limit
            PayloadTooLarge(),   # 1 shrink needed
            {"name": "FakeXYZ", "source_url": "", "founded_year": None, "funding_total_usd": None},  # grounding fail
        ],
    )
    p2 = MockProvider(
        "p2",
        [{"name": "Acme Corp", "source_url": "", "founded_year": None, "funding_total_usd": None}],
    )

    orch = _make_orchestrator([p1, p2])
    result = await orch.extract(SOURCE_TEXT, _StartupSchema, source_url="http://example.com")

    s = orch.summary()
    assert s.get("p1.rate_limited", 0) >= 1
    assert s.get("total.rate_limited", 0) >= 1
    assert result["extracted_by"] == "p2"


@pytest.mark.asyncio
async def test_provider_down_moves_to_next():
    """ProviderDown should trigger fallback to the next provider."""
    p1 = MockProvider("p1", [ProviderDown("server error")])
    p2 = MockProvider(
        "p2",
        [{"name": "Acme Corp", "source_url": "", "founded_year": None, "funding_total_usd": None}],
    )

    orch = _make_orchestrator([p1, p2])
    result = await orch.extract(SOURCE_TEXT, _StartupSchema, source_url="http://example.com")

    assert result["extracted_by"] == "p2"


@pytest.mark.asyncio
async def test_bad_output_moves_to_next():
    """BadOutput should trigger fallback to the next provider."""
    p1 = MockProvider("p1", [BadOutput("not json")])
    p2 = MockProvider(
        "p2",
        [{"name": "Acme Corp", "source_url": "", "founded_year": None, "funding_total_usd": None}],
    )

    orch = _make_orchestrator([p1, p2])
    result = await orch.extract(SOURCE_TEXT, _StartupSchema, source_url="http://example.com")

    assert result["extracted_by"] == "p2"
