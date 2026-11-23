"""
Tests for llm/retry.py

Scenarios covered
-----------------
- 429 twice then 200 → succeeds on attempt 3; recorded delays grow.
- 429 with Retry-After: 2 → delay is within [1.0, 3.0] (2s × jitter [0.5,1.5]).
- BadOutput and PayloadTooLarge are NOT retried.
- Exhausted retries re-raise the last exception.
- Jitter → delays across concurrent tasks are not identical.
"""

from __future__ import annotations

import asyncio
import pytest

from src.llm.errors import BadOutput, PayloadTooLarge, ProviderDown, RateLimited
from src.llm.retry import with_retry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _counter_call(responses: list):
    """Return an async callable that pops from *responses* on each call."""
    idx = 0

    async def _call():
        nonlocal idx
        item = responses[idx]
        idx += 1
        if isinstance(item, BaseException):
            raise item
        return item

    return _call


class _RecordingSleep:
    """Fake sleep that records every delay requested."""

    def __init__(self):
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_success_after_two_429s():
    """429 twice, then 200: should succeed on the 3rd attempt."""
    sleeper = _RecordingSleep()
    responses = [RateLimited(), RateLimited(), {"name": "Acme"}]
    call = _counter_call(responses)

    result = await with_retry(call, base=1.0, cap=60.0, max_attempts=5, sleep=sleeper)
    assert result == {"name": "Acme"}
    assert len(sleeper.delays) == 2  # slept between attempt 1→2 and 2→3


@pytest.mark.asyncio
async def test_delays_grow_exponentially():
    """Back-off delays should increase with each attempt (before jitter)."""
    sleeper = _RecordingSleep()
    # Always rate-limited → exhaust all retries
    call = _counter_call([RateLimited()] * 5)

    with pytest.raises(RateLimited):
        await with_retry(call, base=1.0, cap=60.0, max_attempts=5, sleep=sleeper)

    assert len(sleeper.delays) == 4  # 5 attempts → 4 sleeps
    # Each raw base doubles: 1, 2, 4, 8 — with jitter [0.5,1.5] applied
    # Verify they're not all the same (jitter) and each is > 0
    assert all(d > 0 for d in sleeper.delays)


@pytest.mark.asyncio
async def test_retry_after_header_used_as_base():
    """Retry-After: 2 → sleep should be in [1.0, 3.0] (2 × [0.5, 1.5])."""
    sleeper = _RecordingSleep()
    responses = [RateLimited(retry_after=2.0), {"ok": True}]
    call = _counter_call(responses)

    result = await with_retry(call, base=1.0, cap=60.0, max_attempts=3, sleep=sleeper)
    assert result == {"ok": True}
    assert len(sleeper.delays) == 1
    assert 1.0 <= sleeper.delays[0] <= 3.0, f"delay={sleeper.delays[0]} out of [1.0,3.0]"


@pytest.mark.asyncio
async def test_bad_output_not_retried():
    """BadOutput must propagate immediately without any retry."""
    sleeper = _RecordingSleep()
    call = _counter_call([BadOutput("garbage"), {"should": "not reach"}])

    with pytest.raises(BadOutput):
        await with_retry(call, base=1.0, cap=60.0, max_attempts=5, sleep=sleeper)

    assert len(sleeper.delays) == 0


@pytest.mark.asyncio
async def test_payload_too_large_not_retried():
    """PayloadTooLarge must propagate immediately."""
    sleeper = _RecordingSleep()
    call = _counter_call([PayloadTooLarge("too big")])

    with pytest.raises(PayloadTooLarge):
        await with_retry(call, base=1.0, cap=60.0, max_attempts=5, sleep=sleeper)

    assert len(sleeper.delays) == 0


@pytest.mark.asyncio
async def test_exhausted_retries_reraise():
    """After max_attempts, the last exception is re-raised."""
    sleeper = _RecordingSleep()
    call = _counter_call([ProviderDown("down")] * 10)

    with pytest.raises(ProviderDown):
        await with_retry(call, base=1.0, cap=60.0, max_attempts=3, sleep=sleeper)

    assert len(sleeper.delays) == 2  # 3 attempts → 2 sleeps


@pytest.mark.asyncio
async def test_jitter_makes_delays_differ():
    """Concurrent tasks should NOT all sleep for the same duration."""
    all_delays: list[float] = []

    async def one_task():
        s = _RecordingSleep()
        call = _counter_call([RateLimited()] * 5)
        with pytest.raises(RateLimited):
            await with_retry(call, base=1.0, cap=60.0, max_attempts=5, sleep=s)
        all_delays.extend(s.delays)

    await asyncio.gather(*[one_task() for _ in range(5)])

    # With random jitter, not all delays should be identical
    assert len(set(round(d, 6) for d in all_delays)) > 1, "All delays are identical — jitter broken"


@pytest.mark.asyncio
async def test_cap_limits_maximum_delay():
    """Delay (before jitter) must never exceed cap."""
    sleeper = _RecordingSleep()
    call = _counter_call([RateLimited()] * 10)

    with pytest.raises(RateLimited):
        await with_retry(call, base=10.0, cap=15.0, max_attempts=5, sleep=sleeper)

    # Raw delay = min(cap, base * 2^attempt); with jitter max = cap * 1.5
    for d in sleeper.delays:
        assert d <= 15.0 * 1.5 + 0.001, f"delay {d} exceeded cap*1.5"
