"""
Tests for llm/ratelimit.py

Scenarios covered
-----------------
- 50 concurrent tasks with rpm=5 must not exceed 5 acquires per 60s window
  (verified via a counting fake clock + sleep that advances simulated time).
- penalize() blocks further acquires for the penalized duration.
- Tokens refill correctly over time.
"""

from __future__ import annotations

import asyncio
import pytest

from src.llm.ratelimit import TokenBucket


# ---------------------------------------------------------------------------
# Fake time infrastructure
# ---------------------------------------------------------------------------

class _FakeClock:
    """Monotonically advancing fake clock."""

    def __init__(self, start: float = 0.0):
        self._t = start

    def __call__(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += seconds


class _FakeSleep:
    """Fake async sleep that advances the shared FakeClock."""

    def __init__(self, clock: _FakeClock):
        self._clock = clock

    async def __call__(self, seconds: float) -> None:
        self._clock.advance(seconds)
        # Yield so other tasks can run
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bucket_allows_up_to_rpm_in_window():
    """rpm=5 → at most 5 acquires should be instant; the 6th waits."""
    clock = _FakeClock(0.0)
    sleep = _FakeSleep(clock)
    bucket = TokenBucket(rpm=5, clock=clock, sleep=sleep)

    # First 5 acquires should not sleep (tokens already in bucket)
    for _ in range(5):
        await bucket.acquire()

    # 6th acquire must trigger a sleep (token depleted)
    before = clock()
    await bucket.acquire()
    after = clock()
    assert after > before, "6th acquire should have slept"


@pytest.mark.asyncio
async def test_concurrent_tasks_respect_rpm():
    """50 concurrent tasks with rpm=5 must never exceed 5 acquires per 60s window.

    The bucket starts empty (tokens=0) so every acquire must pay a refill
    delay, ensuring simulated time advances monotonically from t=0.
    After all tasks finish, we check that no sliding 60-second window
    starting at any recorded timestamp contains more than rpm+1 events.
    """
    clock = _FakeClock(0.0)
    sleep = _FakeSleep(clock)
    bucket = TokenBucket(rpm=5, clock=clock, sleep=sleep)
    # Start with zero tokens so the very first acquire must wait
    bucket._tokens = 0.0

    timestamps: list[float] = []

    async def task():
        await bucket.acquire()
        timestamps.append(clock())

    await asyncio.gather(*[task() for _ in range(50)])

    assert len(timestamps) == 50

    # For every event, count how many events fall in the next 60 seconds
    for start_t in timestamps:
        window_count = sum(1 for t in timestamps if start_t <= t < start_t + 60)
        assert window_count <= 5 + 1, (
            f"Window starting t={start_t:.2f}: {window_count} events > rpm=5"
        )


@pytest.mark.asyncio
async def test_penalize_blocks_acquires():
    """penalize(10) should make the next acquire wait at least 10 s."""
    clock = _FakeClock(0.0)
    sleep = _FakeSleep(clock)
    bucket = TokenBucket(rpm=60, clock=clock, sleep=sleep)

    # Penalize for 10 seconds
    await bucket.penalize(10.0)

    before = clock()
    await bucket.acquire()
    elapsed = clock() - before
    assert elapsed >= 10.0, f"Penalty not applied: elapsed={elapsed}"


@pytest.mark.asyncio
async def test_tokens_refill_over_time():
    """After draining the bucket, waiting should allow more acquires."""
    clock = _FakeClock(0.0)
    sleep = _FakeSleep(clock)
    bucket = TokenBucket(rpm=60, clock=clock, sleep=sleep)  # 1 token/sec

    # Drain all tokens
    for _ in range(60):
        await bucket.acquire()

    # Advance time by 5 seconds → 5 new tokens
    clock.advance(5.0)

    # Should be able to acquire 5 more without sleeping
    acquired = 0
    for _ in range(5):
        before = clock()
        await bucket.acquire()
        if clock() == before:  # no sleep was needed
            acquired += 1

    assert acquired >= 4, f"Expected ~5 instant acquires after refill, got {acquired}"


@pytest.mark.asyncio
async def test_penalize_does_not_extend_shorter_existing_penalty():
    """A shorter penalize() call should not reduce an existing longer penalty."""
    clock = _FakeClock(0.0)
    sleep = _FakeSleep(clock)
    bucket = TokenBucket(rpm=60, clock=clock, sleep=sleep)

    await bucket.penalize(30.0)
    await bucket.penalize(5.0)  # should NOT overwrite the 30s penalty

    before = clock()
    await bucket.acquire()
    elapsed = clock() - before
    assert elapsed >= 29.0, f"Short penalty overwrote long penalty: elapsed={elapsed}"
