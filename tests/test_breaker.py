"""
Tests for llm/breaker.py

Scenarios covered
-----------------
- 5 consecutive failures → breaker opens; subsequent calls skip the provider.
- After cooldown, one HALF_OPEN trial is allowed.
- Successful trial → back to CLOSED.
- Failed trial → back to OPEN (another cooldown starts).
- Non-consecutive failures don't open the breaker.
- Success resets failure counter.
"""

from __future__ import annotations

import pytest

from src.llm.breaker import CircuitBreaker, State


# ---------------------------------------------------------------------------
# Fake clock
# ---------------------------------------------------------------------------

class _FakeClock:
    def __init__(self, t: float = 0.0):
        self._t = t

    def __call__(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += seconds


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_starts_closed():
    cb = CircuitBreaker("test", threshold=5, cooldown=60)
    assert cb.state is State.CLOSED
    assert not cb.is_open()


def test_opens_after_threshold_failures():
    clock = _FakeClock()
    cb = CircuitBreaker("test", threshold=5, cooldown=60, clock=clock)

    for i in range(4):
        cb.record_failure()
        assert cb.state is State.CLOSED, f"Should still be CLOSED after {i+1} failures"

    cb.record_failure()  # 5th failure
    assert cb.state is State.OPEN
    assert cb.is_open()


def test_open_breaker_skips_calls():
    clock = _FakeClock()
    cb = CircuitBreaker("test", threshold=5, cooldown=60, clock=clock)

    for _ in range(5):
        cb.record_failure()

    assert cb.is_open()
    # Further failures don't change state
    cb.record_failure()
    assert cb.is_open()


def test_half_open_after_cooldown():
    clock = _FakeClock()
    cb = CircuitBreaker("test", threshold=5, cooldown=60, clock=clock)

    for _ in range(5):
        cb.record_failure()

    assert cb.is_open()

    clock.advance(60.0)  # cooldown elapsed
    assert cb.state is State.HALF_OPEN
    assert cb.allow_trial()
    assert not cb.is_open()


def test_successful_trial_closes_breaker():
    clock = _FakeClock()
    cb = CircuitBreaker("test", threshold=5, cooldown=60, clock=clock)

    for _ in range(5):
        cb.record_failure()

    clock.advance(60.0)
    assert cb.state is State.HALF_OPEN

    cb.record_success()
    assert cb.state is State.CLOSED
    assert not cb.is_open()


def test_failed_trial_reopens_breaker():
    clock = _FakeClock()
    cb = CircuitBreaker("test", threshold=5, cooldown=60, clock=clock)

    for _ in range(5):
        cb.record_failure()

    clock.advance(60.0)
    assert cb.state is State.HALF_OPEN

    cb.record_failure()  # trial fails
    assert cb.state is State.OPEN

    # Must wait another full cooldown
    clock.advance(30.0)
    assert cb.is_open()

    clock.advance(30.0)  # total 60 s after re-opening
    assert cb.state is State.HALF_OPEN


def test_success_resets_counter():
    clock = _FakeClock()
    cb = CircuitBreaker("test", threshold=5, cooldown=60, clock=clock)

    for _ in range(4):
        cb.record_failure()

    cb.record_success()  # resets counter
    assert cb.state is State.CLOSED

    # 5 more failures needed to open again
    for i in range(4):
        cb.record_failure()
        assert cb.state is State.CLOSED

    cb.record_failure()
    assert cb.state is State.OPEN


def test_cooldown_not_elapsed_stays_open():
    clock = _FakeClock()
    cb = CircuitBreaker("test", threshold=5, cooldown=60, clock=clock)

    for _ in range(5):
        cb.record_failure()

    clock.advance(59.9)
    assert cb.state is State.OPEN  # just under cooldown

    clock.advance(0.2)
    assert cb.state is State.HALF_OPEN  # now over
