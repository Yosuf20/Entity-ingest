"""
Per-provider circuit breaker: CLOSED → OPEN → HALF_OPEN → CLOSED.

States
------
CLOSED    Normal operation; failures are counted.
OPEN      Provider is assumed down; calls are skipped immediately.
HALF_OPEN One trial request is allowed to probe recovery.

Transitions
-----------
CLOSED  → OPEN      after *threshold* consecutive failures.
OPEN    → HALF_OPEN after *cooldown* seconds.
HALF_OPEN → CLOSED  on success of the trial request.
HALF_OPEN → OPEN    on failure of the trial request.

The ``clock`` parameter is injectable so tests can control time without
sleeping.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from enum import Enum, auto
import time

log = logging.getLogger(__name__)


class State(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class CircuitBreaker:
    """Circuit breaker for a single LLM provider.

    Parameters
    ----------
    name:
        Provider label used in log messages.
    threshold:
        Number of consecutive failures before the breaker opens.
    cooldown:
        Seconds to wait in OPEN state before moving to HALF_OPEN.
    clock:
        Callable returning the current time in seconds — injectable for tests.
    """

    def __init__(
        self,
        name: str,
        *,
        threshold: int = 5,
        cooldown: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.name = name
        self._threshold = threshold
        self._cooldown = cooldown
        self._clock = clock

        self._state: State = State.CLOSED
        self._failures: int = 0
        self._opened_at: float = 0.0

    # ------------------------------------------------------------------
    # State queries
    # ------------------------------------------------------------------

    @property
    def state(self) -> State:
        """Return current state, automatically transitioning OPEN → HALF_OPEN."""
        if self._state is State.OPEN:
            if self._clock() - self._opened_at >= self._cooldown:
                log.info("CircuitBreaker[%s]: cooldown elapsed → HALF_OPEN", self.name)
                self._state = State.HALF_OPEN
        return self._state

    def is_open(self) -> bool:
        """Return True if the breaker is in OPEN state (calls should be skipped)."""
        return self.state is State.OPEN

    def allow_trial(self) -> bool:
        """Return True if the breaker is HALF_OPEN (one trial request allowed)."""
        return self.state is State.HALF_OPEN

    # ------------------------------------------------------------------
    # Recording outcomes
    # ------------------------------------------------------------------

    def record_success(self) -> None:
        """Call after a successful provider response."""
        if self._state in (State.HALF_OPEN, State.OPEN):
            log.info("CircuitBreaker[%s]: success → CLOSED", self.name)
        self._failures = 0
        self._state = State.CLOSED

    def record_failure(self) -> None:
        """Call after a provider failure (any error type)."""
        self._failures += 1
        if self._state is State.HALF_OPEN:
            # Trial failed: go back to OPEN
            self._opened_at = self._clock()
            self._state = State.OPEN
            log.warning(
                "CircuitBreaker[%s]: trial failed → OPEN (cooldown %.1fs)",
                self.name, self._cooldown,
            )
        elif self._failures >= self._threshold and self._state is State.CLOSED:
            self._opened_at = self._clock()
            self._state = State.OPEN
            log.warning(
                "CircuitBreaker[%s]: %d consecutive failures → OPEN",
                self.name, self._failures,
            )
        else:
            log.debug(
                "CircuitBreaker[%s]: failure %d/%d",
                self.name, self._failures, self._threshold,
            )
