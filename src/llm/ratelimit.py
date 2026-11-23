"""
Async token-bucket rate limiter — one per provider.

Each bucket allows at most *rpm* requests per 60-second window.
``acquire()`` suspends the caller until a token is available.
``penalize(seconds)`` idles the whole bucket after a 429, so all concurrent
tasks stop immediately rather than piling up more requests.

.. note::
   Production deployment: replace this in-process bucket with a Redis-backed
   implementation (e.g. ``redis-py`` with a Lua script or the ``redis-cell``
   module) so that the limit is shared across multiple worker processes or
   pods, preventing thundering-herd against provider APIs.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable

log = logging.getLogger(__name__)


class TokenBucket:
    """Async token bucket for a single provider.

    Parameters
    ----------
    rpm:
        Allowed requests per minute.
    clock:
        Callable returning the current time in seconds — injectable for tests.
    sleep:
        Async sleep function — injectable for tests.
    """

    def __init__(
        self,
        rpm: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], "asyncio.Future[None]"] = asyncio.sleep,
    ) -> None:
        self._rpm = rpm
        self._tokens: float = float(rpm)
        self._max_tokens: float = float(rpm)
        # Refill rate: tokens per second
        self._rate: float = rpm / 60.0
        self._last_refill: float = clock()
        self._lock = asyncio.Lock()
        self._penalized_until: float = 0.0
        self._clock = clock
        self._sleep = sleep

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def acquire(self) -> None:
        """Wait until a request token is available, then consume it."""
        async with self._lock:
            await self._wait_for_penalty()
            self._refill()

            if self._tokens < 1:
                wait = (1 - self._tokens) / self._rate
                log.debug("TokenBucket: no token available, waiting %.2fs", wait)
                await self._sleep(wait)
                self._refill()

            self._tokens -= 1
            log.debug("TokenBucket: token acquired (%.1f remaining)", self._tokens)

    async def penalize(self, seconds: float) -> None:
        """Pause all token acquisitions for *seconds* after a 429.

        This prevents concurrent tasks from flooding the provider while
        the rate-limit window resets.
        """
        async with self._lock:
            until = self._clock() + seconds
            if until > self._penalized_until:
                self._penalized_until = until
            log.info("TokenBucket: penalized for %.1fs", seconds)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._last_refill
        self._tokens = min(self._max_tokens, self._tokens + elapsed * self._rate)
        self._last_refill = now

    async def _wait_for_penalty(self) -> None:
        """Must be called while holding ``_lock``."""
        remaining = self._penalized_until - self._clock()
        if remaining > 0:
            log.info("TokenBucket: in penalty period, sleeping %.2fs", remaining)
            # Release the lock while sleeping so other tasks can observe the penalty too
            self._lock.release()
            try:
                await self._sleep(remaining)
            finally:
                await self._lock.acquire()
            self._penalized_until = 0.0
