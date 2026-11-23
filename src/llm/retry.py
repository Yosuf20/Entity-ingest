"""
Async retry helper with exponential back-off and jitter.

Only :class:`~llm.errors.RateLimited` and :class:`~llm.errors.ProviderDown`
are retried.  :class:`~llm.errors.BadOutput` and
:class:`~llm.errors.PayloadTooLarge` propagate immediately.

The ``sleep`` parameter is injectable so tests can replace it with a no-op
(or a recording stub) and run in microseconds.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from .errors import BadOutput, LLMError, PayloadTooLarge, ProviderDown, RateLimited

log = logging.getLogger(__name__)

T = TypeVar("T")

# Errors that allow retry
_RETRYABLE = (RateLimited, ProviderDown)


async def with_retry(
    call: Callable[[], Awaitable[T]],
    *,
    base: float = 1.0,
    cap: float = 60.0,
    max_attempts: int = 5,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Call *call* up to *max_attempts* times, backing off on retryable errors.

    Parameters
    ----------
    call:
        Zero-argument async callable to invoke.
    base:
        Base delay in seconds for the first retry.
    cap:
        Maximum delay in seconds (before jitter).
    max_attempts:
        Total number of attempts (1 means no retries).
    sleep:
        Async sleep function — replace with a stub in tests.

    Returns
    -------
    The return value of *call* on success.

    Raises
    ------
    RateLimited | ProviderDown
        Re-raised after all attempts are exhausted.
    BadOutput | PayloadTooLarge
        Propagated immediately without retry.
    """
    last_exc: LLMError | None = None

    for attempt in range(max_attempts):
        try:
            return await call()
        except (BadOutput, PayloadTooLarge):
            # Non-retryable: surface immediately
            raise
        except _RETRYABLE as exc:
            last_exc = exc
            if attempt == max_attempts - 1:
                break  # will re-raise below

            # Determine base delay: Retry-After takes priority
            if isinstance(exc, RateLimited) and exc.retry_after is not None:
                delay_base = exc.retry_after
            else:
                delay_base = min(cap, base * (2 ** attempt))

            jitter = random.uniform(0.5, 1.5)
            delay = delay_base * jitter

            log.info(
                "Retryable error on attempt %d/%d — sleeping %.2fs | error=%s",
                attempt + 1,
                max_attempts,
                delay,
                exc,
            )
            await sleep(delay)

    raise last_exc  # type: ignore[misc]
