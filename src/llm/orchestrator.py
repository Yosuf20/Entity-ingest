"""
Orchestrator: drives the multi-provider fallback chain.

Flow per provider
-----------------
1. Skip if circuit breaker is OPEN.
2. ``await bucket.acquire()`` — honours RPM limit.
3. Call the provider through ``with_retry``.
4. On ``RateLimited`` after all retries → ``bucket.penalize()`` +
   ``breaker.record_failure()`` → next provider.
5. On ``PayloadTooLarge`` → ``chunker.shrink()`` + retry the SAME provider
   (up to ``MAX_SHRINKS`` times).
6. On ``ProviderDown`` or ``BadOutput`` → ``breaker.record_failure()`` →
   next provider.
7. Validate result with Pydantic schema.
8. Run grounding check: string fields must be substrings of the source text;
   numeric fields must appear verbatim.  Reject hallucinated results.
9. If all providers fail → save to ``pending_extractions`` → raise
   ``AllProvidersFailed``.
"""

from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Type

from . import chunker
from .breaker import CircuitBreaker
from .errors import (
    AllProvidersFailed,
    BadOutput,
    LLMError,
    PayloadTooLarge,
    ProviderDown,
    RateLimited,
)
from .providers import Provider
from .ratelimit import TokenBucket
from .retry import with_retry

log = logging.getLogger(__name__)

MAX_SHRINKS = 3
_NEXT_ATTEMPT_DELTA = timedelta(minutes=30)


class OrchestratorConfig:
    """Thin config bag passed to the orchestrator."""

    def __init__(
        self,
        retry_base: float = 1.0,
        retry_cap: float = 60.0,
        retry_max_attempts: int = 5,
        penalize_seconds: float = 60.0,
    ) -> None:
        self.retry_base = retry_base
        self.retry_cap = retry_cap
        self.retry_max_attempts = retry_max_attempts
        self.penalize_seconds = penalize_seconds


class Orchestrator:
    """Multi-provider LLM orchestrator with fallback, retry, and grounding.

    Parameters
    ----------
    chain:
        Ordered list of providers (most preferred first).
    buckets:
        Mapping from provider name to its ``TokenBucket``.
    breakers:
        Mapping from provider name to its ``CircuitBreaker``.
    config:
        Retry and penalize settings.
    db_conn:
        Open SQLite connection for persisting failed records. If ``None``,
        failed records are not persisted (useful in tests).
    sleep:
        Injectable async sleep — replace with a stub in tests.
    """

    def __init__(
        self,
        chain: list[Provider],
        buckets: dict[str, TokenBucket],
        breakers: dict[str, CircuitBreaker],
        config: OrchestratorConfig,
        *,
        db_conn=None,
        sleep=asyncio.sleep,
    ) -> None:
        self._chain = chain
        self._buckets = buckets
        self._breakers = breakers
        self._cfg = config
        self._db = db_conn
        self._sleep = sleep

        # Counters
        self._counts: dict[str, int] = defaultdict(int)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def extract(
        self,
        text: str,
        schema: type,
        *,
        source_url: str = "",
    ) -> dict[str, Any]:
        """Extract structured data from *text* using the provider chain.

        Parameters
        ----------
        text:
            Pre-crawled page text (may be HTML or plain text).
        schema:
            Pydantic model class defining the target structure.
        source_url:
            URL the text was fetched from — stamped onto every result.

        Returns
        -------
        dict
            Validated, grounded extraction result with ``source_url`` and
            ``extracted_by`` added.

        Raises
        ------
        AllProvidersFailed
            If every provider in the chain was tried and failed.
        """
        current_text = text

        for provider in self._chain:
            pname = provider.name
            breaker = self._breakers.get(pname)
            bucket = self._buckets.get(pname)

            # 1. Circuit-breaker check
            if breaker and breaker.is_open():
                log.info("Orchestrator: skipping %s — breaker OPEN", pname)
                self._counts[f"{pname}.skipped"] += 1
                continue

            # 2. Per-provider shrink loop (for 413)
            shrink_text = current_text
            for shrink_attempt in range(MAX_SHRINKS + 1):
                if bucket:
                    await bucket.acquire()

                try:
                    result = await with_retry(
                        lambda t=shrink_text: provider.extract(t, schema),
                        base=self._cfg.retry_base,
                        cap=self._cfg.retry_cap,
                        max_attempts=self._cfg.retry_max_attempts,
                        sleep=self._sleep,
                    )
                except RateLimited as exc:
                    self._counts[f"{pname}.rate_limited"] += 1
                    self._counts["total.rate_limited"] += 1
                    if bucket:
                        await bucket.penalize(
                            exc.retry_after if exc.retry_after else self._cfg.penalize_seconds
                        )
                    if breaker:
                        breaker.record_failure()
                    log.warning(
                        "Orchestrator: %s rate-limited after retries → next provider", pname
                    )
                    break  # move to next provider

                except PayloadTooLarge:
                    self._counts[f"{pname}.shrinks"] += 1
                    self._counts["total.shrinks"] += 1
                    if shrink_attempt >= MAX_SHRINKS:
                        log.warning(
                            "Orchestrator: %s — max shrinks reached → next provider", pname
                        )
                        if breaker:
                            breaker.record_failure()
                        break
                    log.info(
                        "Orchestrator: %s — payload too large, shrink attempt %d/%d",
                        pname, shrink_attempt + 1, MAX_SHRINKS,
                    )
                    shrink_text = chunker.shrink(shrink_text)
                    continue  # retry with smaller text

                except (ProviderDown, BadOutput) as exc:
                    self._counts[f"{pname}.failures"] += 1
                    self._counts["total.failures"] += 1
                    if breaker:
                        breaker.record_failure()
                    log.warning("Orchestrator: %s failed (%s) → next provider", pname, exc)
                    break  # move to next provider

                else:
                    # 7. Schema validation
                    try:
                        validated = schema(**result)
                    except Exception as exc:
                        self._counts[f"{pname}.bad_output"] += 1
                        if breaker:
                            breaker.record_failure()
                        log.warning(
                            "Orchestrator: %s — schema validation failed: %s", pname, exc
                        )
                        break  # move to next provider

                    # 8. Grounding check
                    if not _grounding_ok(validated.model_dump(), text):
                        self._counts[f"{pname}.grounding_failed"] += 1
                        self._counts["total.grounding_failed"] += 1
                        if breaker:
                            breaker.record_failure()
                        log.warning(
                            "Orchestrator: %s — grounding check failed (hallucination detected)",
                            pname,
                        )
                        break  # move to next provider

                    # Success
                    if breaker:
                        breaker.record_success()
                    self._counts[f"{pname}.success"] += 1
                    self._counts["total.success"] += 1

                    result_dict = validated.model_dump()
                    result_dict["source_url"] = source_url
                    result_dict["extracted_by"] = pname
                    log.info(
                        "Orchestrator: extracted by %s | url=%s", pname, source_url
                    )
                    return result_dict

        # 9. All providers failed
        self._counts["total.all_failed"] += 1
        failure_reason = "All providers failed"
        if self._db is not None:
            from src.storage.db import save_pending  # local import to avoid circular
            save_pending(
                self._db,
                source_url=source_url,
                schema_name=schema.__name__,
                raw_text=text,
                failure_reason=failure_reason,
                next_attempt_at=datetime.now(timezone.utc) + _NEXT_ATTEMPT_DELTA,
            )
            log.warning("Orchestrator: saved failed record to pending_extractions")

        raise AllProvidersFailed(f"No provider succeeded for url={source_url!r}")

    def summary(self) -> dict[str, int]:
        """Return a snapshot of all event counters."""
        return dict(self._counts)


# ---------------------------------------------------------------------------
# Grounding helpers
# ---------------------------------------------------------------------------

def _normalize(s: str) -> str:
    """Lowercase, collapse whitespace, strip punctuation."""
    s = unicodedata.normalize("NFKD", s).lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _grounding_ok(fields: dict[str, Any], source_text: str) -> bool:
    """Return True if every non-null extracted value is grounded in *source_text*.

    Rules
    -----
    - ``source_url`` and ``extracted_by`` are skipped (meta fields).
    - ``str``  values: normalised substring match.
    - ``int`` / ``float`` values: verbatim appearance of the numeric literal.
    - ``list`` values: each element is checked recursively.
    - ``bool`` / ``None``: always pass.
    """
    norm_source = _normalize(source_text)

    def check(value: Any) -> bool:
        if value is None or isinstance(value, bool):
            return True
        if isinstance(value, str):
            if not value:
                return True
            return _normalize(value) in norm_source
        if isinstance(value, (int, float)):
            # Numeric must appear verbatim (as a substring of raw source)
            return str(value) in source_text or str(int(value)) in source_text
        if isinstance(value, list):
            return all(check(item) for item in value)
        if isinstance(value, dict):
            return all(check(v) for v in value.values())
        return True  # unknown types pass

    skip = {"source_url", "extracted_by"}
    for key, value in fields.items():
        if key in skip:
            continue
        if not check(value):
            log.debug("Grounding FAIL: field=%r value=%r", key, value)
            return False
    return True
