"""
Custom exceptions for the LLM resilience layer.

Hierarchy
---------
LLMError
├── RateLimited        – 429 / RESOURCE_EXHAUSTED; optionally carries retry_after
├── PayloadTooLarge    – 413; input must be shrunk before retry
├── ProviderDown       – 5xx / network timeout
├── BadOutput          – 400 / non-JSON / schema mismatch; do NOT retry
└── AllProvidersFailed – every provider in the chain was exhausted
"""

from __future__ import annotations


class LLMError(Exception):
    """Base class for all LLM-layer errors."""


class RateLimited(LLMError):
    """Provider returned 429 or RESOURCE_EXHAUSTED.

    Parameters
    ----------
    retry_after:
        Seconds to wait before the next attempt, as indicated by the
        ``Retry-After`` response header.  ``None`` if the header was absent.
    """

    def __init__(self, message: str = "Rate limited", retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class PayloadTooLarge(LLMError):
    """Provider returned 413 — input text must be shrunk before retrying."""


class ProviderDown(LLMError):
    """Provider returned 5xx or the request timed out."""


class BadOutput(LLMError):
    """Provider returned 400, non-parseable JSON, or failed schema validation.

    Do not retry: the same input will produce the same bad output.
    """


class AllProvidersFailed(LLMError):
    """Every provider in the fallback chain was exhausted without success."""
