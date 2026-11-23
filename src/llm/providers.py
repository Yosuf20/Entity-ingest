"""
LLM provider abstractions.

Each provider:
- Accepts plain text + a Pydantic schema class.
- Returns a ``dict`` of extracted fields.
- Maps HTTP status codes / API error codes to the exceptions in ``errors.py``.

Provider prompt rule (enforced in every subclass):
    "Extract only information explicitly stated in the text.
     Return null for anything absent. Output JSON only."

Temperature is always 0 to minimise hallucination risk.

``MockProvider`` has a scriptable response queue for deterministic tests.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from collections import deque
from typing import Any, Deque, Type, Union

import httpx

from .errors import BadOutput, PayloadTooLarge, ProviderDown, RateLimited

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared prompt template
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "You are a structured data extractor. "
    "Extract ONLY information explicitly stated in the provided text. "
    "Return null for any field not present in the text. "
    "Output valid JSON only — no markdown fences, no commentary."
)

_USER_TEMPLATE = (
    "Extract the following fields from the text below and return a JSON object.\n"
    "Schema fields: {fields}\n\n"
    "TEXT:\n{text}"
)

_TIMEOUT = httpx.Timeout(30.0)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class Provider(ABC):
    """Abstract async LLM provider."""

    name: str = "base"

    @abstractmethod
    async def extract(self, text: str, schema: type) -> dict[str, Any]:
        """Extract structured data from *text* according to *schema*.

        Parameters
        ----------
        text:
            Pre-processed source text (already chunked to fit the token budget).
        schema:
            A Pydantic model class whose field names define what to extract.

        Returns
        -------
        dict
            Raw extracted fields (not yet validated against the schema).

        Raises
        ------
        RateLimited, PayloadTooLarge, ProviderDown, BadOutput
        """

    @staticmethod
    def _build_prompt(text: str, schema: type) -> str:
        fields = list(schema.model_fields.keys())
        return _USER_TEMPLATE.format(fields=", ".join(fields), text=text)

    @staticmethod
    def _parse_json(raw: str) -> dict[str, Any]:
        """Parse JSON from the model reply, stripping markdown fences."""
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        try:
            result = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise BadOutput(f"Invalid JSON from provider: {exc}") from exc
        if not isinstance(result, dict):
            raise BadOutput(f"Expected JSON object, got {type(result).__name__}")
        return result

    @staticmethod
    def _handle_http_error(response: httpx.Response) -> None:
        """Map HTTP status codes to our exception hierarchy."""
        status = response.status_code
        if status == 429:
            retry_after = _parse_retry_after(response)
            raise RateLimited(f"HTTP 429 from provider", retry_after=retry_after)
        if status == 413:
            raise PayloadTooLarge(f"HTTP 413 from provider")
        if status >= 500:
            raise ProviderDown(f"HTTP {status} from provider")
        if status >= 400:
            raise BadOutput(f"HTTP {status} from provider: {response.text[:200]}")


def _parse_retry_after(response: httpx.Response) -> float | None:
    """Parse the ``Retry-After`` header value (seconds or HTTP-date)."""
    value = response.headers.get("retry-after")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------

class GeminiProvider(Provider):
    """Google Gemini via the REST generateContent endpoint."""

    name = "gemini"
    _BASE = "https://generativelanguage.googleapis.com/v1beta/models"

    def __init__(self, api_key: str, model: str = "gemini-1.5-flash") -> None:
        self._key = api_key
        self._model = model

    async def extract(self, text: str, schema: type) -> dict[str, Any]:
        url = f"{self._BASE}/{self._model}:generateContent?key={self._key}"
        payload = {
            "system_instruction": {"parts": [{"text": _SYSTEM_PROMPT}]},
            "contents": [{"parts": [{"text": self._build_prompt(text, schema)}]}],
            "generationConfig": {"temperature": 0, "responseMimeType": "application/json"},
        }
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(url, json=payload)
        except httpx.TimeoutException as exc:
            raise ProviderDown("Gemini request timed out") from exc

        # Gemini returns RESOURCE_EXHAUSTED as a 429-equivalent in the body
        if resp.status_code == 429 or (
            resp.status_code == 200 and "RESOURCE_EXHAUSTED" in resp.text
        ):
            retry_after = _parse_retry_after(resp)
            raise RateLimited("Gemini RESOURCE_EXHAUSTED", retry_after=retry_after)

        self._handle_http_error(resp)

        try:
            body = resp.json()
            raw = body["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError, json.JSONDecodeError) as exc:
            raise BadOutput(f"Unexpected Gemini response shape: {exc}") from exc

        return self._parse_json(raw)


# ---------------------------------------------------------------------------
# Groq
# ---------------------------------------------------------------------------

class GroqProvider(Provider):
    """Groq Cloud (Llama 3) via the OpenAI-compatible chat completions API."""

    name = "groq"
    _BASE = "https://api.groq.com/openai/v1/chat/completions"

    def __init__(self, api_key: str, model: str = "llama3-70b-8192") -> None:
        self._key = api_key
        self._model = model

    async def extract(self, text: str, schema: type) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self._key}", "Content-Type": "application/json"}
        payload = {
            "model": self._model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": self._build_prompt(text, schema)},
            ],
            "response_format": {"type": "json_object"},
        }
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(self._BASE, json=payload, headers=headers)
        except httpx.TimeoutException as exc:
            raise ProviderDown("Groq request timed out") from exc

        self._handle_http_error(resp)

        try:
            body = resp.json()
            raw = body["choices"][0]["message"]["content"]
        except (KeyError, IndexError, json.JSONDecodeError) as exc:
            raise BadOutput(f"Unexpected Groq response shape: {exc}") from exc

        return self._parse_json(raw)


# ---------------------------------------------------------------------------
# DeepSeek
# ---------------------------------------------------------------------------

class DeepSeekProvider(Provider):
    """DeepSeek via its OpenAI-compatible chat completions endpoint."""

    name = "deepseek"
    _BASE = "https://api.deepseek.com/v1/chat/completions"

    def __init__(self, api_key: str, model: str = "deepseek-chat") -> None:
        self._key = api_key
        self._model = model

    async def extract(self, text: str, schema: type) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self._key}", "Content-Type": "application/json"}
        payload = {
            "model": self._model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": self._build_prompt(text, schema)},
            ],
            "response_format": {"type": "json_object"},
        }
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(self._BASE, json=payload, headers=headers)
        except httpx.TimeoutException as exc:
            raise ProviderDown("DeepSeek request timed out") from exc

        self._handle_http_error(resp)

        try:
            body = resp.json()
            raw = body["choices"][0]["message"]["content"]
        except (KeyError, IndexError, json.JSONDecodeError) as exc:
            raise BadOutput(f"Unexpected DeepSeek response shape: {exc}") from exc

        return self._parse_json(raw)


# ---------------------------------------------------------------------------
# MockProvider — for tests
# ---------------------------------------------------------------------------

_Response = Union[dict[str, Any], Exception]


class MockProvider(Provider):
    """Scriptable provider for unit tests.

    Pass a list of responses/exceptions to the constructor.  Each call to
    ``extract`` pops the next item.  If the queue is exhausted, raises
    ``ProviderDown``.

    Example
    -------
    >>> p = MockProvider("mock1", [{"name": "Acme"}, RateLimited(retry_after=2)])
    >>> await p.extract(text, schema)   # returns {"name": "Acme"}
    >>> await p.extract(text, schema)   # raises RateLimited(retry_after=2)
    """

    def __init__(self, name: str, responses: list[_Response]) -> None:
        self.name = name
        self._queue: Deque[_Response] = deque(responses)
        self.call_count: int = 0

    async def extract(self, text: str, schema: type) -> dict[str, Any]:
        self.call_count += 1
        if not self._queue:
            raise ProviderDown(f"MockProvider '{self.name}' queue exhausted")
        item = self._queue.popleft()
        if isinstance(item, BaseException):
            raise item
        return item
