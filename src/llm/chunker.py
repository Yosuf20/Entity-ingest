"""
Text chunker / compressor for LLM input budget management.

``fit(text, max_tokens)``
    Clean HTML boilerplate, preserve the title and headings, keep the highest-
    information-density paragraphs, and truncate to the token budget.

``shrink(text, factor)``
    Quick proportional shrink for the 413-retry path in the orchestrator.
"""

from __future__ import annotations

import re
import unicodedata
from typing import List

import tiktoken

# Use the cl100k_base tokeniser (GPT-4 / Gemini approximation).
# This is an *approximation* — different models have different tokenisers.
_ENC = tiktoken.get_encoding("cl100k_base")


def _count_tokens(text: str) -> int:
    return len(_ENC.encode(text))


def _strip_boilerplate(html: str) -> str:
    """Remove nav/footer/script/style tags and return plain text."""
    try:
        from bs4 import BeautifulSoup  # lazy import
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        return soup.get_text(separator="\n")
    except ImportError:
        # Fallback: strip all HTML tags with regex
        return re.sub(r"<[^>]+>", " ", html)


def _extract_structure(text: str) -> tuple[str, list[str], list[str]]:
    """Return (title, headings, paragraphs) from plain text."""
    lines = [l.strip() for l in text.splitlines()]
    title = ""
    headings: list[str] = []
    paragraphs: list[str] = []
    buffer: list[str] = []

    heading_re = re.compile(r"^#{1,6}\s+.+|^[A-Z][A-Z 0-9]{3,}$")

    for line in lines:
        if not line:
            if buffer:
                paragraphs.append(" ".join(buffer))
                buffer = []
            continue
        if heading_re.match(line):
            if buffer:
                paragraphs.append(" ".join(buffer))
                buffer = []
            if not title:
                title = line
            else:
                headings.append(line)
        else:
            buffer.append(line)
    if buffer:
        paragraphs.append(" ".join(buffer))

    return title, headings, paragraphs


def _density_score(para: str) -> float:
    """Score a paragraph by information density.

    Signals used:
    - Length (longer → more content)
    - Digit density (numbers are hard facts)
    - Capitalised tokens (named entities, proper nouns)
    """
    words = para.split()
    if not words:
        return 0.0
    digit_ratio = sum(1 for w in words if re.search(r"\d", w)) / len(words)
    cap_ratio = sum(1 for w in words if w and w[0].isupper()) / len(words)
    length_score = min(1.0, len(para) / 500)
    return length_score + 2 * digit_ratio + cap_ratio


def fit(text: str, max_tokens: int, *, is_html: bool = False) -> str:
    """Return a cleaned, condensed version of *text* within *max_tokens*.

    Parameters
    ----------
    text:
        Raw page content (HTML or plain text).
    max_tokens:
        Hard token ceiling.
    is_html:
        Set True when *text* is HTML so boilerplate stripping runs first.
    """
    if is_html:
        text = _strip_boilerplate(text)

    if _count_tokens(text) <= max_tokens:
        return text  # already fits

    title, headings, paragraphs = _extract_structure(text)

    # Always keep title and headings
    fixed_parts = ([title] if title else []) + headings
    fixed_text = "\n".join(fixed_parts)
    budget = max_tokens - _count_tokens(fixed_text)

    if budget <= 0:
        # Even headings overflow: just truncate the headings
        return _truncate_to_budget(fixed_text, max_tokens)

    # Keep first 3 paragraphs verbatim (likely intro / abstract)
    first_paras = paragraphs[:3]
    ranked_rest = sorted(paragraphs[3:], key=_density_score, reverse=True)

    result_paras: list[str] = []
    for para in first_paras + ranked_rest:
        para_tokens = _count_tokens(para)
        if budget - para_tokens >= 0:
            result_paras.append(para)
            budget -= para_tokens
        else:
            # Partial inclusion of this paragraph
            if budget > 20:
                result_paras.append(_truncate_to_budget(para, budget))
            break

    return fixed_text + "\n\n" + "\n\n".join(result_paras)


def shrink(text: str, factor: float = 0.6) -> str:
    """Return a proportionally shorter version of *text*.

    Used by the orchestrator on a 413 PayloadTooLarge response before
    retrying the same provider.

    Parameters
    ----------
    factor:
        Fraction of the original text to retain (default 0.6 → 60 %).
    """
    target_tokens = max(1, int(_count_tokens(text) * factor))
    return _truncate_to_budget(text, target_tokens)


def _truncate_to_budget(text: str, max_tokens: int) -> str:
    """Hard-truncate *text* to *max_tokens* by encoding and slicing."""
    tokens = _ENC.encode(text)[:max_tokens]
    return _ENC.decode(tokens)
