"""
Pydantic models for all entity types the pipeline extracts.

``_Base`` carries provenance (source_url, extracted_by) shared by LLM-extracted
records.  ``ResearchPaper`` has its own provenance model: source_url comes from
the crawled arXiv page URL and collected_at is the UTC wall-clock time of
collection.  LLMs never touch research paper data.
"""

from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# LLM-extracted entities (Phase III)
# ---------------------------------------------------------------------------

class _Base(BaseModel):
    source_url: str               # crawled page the data came from
    extracted_by: Optional[str] = None  # LLM provider that produced this record


class Startup(_Base):
    name: Optional[str] = None
    description: Optional[str] = None
    founded_year: Optional[int] = None
    founders: Optional[List[str]] = None
    funding_total_usd: Optional[float] = None
    stage: Optional[str] = None
    website: Optional[str] = None
    tags: Optional[List[str]] = None


class Product(_Base):
    name: Optional[str] = None
    description: Optional[str] = None
    price_usd: Optional[float] = None
    category: Optional[str] = None
    launch_date: Optional[str] = None


class Paper(_Base):
    """LLM-extracted paper stub (placeholder; ResearchPaper is the real model)."""
    title: Optional[str] = None
    authors: Optional[List[str]] = None
    year: Optional[int] = None
    abstract: Optional[str] = None
    doi: Optional[str] = None
    venue: Optional[str] = None


class Job(_Base):
    title: Optional[str] = None
    company: Optional[str] = None
    location: Optional[str] = None
    salary_usd: Optional[float] = None
    remote: Optional[bool] = None
    posted_date: Optional[str] = None
    description: Optional[str] = None
    date_method: Optional[str] = None
    date_confidence: Optional[str] = None
    feed_url: Optional[str] = None
    collected_at: Optional[str] = None
    board_type: Optional[str] = None


class News(_Base):
    headline: Optional[str] = None
    summary: Optional[str] = None
    published_at: Optional[str] = None
    author: Optional[str] = None
    outlet: Optional[str] = None
    full_text: Optional[str] = None
    date_method: Optional[str] = None
    date_confidence: Optional[str] = None
    feed_url: Optional[str] = None
    collected_at: Optional[str] = None
    hn_url: Optional[str] = None


# ---------------------------------------------------------------------------
# Deterministically-collected research paper (Phase I — no LLM)
# ---------------------------------------------------------------------------

class ResearchPaper(BaseModel):
    """
    Structured record for an arXiv paper.

    All fields are populated deterministically from the arXiv API and GitHub
    REST API — the LLM layer is never involved.  Every record carries
    ``source_url`` (the arXiv abstract page) and ``collected_at`` (UTC ISO-8601).
    """

    # Required identity
    arxiv_id: str                             # canonical arXiv ID (e.g. "2301.00001")
    title: str
    authors: List[str]
    paper_url: str                            # arXiv abstract URL
    published_date: str                       # UTC ISO-8601 from API

    # Derived / enriched
    pdf_url: Optional[str] = None
    abstract: Optional[str] = None
    comment: Optional[str] = None            # arxiv:comment field
    journal_ref: Optional[str] = None

    # Code link (regex / HF papers lookup — deterministic, no LLM)
    github_url: Optional[str] = None
    github_stars: Optional[int] = None
    github_stars_fetched_at: Optional[str] = None  # UTC ISO-8601
    github_404: bool = False                  # True when GitHub returned 404

    # Provenance (hard rule: every record must carry these)
    source_url: str                           # = paper_url (arXiv abs page)
    collected_at: str                         # UTC ISO-8601 wall-clock at collection
