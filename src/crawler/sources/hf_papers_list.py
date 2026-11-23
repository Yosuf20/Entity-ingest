"""Paginated Hugging Face daily-papers listing through AsyncFetcher."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from src.crawler.base import AsyncFetcher
from src.crawler.sources.github import normalize_repo_url

log = logging.getLogger(__name__)

HF_DAILY_PAPERS_URL = "https://huggingface.co/api/daily_papers"


@dataclass(slots=True)
class HuggingFacePaper:
    arxiv_id: str
    title: str | None
    authors: list[str]
    published: datetime | None
    published_date: str | None
    abstract: str | None
    github_url: str | None
    source_url: str
    pdf_url: str | None = None


def _github_url(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = normalize_repo_url(value)
    if normalized is None:
        return None
    owner, repo = normalized
    return f"https://github.com/{owner}/{repo}"


def _datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_hf_papers_list(content: str) -> list[HuggingFacePaper]:
    try:
        payload = json.loads(content)
    except (json.JSONDecodeError, TypeError):
        log.warning("malformed Hugging Face daily-papers JSON response")
        return []
    if not isinstance(payload, list):
        return []

    papers: list[HuggingFacePaper] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        nested = item.get("paper")
        paper = nested if isinstance(nested, dict) else item

        arxiv_id = paper.get("id")
        if not isinstance(arxiv_id, str) or not arxiv_id.strip():
            continue
        arxiv_id = arxiv_id.strip()

        title = paper.get("title")
        if not isinstance(title, str):
            title = item.get("title") if isinstance(item.get("title"), str) else None

        authors: list[str] = []
        raw_authors = paper.get("authors")
        if isinstance(raw_authors, list):
            authors = [
                name
                for author in raw_authors
                if isinstance(author, dict)
                and isinstance((name := author.get("name")), str)
                and name
            ]

        raw_published = paper.get("publishedAt")
        if not isinstance(raw_published, str):
            raw_published = item.get("publishedAt")
        published = _datetime(raw_published)

        abstract = paper.get("summary")
        if not isinstance(abstract, str):
            abstract = item.get("summary") if isinstance(item.get("summary"), str) else None

        github_url = _github_url(paper.get("githubRepo"))
        if github_url is None:
            github_url = _github_url(item.get("githubRepo"))

        papers.append(
            HuggingFacePaper(
                arxiv_id=arxiv_id,
                title=title,
                authors=authors,
                published=published,
                published_date=published.isoformat() if published is not None else None,
                abstract=abstract,
                github_url=github_url,
                source_url=f"https://huggingface.co/papers/{arxiv_id}",
            )
        )
    return papers


async def iter_hf_papers(
    fetcher: AsyncFetcher,
    page_size: int = 100,
    max_pages: int | None = None,
) -> AsyncIterator[HuggingFacePaper]:
    """Iterate Hugging Face daily-paper pages by date, de-duplicating arXiv IDs."""
    del page_size
    seen: set[str] = set()
    date = datetime.now(timezone.utc).date()

    for _ in range(max_pages or 2**31):
        result = await fetcher.get(HF_DAILY_PAPERS_URL, params={"date": date.isoformat()})
        papers = parse_hf_papers_list(result.text)
        if not papers:
            date -= timedelta(days=1)
            continue
        for paper in papers:
            if paper.arxiv_id in seen:
                continue
            seen.add(paper.arxiv_id)
            yield paper
        date -= timedelta(days=1)
