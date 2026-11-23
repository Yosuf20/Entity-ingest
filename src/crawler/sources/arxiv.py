"""arXiv API page fetching and Atom XML parsing."""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.config import AppConfig, CrawlerConfig, PapersPipelineConfig

if TYPE_CHECKING:
    from src.crawler.base import AsyncFetcher

log = logging.getLogger(__name__)

ARXIV_API_URL = "https://export.arxiv.org/api/query"
_ATOM = "{http://www.w3.org/2005/Atom}"
_ARXIV = "{http://arxiv.org/schemas/atom}"
_VERSION_RE = re.compile(r"v(\d+)$", re.IGNORECASE)


class ArxivParseError(ValueError):
    """Raised when an arXiv Atom response cannot be parsed safely."""


@dataclass(slots=True)
class ArxivPaper:
    """One paper parsed from an arXiv Atom entry."""

    arxiv_id: str
    version: str | None
    title: str
    authors: list[str]
    abs_url: str
    pdf_url: str | None
    published: datetime
    updated: datetime | None
    abstract: str | None
    comment: str | None
    journal_ref: str | None
    primary_category: str | None


def canonical_arxiv_id(value: str) -> tuple[str, str | None]:
    """Return the versionless ID and version suffix for old- and new-style IDs."""
    value = value.strip()
    match = _VERSION_RE.search(value)
    version = match.group(1) if match else None
    if match:
        value = value[:match.start()]
    return value, version


def _text(element: ET.Element | None) -> str | None:
    if element is None:
        return None
    value = " ".join("".join(element.itertext()).split())
    return value or None


def _datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ArxivParseError(f"invalid arXiv timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class ArxivSource:
    """Fetch and parse the official arXiv API through AsyncFetcher."""

    def __init__(
        self,
        fetcher: AsyncFetcher,
        *,
        config: CrawlerConfig | AppConfig | None = None,
    ) -> None:
        """Create an arXiv source from an existing fetcher."""
        self.fetcher = fetcher
        if config is not None:
            app = config if isinstance(config, AppConfig) else None
            self.categories = (
                app.papers_pipeline.categories
                if app is not None
                else getattr(fetcher.config, "categories", [])
            )
        else:
            self.categories = getattr(fetcher.config, "categories", [])

    async def fetch_page(
        self,
        category: str,
        start: int,
        max_results: int,
    ) -> list[ArxivPaper]:
        """Fetch one paginated arXiv API result page."""
        result = await self.fetcher.get(
            ARXIV_API_URL,
            params={
                "search_query": f"cat:{category}",
                "start": start,
                "max_results": max_results,
                "sortBy": "submittedDate",
                "sortOrder": "descending",
            },
        )
        return parse_arxiv_atom(result.text)


def parse_arxiv_atom(xml_text: str) -> list[ArxivPaper]:
    """Parse an Atom response without resolving XML entities."""
    if re.search(r"<!\s*(?:DOCTYPE|ENTITY)\b", xml_text, re.IGNORECASE):
        raise ArxivParseError("malformed arXiv XML: document/entity declarations are not allowed")
    try:
        root = ET.fromstring(xml_text)
    except (ET.ParseError, ValueError) as exc:
        raise ArxivParseError(f"malformed arXiv XML: {exc}") from exc

    papers: list[ArxivPaper] = []
    for entry in root.findall(f"{_ATOM}entry"):
        id_element = entry.find(f"{_ATOM}id")
        if id_element is None or not id_element.text:
            continue

        raw_id = id_element.text.strip().rstrip("/")
        parts = raw_id.split("/abs/")
        identifier = parts[-1] if len(parts) > 1 else raw_id.rsplit("/", 1)[-1]
        arxiv_id, version = canonical_arxiv_id(identifier)

        title = _text(entry.find(f"{_ATOM}title"))
        if title is None:
            continue

        authors = [
            name
            for author in entry.findall(f"{_ATOM}author")
            if (name := _text(author.find(f"{_ATOM}name"))) is not None
        ]

        abs_url = raw_id
        pdf_url: str | None = None
        for link in entry.findall(f"{_ATOM}link"):
            href = link.attrib.get("href")
            if not href:
                continue
            if link.attrib.get("title") == "pdf":
                pdf_url = href

        published = _datetime(_text(entry.find(f"{_ATOM}published")))
        if published is None:
            continue

        primary_category: str | None = None
        primary_element = entry.find(f"{_ARXIV}primary_category")
        if primary_element is not None:
            primary_category = primary_element.attrib.get("term")

        papers.append(
            ArxivPaper(
                arxiv_id=arxiv_id,
                version=version,
                title=title,
                authors=authors,
                abs_url=abs_url,
                pdf_url=pdf_url,
                published=published,
                updated=_datetime(_text(entry.find(f"{_ATOM}updated"))),
                abstract=_text(entry.find(f"{_ATOM}summary")),
                comment=_text(entry.find(f"{_ARXIV}comment")),
                journal_ref=_text(entry.find(f"{_ARXIV}journal_ref")),
                primary_category=primary_category,
            )
        )
    return papers


async def iter_papers(
    fetcher: AsyncFetcher,
    categories: Sequence[str] | None = None,
    page_size: int = 100,
    max_pages: int | None = None,
) -> AsyncIterator[ArxivPaper]:
    """Page through categories, de-duplicating papers across category feeds."""
    if categories is None:
        categories = getattr(fetcher.config, "categories", None)
    if categories is None:
        categories = ["cs.AI", "cs.LG", "cs.CL", "cs.CV"]

    source = ArxivSource(fetcher)
    seen: set[str] = set()
    for category in categories:
        for page_number in range(max_pages or 2**31):
            start = page_number * page_size
            papers = await source.fetch_page(category, start, page_size)
            if not papers:
                break
            for paper in papers:
                if paper.arxiv_id in seen:
                    continue
                seen.add(paper.arxiv_id)
                yield paper
