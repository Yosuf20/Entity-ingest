"""Tests for the arXiv API source."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from src.config import CrawlerConfig, DomainConfig
from src.crawler.base import AsyncFetcher
from src.crawler.sources.arxiv import (
    ARXIV_API_URL,
    ArxivParseError,
    ArxivSource,
    iter_papers,
    parse_arxiv_atom,
)

FIXTURE = Path(__file__).parent / "fixtures" / "arxiv_sample.xml"


def test_fixture_parses_all_fields():
    papers = parse_arxiv_atom(FIXTURE.read_text(encoding="utf-8"))

    assert len(papers) == 3
    first = papers[0]
    assert first.arxiv_id == "2411.01234"
    assert first.version == "2"
    assert first.title == "New Attention Model"
    assert first.authors == ["Alice Example", "Bob Example"]
    assert first.abs_url == "http://arxiv.org/abs/2411.01234v2"
    assert first.pdf_url == "http://arxiv.org/pdf/2411.01234v2"
    assert first.published.isoformat() == "2024-11-14T09:15:30+00:00"
    assert first.updated.isoformat() == "2024-11-15T12:30:45+00:00"
    assert first.comment == "Code at https://github.com/Example/Attention-Repo.git."
    assert first.journal_ref == "Proceedings of ExampleConf 2024"
    assert first.primary_category == "cs.AI"

    second = papers[1]
    assert second.comment is None
    assert second.pdf_url is None
    assert len(second.authors) == 1

    old = papers[2]
    assert old.arxiv_id == "cs/0501001"
    assert old.version == "3"
    assert old.authors == ["Dan Oldstyle", "Erin Oldstyle"]


def test_malformed_xml_raises_clear_error():
    with pytest.raises(ArxivParseError, match="malformed arXiv XML"):
        parse_arxiv_atom("<feed><entry>")


def test_entity_expansion_is_rejected():
    xml = '<?xml version="1.0"?><!DOCTYPE feed [<!ENTITY x "expanded">]><feed>&x;</feed>'
    with pytest.raises(ArxivParseError, match="entity declarations"):
        parse_arxiv_atom(xml)


@pytest.mark.asyncio
async def test_fetch_page_calls_right_params():
    calls: list[httpx.Request] = []
    fixture = FIXTURE.read_text(encoding="utf-8")

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, text=fixture)

    config = CrawlerConfig(domains={
        "export.arxiv.org": DomainConfig(concurrency=1, min_interval=3.0),
    })
    fetcher = AsyncFetcher(
        config,
        transport=httpx.MockTransport(handler),
    )
    async with fetcher:
        papers = await ArxivSource(fetcher).fetch_page("cs.AI", 200, 100)

    assert len(papers) == 3
    assert len(calls) == 2
    request = calls[1]
    assert str(request.url).startswith(ARXIV_API_URL)
    assert request.url.params["search_query"] == "cat:cs.AI"
    assert request.url.params["start"] == "200"
    assert request.url.params["max_results"] == "100"
    assert request.url.params["sortBy"] == "submittedDate"
    assert request.url.params["sortOrder"] == "descending"


@pytest.mark.asyncio
async def test_empty_feed_ends_pagination():
    pages = [
        FIXTURE.read_text(encoding="utf-8"),
        '<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"/>',
    ]

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, text=pages.pop(0))

    config = CrawlerConfig(domains={
        "export.arxiv.org": DomainConfig(concurrency=1, min_interval=0.0),
    })
    fetcher = AsyncFetcher(
        config,
        transport=httpx.MockTransport(handler),
    )
    async with fetcher:
        papers = [paper async for paper in iter_papers(
            fetcher,
            categories=["cs.AI"],
            page_size=100,
            max_pages=5,
        )]

    assert len(papers) == 3
    assert fetcher is not None


@pytest.mark.asyncio
async def test_cross_listed_paper_appears_once():
    fixture = FIXTURE.read_text(encoding="utf-8")

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=fixture)

    config = CrawlerConfig(domains={
        "export.arxiv.org": DomainConfig(concurrency=1, min_interval=0.0),
    })
    fetcher = AsyncFetcher(
        config,
        transport=httpx.MockTransport(handler),
    )
    async with fetcher:
        papers = [paper async for paper in iter_papers(
            fetcher,
            categories=["cs.AI", "cs.LG"],
            page_size=100,
            max_pages=1,
        )]

    assert len(papers) == 3
