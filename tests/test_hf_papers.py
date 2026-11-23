"""Tests for the Hugging Face papers code-link lookup."""

from __future__ import annotations

import sqlite3

import httpx
import pytest

from src.config import CrawlerConfig, DomainConfig
from src.crawler.base import AsyncFetcher
from src.crawler.sources.hf_papers import (
    HuggingFacePapersSource,
    find_code_link,
)


def _fetcher(handler, db: sqlite3.Connection | None = None) -> AsyncFetcher:
    config = CrawlerConfig(
        domains={
            "huggingface.co": DomainConfig(concurrency=3, min_interval=0.0),
        }
    )
    return AsyncFetcher(
        config,
        db=db,
        transport=httpx.MockTransport(handler),
    )


@pytest.mark.asyncio
async def test_repo_present():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(
            200,
            json={"githubRepo": "https://github.com/Owner/Repo.git"},
        )

    fetcher = _fetcher(handler)
    async with fetcher:
        link = await HuggingFacePapersSource(fetcher).find_code_link("2411.01234")
        assert link == "https://github.com/Owner/Repo"


@pytest.mark.asyncio
async def test_repo_absent():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(
            200,
            json={"id": "2411.01234", "linkedModels": [], "linkedSpaces": []},
        )

    fetcher = _fetcher(handler)
    async with fetcher:
        link = await find_code_link("2411.01234", fetcher)

    assert link is None


@pytest.mark.asyncio
async def test_404_returns_none():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(404)

    fetcher = _fetcher(handler)
    async with fetcher:
        link = await HuggingFacePapersSource(fetcher).find_code_link("9999.99999")

    assert link is None


@pytest.mark.asyncio
async def test_malformed_json_returns_none():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, text="{not-json")

    fetcher = _fetcher(handler)
    async with fetcher:
        link = await HuggingFacePapersSource(fetcher).find_code_link("2411.01234")

    assert link is None


@pytest.mark.asyncio
async def test_cached_result_is_not_refetched():
    from src.storage.db import init_db

    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        calls += 1
        return httpx.Response(
            200,
            json={"githubRepo": "https://github.com/owner/repo"},
        )

    db = init_db(":memory:")
    fetcher = _fetcher(handler, db=db)
    async with fetcher:
        first = await HuggingFacePapersSource(fetcher).find_code_link("2411.01234")
        second = await HuggingFacePapersSource(fetcher).find_code_link("2411.01234")

    assert first == "https://github.com/owner/repo"
    assert second == first
    assert calls == 1
