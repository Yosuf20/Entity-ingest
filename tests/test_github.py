"""Tests for deterministic GitHub repository extraction and star lookups."""

from __future__ import annotations

import asyncio
import json
import logging
import time

import httpx
import pytest

from src.config import CrawlerConfig, DomainConfig, RetryConfig
from src.crawler.base import AsyncFetcher, FetchResult
from src.crawler.sources.github import (
    GitHubSource,
    extract_github_repos,
    normalize_repo_key,
    normalize_repo_url,
)


def test_github_url_extraction_accepts_valid_and_rejects_reserved():
    text = """
    https://github.com/OpenAI/Gym
    https://github.com/features
    https://github.com/about
    https://github.com/torvalds/linux
    """
    assert extract_github_repos(text) == [
        "https://github.com/OpenAI/Gym",
        "https://github.com/torvalds/linux",
    ]


def test_normalize_repo_url_handles_git_paths_punctuation_and_case():
    assert normalize_repo_url("https://github.com/Owner/Repo.git") == ("Owner", "Repo")
    assert normalize_repo_url("https://github.com/owner/repo/tree/main/src)") == ("owner", "repo")
    assert normalize_repo_url("https://github.com/owner/repo/blob/main/readme.md.") == ("owner", "repo")
    assert normalize_repo_url("github.com/owner/repo?tab=readme#code") == ("owner", "repo")
    assert normalize_repo_key("https://github.com/Owner/Repo") == "owner/repo"
    assert normalize_repo_key("https://github.com/owner/repo.git") == "owner/repo"


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/features",
        "https://github.com/about",
        "https://github.com/orgs/example",
        "https://github.com/topics/python",
        "https://github.com/settings/profile",
        "https://owner.github.io/repository",
    ],
)
def test_normalize_repo_url_rejects_reserved_and_github_io(url):
    assert normalize_repo_url(url) is None


@pytest.mark.asyncio
async def test_404_returns_null_stars_and_flag():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(404)

    fetcher = AsyncFetcher(
        CrawlerConfig(domains={
            "api.github.com": DomainConfig(concurrency=1, min_interval=0.0),
        }),
        transport=httpx.MockTransport(handler),
    )
    async with fetcher:
        info = await GitHubSource(fetcher).get_repo_stars("gone", "repo")

    assert info.stars is None
    assert info.flag == "repo_not_found"
    assert info.status == 404


@pytest.mark.asyncio
async def test_301_rename_is_followed_and_flagged():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path == "/repos/old/name":
            return httpx.Response(301, headers={"Location": "https://api.github.com/repos/new/name"})
        return httpx.Response(
            200,
            json={"full_name": "new/name", "stargazers_count": 42},
        )

    fetcher = AsyncFetcher(
        CrawlerConfig(domains={
            "api.github.com": DomainConfig(concurrency=1, min_interval=0.0),
        }),
        transport=httpx.MockTransport(handler),
    )
    async with fetcher:
        info = await GitHubSource(fetcher).get_repo_stars("old", "name")

    assert info.stars == 42
    assert info.flag == "renamed"
    assert info.full_name == "new/name"


@pytest.mark.asyncio
async def test_rate_limit_reset_is_waited_and_retried(monkeypatch):
    reset = str(int(time.time()) + 120)
    calls = 0
    delays: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        calls += 1
        if calls == 1:
            return httpx.Response(
                429,
                headers={
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": reset,
                },
            )
        return httpx.Response(200, json={"full_name": "owner/repo", "stargazers_count": 7})

    class FakeClock:
        def __call__(self) -> float:
            return float(reset) - 120

    async def fake_sleep(seconds: float) -> None:
        delays.append(seconds)

    fetcher = AsyncFetcher(
        CrawlerConfig(
            retry=RetryConfig(base=1.0, cap=60.0, max_attempts=2),
            domains={
                "api.github.com": DomainConfig(concurrency=1, min_interval=0.0),
            },
        ),
        transport=httpx.MockTransport(handler),
        sleep=fake_sleep,
        clock=FakeClock(),
    )
    async with fetcher:
        source = GitHubSource(
            fetcher,
            sleep=fake_sleep,
            clock=FakeClock(),
            max_rate_limit_wait=180,
        )
        info = await source.get_repo_stars("owner", "repo")

    assert info.stars == 7
    assert len(delays) == 1
    assert 60.0 <= delays[0] <= 180.0
    assert calls == 2


@pytest.mark.asyncio
async def test_token_never_appears_in_log_output(monkeypatch, caplog):
    monkeypatch.setenv("GITHUB_TOKEN", "TOP_SECRET")

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        assert request.headers["Authorization"] == "Bearer TOP_SECRET"
        return httpx.Response(200, json={"full_name": "owner/repo", "stargazers_count": 9})

    fetcher = AsyncFetcher(
        CrawlerConfig(domains={
            "api.github.com": DomainConfig(concurrency=1, min_interval=0.0),
        }),
        transport=httpx.MockTransport(handler),
    )
    with caplog.at_level(logging.INFO):
        async with fetcher:
            info = await GitHubSource(fetcher).get_repo_stars("owner", "repo")

    assert info.stars == 9
    assert "TOP_SECRET" not in caplog.text


@pytest.mark.asyncio
async def test_stars_is_int_and_archived_flag():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(
            200,
            json={"full_name": "owner/repo", "stargazers_count": 123, "archived": True},
        )

    fetcher = AsyncFetcher(
        CrawlerConfig(domains={
            "api.github.com": DomainConfig(concurrency=1, min_interval=0.0),
        }),
        transport=httpx.MockTransport(handler),
    )
    async with fetcher:
        info = await GitHubSource(fetcher).get_repo_stars("owner", "repo")

    assert isinstance(info.stars, int)
    assert info.stars == 123
    assert info.flag == "archived"
