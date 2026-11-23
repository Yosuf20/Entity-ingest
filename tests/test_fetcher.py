"""Tests for the asynchronous HTTP fetcher."""

from __future__ import annotations

import asyncio
import logging

import httpx
import pytest

from src.config import CrawlerConfig, DomainConfig, RetryConfig
from src.crawler.base import AsyncFetcher, FetchResult, RobotsDisallowed
from src.storage.db import init_db, save_raw_page


class _FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class _FakeSleep:
    def __init__(self, clock: _FakeClock) -> None:
        self.clock = clock
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        self.clock.value += seconds


def _config(
    *,
    global_concurrency: int = 20,
    domain_concurrency: int = 5,
    min_interval: float = 0.0,
) -> CrawlerConfig:
    return CrawlerConfig(
        global_concurrency=global_concurrency,
        user_agent="TestBot/1.0",
        retry=RetryConfig(base=1.0, cap=60.0, max_attempts=5),
        domains={
            "example.test": DomainConfig(
                concurrency=domain_concurrency,
                min_interval=min_interval,
            )
        },
    )


@pytest.mark.asyncio
async def test_100_concurrent_urls_respect_limits():
    clock = _FakeClock()
    sleep = _FakeSleep(clock)
    peak_global = 0
    peak_domain = 0
    active_global = 0
    active_domain = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal peak_global, peak_domain, active_global, active_domain
        active_global += 1
        active_domain += 1
        peak_global = max(peak_global, active_global)
        peak_domain = max(peak_domain, active_domain)
        await asyncio.sleep(0.01)
        active_global -= 1
        active_domain -= 1
        return httpx.Response(200, text="ok", request=request)

    transport = httpx.MockTransport(handler)
    fetcher = AsyncFetcher(
        _config(global_concurrency=7, domain_concurrency=3),
        transport=transport,
        sleep=sleep,
        clock=clock,
    )
    urls = [f"https://example.test/item/{i}" for i in range(100)]
    async with fetcher:
        results = await fetcher.gather(urls)

    assert all(isinstance(result, FetchResult) for result in results)
    assert peak_global <= 7
    assert peak_domain <= 3


@pytest.mark.asyncio
async def test_per_domain_min_interval_is_enforced():
    clock = _FakeClock()
    sleep = _FakeSleep(clock)
    request_times: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        request_times.append(clock())
        return httpx.Response(200, text="ok", request=request)

    fetcher = AsyncFetcher(
        _config(domain_concurrency=5, min_interval=0.25),
        transport=httpx.MockTransport(handler),
        sleep=sleep,
        clock=clock,
    )
    async with fetcher:
        await asyncio.gather(
            fetcher.get("https://example.test/a"),
            fetcher.get("https://example.test/b"),
            fetcher.get("https://example.test/c"),
        )

    gaps = [b - a for a, b in zip(request_times, request_times[1:])]
    assert all(gap >= 0.25 for gap in gaps)


@pytest.mark.asyncio
async def test_robots_disallow_prevents_target_request():
    requested: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nDisallow: /private\n")
        return httpx.Response(200, text="should not fetch")

    fetcher = AsyncFetcher(
        _config(),
        transport=httpx.MockTransport(handler),
    )
    async with fetcher:
        with pytest.raises(RobotsDisallowed):
            await fetcher.get("https://example.test/private/item")

    assert requested == ["https://example.test/robots.txt"]


@pytest.mark.asyncio
async def test_robots_404_allowed():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, text="allowed")

    fetcher = AsyncFetcher(
        _config(),
        transport=httpx.MockTransport(handler),
    )
    async with fetcher:
        result = await fetcher.get("https://example.test/open")

    assert result.status == 200


@pytest.mark.asyncio
async def test_robots_500_allowed_with_warning(caplog):
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(500)
        return httpx.Response(200, text="allowed")

    fetcher = AsyncFetcher(
        _config(),
        transport=httpx.MockTransport(handler),
    )
    with caplog.at_level(logging.WARNING):
        async with fetcher:
            result = await fetcher.get("https://example.test/open")

    assert result.status == 200
    assert "robots.txt returned status 500" in caplog.text


@pytest.mark.asyncio
async def test_429_retry_after_retries_after_two_seconds():
    clock = _FakeClock()
    sleep = _FakeSleep(clock)
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "2"})
        return httpx.Response(200, text="ok")

    fetcher = AsyncFetcher(
        _config(),
        transport=httpx.MockTransport(handler),
        sleep=sleep,
        clock=clock,
    )
    async with fetcher:
        result = await fetcher.get("https://example.test/rate")

    assert result.status == 200
    assert len(sleep.delays) == 1
    assert 1.0 <= sleep.delays[0] <= 3.0


@pytest.mark.asyncio
async def test_5xx_retried_then_succeeds_and_404_is_not_retried():
    clock = _FakeClock()
    sleep = _FakeSleep(clock)
    server_attempts = 0
    not_found_attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal server_attempts, not_found_attempts
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path == "/server":
            server_attempts += 1
            if server_attempts == 1:
                return httpx.Response(503)
            return httpx.Response(200, text="recovered")
        not_found_attempts += 1
        return httpx.Response(404)

    fetcher = AsyncFetcher(
        _config(),
        transport=httpx.MockTransport(handler),
        sleep=sleep,
        clock=clock,
    )
    async with fetcher:
        recovered = await fetcher.get("https://example.test/server")
        with pytest.raises(httpx.HTTPStatusError):
            await fetcher.get("https://example.test/missing")

    assert recovered.status == 200
    assert server_attempts == 2
    assert not_found_attempts == 1


@pytest.mark.asyncio
async def test_304_returns_cached_body_for_etag():
    db = init_db(":memory:")
    save_raw_page(
        db,
        url="https://example.test/cached",
        content="cached text",
        status_code=200,
        etag='"abc123"',
    )
    seen_headers: dict[str, str] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        seen_headers.update(request.headers)
        return httpx.Response(304, headers={"ETag": '"abc123"'})

    fetcher = AsyncFetcher(
        _config(),
        db=db,
        transport=httpx.MockTransport(handler),
    )
    async with fetcher:
        result = await fetcher.get("https://example.test/cached")

    assert result.status == 304
    assert result.from_cache is True
    assert result.text == "cached text"
    assert seen_headers["if-none-match"] == '"abc123"'


@pytest.mark.asyncio
async def test_gather_keeps_other_results_when_one_fails():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path == "/bad":
            raise httpx.ConnectError("offline", request=request)
        return httpx.Response(200, text="ok")

    fetcher = AsyncFetcher(
        _config(),
        transport=httpx.MockTransport(handler),
    )
    async with fetcher:
        results = await fetcher.gather([
            "https://example.test/good",
            "https://example.test/bad",
        ])

    assert isinstance(results[0], FetchResult)
    assert isinstance(results[1], Exception)


@pytest.mark.asyncio
async def test_api_key_query_is_not_in_log_output(caplog):
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, text="ok")

    fetcher = AsyncFetcher(
        _config(),
        transport=httpx.MockTransport(handler),
    )
    with caplog.at_level(logging.INFO):
        async with fetcher:
            await fetcher.get(
                "https://example.test/search",
                params={"api_key": "TOP_SECRET", "q": "papers"},
            )

    assert "TOP_SECRET" not in caplog.text
    assert "api_key" not in caplog.text
