"""Asynchronous HTTP fetching with concurrency limits and robots.txt support."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

import httpx

from src.config import AppConfig, CrawlerConfig
from src.llm.errors import ProviderDown, RateLimited
from src.llm.retry import with_retry
from src.storage.db import init_db, save_raw_page

log = logging.getLogger(__name__)


class RobotsDisallowed(Exception):
    """Raised when robots.txt disallows the requested URL."""


@dataclass(slots=True)
class FetchResult:
    """Small result container for an HTTP fetch."""

    url: str
    final_url: str
    status: int
    text: str
    headers: httpx.Headers
    from_cache: bool
    fetched_at: datetime


class AsyncFetcher:
    """Shared async HTTP fetcher for all configured crawler sources."""

    def __init__(
        self,
        config: CrawlerConfig | AppConfig,
        *,
        db: sqlite3.Connection | str | Path | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create a fetcher with injected HTTP, sleep, clock, and storage state."""
        crawler_config = config.crawler if isinstance(config, AppConfig) else config
        self.config = crawler_config
        self.sleep = sleep
        self.clock = clock
        self._transport = transport
        self._external_client = client
        self._client: httpx.AsyncClient | None = client
        self._owns_client = client is None
        self._httpx_logger = logging.getLogger("httpx")
        self._previous_httpx_level = self._httpx_logger.level
        self._global_semaphore = asyncio.Semaphore(crawler_config.global_concurrency)
        self._domain_semaphores: dict[str, asyncio.Semaphore] = {}
        self._domain_locks: dict[str, asyncio.Lock] = {}
        self._last_request_at: dict[str, float] = {}
        self._robots: dict[str, RobotFileParser | None] = {}
        self._robots_locks: dict[str, asyncio.Lock] = {}

        if db is None:
            self.db = None
        elif isinstance(db, sqlite3.Connection):
            self.db = db
        else:
            self.db = init_db(db)

    async def __aenter__(self) -> AsyncFetcher:
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        """Close only a client owned by this fetcher."""
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None
        self._httpx_logger.setLevel(self._previous_httpx_level)

    async def _ensure_client(self) -> httpx.AsyncClient:
        self._previous_httpx_level = self._httpx_logger.level
        self._httpx_logger.setLevel(logging.WARNING)
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers={"User-Agent": self.config.user_agent},
                transport=self._transport,
            )
            self._owns_client = True
        return self._client

    def _domain(self, url: str) -> str:
        return urlsplit(url).netloc.lower()

    def _domain_config(self, domain: str):
        return self.config.domains.get(domain)

    def _domain_semaphore(self, domain: str) -> asyncio.Semaphore:
        semaphore = self._domain_semaphores.get(domain)
        if semaphore is None:
            domain_config = self._domain_config(domain)
            concurrency = domain_config.concurrency if domain_config else 5
            semaphore = asyncio.Semaphore(concurrency)
            self._domain_semaphores[domain] = semaphore
        return semaphore

    def _domain_lock(self, domain: str) -> asyncio.Lock:
        lock = self._domain_locks.get(domain)
        if lock is None:
            lock = asyncio.Lock()
            self._domain_locks[domain] = lock
        return lock

    def _robots_lock(self, domain: str) -> asyncio.Lock:
        lock = self._robots_locks.get(domain)
        if lock is None:
            lock = asyncio.Lock()
            self._robots_locks[domain] = lock
        return lock

    def _safe_url(self, url: str) -> str:
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

    def _utc_now(self) -> datetime:
        return datetime.now(timezone.utc)

    async def _wait_for_domain_slot(self, domain: str) -> None:
        domain_config = self._domain_config(domain)
        interval = domain_config.min_interval if domain_config else 0.0
        if interval <= 0:
            self._last_request_at[domain] = self.clock()
            return

        async with self._domain_lock(domain):
            previous = self._last_request_at.get(domain)
            if previous is not None:
                elapsed = self.clock() - previous
                remaining = interval - elapsed
                if remaining > 0:
                    await self.sleep(remaining)
            self._last_request_at[domain] = self.clock()

    def _log_request(
        self,
        url: str,
        status: int,
        started_at: float,
        attempt: int,
    ) -> None:
        latency_ms = max(0.0, (self.clock() - started_at) * 1000.0)
        log.info(
            "http_request url=%s status=%d latency_ms=%.1f attempt=%d",
            self._safe_url(url),
            status,
            latency_ms,
            attempt,
        )

    async def _robots_allowed(self, url: str) -> bool:
        parts = urlsplit(url)
        domain = parts.netloc.lower()
        async with self._robots_lock(domain):
            if domain in self._robots:
                parser = self._robots[domain]
                return parser is None or parser.can_fetch(self.config.user_agent, url)

            robots_url = urlunsplit((parts.scheme, parts.netloc, "/robots.txt", "", ""))
            client = await self._ensure_client()

            async with self._global_semaphore:
                async with self._domain_semaphore(domain):
                    await self._wait_for_domain_slot(domain)
                    started_at = self.clock()
                    try:
                        response = await client.get(
                            robots_url,
                            headers={"User-Agent": self.config.user_agent},
                        )
                        self._log_request(robots_url, response.status_code, started_at, 1)
                    except (httpx.TimeoutException, httpx.TransportError) as exc:
                        self._log_request(robots_url, 0, started_at, 1)
                        log.warning("robots.txt unreachable for %s; allowing requests: %s", domain, exc)
                        self._robots[domain] = None
                        return True

            if response.status_code == 404:
                self._robots[domain] = None
                return True
            if response.status_code >= 500:
                log.warning(
                    "robots.txt returned status %d for %s; allowing requests",
                    response.status_code,
                    domain,
                )
                self._robots[domain] = None
                return True

            parser = RobotFileParser(robots_url)
            parser.parse(response.text.splitlines())
            self._robots[domain] = parser
            return parser.can_fetch(self.config.user_agent, url)

    def _cached_etag(self, request_url: str) -> str | None:
        if self.db is None:
            return None
        row = self.db.execute(
            "SELECT etag FROM raw_pages WHERE url = ?",
            (request_url,),
        ).fetchone()
        if row is None:
            return None
        return row["etag"] if isinstance(row, sqlite3.Row) else row[0]

    def _cached_body(self, request_url: str) -> str | None:
        if self.db is None:
            return None
        row = self.db.execute(
            "SELECT content FROM raw_pages WHERE url = ?",
            (request_url,),
        ).fetchone()
        if row is None:
            return None
        return row["content"] if isinstance(row, sqlite3.Row) else row[0]

    def _retry_after(self, response: httpx.Response) -> float | None:
        value = response.headers.get("Retry-After")
        if value is None:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                return max(0.0, (retry_at - self._utc_now()).total_seconds())
            except (TypeError, ValueError, OverflowError):
                return None

    def _request_url(self, url: str, params: Mapping[str, object] | None) -> str:
        return str(httpx.URL(url, params=params))

    async def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, object] | None = None,
    ) -> FetchResult:
        """Fetch one URL, respecting robots.txt and configured limits."""
        if not await self._robots_allowed(url):
            raise RobotsDisallowed(f"robots.txt disallows {self._safe_url(url)}")

        request_url = self._request_url(url, params)
        domain = self._domain(request_url)
        client = await self._ensure_client()
        request_headers = dict(headers or {})
        etag = self._cached_etag(request_url)
        if etag:
            request_headers["If-None-Match"] = etag

        last_response: httpx.Response | None = None
        last_timeout: httpx.TimeoutException | None = None
        attempt = 0

        async def attempt_request() -> httpx.Response:
            nonlocal attempt, last_response, last_timeout
            attempt += 1
            await self._wait_for_domain_slot(domain)
            started_at = self.clock()
            try:
                response = await client.get(
                    url,
                    headers=request_headers,
                    params=params,
                )
            except httpx.TimeoutException as exc:
                last_timeout = exc
                self._log_request(request_url, 0, started_at, attempt)
                raise ProviderDown("request timed out") from exc

            last_response = response
            self._log_request(request_url, response.status_code, started_at, attempt)
            if response.status_code == 429:
                raise RateLimited(
                    "rate limited",
                    retry_after=self._retry_after(response),
                )
            if 500 <= response.status_code <= 599:
                raise ProviderDown(f"upstream returned {response.status_code}")
            if response.status_code == 304:
                return response
            if response.is_error:
                response.raise_for_status()
            return response

        async with self._global_semaphore:
            async with self._domain_semaphore(domain):
                try:
                    response = await with_retry(
                        attempt_request,
                        base=self.config.retry.base,
                        cap=self.config.retry.cap,
                        max_attempts=self.config.retry.max_attempts,
                        sleep=self.sleep,
                    )
                except RateLimited as exc:
                    if last_response is not None:
                        raise httpx.HTTPStatusError(
                            "exhausted retries for 429",
                            request=last_response.request,
                            response=last_response,
                        ) from exc
                    raise
                except ProviderDown as exc:
                    if last_timeout is not None:
                        raise last_timeout from exc
                    if last_response is not None:
                        raise httpx.HTTPStatusError(
                            "exhausted retries for upstream 5xx",
                            request=last_response.request,
                            response=last_response,
                        ) from exc
                    raise

        if response.status_code == 304:
            cached = self._cached_body(request_url)
            if cached is None:
                raise httpx.HTTPStatusError(
                    "304 returned without a cached body",
                    request=response.request,
                    response=response,
                )
            return FetchResult(
                url=request_url,
                final_url=str(response.url),
                status=304,
                text=cached,
                headers=response.headers,
                from_cache=True,
                fetched_at=self._utc_now(),
            )

        if self.db is not None:
            save_raw_page(
                self.db,
                url=request_url,
                content=response.text,
                status_code=response.status_code,
                etag=response.headers.get("ETag"),
            )

        return FetchResult(
            url=request_url,
            final_url=str(response.url),
            status=response.status_code,
            text=response.text,
            headers=response.headers,
            from_cache=False,
            fetched_at=self._utc_now(),
        )

    async def gather(
        self,
        urls: list[str],
    ) -> list[FetchResult | Exception]:
        """Fetch many URLs; return per-task exceptions instead of cancelling."""
        results = await asyncio.gather(
            *(self.get(url) for url in urls),
            return_exceptions=True,
        )
        return list(results)
