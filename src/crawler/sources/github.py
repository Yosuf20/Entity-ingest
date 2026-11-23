"""Deterministic GitHub repository link and star lookups."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlsplit

import httpx

from src.crawler.base import AsyncFetcher, FetchResult

log = logging.getLogger(__name__)

GITHUB_API_URL = "https://api.github.com"
_GITHUB_RE = re.compile(
    r"github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)",
    re.IGNORECASE,
)
_RESERVED_OWNERS = {
    "about", "features", "orgs", "sponsors", "topics", "marketplace",
    "settings", "login", "pricing", "site", "collections", "events",
    "notifications", "explore", "codespaces", "security", "customer",
    "enterprise", "organizations", "pulls", "issues", "account",
}
DEFAULT_MAX_RATE_LIMIT_WAIT = 3600.0


@dataclass(slots=True)
class RepoInfo:
    """Resolved GitHub repository information."""

    stars: int | None
    fetched_at: datetime
    status: int
    flag: str | None = None
    full_name: str | None = None


def extract_github_repos(text: str) -> list[str]:
    """Extract normalized GitHub repository URLs from text."""
    found: list[str] = []
    seen: set[str] = set()
    for match in _GITHUB_RE.finditer(text or ""):
        url = f"https://github.com/{match.group(1)}/{match.group(2)}"
        normalized = normalize_repo_url(url)
        if normalized is None:
            continue
        owner, repo = normalized
        key = repo_key(owner, repo)
        if key not in seen:
            seen.add(key)
            found.append(f"https://github.com/{owner}/{repo}")
    return found


def normalize_repo_url(url: str) -> tuple[str, str] | None:
    """Normalize a GitHub repository URL and reject reserved/non-repository paths."""
    value = (url or "").strip().strip(".,;:!?)]}>")
    if "://" not in value:
        value = f"https://{value}"
    parts = urlsplit(value)
    host = parts.netloc.lower().split("@")[-1].split(":")[0]
    if host != "github.com" and not host.endswith(".github.com"):
        return None

    path = parts.path.strip("/")
    segments = path.split("/")
    if len(segments) < 2:
        return None

    owner = segments[0]
    repo = segments[1]
    if owner.lower() in _RESERVED_OWNERS:
        return None
    if repo.lower() in {"github.io", "github.io.git"} or owner.lower().endswith("github"):
        return None
    if host.lower().endswith(".github.io"):
        return None

    repo = repo.strip(".,;:!?)]}>")
    if repo.lower().endswith(".git"):
        repo = repo[:-4]
    if not owner or not repo:
        return None
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", owner):
        return None
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", repo):
        return None
    return owner, repo


def repo_key(owner: str, repo: str) -> str:
    """Return a case-insensitive repository dedupe key."""
    return f"{owner.lower()}/{repo.lower()}"


def normalize_repo_key(url: str) -> str | None:
    """Return the lowercase dedupe key for a repository URL."""
    normalized = normalize_repo_url(url)
    if normalized is None:
        return None
    return repo_key(*normalized)


class GitHubSource:
    """GitHub repository metadata source using AsyncFetcher."""

    _unauthenticated_warning_logged = False

    def __init__(
        self,
        fetcher: AsyncFetcher,
        *,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        clock: Callable[[], float] = time.time,
        max_rate_limit_wait: float = DEFAULT_MAX_RATE_LIMIT_WAIT,
    ) -> None:
        """Create a GitHub source with injectable waiting."""
        self.fetcher = fetcher
        self.sleep = sleep or fetcher.sleep
        self.clock = clock
        self.max_rate_limit_wait = max_rate_limit_wait

    async def get_repo_stars(
        self,
        owner: str,
        repo: str,
    ) -> RepoInfo:
        """Fetch one repository's stars and resolution status."""
        token = os.environ.get("GITHUB_TOKEN")
        headers: dict[str, str] = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        elif not GitHubSource._unauthenticated_warning_logged:
            log.warning("GITHUB_TOKEN is not set; GitHub API rate limits will be lower")
            GitHubSource._unauthenticated_warning_logged = True

        url = f"{GITHUB_API_URL}/repos/{owner}/{repo}"
        try:
            result = await self.fetcher.get(url, headers=headers)
        except httpx.HTTPStatusError as exc:
            response = exc.response
            if response.status_code == 404:
                return RepoInfo(
                    stars=None,
                    fetched_at=datetime.now(timezone.utc),
                    status=404,
                    flag="repo_not_found",
                )
            if response.status_code in (403, 429):
                remaining = response.headers.get("X-RateLimit-Remaining")
                reset = response.headers.get("X-RateLimit-Reset")
                if remaining == "0" and reset:
                    wait = await self._rate_limit_wait(reset)
                    if wait is not None:
                        try:
                            result = await self.fetcher.get(url, headers=headers)
                        except httpx.HTTPStatusError as retry_exc:
                            retry_response = retry_exc.response
                            if retry_response.status_code == 404:
                                return RepoInfo(
                                    stars=None,
                                    fetched_at=datetime.now(timezone.utc),
                                    status=404,
                                    flag="repo_not_found",
                                )
                            return RepoInfo(
                                stars=None,
                                fetched_at=datetime.now(timezone.utc),
                                status=retry_response.status_code,
                                flag="rate_limited",
                            )
                    else:
                        return RepoInfo(
                            stars=None,
                            fetched_at=datetime.now(timezone.utc),
                            status=response.status_code,
                            flag="rate_limited",
                        )
                else:
                    return RepoInfo(
                        stars=None,
                        fetched_at=datetime.now(timezone.utc),
                        status=response.status_code,
                        flag="rate_limited",
                    )
            else:
                raise

        if result.status == 404:
            return RepoInfo(
                stars=None,
                fetched_at=datetime.now(timezone.utc),
                status=404,
                flag="repo_not_found",
            )
        if result.status == 301:
            return await self._follow_rename(result, headers)
        if result.status in (403, 429):
            return RepoInfo(
                stars=None,
                fetched_at=datetime.now(timezone.utc),
                status=result.status,
                flag="rate_limited",
            )

        payload = json.loads(result.text)
        stars = payload.get("stargazers_count")
        if not isinstance(stars, int):
            stars = None
        full_name = payload.get("full_name")
        flag = "archived" if payload.get("archived") or payload.get("disabled") else None
        return RepoInfo(
            stars=stars,
            fetched_at=datetime.now(timezone.utc),
            status=200,
            flag=flag,
            full_name=full_name if isinstance(full_name, str) else None,
        )

    async def _rate_limit_wait(self, reset: str) -> float | None:
        """Sleep until reset only when the wait is within the configured limit."""
        try:
            reset_at = float(reset)
        except (TypeError, ValueError, OverflowError):
            return None
        wait = max(0.0, reset_at - self.clock())
        if wait > self.max_rate_limit_wait:
            return None
        await self.sleep(wait)
        return wait

    async def _follow_rename(
        self,
        result: FetchResult,
        headers: dict[str, str],
    ) -> RepoInfo:
        """Follow a GitHub 301 Location header and return renamed repository data."""
        location = result.headers.get("Location")
        if not location:
            return RepoInfo(
                stars=None,
                fetched_at=datetime.now(timezone.utc),
                status=301,
                flag="renamed",
            )
        renamed = await self.fetcher.get(location, headers=headers)
        payload = json.loads(renamed.text)
        stars = payload.get("stargazers_count")
        if not isinstance(stars, int):
            stars = None
        full_name = payload.get("full_name")
        flag = "archived" if payload.get("archived") or payload.get("disabled") else "renamed"
        return RepoInfo(
            stars=stars,
            fetched_at=datetime.now(timezone.utc),
            status=renamed.status,
            flag=flag,
            full_name=full_name if isinstance(full_name, str) else None,
        )
