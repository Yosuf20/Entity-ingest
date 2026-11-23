"""Hugging Face paper code-link lookup."""

from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any

import httpx

from src.crawler.base import AsyncFetcher
from src.crawler.sources.github import normalize_repo_url
from src.storage.db import save_raw_page

log = logging.getLogger(__name__)

HF_PAPERS_API_URL = "https://huggingface.co/api/papers"
_REPOSITORY_KEYS = {"github", "githubrepo", "githubrepourl", "repository", "repositories", "repo", "repos", "code"}


class HuggingFacePapersSource:
    """Look up explicitly linked repositories for arXiv papers."""

    def __init__(self, fetcher: AsyncFetcher) -> None:
        """Create a source backed by the shared fetcher."""
        self.fetcher = fetcher

    async def find_code_link(self, arxiv_id: str) -> str | None:
        """Return an explicit GitHub repository URL for an arXiv ID."""
        url = f"{HF_PAPERS_API_URL}/{arxiv_id.strip('/')}"
        cached = self._cached_response(url)
        if cached is not None:
            status, content = cached
            if status == 404:
                return None
            return _parse_explicit_github_url(content)

        try:
            result = await self.fetcher.get(url)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                if self.fetcher.db is not None:
                    save_raw_page(
                        self.fetcher.db,
                        url=url,
                        content="",
                        status_code=404,
                    )
                return None
            raise

        if result.status == 404:
            return None
        return _parse_explicit_github_url(result.text)

    def _cached_response(self, url: str) -> tuple[int, str] | None:
        if self.fetcher.db is None:
            return None
        row = self.fetcher.db.execute(
            "SELECT content, status_code FROM raw_pages WHERE url = ?",
            (url,),
        ).fetchone()
        if row is None:
            return None
        if isinstance(row, sqlite3.Row):
            return int(row["status_code"]), row["content"]
        return int(row[1]), row[0]


async def find_code_link(
    arxiv_id: str | AsyncFetcher,
    fetcher: AsyncFetcher | str,
) -> str | None:
    """Return an explicit GitHub URL, accepting either public argument order."""
    if isinstance(arxiv_id, AsyncFetcher) and isinstance(fetcher, str):
        return await HuggingFacePapersSource(arxiv_id).find_code_link(fetcher)
    if isinstance(arxiv_id, str) and isinstance(fetcher, AsyncFetcher):
        return await HuggingFacePapersSource(fetcher).find_code_link(arxiv_id)
    raise TypeError("find_code_link requires an arXiv ID and AsyncFetcher")


def _parse_explicit_github_url(content: str) -> str | None:
    try:
        payload: Any = json.loads(content)
    except (json.JSONDecodeError, TypeError):
        log.warning("malformed Hugging Face papers JSON response")
        return None
    return _search_explicit_github(payload)


def _search_explicit_github(value: Any, key: str = "") -> str | None:
    if isinstance(value, dict):
        for child_key, child_value in value.items():
            normalized_key = str(child_key).lower().replace("_", "").replace("-", "")
            if normalized_key in _REPOSITORY_KEYS or "github" in normalized_key or "repo" in normalized_key:
                found = _github_url_from_value(child_value)
                if found is not None:
                    return found
            found = _search_explicit_github(child_value, str(child_key))
            if found is not None:
                return found
        return None
    if isinstance(value, list):
        for item in value:
            found = _github_url_from_value(item)
            if found is not None:
                return found
            found = _search_explicit_github(item, key)
            if found is not None:
                return found
    return None


def _github_url_from_value(value: Any) -> str | None:
    if isinstance(value, str):
        return _normalize_github_url(value)
    if isinstance(value, dict):
        for key in ("url", "htmlUrl", "html_url", "link", "repo", "repository", "github"):
            candidate = value.get(key)
            if isinstance(candidate, str):
                found = _normalize_github_url(candidate)
                if found is not None:
                    return found
    if isinstance(value, list):
        for item in value:
            found = _github_url_from_value(item)
            if found is not None:
                return found
    return None


def _normalize_github_url(value: str) -> str | None:
    normalized = normalize_repo_url(value)
    if normalized is None:
        return None
    owner, repo = normalized
    return f"https://github.com/{owner}/{repo}"
