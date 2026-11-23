"""Mock-only tests for the research-papers pipeline."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.config import CrawlerConfig, DomainConfig, PapersPipelineConfig
from src.crawler.sources.arxiv import ArxivPaper
from src.crawler.sources.github import RepoInfo
from src.pipelines.papers import run_papers
from src.storage.db import init_db


def paper(
    arxiv_id: str,
    *,
    abstract: str | None = None,
    comment: str | None = None,
) -> ArxivPaper:
    return ArxivPaper(
        arxiv_id=arxiv_id,
        version="1",
        title=f"Paper {arxiv_id}",
        authors=["Alice Author"],
        abs_url=f"http://arxiv.org/abs/{arxiv_id}",
        pdf_url=f"http://arxiv.org/pdf/{arxiv_id}",
        published=datetime(2024, 1, 1, tzinfo=timezone.utc),
        updated=datetime(2024, 1, 2, tzinfo=timezone.utc),
        abstract=abstract,
        comment=comment,
        journal_ref=None,
        primary_category="cs.AI",
    )


class _FakeHF:
    def __init__(self, link: str | None = None) -> None:
        self.link = link
        self.calls: list[str] = []

    async def find_code_link(self, arxiv_id: str) -> str | None:
        self.calls.append(arxiv_id)
        return self.link


class _FakeGitHub:
    def __init__(self, stars: dict[str, int | None] | None = None, statuses: dict[str, int] | None = None) -> None:
        self.stars = stars or {}
        self.statuses = statuses or {}
        self.calls: list[tuple[str, str]] = []

    async def get_repo_stars(self, owner: str, repo: str) -> RepoInfo:
        key = f"{owner}/{repo}"
        self.calls.append((owner, repo))
        status = self.statuses.get(key, 200)
        stars = self.stars.get(key)
        return RepoInfo(
            stars=stars,
            fetched_at=datetime.now(timezone.utc),
            status=status,
            flag="repo_not_found" if status == 404 else None,
        )


def _config() -> CrawlerConfig:
    return CrawlerConfig(
        domains={
            "api.github.com": DomainConfig(concurrency=2, min_interval=0.0),
            "huggingface.co": DomainConfig(concurrency=2, min_interval=0.0),
        }
    )


async def _iterator(papers):
    for item in papers:
        yield item


@pytest.mark.asyncio
async def test_end_to_end_and_rerun_have_zero_duplicates():
    db = init_db(":memory:")
    papers = [
        paper("2411.00001", abstract="Code: https://github.com/Owner/Repo.git."),
        paper("2411.00002", abstract="No repository here"),
    ]
    hf = _FakeHF()
    github = _FakeGitHub(stars={"Owner/Repo": 42})

    first = await run_papers(
        object(),
        db,
        config=_config(),
        target=1,
        hf_source=hf,
        github_source=github,
        paper_iterator=_iterator(papers),
    )
    second = await run_papers(
        object(),
        db,
        config=_config(),
        target=1,
        hf_source=hf,
        github_source=github,
        paper_iterator=_iterator(papers),
    )

    assert first.stars_resolved == 1
    assert first.with_repo == 1
    assert db.execute("SELECT COUNT(*) FROM papers").fetchone()[0] == 1
    assert second.duplicates_skipped >= 1
    assert db.execute("SELECT COUNT(*) FROM papers").fetchone()[0] == 1
    row = db.execute("SELECT github_stars, github_url FROM papers").fetchone()
    assert row["github_stars"] == 42
    assert row["github_url"] == "https://github.com/Owner/Repo"


@pytest.mark.asyncio
async def test_paper_without_repo_is_skipped():
    db = init_db(":memory:")
    hf = _FakeHF()
    github = _FakeGitHub()

    summary = await run_papers(
        object(),
        db,
        config=_config(),
        target=1,
        hf_source=hf,
        github_source=github,
        paper_iterator=_iterator([paper("2411.00003", abstract="No code link")]),
    )

    assert summary.with_repo == 0
    assert summary.stars_resolved == 0
    assert github.calls == []
    assert db.execute("SELECT COUNT(*) FROM papers").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_hf_fallback_and_github_404_are_handled():
    db = init_db(":memory:")
    hf = _FakeHF("https://github.com/gone/repo")
    github = _FakeGitHub(statuses={"gone/repo": 404})

    summary = await run_papers(
        object(),
        db,
        config=_config(),
        target=1,
        hf_source=hf,
        github_source=github,
        paper_iterator=_iterator([paper("2411.00004", abstract="No direct repo")]),
    )

    assert hf.calls == ["2411.00004"]
    assert summary.with_repo == 1
    assert summary.not_found_404 == 1
    assert summary.stars_resolved == 0


@pytest.mark.asyncio
async def test_target_stops_further_pages():
    db = init_db(":memory:")
    yielded: list[str] = []

    async def iterator():
        for arxiv_id in ("2411.10001", "2411.10002", "2411.10003"):
            yielded.append(arxiv_id)
            yield paper(arxiv_id, abstract="https://github.com/owner/repo")

    github = _FakeGitHub(stars={"owner/repo": 10})
    summary = await run_papers(
        object(),
        db,
        config=_config(),
        target=1,
        hf_source=_FakeHF(),
        github_source=github,
        paper_iterator=iterator(),
    )

    assert summary.stars_resolved == 1
    assert yielded == ["2411.10001"]
