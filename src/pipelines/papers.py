"""Research-papers pipeline."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import AsyncIterator, Iterable

from src.config import AppConfig, CrawlerConfig
from src.crawler.sources.hf_papers_list import HuggingFacePaper, iter_hf_papers
from src.crawler.sources.github import (
    GitHubSource,
    extract_github_repos,
    normalize_repo_url,
)
from src.crawler.sources.hf_papers import HuggingFacePapersSource
from src.schemas import ResearchPaper
from src.storage.db import upsert_paper

log = logging.getLogger(__name__)


@dataclass(slots=True)
class PapersSummary:
    """Counters collected during a papers pipeline run."""

    fetched: int = 0
    with_repo: int = 0
    stars_resolved: int = 0
    not_found_404: int = 0
    duplicates_skipped: int = 0
    elapsed_seconds: float = 0.0

    def to_dict(self) -> dict[str, int | float]:
        """Return a plain summary mapping."""
        return asdict(self)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _existing_paper_ids(conn) -> set[str]:
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_papers_arxiv_id
        ON papers(arxiv_id)
        """
    )
    rows = conn.execute("SELECT arxiv_id FROM papers").fetchall()
    return {row["arxiv_id"] if hasattr(row, "keys") else row[0] for row in rows}


def _existing_metric_count(conn) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM papers
        WHERE github_url IS NOT NULL AND github_stars IS NOT NULL
        """
    ).fetchone()
    return int(row["COUNT(*)"] if hasattr(row, "keys") else row[0])


def find_repo_link(paper: HuggingFacePaper, hf_link: str | None = None) -> str | None:
    """Find a repository from abstract/comment first, then optional HF fallback."""
    text = "\n".join(
        value
        for value in (
            getattr(paper, "abstract", None),
            getattr(paper, "comment", None),
        )
        if value
    )
    if hf_link is None:
        hf_link = getattr(paper, "github_url", None)
    repos = extract_github_repos(text)
    if repos:
        return repos[0]
    if hf_link is None:
        return None
    normalized = normalize_repo_url(hf_link)
    if normalized is None:
        return None
    owner, repo = normalized
    return f"https://github.com/{owner}/{repo}"


class PapersPipeline:
    """Orchestrate Hugging Face, arXiv-link, and GitHub-star collection."""

    def __init__(
        self,
        fetcher,
        conn,
        *,
        config: CrawlerConfig | AppConfig,
        hf_source=None,
        github_source=None,
        paper_iterator=None,
        sleep=None,
    ) -> None:
        """Create a pipeline with injectable sources and iterator."""
        app = config if isinstance(config, AppConfig) else None
        crawler = app.crawler if app is not None else config
        pipeline_config = app.papers_pipeline if app is not None else None

        self.fetcher = fetcher
        self.conn = conn
        self.config = crawler
        self.pipeline_config = pipeline_config
        self.hf_source = hf_source or HuggingFacePapersSource(fetcher)
        self.github_source = github_source or GitHubSource(fetcher)
        self.paper_iterator = paper_iterator

        github_domain = crawler.domains.get("api.github.com")
        self.worker_count = github_domain.concurrency if github_domain else 5
        self.categories = (
            pipeline_config.categories
            if pipeline_config is not None
            else ["cs.AI", "cs.LG", "cs.CL", "cs.CV"]
        )
        self.page_size = pipeline_config.page_size if pipeline_config is not None else 100
        self.sleep = sleep

    async def run(
        self,
        *,
        target: int | None = None,
        categories: Iterable[str] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
    ) -> PapersSummary:
        """Run until the with-metrics target or arXiv sources are exhausted."""
        started = time.monotonic()
        target_value = (
            target
            if target is not None
            else (
                self.pipeline_config.target
                if self.pipeline_config is not None
                else 1000
            )
        )
        categories_value = list(categories or self.categories)
        page_size_value = page_size or self.page_size

        summary = PapersSummary(
            stars_resolved=0,
            with_repo=0,
            not_found_404=0,
            duplicates_skipped=0,
        )
        summary.stars_resolved = _existing_metric_count(self.conn)
        existing_ids = _existing_paper_ids(self.conn)

        if summary.stars_resolved >= target_value:
            summary.duplicates_skipped = len(existing_ids)
            summary.elapsed_seconds = time.monotonic() - started
            log.info(
                "papers summary fetched=%d with_repo=%d stars_resolved=%d 404s=%d dupes_skipped=%d elapsed=%.3fs",
                summary.fetched,
                summary.with_repo,
                summary.stars_resolved,
                summary.not_found_404,
                summary.duplicates_skipped,
                summary.elapsed_seconds,
            )
            return summary

        queue: asyncio.Queue[tuple[HuggingFacePaper | None, asyncio.Future[None] | None]] = asyncio.Queue(
            maxsize=max(1, self.worker_count)
        )
        stop = asyncio.Event()

        async def worker() -> None:
            while True:
                item = await queue.get()
                paper, ack = item
                try:
                    if paper is None:
                        return
                    if stop.is_set():
                        continue
                    await self._process_paper(paper, existing_ids, summary, stop, target_value)
                finally:
                    if ack is not None:
                        ack.set_result(None)
                    queue.task_done()

        workers = [
            asyncio.create_task(worker())
            for _ in range(max(1, self.worker_count))
        ]

        source = self.paper_iterator
        if source is None:
            async def source_iterator() -> AsyncIterator[HuggingFacePaper]:
                async for paper in iter_hf_papers(
                    self.fetcher,
                    page_size=page_size_value,
                    max_pages=max_pages,
                ):
                    yield paper
            source = source_iterator()
        elif callable(source):
            source = source()

        try:
            async for paper in source:
                if stop.is_set():
                    break
                summary.fetched += 1
                ack: asyncio.Future[None] = asyncio.get_running_loop().create_future()
                await queue.put((paper, ack))
                await ack
                if stop.is_set():
                    break
        finally:
            await queue.join()
            for _ in workers:
                await queue.put((None, None))
            await asyncio.gather(*workers)

        summary.elapsed_seconds = time.monotonic() - started
        log.info(
            "papers summary fetched=%d with_repo=%d stars_resolved=%d 404s=%d dupes_skipped=%d elapsed=%.3fs",
            summary.fetched,
            summary.with_repo,
            summary.stars_resolved,
            summary.not_found_404,
            summary.duplicates_skipped,
            summary.elapsed_seconds,
        )
        return summary

    async def _process_paper(
        self,
        paper: HuggingFacePaper,
        existing_ids: set[str],
        summary: PapersSummary,
        stop: asyncio.Event,
        target: int,
    ) -> None:
        if paper.arxiv_id in existing_ids:
            summary.duplicates_skipped += 1
            return

        hf_link: str | None = None
        repo_link = find_repo_link(paper)
        if repo_link is None:
            try:
                hf_link = await self.hf_source.find_code_link(paper.arxiv_id)
            except Exception as exc:
                log.warning("Hugging Face lookup failed for %s: %s", paper.arxiv_id, exc)
            repo_link = find_repo_link(paper, hf_link)

        if repo_link is None:
            return
        summary.with_repo += 1

        normalized = normalize_repo_url(repo_link)
        if normalized is None:
            return
        owner, repo = normalized
        try:
            repo_info = await self.github_source.get_repo_stars(owner, repo)
        except Exception as exc:
            log.warning("GitHub lookup failed for %s/%s: %s", owner, repo, exc)
            return

        if repo_info.status == 404 or repo_info.flag == "repo_not_found":
            summary.not_found_404 += 1
            log.info("GitHub 404 for arXiv paper %s: %s", paper.arxiv_id, repo_link)
            return
        if not isinstance(repo_info.stars, int):
            return

        summary.stars_resolved += 1
        collected_at = _utc_now()
        published = getattr(paper, "published", None)
        published_date = (
            getattr(paper, "published_date", None)
            or (published.isoformat() if published is not None else "")
        )
        record = ResearchPaper(
            arxiv_id=paper.arxiv_id,
            title=paper.title or paper.arxiv_id,
            authors=paper.authors,
            paper_url=f"https://arxiv.org/abs/{paper.arxiv_id}",
            published_date=published_date,
            pdf_url=getattr(paper, "pdf_url", None) or getattr(paper, "abs_url", None),
            abstract=paper.abstract,
            comment=None,
            journal_ref=None,
            github_url=repo_link,
            github_stars=repo_info.stars,
            github_stars_fetched_at=repo_info.fetched_at.isoformat(),
            github_404=False,
            source_url=getattr(paper, "source_url", None)
            or getattr(paper, "abs_url", None)
            or f"https://huggingface.co/papers/{paper.arxiv_id}",
            collected_at=collected_at,
        )

        inserted = upsert_paper(self.conn, record)
        if inserted:
            existing_ids.add(paper.arxiv_id)
        else:
            summary.duplicates_skipped += 1

        if summary.stars_resolved >= target:
            stop.set()


async def run_papers(
    fetcher,
    conn,
    *,
    config: CrawlerConfig | AppConfig,
    target: int | None = None,
    categories: Iterable[str] | None = None,
    page_size: int | None = None,
    max_pages: int | None = None,
    hf_source=None,
    github_source=None,
    paper_iterator=None,
) -> PapersSummary:
    """Convenience entry point for the papers pipeline."""
    pipeline = PapersPipeline(
        fetcher,
        conn,
        config=config,
        hf_source=hf_source,
        github_source=github_source,
        paper_iterator=paper_iterator,
    )
    return await pipeline.run(
        target=target,
        categories=categories,
        page_size=page_size,
        max_pages=max_pages,
    )
