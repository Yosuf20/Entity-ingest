from __future__ import annotations

import json
import logging
import re
import html
from datetime import datetime, timezone
from typing import Iterable

from bs4 import BeautifulSoup

from src.crawler.base import AsyncFetcher
from src.parsers.dates import parse_published

log = logging.getLogger(__name__)

JOB_SOURCES = {
    "greenhouse_anthropic": "https://boards-api.greenhouse.io/v1/boards/anthropic/jobs?content=true",
    "greenhouse_scaleai": "https://boards-api.greenhouse.io/v1/boards/scaleai/jobs?content=true",
    "lever_field_ai": "https://api.lever.co/v0/postings/field-ai?mode=json",
    "ashby_openai": "https://api.ashbyhq.com/posting-api/job-board/openai",
    "ashby_cohere": "https://api.ashbyhq.com/posting-api/job-board/cohere",
    "ashby_perplexity": "https://api.ashbyhq.com/posting-api/job-board/perplexity",
    "remoteok": "https://remoteok.com/api",
    "weworkremotely": "https://weworkremotely.com/remote-jobs.rss",
}


def _matches_ai_keywords(title: str, tags: object, keywords: list[str]) -> bool:
    haystack = f"{title} {' '.join(str(tag) for tag in tags if isinstance(tag, str)) if isinstance(tags, list) else str(tags or '')}".lower()
    return any(re.search(rf"(?<!\w){re.escape(keyword.lower())}(?!\w)", haystack) for keyword in keywords)


def _role_family(title: str) -> str:
    text = title.lower()
    if any(word in text for word in ("machine learning", "ml ", "ai ", "research", "scientist")):
        return "ai_research"
    if any(word in text for word in ("engineer", "developer", "software")):
        return "engineering"
    if any(word in text for word in ("product", "program", "project")):
        return "product"
    return "other"


def _clean_description(raw: str) -> str:
    if not raw:
        return ""
    soup = BeautifulSoup(html.unescape(raw), "html.parser")
    return " ".join(soup.stripped_strings)


def _split_wwr_title(raw_title: str) -> tuple[str, str]:
    title = raw_title.strip()
    if ": " not in title:
        log.warning("We Work Remotely job has no company title prefix: %s", title)
        return "", title
    company, job_title = title.split(": ", 1)
    return company.strip(), job_title.strip()


def _is_remote(
    *,
    location: str = "",
    workplace_type: str | None = None,
    is_remote: bool | None = None,
    weworkremotely: bool = False,
) -> bool:
    if weworkremotely:
        return True
    if is_remote is True:
        return True
    workplace = (workplace_type or "").lower()
    if workplace == "remote":
        return True
    return "remote" in location.lower()


def _job_record(
    *,
    title: str,
    url: str,
    company: str,
    location: str,
    description: str,
    source_name: str,
    source_url: str,
    now: datetime,
    content_date: dict | None = None,
    board_type: str = "ai_company",
    is_remote_override: bool | None = None,
    workplace_type: str | None = None,
    weworkremotely: bool = False,
) -> dict:
    item = dict(content_date or {})
    item.update({
        "title": title,
        "url": url,
    })
    published, method, confidence = parse_published(item, now)
    collected_at = datetime.now(timezone.utc).isoformat()
    is_remote = _is_remote(
        location=location,
        workplace_type=workplace_type,
        is_remote=is_remote_override,
        weworkremotely=weworkremotely,
    )
    return {
        "title": title,
        "url": url,
        "published": published,
        "date_method": method,
        "date_confidence": confidence,
        "full_text": description,
        "extracted_at": collected_at,
        "content": {
            "company": company,
            "date": published.isoformat() if published else None,
            "is_remote": is_remote,
            "role_family": _role_family(title),
        },
        "feed_url": source_url,
        "board_type": board_type,
        "source": {"name": source_name, "url": url},
        "collectedAt": collected_at,
    }


async def collect_jobs(
    fetcher: AsyncFetcher,
    now: datetime,
    ai_keywords: list[str] | None = None,
) -> list[dict]:
    now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    ai_keywords = ai_keywords or []
    if not ai_keywords:
        log.warning("ai_keywords is empty; general job boards may not be filtered")
    records: list[dict] = []

    try:
        result = await fetcher.get(JOB_SOURCES["greenhouse_anthropic"])
        payload = json.loads(result.text)
        for job in payload.get("jobs", []):
            locations = job.get("location") or {}
            records.append(_job_record(
                title=job.get("title", ""), url=job.get("absolute_url", ""),
                company="Anthropic", location=locations.get("name", ""),
                description=_clean_description(job.get("content") or ""),
                source_name="greenhouse_anthropic", source_url=JOB_SOURCES["greenhouse_anthropic"],
                now=now,
                content_date={"first_published": job.get("first_published")},
            ))
    except Exception as exc:
        log.warning("job source failed source=greenhouse_anthropic error=%s", exc)

    for source_name, company in (
        ("greenhouse_scaleai", "Scale AI"),
    ):
        try:
            result = await fetcher.get(JOB_SOURCES[source_name])
            payload = json.loads(result.text)
            for job in payload.get("jobs", []):
                location = job.get("location") or {}
                records.append(_job_record(
                    title=job.get("title", ""), url=job.get("absolute_url", ""),
                    company=company, location=location.get("name", ""),
                    description=_clean_description(job.get("content") or ""),
                    source_name=source_name, source_url=JOB_SOURCES[source_name],
                    now=now,
                    content_date={"first_published": job.get("first_published")},
                ))
        except Exception as exc:
            log.warning("job source failed source=%s error=%s", source_name, exc)

    try:
        result = await fetcher.get(JOB_SOURCES["lever_field_ai"])
        payload = json.loads(result.text)
        for job in payload:
            cats = job.get("categories") or {}
            records.append(_job_record(
                title=job.get("text", ""), url=job.get("hostedUrl") or job.get("applyUrl", ""),
                company="FieldAI",
                location=cats.get("location") or job.get("workplaceType") or "",
                description=job.get("descriptionPlain") or job.get("description") or "",
                source_name="lever_field_ai", source_url=JOB_SOURCES["lever_field_ai"], now=now,
                content_date={
                    "createdAt": job.get("createdAt"),
                    "created_at": job.get("createdAt"),
                },
                workplace_type=job.get("workplaceType"),
            ))
    except Exception as exc:
        log.warning("job source failed source=lever_field_ai error=%s", exc)

    try:
        result = await fetcher.get(JOB_SOURCES["ashby_openai"])
        payload = json.loads(result.text)
        for job in payload.get("jobs", []):
            records.append(_job_record(
                title=job.get("title", ""), url=job.get("jobUrl") or job.get("applyUrl", ""),
                company="OpenAI",
                location=job.get("location") or job.get("secondaryLocationsText") or "",
                description=job.get("descriptionPlain") or job.get("descriptionHtml") or "",
                source_name="ashby_openai", source_url=JOB_SOURCES["ashby_openai"], now=now,
                content_date={"publishedAt": job.get("publishedAt")},
                is_remote_override=job.get("isRemote"),
                workplace_type=job.get("workplaceType"),
            ))
    except Exception as exc:
        log.warning("job source failed source=ashby_openai error=%s", exc)

    for source_name, company in (
        ("ashby_cohere", "Cohere"),
        ("ashby_perplexity", "Perplexity"),
    ):
        try:
            result = await fetcher.get(JOB_SOURCES[source_name])
            payload = json.loads(result.text)
            for job in payload.get("jobs", []):
                records.append(_job_record(
                    title=job.get("title", ""), url=job.get("jobUrl") or job.get("applyUrl", ""),
                    company=company,
                    location=job.get("location") or job.get("secondaryLocationsText") or "",
                    description=job.get("descriptionPlain") or job.get("descriptionHtml") or "",
                    source_name=source_name, source_url=JOB_SOURCES[source_name], now=now,
                    content_date={"publishedAt": job.get("publishedAt")},
                    is_remote_override=job.get("isRemote"),
                    workplace_type=job.get("workplaceType"),
                ))
        except Exception as exc:
            log.warning("job source failed source=%s error=%s", source_name, exc)

    try:
        result = await fetcher.get(JOB_SOURCES["remoteok"])
        payload = json.loads(result.text)
        dated = sorted(
            [job for job in payload if isinstance(job, dict) and job.get("date")],
            key=lambda job: str(job["date"]),
            reverse=True,
        )
        log.info(
            "RemoteOK newest dates utc_now=%s dates=%s",
            datetime.now(timezone.utc).isoformat(),
            [job["date"] for job in dated[:5]],
        )
        for job in payload:
            if not isinstance(job, dict):
                continue
            if "legal" in job and not job.get("position"):
                continue
            title = str(job.get("position") or job.get("title") or "")
            if not _matches_ai_keywords(title, job.get("tags"), ai_keywords):
                continue
            date_value = job.get("date")
            if not date_value:
                epoch = job.get("epoch")
                if epoch:
                    date_value = datetime.fromtimestamp(float(epoch), timezone.utc).isoformat()
            records.append(_job_record(
                title=job.get("position") or job.get("title", ""),
                url=job.get("url") or job.get("apply_url", ""),
                company=job.get("company", ""),
                location=job.get("location") or "Remote",
                description=_clean_description(job.get("description") or ""),
                source_name="remoteok", source_url=JOB_SOURCES["remoteok"],
                now=now,
                content_date={"published": date_value},
                board_type="general_filtered",
            ))
    except Exception as exc:
        log.warning("job source failed source=remoteok error=%s", exc)

    try:
        result = await fetcher.get(JOB_SOURCES["weworkremotely"])
        from src.crawler.sources.news import _rss_items
        for item in _rss_items(result.text):
            raw_title = str(item["title"])
            company, title = _split_wwr_title(raw_title)
            if not _matches_ai_keywords(title, None, ai_keywords):
                continue
            records.append(_job_record(
                title=title,
                url=str(item["url"]),
                company=company,
                location="Remote",
                description=_clean_description(str(item.get("html") or "")),
                source_name="weworkremotely",
                source_url=JOB_SOURCES["weworkremotely"],
                now=now,
                content_date={"published": item.get("published")},
                board_type="general_filtered",
                weworkremotely=True,
            ))
    except Exception as exc:
        log.warning("job source failed source=weworkremotely error=%s", exc)

    return records
