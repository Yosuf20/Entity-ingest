from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Iterable

from src.crawler.base import AsyncFetcher
from src.parsers.dates import parse_published

log = logging.getLogger(__name__)

JOB_SOURCES = {
    "greenhouse_anthropic": "https://boards-api.greenhouse.io/v1/boards/anthropic/jobs?content=true",
    "lever_field_ai": "https://api.lever.co/v0/postings/field-ai?mode=json",
    "ashby_openai": "https://api.ashbyhq.com/posting-api/job-board/openai",
    "remoteok": "https://remoteok.com/api",
    "weworkremotely": "https://weworkremotely.com/remote-jobs.rss",
}


def _role_family(title: str) -> str:
    text = title.lower()
    if any(word in text for word in ("machine learning", "ml ", "ai ", "research", "scientist")):
        return "ai_research"
    if any(word in text for word in ("engineer", "developer", "software")):
        return "engineering"
    if any(word in text for word in ("product", "program", "project")):
        return "product"
    return "other"


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
) -> dict:
    item = dict(content_date or {})
    item.update({
        "title": title,
        "url": url,
        "html": description,
    })
    published, method, confidence = parse_published(item, now)
    collected_at = datetime.now(timezone.utc).isoformat()
    is_remote = any(word in (location + " " + description).lower()
                    for word in ("remote", "distributed", "anywhere"))
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
        "source": {"name": source_name, "url": url},
        "collectedAt": collected_at,
    }


async def collect_jobs(fetcher: AsyncFetcher, now: datetime) -> list[dict]:
    now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    records: list[dict] = []

    try:
        result = await fetcher.get(JOB_SOURCES["greenhouse_anthropic"])
        payload = json.loads(result.text)
        for job in payload.get("jobs", []):
            locations = job.get("location") or {}
            records.append(_job_record(
                title=job.get("title", ""), url=job.get("absolute_url", ""),
                company="Anthropic", location=locations.get("name", ""),
                description=job.get("content") or "",
                source_name="greenhouse_anthropic", source_url=JOB_SOURCES["greenhouse_anthropic"],
                now=now,
            ))
    except Exception as exc:
        log.warning("job source failed source=greenhouse_anthropic error=%s", exc)

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
            ))
    except Exception as exc:
        log.warning("job source failed source=ashby_openai error=%s", exc)

    try:
        result = await fetcher.get(JOB_SOURCES["remoteok"])
        payload = json.loads(result.text)
        for job in payload:
            if not isinstance(job, dict):
                continue
            if "legal" in job and not job.get("position"):
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
                description=job.get("description") or "",
                source_name="remoteok", source_url=JOB_SOURCES["remoteok"],
                now=now,
                content_date={"published": date_value},
            ))
    except Exception as exc:
        log.warning("job source failed source=remoteok error=%s", exc)

    try:
        result = await fetcher.get(JOB_SOURCES["weworkremotely"])
        from src.crawler.sources.news import _rss_items
        for item in _rss_items(result.text):
            records.append(_job_record(
                title=str(item["title"]),
                url=str(item["url"]),
                company="",
                location="Remote",
                description=str(item.get("html") or ""),
                source_name="weworkremotely",
                source_url=JOB_SOURCES["weworkremotely"],
                now=now,
                content_date={"published": item.get("published")},
            ))
    except Exception as exc:
        log.warning("job source failed source=weworkremotely error=%s", exc)

    return records
