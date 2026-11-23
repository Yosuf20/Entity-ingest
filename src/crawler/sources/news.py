from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping
from urllib.parse import urlencode

from bs4 import BeautifulSoup
import feedparser

from src.crawler.base import AsyncFetcher
from src.parsers.dates import parse_published

log = logging.getLogger(__name__)

NEWS_SOURCES: dict[str, str] = {
    "techcrunch_ai": "https://techcrunch.com/category/artificial-intelligence/feed/",
    "wired_ai": "https://www.wired.com/feed/tag/ai/latest/rss",
    "theverge_ai": "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml",
    "venturebeat_ai": "https://feeds.feedburner.com/venturebeat/SZYF",
    "hacker_news": "https://hn.algolia.com/api/v1/search_by_date",
}


def _rss_items(xml_text: str) -> list[dict[str, object]]:
    parsed = feedparser.parse(xml_text)
    items: list[dict[str, object]] = []
    for entry in parsed.entries:
        title = entry.get("title") or ""
        url = entry.get("link") or ""
        published = entry.get("published") or entry.get("updated")
        description = entry.get("summary") or entry.get("description") or ""
        items.append({
            "title": title.strip(),
            "url": url.strip(),
            "published": published,
            "html": description,
        })
    return items


def _extract_full_text(html: str) -> str:
    try:
        import trafilatura
        text = trafilatura.extract(html, include_comments=False, include_tables=False)
        if text:
            return text
    except Exception:
        pass
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    return " ".join(soup.stripped_strings)


def _is_ai_related(title: str) -> bool:
    text = f" {title.lower()} "
    terms = (
        " ai ", " ai-", "artificial intelligence", "machine learning",
        "llm", "gpt", "openai", "anthropic", "generative", "robot",
        "neural", "transformer", "model", "intelligence",
    )
    return any(term in text for term in terms)


async def collect_news(
    fetcher: AsyncFetcher,
    now: datetime,
) -> list[dict[str, object]]:
    """Collect five public news sources and their full article text."""
    now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    items: list[dict[str, object]] = []

    rss_sources = [(name, url) for name, url in NEWS_SOURCES.items() if name != "hacker_news"]
    for source_name, source_url in rss_sources:
        try:
            result = await fetcher.get(source_url)
            raw_items = _rss_items(result.text)
        except Exception as exc:
            log.warning("news source failed source=%s error=%s", source_name, exc)
            continue
        article_urls = [str(item["url"]) for item in raw_items if item.get("url")]
        article_results = await fetcher.gather(article_urls)
        for raw, article_result in zip(raw_items, article_results):
            url = str(raw["url"])
            html = ""
            if hasattr(article_result, "text"):
                html = article_result.text
            full_text = _extract_full_text(html) if html else str(raw.get("html") or "")
            collected_at = datetime.now(timezone.utc).isoformat()
            published, method, confidence = parse_published({
                **raw,
                "html": html or str(raw.get("html") or ""),
            }, now)
            items.append({
                "title": str(raw["title"]),
                "url": url,
                "published": published,
                "date_method": method,
                "date_confidence": confidence,
                "full_text": full_text,
                "extracted_at": collected_at,
                "feed_url": source_url,
                "source": {"name": source_name, "url": url},
                "collectedAt": collected_at,
            })

    start = int((now - timedelta(hours=24)).timestamp())
    hn_url = NEWS_SOURCES["hacker_news"] + "?" + urlencode({
        "tags": "story",
        "numericFilters": f"created_at_i>{start}",
        "hitsPerPage": 100,
    })
    try:
        result = await fetcher.get(hn_url)
        payload = json.loads(result.text)
    except Exception as exc:
        log.warning("news source failed source=hacker_news error=%s", exc)
        return items

    hn_items = []
    for hit in payload.get("hits", []):
        url = hit.get("url") or f"https://news.ycombinator.com/item?id={hit.get('objectID')}"
        title = hit.get("title") or ""
        if not _is_ai_related(title):
            continue
        published = hit.get("created_at")
        hn_items.append({
            "title": title,
            "url": url,
            "published": published,
            "html": hit.get("_highlightResult", {}),
        })

    hn_urls = [str(item["url"]) for item in hn_items]
    hn_results = await fetcher.gather(hn_urls)
    for raw, article_result in zip(hn_items, hn_results):
        html = article_result.text if hasattr(article_result, "text") else ""
        collected_at = datetime.now(timezone.utc).isoformat()
        published, method, confidence = parse_published(raw, now)
        items.append({
            "title": str(raw["title"]),
            "url": str(raw["url"]),
            "published": published,
            "date_method": method,
            "date_confidence": confidence,
            "full_text": _extract_full_text(html) if html else "",
            "extracted_at": collected_at,
            "feed_url": NEWS_SOURCES["hacker_news"],
            "source": {"name": "hacker_news", "url": str(raw["url"])},
            "collectedAt": collected_at,
        })
    return items
