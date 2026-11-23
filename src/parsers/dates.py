from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Mapping
from urllib.parse import urlsplit

from bs4 import BeautifulSoup


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_string(raw: object) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip()
    try:
        dt = parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError):
        dt = None
    if dt is not None:
        return _utc(dt)
    iso = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        return _utc(datetime.fromisoformat(iso))
    except (TypeError, ValueError):
        return None


def parse_published(
    raw_item: Mapping[str, object] | object,
    now: datetime,
) -> tuple[datetime | None, str | None, float]:
    """Parse a publication timestamp using a deterministic source priority."""
    if isinstance(raw_item, str):
        raw_item = {"published": raw_item}
    elif not isinstance(raw_item, Mapping):
        raw_item = {}
    now = _utc(now)

    for key in (
        "published", "pubDate", "published_at", "publishedAt",
        "date_published", "datePublished", "created_at", "createdAt",
        "timestamp",
    ):
        value = raw_item.get(key)
        if isinstance(value, (str, int, float)):
            dt = _parse_string(str(value)) if not isinstance(value, (int, float)) else None
            if dt is None and isinstance(value, (int, float)):
                try:
                    dt = datetime.fromtimestamp(float(value), timezone.utc)
                except (OverflowError, OSError, ValueError):
                    dt = None
            if dt is not None:
                naive = isinstance(value, str) and not re.search(r"(Z|[+-]\d{2}:?\d{2})$", value.strip(), re.I)
                return dt, "rss_api_timestamp", 0.65 if naive else 0.95

    html = raw_item.get("html")
    direct_json_ld = raw_item.get("json_ld") or raw_item.get("jsonld")
    if isinstance(direct_json_ld, (dict, list)):
        nodes = direct_json_ld if isinstance(direct_json_ld, list) else [direct_json_ld]
        for node in nodes:
            if isinstance(node, dict):
                candidate = node.get("datePublished")
                dt = _parse_string(candidate)
                if dt is not None:
                    naive = isinstance(candidate, str) and not re.search(r"(Z|[+-]\d{2}:?\d{2})$", candidate.strip(), re.I)
                    return dt, "json_ld", 0.65 if naive else 0.95
    if isinstance(html, str) and html:
        soup = BeautifulSoup(html, "html.parser")
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(script.string or script.get_text())
            except (json.JSONDecodeError, AttributeError, TypeError):
                continue
            values = data if isinstance(data, list) else [data]
            for node in values:
                if isinstance(node, dict):
                    candidates = [node.get("datePublished")]
                    candidates.extend(
                        g.get("datePublished")
                        for g in node.get("@graph", [])
                        if isinstance(g, dict)
                    )
                    for candidate in candidates:
                        dt = _parse_string(candidate)
                        if dt is not None:
                            naive = isinstance(candidate, str) and not re.search(r"(Z|[+-]\d{2}:?\d{2})$", candidate.strip(), re.I)
                            return dt, "json_ld", 0.65 if naive else 0.95

        meta = soup.find("meta", attrs={"property": "article:published_time"})
        if meta is None:
            meta = soup.find("meta", attrs={"property": re.compile(r"^og:", re.I)})
        if meta is not None:
            dt = _parse_string(meta.get("content"))
            if dt is not None:
                return dt, "meta", 0.95
        time_tag = soup.find("time", attrs={"datetime": True})
        if time_tag is not None:
            dt = _parse_string(time_tag.get("datetime"))
            if dt is not None:
                return dt, "time_datetime", 0.95

    for key in ("relative", "date_text", "published", "summary"):
        text = raw_item.get(key)
        dt = _parse_relative(text, now)
        if dt is not None:
            return dt, "relative", 0.75

    url = raw_item.get("url") or raw_item.get("link")
    if isinstance(url, str):
        match = re.search(r"/(?:20\d{2})/([01]\d)/([0-3]\d)/", urlsplit(url).path)
        if match:
            year_match = re.search(r"/(20\d{2})/", urlsplit(url).path)
            if year_match:
                try:
                    return datetime(
                        int(year_match.group(1)), int(match.group(1)),
                        int(match.group(2)), tzinfo=timezone.utc,
                    ), "url_path", 0.70
                except ValueError:
                    pass
    return None, None, 0.0


def _parse_relative(raw: object, now: datetime) -> datetime | None:
    if not isinstance(raw, str):
        return None
    text = raw.strip().lower()
    if text in {"just now", "now"}:
        return now
    if text == "yesterday":
        return now - timedelta(days=1)
    match = re.fullmatch(r"(\d+)\s+(second|minute|hour|day)s?\s+ago", text)
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2)
    return now - {
        "second": timedelta(seconds=amount),
        "minute": timedelta(minutes=amount),
        "hour": timedelta(hours=amount),
        "day": timedelta(days=amount),
    }[unit]
