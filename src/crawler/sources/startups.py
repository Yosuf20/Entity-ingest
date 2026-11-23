"""Y Combinator startup sources."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from html import unescape

YC_DATASET_URL = (
    "https://raw.githubusercontent.com/akshaybhalotia/yc_company_scraper/"
    "main/data/yc_essential_data.json"
)
YC_SOURCE_NAME = "Y Combinator"


def canonical_yc_url(slug: str) -> str:
    normalized = re.sub(r"[^a-z0-9-]+", "-", slug.strip().lower()).strip("-")
    if not normalized:
        raise ValueError("Y Combinator company slug is empty")
    return f"https://www.ycombinator.com/companies/{normalized}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _employee_count(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str):
        match = re.fullmatch(r"\s*(\d+)\s*", value)
        if match:
            return int(match.group(1))
    return None


def startup_record(
    name: object,
    slug: object,
    team_size: object,
    *,
    collected_at: str | None = None,
) -> dict | None:
    if not isinstance(name, str) or not name.strip():
        return None
    if not isinstance(slug, str) or not slug.strip():
        return None
    source_url = canonical_yc_url(slug)
    return {
        "schemaVersion": "1.0",
        "recordType": "STARTUP",
        "source": {"name": YC_SOURCE_NAME, "url": source_url},
        "content": {
            "entityName": name.strip(),
            "data": {"employeeCount": _employee_count(team_size)},
        },
        "collectedAt": collected_at or _utc_now(),
    }


def parse_yc_dataset(
    json_text: str,
    *,
    collected_at: str | None = None,
) -> list[dict]:
    """Parse the MIT-licensed YC community dataset fixture/API response."""
    json_text = json_text.lstrip("\ufeff")
    try:
        rows = json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise ValueError("malformed Y Combinator dataset JSON") from exc
    if not isinstance(rows, list):
        raise ValueError("Y Combinator dataset must be a JSON array")

    records: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        record = startup_record(
            row.get("name"),
            row.get("slug"),
            row.get("team_size"),
            collected_at=collected_at,
        )
        if record is None:
            continue
        canonical_url = record["source"]["url"]
        if canonical_url in seen:
            continue
        seen.add(canonical_url)
        records.append(record)
    return records


def parse_yc_company_html(
    html: str,
    *,
    source_url: str,
    collected_at: str | None = None,
) -> dict | None:
    """Parse fields visible in one static Y Combinator company page."""
    title_match = re.search(r"<title>\s*([^:<]+?)\s*(?::| \| Y Combinator)", html, re.I)
    name = unescape(title_match.group(1).strip()) if title_match else None
    if name is None:
        heading_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.I | re.S)
        name = unescape(re.sub(r"<[^>]+>", "", heading_match.group(1))).strip() if heading_match else None

    description_match = re.search(
        r'<meta[^>]+(?:name="description"|property="og:description")[^>]+content="([^"]*)"',
        html,
        re.I,
    )
    if description_match is None:
        description_match = re.search(
            r'<meta[^>]+content="([^"]*)"[^>]+(?:name="description"|property="og:description")',
            html,
            re.I,
        )

    team_match = re.search(r"<span>Team Size:</span><span>(\d+)</span>", html, re.I)
    slug_match = re.search(r"/companies/([^/?#]+)", source_url)
    slug = slug_match.group(1) if slug_match else None
    if not name or not slug:
        return None
    return startup_record(
        name,
        slug,
        team_match.group(1) if team_match else None,
        collected_at=collected_at,
    )
