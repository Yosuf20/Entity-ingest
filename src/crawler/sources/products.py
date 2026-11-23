"""Hugging Face organization-owned model product source."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone

log = logging.getLogger(__name__)


HF_API_URL = "https://huggingface.co/api/models"
HF_SOURCE_NAME = "Hugging Face"

# Explicit open-source license rule: only these SPDX/license tags set FREE.
OPEN_SOURCE_LICENSE_TAGS = frozenset({
    "apache-2.0",
    "mit",
    "bsd-2-clause",
    "bsd-3-clause",
    "mpl-2.0",
    "cc-by-4.0",
    "cc-by-sa-4.0",
})


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_model_url(model_id: str) -> str:
    model_id = model_id.strip().strip("/")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", model_id):
        raise ValueError("invalid Hugging Face model id")
    return f"https://huggingface.co/{model_id}"


def split_model_id(model_id: str) -> tuple[str | None, str | None]:
    parts = model_id.strip().strip("/").split("/", 1)
    if len(parts) != 2:
        return None, None
    return parts[0].strip() or None, parts[1].strip() or None


def pricing_model_from_tags(tags: object) -> str | None:
    """Set FREE only for an explicit recognized open-source license tag."""
    if not isinstance(tags, list):
        return None
    normalized = set()
    for tag in tags:
        if not isinstance(tag, str):
            continue
        value = tag.strip().casefold()
        if value.startswith("license:"):
            value = value.split(":", 1)[1]
        normalized.add(value)
    if normalized & OPEN_SOURCE_LICENSE_TAGS:
        return "FREE"
    return None


def product_record(row: dict, *, collected_at: str | None = None) -> dict | None:
    model_id = row.get("id") or row.get("modelId")
    if not isinstance(model_id, str) or not model_id.strip():
        return None
    owner, _name = split_model_id(model_id)
    if owner is None:
        return None
    try:
        source_url = canonical_model_url(model_id)
    except ValueError:
        return None
    return {
        "schemaVersion": "1.0",
        "recordType": "PRODUCT",
        "source": {"name": HF_SOURCE_NAME, "url": source_url},
        "content": {
            "startupName": owner,
            "pricingModel": pricing_model_from_tags(row.get("tags")),
        },
        "collectedAt": collected_at or _utc_now(),
    }


def parse_hf_models_json(
    json_text: str,
    *,
    collected_at: str | None = None,
) -> list[dict]:
    try:
        rows = json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise ValueError("malformed Hugging Face models JSON") from exc
    if not isinstance(rows, list):
        raise ValueError("Hugging Face models response must be a JSON array")

    records: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        record = product_record(row, collected_at=collected_at)
        if record is None:
            continue
        source_url = record["source"]["url"]
        if source_url in seen:
            continue
        seen.add(source_url)
        records.append(record)
    return records


class HuggingFaceModelsSource:
    """Async Hugging Face API pager."""

    def __init__(self, fetcher) -> None:
        self.fetcher = fetcher
        self._organization_cache: dict[str, bool] = {}
        self.organization_checks = 0
        self.organization_failures = 0

    def _check_failed_rate(self) -> bool:
        if self.organization_checks == 0:
            return False
        return self.organization_failures / self.organization_checks > 0.50

    async def is_organization(self, owner: str) -> bool:
        if owner in self._organization_cache:
            return self._organization_cache[owner]
        self.organization_checks += 1
        try:
            result = await self.fetcher.get(
                f"https://huggingface.co/api/organizations/{owner}/overview",
            )
            if result.status == 304:
                try:
                    payload = json.loads(result.text)
                    is_org = isinstance(payload, dict) and bool(payload.get("name"))
                except json.JSONDecodeError:
                    is_org = False
            else:
                is_org = result.status == 200
            if result.status == 404:
                is_org = False
            elif not is_org:
                self.organization_failures += 1
                log.warning(
                    "huggingface org check failed owner=%s status=%s",
                    owner,
                    result.status,
                )
        except Exception as exc:
            message = str(exc)
            if "404" in message:
                is_org = False
            else:
                self.organization_failures += 1
                log.warning(
                    "huggingface org check failed owner=%s",
                    owner,
                )
                is_org = False
        self._organization_cache[owner] = is_org
        if self.organization_checks >= 2 and self._check_failed_rate():
            raise RuntimeError(
                "Hugging Face organization check endpoint looks broken: "
                "more than 50 percent of org checks failed"
            )
        return is_org

    async def iter_org_models(
        self,
        *,
        page_size: int = 100,
        max_pages: int | None = None,
        collected_at: str | None = None,
    ):
        seen: set[str] = set()
        limit = page_size
        for page_number in range(max_pages or 2**31):
            skip = page_number * limit
            result = await self.fetcher.get(
                HF_API_URL,
                params={"limit": limit, "skip": skip, "full": "true"},
            )
            if result.status not in (200, 304):
                break
            rows = parse_hf_models_json(result.text, collected_at=collected_at)
            if not rows:
                break
            emitted = 0
            for record in rows:
                model_id = record["source"]["url"].removeprefix(
                    "https://huggingface.co/"
                )
                owner, _ = split_model_id(model_id)
                if owner is None or not await self.is_organization(owner):
                    continue
                if model_id in seen:
                    continue
                seen.add(model_id)
                emitted += 1
                yield record
            if len(rows) < page_size:
                break
