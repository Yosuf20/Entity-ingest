from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.crawler.sources.products import (
    HuggingFaceModelsSource,
    OPEN_SOURCE_LICENSE_TAGS,
    parse_hf_models_json,
    pricing_model_from_tags,
)
from src.crawler.sources.startups import (
    parse_yc_company_html,
    parse_yc_dataset,
)
from src.pipelines.startups_products import (
    init_phase_one_tables,
    save_products,
    save_startups,
)
from src.storage.db import init_db


FIXTURES = Path(__file__).parent / "fixtures"
COLLECTED_AT = "2026-11-23T12:00:00+00:00"


def test_yc_dataset_parsing_and_canonical_urls():
    text = (FIXTURES / "yc_companies.json").read_text(encoding="utf-8")
    records = parse_yc_dataset(text, collected_at=COLLECTED_AT)

    assert len(records) == 3
    assert records[0]["recordType"] == "STARTUP"
    assert records[0]["schemaVersion"] == "1.0"
    assert records[0]["source"]["name"] == "Y Combinator"
    assert records[0]["source"]["url"] == (
        "https://www.ycombinator.com/companies/red-barn-robotics"
    )
    assert records[0]["content"]["data"]["employeeCount"] == 3
    assert records[0]["collectedAt"] == COLLECTED_AT


def test_yc_static_html_parsing():
    text = (FIXTURES / "yc_stripe.html").read_text(encoding="utf-8")
    record = parse_yc_company_html(
        text,
        source_url="https://www.ycombinator.com/companies/stripe",
        collected_at=COLLECTED_AT,
    )

    assert record is not None
    assert record["content"]["entityName"] == "Stripe"
    assert record["content"]["data"]["employeeCount"] == 7000
    assert record["source"]["url"] == "https://www.ycombinator.com/companies/stripe"


def test_missing_fields_remain_null():
    records = parse_yc_dataset(
        json.dumps([{"name": "No Team", "slug": "no-team", "team_size": None}]),
        collected_at=COLLECTED_AT,
    )
    assert records[0]["content"]["data"]["employeeCount"] is None

    hf = parse_hf_models_json(
        json.dumps([{"id": "acme/widget"}]),
        collected_at=COLLECTED_AT,
    )
    assert hf[0]["content"]["pricingModel"] is None
    assert hf[0]["content"]["startupName"] == "acme"


def test_hf_explicit_open_source_license_sets_free_only():
    assert pricing_model_from_tags(["license:mit"]) == "FREE"
    assert pricing_model_from_tags(["license:apache-2.0"]) == "FREE"
    assert pricing_model_from_tags(["license:other"]) is None
    assert pricing_model_from_tags(["text-generation"]) is None
    assert pricing_model_from_tags(None) is None
    assert "mit" in OPEN_SOURCE_LICENSE_TAGS


@pytest.mark.parametrize(
    "value",
    ["FREE", "FREEMIUM", "PAID", "ENTERPRISE", None],
)
def test_product_pricing_enum_accepts_schema_values(value):
    record = {
        "schemaVersion": "1.0",
        "recordType": "PRODUCT",
        "source": {"name": "Hugging Face", "url": "https://huggingface.co/acme/widget"},
        "content": {"startupName": "acme", "pricingModel": value},
        "collectedAt": COLLECTED_AT,
    }
    assert record["content"]["pricingModel"] in {
        "FREE",
        "FREEMIUM",
        "PAID",
        "ENTERPRISE",
        None,
    }


def test_hf_fixture_deduplicates_canonical_model_urls():
    text = (FIXTURES / "hf_models.json").read_text(encoding="utf-8")
    records = parse_hf_models_json(text, collected_at=COLLECTED_AT)
    duplicate = parse_hf_models_json(text, collected_at=COLLECTED_AT)

    assert len(records) == len({record["source"]["url"] for record in records})
    assert len(duplicate) == len(records)


@pytest.mark.asyncio
async def test_hf_org_filter_skips_personal_accounts():
    class FakeResult:
        def __init__(self, status: int) -> None:
            self.status = status

    class FakeFetcher:
        async def get(self, url: str, params=None):
            if "organizations/acme" in url:
                return FakeResult(200)
            if "organizations/alice" in url:
                return FakeResult(404)
            return FakeResult(404)

    source = HuggingFaceModelsSource(FakeFetcher())
    assert await source.is_organization("acme") is True
    assert await source.is_organization("alice") is False


@pytest.mark.asyncio
async def test_hf_org_error_is_skipped_without_aborting():
    class FakeFetcher:
        async def get(self, url: str, params=None):
            raise RuntimeError("404 Not Found")

    source = HuggingFaceModelsSource(FakeFetcher())

    assert await source.is_organization("thinkingmachines") is False


@pytest.mark.asyncio
async def test_hf_org_failure_majority_stops_with_broken_endpoint_error():
    class FakeResult:
        def __init__(self, status: int) -> None:
            self.status = status

    class FakeFetcher:
        async def get(self, url: str, params=None):
            return FakeResult(500)

    source = HuggingFaceModelsSource(FakeFetcher())

    assert await source.is_organization("broken-one") is False
    with pytest.raises(RuntimeError, match="looks broken"):
        await source.is_organization("broken-two")


def test_save_tables_dedupe_on_rerun():
    conn = init_db(":memory:")
    init_phase_one_tables(conn)

    yc = parse_yc_dataset(
        json.dumps([{"name": "Stripe", "slug": "stripe", "team_size": 7000}]),
        collected_at=COLLECTED_AT,
    )
    hf = parse_hf_models_json(
        json.dumps([{"id": "acme/widget", "tags": ["license:mit"]}]),
        collected_at=COLLECTED_AT,
    )

    first_yc = save_startups(conn, yc)
    second_yc = save_startups(conn, yc)
    first_hf = save_products(conn, hf)
    second_hf = save_products(conn, hf)

    assert first_yc["inserted"] == 1
    assert second_yc["duplicates"] == 1
    assert first_hf["inserted"] == 1
    assert second_hf["duplicates"] == 1
    assert conn.execute("SELECT COUNT(*) FROM startups").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM products").fetchone()[0] == 1
