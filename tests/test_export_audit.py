"""Practical tests for Sheets flattening and quality audits."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src.export.audit import run_audit
from src.export.sheets import collect_sheet_data
from src.storage.db import init_db


def _paper_row(conn: sqlite3.Connection, arxiv_id: str, stars: int | None) -> None:
    conn.execute(
        """
        INSERT INTO papers (
            arxiv_id, title, authors, paper_url, published_date,
            github_url, github_stars, github_stars_fetched_at,
            source_url, collected_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            arxiv_id,
            "Paper",
            json.dumps(["Author"]),
            f"https://arxiv.org/abs/{arxiv_id}",
            "2026-11-23T10:00:00+00:00",
            "https://github.com/owner/repo",
            stars,
            "2026-11-23T12:00:00+00:00",
            f"https://arxiv.org/abs/{arxiv_id}",
            "2026-11-23T12:00:00+00:00",
            "2026-11-23T12:00:00+00:00",
            "2026-11-23T12:00:00+00:00",
        ),
    )
    conn.commit()


def test_collect_sheet_data_has_exactly_six_tabs():
    conn = init_db(":memory:")
    data = collect_sheet_data(conn)
    assert set(data) == {
        "Startups",
        "Products",
        "Research Papers",
        "Jobs",
        "News",
        "Entity Mapping Log",
    }


def test_research_paper_export_includes_github_columns():
    conn = init_db(":memory:")
    _paper_row(conn, "2411.01234", 123)
    data = collect_sheet_data(conn)
    row = data["Research Papers"][0]
    assert row["arxiv_id"] == "2411.01234"
    assert row["github_url"] == "https://github.com/owner/repo"
    assert row["github_stars"] == 123
    assert row["github_stars_fetched_at"] == "2026-11-23T12:00:00+00:00"


def test_audit_pricing_enum_duplicate_and_freshness_checks():
    conn = init_db(":memory:")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            source_url TEXT PRIMARY KEY,
            startup_name TEXT,
            pricing_model TEXT,
            collected_at TEXT,
            record_json TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO products VALUES (?, ?, ?, ?, ?)",
        ("https://example.com/product", "Acme", "OTHER", "2026-11-23T12:00:00+00:00", "{}"),
    )
    conn.execute(
        """
        INSERT INTO news_signals (
            id, title, url, published, date_method, date_confidence,
            full_text, extracted_at, source_url, feed_url, collectedAt
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "old", "Old news", "https://example.com/news",
            "2020-01-01T00:00:00+00:00", "rss", "high",
            "text", "2020-01-01T00:00:00+00:00",
            "https://example.com/news", None, "2026-11-23T12:00:00+00:00",
        ),
    )
    conn.commit()

    class _Client:
        def head(self, url: str) -> httpx.Response:
            return httpx.Response(404, request=httpx.Request("HEAD", url))

        def get(self, url: str) -> httpx.Response:
            return httpx.Response(404, request=httpx.Request("GET", url))

    now = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)
    checks = run_audit(conn, now=now, sample_size=1, random_seed=1, http_client=_Client())
    assert checks["pricingModel uses allowed enum or empty"]["passed"] is False
    assert checks["Jobs and News dates are present and within 24h"]["passed"] is False
    assert checks["Random source URL sample returns 200"]["passed"] is False
