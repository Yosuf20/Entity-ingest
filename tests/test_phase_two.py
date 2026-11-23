from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src.parsers.dates import parse_published
from src.pipelines.signals import is_within_24_hours, process_items, save_signals
from src.storage.db import init_db
from src.crawler.sources.news import _rss_items, _is_ai_related
from src.crawler.sources.jobs import (
    _clean_description,
    _is_remote,
    _matches_ai_keywords,
    _split_wwr_title,
)


NOW = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("item", "method"),
    [
        ({"published": "2026-11-23T11:00:00Z"}, "rss_api_timestamp"),
        ({"json_ld": {"datePublished": "2026-11-23T10:00:00Z"}}, "json_ld"),
        ({"html": '<meta property="article:published_time" content="2026-11-23T09:00:00Z">'}, "meta"),
        ({"html": "<time datetime=\"2026-11-23T08:00:00Z\">Today</time>"}, "time_datetime"),
        ({"relative": "2 hours ago"}, "relative"),
        ({"url": "https://example.com/2026/11/21/story"}, "url_path"),
    ],
)
def test_parameterized_date_methods(item, method):
    published, found_method, confidence = parse_published(item, NOW)
    assert published is not None
    assert found_method == method
    assert confidence > 0


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("just now", NOW),
        ("2 hours ago", NOW - timedelta(hours=2)),
        ("yesterday", NOW - timedelta(days=1)),
    ],
)
def test_relative_dates_fixed_now(raw, expected):
    published, _, _ = parse_published({"relative": raw}, NOW)
    assert published == expected


def test_timezone_less_is_assigned_utc_and_lower_confidence():
    published, method, confidence = parse_published(
        {"published": "2026-11-23T11:00:00"},
        NOW,
    )
    assert published == datetime(2026, 11, 23, 11, 0, tzinfo=timezone.utc)
    assert method == "rss_api_timestamp"
    assert confidence == 0.65


@pytest.mark.parametrize(
    ("published", "accepted"),
    [
        (NOW - timedelta(hours=23, minutes=59), True),
        (NOW - timedelta(hours=24, minutes=1), False),
        (NOW + timedelta(minutes=1), False),
    ],
)
def test_24_hour_freshness_boundary(published, accepted):
    assert is_within_24_hours(published, NOW) is accepted


def test_undated_item_dedupes_across_two_runs():
    conn = init_db(":memory:")
    item = {
        "title": "Undated fixture",
        "url": "https://example.com/jobs/1",
        "full_text": "Fixture job description",
        "published": None,
        "extracted_at": NOW.isoformat(),
        "collectedAt": NOW.isoformat(),
    }
    first = process_items(conn, [item], NOW)
    duplicate = process_items(conn, [item], NOW)
    new_item = dict(item, title="New undated fixture", url="https://example.com/jobs/2")
    second = process_items(conn, [new_item], NOW)

    assert first == []
    assert duplicate == []
    assert len(second) == 1
    assert second[0]["date_confidence"] == "inferred_new_since_last_run"


def test_saved_item_source_is_individual_and_feed_is_separate():
    conn = init_db(":memory:")
    item = {
        "title": "Fixture",
        "url": "https://example.com/article/123",
        "published": NOW,
        "date_method": "rss_api_timestamp",
        "date_confidence": 0.95,
        "full_text": "Article text",
        "extracted_at": NOW.isoformat(),
        "feed_url": "https://example.com/feed.xml",
        "source": {"name": "fixture", "url": "https://example.com/feed.xml"},
        "collectedAt": NOW.isoformat(),
    }
    save_signals(conn, [item])
    row = conn.execute("SELECT source_url, feed_url FROM news_signals").fetchone()
    assert row["source_url"] == "https://example.com/article/123"
    assert row["feed_url"] == "https://example.com/feed.xml"


def test_ashby_published_at_is_verified_timestamp():
    published, method, confidence = parse_published(
        {"publishedAt": "2026-11-23T11:30:00.000+00:00"},
        NOW,
    )
    assert published == datetime(2026, 11, 23, 11, 30, tzinfo=timezone.utc)
    assert method == "rss_api_timestamp"
    assert confidence == 0.95


def test_remoteok_iso_date_is_verified_timestamp():
    published, method, confidence = parse_published(
        {"published": "2026-11-22T02:00:05+00:00"},
        NOW,
    )
    assert published is not None
    assert method == "rss_api_timestamp"


def test_atom_feed_is_parsed():
    atom = """<?xml version="1.0"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <entry><title>AI Atom Story</title><link href="https://example.com/ai"/></entry>
    </feed>"""
    entries = _rss_items(atom)
    assert entries[0]["title"] == "AI Atom Story"
    assert entries[0]["url"] == "https://example.com/ai"


@pytest.mark.parametrize(
    ("title", "related"),
    [
        ("OpenAI releases an AI model", True),
        ("Local elections in a major city", False),
        ("A new LLM transformer benchmark", True),
    ],
)
def test_hacker_news_ai_filter(title, related):
    assert _is_ai_related(title) is related


def test_generic_job_ai_keyword_filter():
    keywords = ["ai", "machine learning", "llm", "data scientist"]
    assert _matches_ai_keywords("Staff Machine Learning Engineer", ["engineering"], keywords)
    assert not _matches_ai_keywords("Finance Manager", ["accounting"], keywords)


def test_greenhouse_first_published_is_verified():
    published, method, confidence = parse_published(
        {"first_published": "2026-11-23T11:30:00-04:00"},
        NOW,
    )
    assert published is not None
    assert method == "rss_api_timestamp"


def test_description_words_do_not_make_job_remote():
    assert not _is_remote(
        location="San Francisco, CA",
        workplace_type="On-site",
        is_remote=None,
    )


def test_ashby_is_remote_flag_is_remote():
    assert _is_remote(
        location="San Francisco",
        workplace_type="Hybrid",
        is_remote=True,
    )


def test_wwr_title_prefix_becomes_company_and_is_removed():
    company, title = _split_wwr_title("Wrike: Senior Product Manager, AI Platform")
    assert company == "Wrike"
    assert title == "Senior Product Manager, AI Platform"


def test_clean_description_unescapes_and_strips_html():
    assert _clean_description("&lt;p&gt;Build &amp; ship AI&lt;/p&gt;") == "Build & ship AI"
