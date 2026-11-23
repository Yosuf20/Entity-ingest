from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Iterable
from urllib.parse import urlsplit, urlunsplit

from src.storage.db import init_db


def canonical_url(url: str) -> str:
    parts = urlsplit(url.strip())
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path or "/", "", "")).rstrip("/") or "/"


def normalize_text(text: str) -> str:
    return " ".join((text or "").casefold().split())


def item_hash(url: str, text: str) -> str:
    return hashlib.sha256(f"{canonical_url(url)}\n{normalize_text(text)}".encode("utf-8")).hexdigest()


def is_within_24_hours(published: datetime | None, now: datetime) -> bool:
    if published is None:
        return False
    if published.tzinfo is None:
        published = published.replace(tzinfo=timezone.utc)
    else:
        published = published.astimezone(timezone.utc)
    now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    return published <= now and now - published <= timedelta(hours=24)


def mark_seen(
    conn: sqlite3.Connection,
    url: str,
    text: str,
) -> bool:
    """Insert a canonical item; return True only if it is unseen."""
    digest = item_hash(url, text)
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO seen_items
            (hash, canonical_url, normalized_text, first_seen_at)
        VALUES (?, ?, ?, ?)
        """,
        (digest, canonical_url(url), normalize_text(text), now),
    )
    conn.commit()
    return cur.rowcount > 0


def process_items(
    conn: sqlite3.Connection,
    items: Iterable[dict],
    now: datetime,
) -> list[dict]:
    """Apply freshness and seen-item heuristics without inventing a real date."""
    output: list[dict] = []
    baseline_run = conn.execute("SELECT COUNT(*) FROM seen_items").fetchone()[0] == 0
    now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    for item in items:
        published = item.get("published")
        if isinstance(published, datetime):
            published = published if published.tzinfo else published.replace(tzinfo=timezone.utc)
            published = published.astimezone(timezone.utc)
            item = dict(item)
            item["published"] = published
            if not is_within_24_hours(published, now):
                continue
            item["date_confidence"] = item.get("date_confidence", 0.0)
        else:
            unseen = mark_seen(
                conn,
                str(item.get("url") or ""),
                str(item.get("title") or "") + "\n" + str(item.get("full_text") or ""),
            )
            if baseline_run:
                continue
            if not unseen:
                continue
            item = dict(item)
            item["published"] = None
            item["date_method"] = "inferred_new_since_last_run"
            item["date_confidence"] = "inferred_new_since_last_run"
        output.append(item)
    return output


def save_signals(conn: sqlite3.Connection, items: Iterable[dict]) -> None:
    conn.execute("DELETE FROM news_signals")
    conn.execute("DELETE FROM job_signals")
    for item in items:
        digest = item_hash(str(item.get("url") or ""), str(item.get("title") or "") + "\n" + str(item.get("full_text") or ""))
        published = item.get("published")
        published_text = published.isoformat() if isinstance(published, datetime) else None
        source = item.get("source") or {}
        confidence = item.get("date_confidence")
        confidence_text = str(confidence) if confidence is not None else None
        content = item.get("content")
        if content is None:
            conn.execute(
                """
                INSERT OR REPLACE INTO news_signals (
                    id, title, url, published, date_method, date_confidence,
                    full_text, extracted_at, source_url, feed_url, hn_url, collectedAt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    digest, item.get("title"), item.get("url"), published_text,
                    item.get("date_method"), confidence_text, item.get("full_text"),
                    item.get("extracted_at"), item.get("url"), item.get("feed_url") or source.get("url"),
                    item.get("hn_url"), item.get("collectedAt"),
                ),
            )
        else:
            conn.execute(
                """
                INSERT OR REPLACE INTO job_signals (
                    id, title, url, published, date_method, date_confidence,
                    full_text, extracted_at, company, is_remote, role_family,
                    source_url, feed_url, board_type, collectedAt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    digest, item.get("title"), item.get("url"), published_text,
                    item.get("date_method"), confidence_text, item.get("full_text"),
                    item.get("extracted_at"), content.get("company"),
                    1 if content.get("is_remote") else 0, content.get("role_family"),
                    item.get("url"), item.get("feed_url") or source.get("url"),
                    item.get("board_type"), item.get("collectedAt"),
                ),
            )
    conn.commit()
