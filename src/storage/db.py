"""
SQLite persistence layer.

Tables
------
pending_extractions  — LLM failures queued for retry (Phase III)
papers               — Deterministically-collected research papers (Phase I)
raw_pages            — Cached raw HTTP responses with ETag support
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_DEFAULT_DB = Path(__file__).parent.parent.parent / "data" / "pipeline.db"


def _connect(db_path) -> sqlite3.Connection:
    """Open a SQLite connection; ``':memory:'`` is handled specially for tests."""
    if isinstance(db_path, str) and db_path == ":memory:":
        conn = sqlite3.connect(":memory:", check_same_thread=False)
    else:
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path=_DEFAULT_DB) -> sqlite3.Connection:
    """Create all tables (idempotent) and return an open connection."""
    conn = _connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pending_extractions (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url       TEXT NOT NULL,
            schema_name      TEXT NOT NULL,
            raw_text         TEXT NOT NULL,
            status           TEXT NOT NULL DEFAULT 'pending',
            failure_reason   TEXT,
            attempt_count    INTEGER NOT NULL DEFAULT 0,
            next_attempt_at  TEXT,
            created_at       TEXT NOT NULL,
            updated_at       TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS papers (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            arxiv_id                 TEXT NOT NULL UNIQUE,
            title                    TEXT NOT NULL,
            authors                  TEXT NOT NULL,   -- JSON array
            paper_url                TEXT NOT NULL,
            pdf_url                  TEXT,
            abstract                 TEXT,
            published_date           TEXT NOT NULL,
            github_url               TEXT,
            github_stars             INTEGER,
            github_stars_fetched_at  TEXT,
            github_404               INTEGER NOT NULL DEFAULT 0,
            comment                  TEXT,
            journal_ref              TEXT,
            source_url               TEXT NOT NULL,
            collected_at             TEXT NOT NULL,
            created_at               TEXT NOT NULL,
            updated_at               TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS raw_pages (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            url          TEXT NOT NULL UNIQUE,
            etag         TEXT,
            content      TEXT NOT NULL,
            fetched_at   TEXT NOT NULL,
            status_code  INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS seen_items (
            hash TEXT PRIMARY KEY,
            canonical_url TEXT NOT NULL,
            normalized_text TEXT NOT NULL,
            first_seen_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS news_signals (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            published TEXT,
            date_method TEXT,
            date_confidence TEXT,
            full_text TEXT,
            extracted_at TEXT NOT NULL,
            source_url TEXT NOT NULL,
            feed_url TEXT,
            collectedAt TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS job_signals (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            published TEXT,
            date_method TEXT,
            date_confidence TEXT,
            full_text TEXT,
            extracted_at TEXT NOT NULL,
            company TEXT,
            is_remote INTEGER NOT NULL DEFAULT 0,
            role_family TEXT,
            source_url TEXT NOT NULL,
            feed_url TEXT,
            collectedAt TEXT NOT NULL
        );
    """)
    for table in ("news_signals", "job_signals"):
        columns = {
            row["name"] if isinstance(row, sqlite3.Row) else row[1]
            for row in conn.execute(f"PRAGMA table_info({table})")
        }
        if "feed_url" not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN feed_url TEXT")
        conn.execute(f"UPDATE {table} SET feed_url = source_url WHERE feed_url IS NULL")
        conn.execute(f"UPDATE {table} SET source_url = url WHERE url IS NOT NULL AND url != ''")
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# LLM failure persistence
# ---------------------------------------------------------------------------

def save_pending(
    conn: sqlite3.Connection,
    *,
    source_url: str,
    schema_name: str,
    raw_text: str,
    failure_reason: str,
    next_attempt_at: Optional[datetime] = None,
) -> int:
    """Insert a failed LLM extraction and return its row id."""
    now = datetime.now(timezone.utc).isoformat()
    next_at = next_attempt_at.isoformat() if next_attempt_at else None
    cur = conn.execute(
        """
        INSERT INTO pending_extractions
            (source_url, schema_name, raw_text, status, failure_reason,
             next_attempt_at, created_at, updated_at)
        VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)
        """,
        (source_url, schema_name, raw_text, failure_reason, next_at, now, now),
    )
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Research paper upsert
# ---------------------------------------------------------------------------

def upsert_paper(conn: sqlite3.Connection, paper) -> bool:
    """Insert a ResearchPaper; skip silently if arxiv_id already exists.

    Returns True if a new row was inserted, False if it was a duplicate.
    """
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO papers (
            arxiv_id, title, authors, paper_url, pdf_url, abstract,
            published_date, github_url, github_stars, github_stars_fetched_at,
            github_404, comment, journal_ref, source_url, collected_at,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            paper.arxiv_id,
            paper.title,
            json.dumps(paper.authors),
            paper.paper_url,
            paper.pdf_url,
            paper.abstract,
            paper.published_date,
            paper.github_url,
            paper.github_stars,
            paper.github_stars_fetched_at,
            1 if paper.github_404 else 0,
            paper.comment,
            paper.journal_ref,
            paper.source_url,
            paper.collected_at,
            now,  # created_at
            now,  # updated_at
        ),
    )
    conn.commit()
    return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Raw page cache
# ---------------------------------------------------------------------------

def save_raw_page(
    conn: sqlite3.Connection,
    *,
    url: str,
    content: str,
    status_code: int,
    etag: Optional[str] = None,
) -> None:
    """Upsert a raw HTTP response into the raw_pages cache table."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO raw_pages (url, etag, content, fetched_at, status_code)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            etag=excluded.etag,
            content=excluded.content,
            fetched_at=excluded.fetched_at,
            status_code=excluded.status_code
        """,
        (url, etag, content, now, status_code),
    )
    conn.commit()
