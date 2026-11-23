"""Quality audits for collected SQLite pipeline data."""

from __future__ import annotations

import logging
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Callable

import httpx

log = logging.getLogger(__name__)

PRICING_ENUM = {"FREE", "FREEMIUM", "PAID", "ENTERPRISE"}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone() is not None


def _count(conn: sqlite3.Connection, table: str) -> int:
    if not _table_exists(conn, table):
        return 0
    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0])


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _source_url_rows(conn: sqlite3.Connection) -> list[tuple[str, str, str | None]]:
    """Return (tab, source_url, collectedAt) for known data tabs."""
    output: list[tuple[str, str, str | None]] = []
    specs = [
        ("Startups", "startups", "source_url", "collected_at"),
        ("Products", "products", "source_url", "collected_at"),
        ("Research Papers", "papers", "source_url", "collected_at"),
        ("Jobs", "job_signals", "source_url", "collectedAt"),
        ("News", "news_signals", "source_url", "collectedAt"),
    ]
    for tab, table, url_column, collected_column in specs:
        if not _table_exists(conn, table):
            continue
        for row in conn.execute(
            f"SELECT {url_column}, {collected_column} FROM {table}"
        ).fetchall():
            output.append((tab, str(row[0] or "").strip(), row[1]))
    return output


def run_audit(
    conn: sqlite3.Connection,
    *,
    now: datetime | None = None,
    sample_size: int = 30,
    random_seed: int | None = None,
    http_client: httpx.Client | None = None,
) -> dict[str, dict[str, object]]:
    """Run quality checks and return a per-check PASS/FAIL report."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    checks: dict[str, dict[str, object]] = {}

    startups = _count(conn, "startups")
    checks["Startups >= 1000"] = {
        "passed": startups >= 1000,
        "detail": f"{startups} rows",
    }
    products = _count(conn, "products")
    checks["Products >= 1000"] = {
        "passed": products >= 1000,
        "detail": f"{products} rows",
    }
    paper_count = 0
    if _table_exists(conn, "papers"):
        paper_row = conn.execute(
            """
            SELECT COUNT(*)
            FROM papers
            WHERE typeof(github_stars) = 'integer'
              AND github_url IS NOT NULL
              AND github_stars IS NOT NULL
            """
        ).fetchone()
        paper_count = int(paper_row[0])
    checks["Papers >= 1000 with integer stars"] = {
        "passed": paper_count >= 1000,
        "detail": f"{paper_count} rows",
    }

    provenance = _source_url_rows(conn)
    missing_provenance = [
        tab for tab, url, collected in provenance
        if not url or not collected
    ]
    checks["Every data row has source_url and collectedAt"] = {
        "passed": not missing_provenance,
        "detail": f"{len(missing_provenance)} rows missing",
    }

    duplicate_failures: list[str] = []
    duplicate_specs = [
        ("Startups", "startups", "source_url"),
        ("Products", "products", "source_url"),
        ("Research Papers", "papers", "source_url"),
        ("Jobs", "job_signals", "source_url"),
        ("News", "news_signals", "source_url"),
    ]
    for tab, table, column in duplicate_specs:
        if not _table_exists(conn, table):
            continue
        duplicate = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT {column} FROM {table} GROUP BY {column} HAVING COUNT(*) > 1)"
        ).fetchone()
        if duplicate and duplicate[0] > 0:
            duplicate_failures.append(f"{tab}: {duplicate[0]} duplicate source_url values")
    if _table_exists(conn, "papers"):
        duplicate = conn.execute(
            "SELECT COUNT(*) FROM (SELECT arxiv_id FROM papers GROUP BY arxiv_id HAVING COUNT(*) > 1)"
        ).fetchone()
        if duplicate and duplicate[0] > 0:
            duplicate_failures.append(f"Research Papers: {duplicate[0]} duplicate arxiv_id values")
    checks["No duplicate source_url or canonical IDs"] = {
        "passed": not duplicate_failures,
        "detail": "; ".join(duplicate_failures) or "none",
    }

    freshness_failures: list[str] = []
    for tab, table in (("Jobs", "job_signals"), ("News", "news_signals")):
        if not _table_exists(conn, table):
            continue
        for row in conn.execute(f"SELECT id, published FROM {table}").fetchall():
            published = _parse_datetime(row[1])
            if published is None or published > now or now - published > timedelta(hours=24):
                freshness_failures.append(f"{tab}/{row[0]}")
    checks["Jobs and News dates are present and within 24h"] = {
        "passed": not freshness_failures,
        "detail": f"{len(freshness_failures)} invalid rows",
    }

    invalid_pricing: list[str] = []
    if _table_exists(conn, "products"):
        for row in conn.execute(
            "SELECT source_url, pricing_model FROM products"
        ).fetchall():
            value = row[1]
            if value not in PRICING_ENUM and value not in (None, ""):
                invalid_pricing.append(str(row[0]))
    checks["pricingModel uses allowed enum or empty"] = {
        "passed": not invalid_pricing,
        "detail": f"{len(invalid_pricing)} invalid rows",
    }

    unresolved_entity_failures: list[str] = []
    if _table_exists(conn, "entity_log"):
        for row in conn.execute(
            "SELECT raw, canonical, method FROM entity_log"
        ).fetchall():
            if row[2] != "UNRESOLVED" and (not row[0] or not row[1]):
                unresolved_entity_failures.append(str(row[0]))
    checks["Entity log raw/canonical populated for resolved rows"] = {
        "passed": not unresolved_entity_failures,
        "detail": f"{len(unresolved_entity_failures)} invalid rows",
    }

    urls = [url for _, url, _ in provenance if url.startswith(("http://", "https://"))]
    rng = random.Random(random_seed)
    sampled = rng.sample(urls, k=min(sample_size, len(urls)))
    failures: list[str] = []
    client_owner = http_client is None
    client = http_client or httpx.Client(timeout=10.0, follow_redirects=True)
    try:
        for url in sampled:
            try:
                response = client.head(url)
                if response.status_code != 200:
                    response = client.get(url)
                if response.status_code != 200:
                    failures.append(f"{url} -> {response.status_code}")
            except httpx.HTTPError as exc:
                failures.append(f"{url} -> {type(exc).__name__}")
    finally:
        if client_owner:
            client.close()
    checks["Random source URL sample returns 200"] = {
        "passed": not failures,
        "detail": f"{len(failures)} failures from {len(sampled)} sampled; " + "; ".join(failures),
    }
    return checks


def print_audit_report(checks: dict[str, dict[str, object]]) -> bool:
    """Log PASS/FAIL checks and return overall success."""
    passed_all = True
    for name, result in checks.items():
        status = "PASS" if result["passed"] else "FAIL"
        log.info("%s | %s | %s", status, name, result["detail"])
        passed_all = passed_all and bool(result["passed"])
    log.info("AUDIT %s", "PASS" if passed_all else "FAIL")
    return passed_all
