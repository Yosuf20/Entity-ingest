"""Deterministic startup and product collection pipeline."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from src.crawler.sources.products import HuggingFaceModelsSource
from src.crawler.sources.startups import YC_DATASET_URL, parse_yc_dataset


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_phase_one_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS startups (
            source_url TEXT PRIMARY KEY,
            entity_name TEXT NOT NULL,
            employee_count INTEGER,
            collected_at TEXT NOT NULL,
            record_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS products (
            source_url TEXT PRIMARY KEY,
            startup_name TEXT NOT NULL,
            pricing_model TEXT,
            collected_at TEXT NOT NULL,
            record_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS startups_products_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


def save_startups(conn: sqlite3.Connection, records: Iterable[dict]) -> dict[str, int]:
    inserted = 0
    duplicate = 0
    for record in records:
        source_url = record["source"]["url"]
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO startups (
                source_url, entity_name, employee_count, collected_at, record_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                source_url,
                record["content"]["entityName"],
                record["content"]["data"]["employeeCount"],
                record["collectedAt"],
                json.dumps(record, ensure_ascii=False),
            ),
        )
        if cur.rowcount > 0:
            inserted += 1
        else:
            duplicate += 1
    conn.commit()
    return {"inserted": inserted, "duplicates": duplicate}


def save_products(conn: sqlite3.Connection, records: Iterable[dict]) -> dict[str, int]:
    inserted = 0
    duplicate = 0
    for record in records:
        source_url = record["source"]["url"]
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO products (
                source_url, startup_name, pricing_model, collected_at, record_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                source_url,
                record["content"]["startupName"],
                record["content"]["pricingModel"],
                record["collectedAt"],
                json.dumps(record, ensure_ascii=False),
            ),
        )
        if cur.rowcount > 0:
            inserted += 1
        else:
            duplicate += 1
    conn.commit()
    return {"inserted": inserted, "duplicates": duplicate}


def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO startups_products_state (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value=excluded.value,
            updated_at=excluded.updated_at
        """,
        (key, value, _utc_now()),
    )
    conn.commit()


async def collect_startups_products(
    fetcher,
    conn: sqlite3.Connection,
    *,
    target: int = 1000,
    hf_max_pages: int | None = None,
) -> dict:
    init_phase_one_tables(conn)
    collected_at = _utc_now()

    yc_result = await fetcher.get(YC_DATASET_URL)
    if yc_result.status not in (200, 304):
        raise RuntimeError(
            f"Y Combinator community dataset fetch failed: HTTP {yc_result.status}"
        )
    startups = parse_yc_dataset(yc_result.text, collected_at=collected_at)
    startup_stats = save_startups(conn, startups)

    hf_source = HuggingFaceModelsSource(fetcher)
    products: list[dict] = []
    async for record in hf_source.iter_org_models(
        page_size=100,
        max_pages=hf_max_pages,
        collected_at=collected_at,
    ):
        products.append(record)
        if len(products) >= target:
            break
    product_stats = save_products(conn, products)

    set_state(conn, "yc_dataset_url", YC_DATASET_URL)
    set_state(conn, "hf_models_page_size", "100")
    set_state(
        conn,
        "hf_last_collected_at",
        collected_at,
    )

    startup_count = conn.execute("SELECT COUNT(*) FROM startups").fetchone()[0]
    product_count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    return {
        "target": target,
        "startups": startup_count,
        "products": product_count,
        "startup_stats": startup_stats,
        "product_stats": product_stats,
    }
