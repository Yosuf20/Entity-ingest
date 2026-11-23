from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from datetime import datetime, timezone

from src.config import CrawlerConfig, load_config
from src.crawler.base import AsyncFetcher
from src.crawler.sources.jobs import collect_jobs
from src.crawler.sources.news import collect_news
from src.pipelines.signals import process_items, save_signals
from src.storage.db import init_db
from src.pipelines.startups_products import collect_startups_products
from src.pipelines.papers import run_papers
from src.export.sheets import export_sheets
from src.export.audit import print_audit_report, run_audit
from src.storage.db import init_db as init_storage_db

log = logging.getLogger(__name__)


async def run_signals(db_path: str = "data/pipeline.db") -> dict:
    now = datetime.now(timezone.utc)
    config = load_config()
    conn = init_db(db_path)
    try:
        async with AsyncFetcher(config, db=conn) as fetcher:
            news = await collect_news(fetcher, now)
            jobs = await collect_jobs(fetcher, now)
        items = process_items(conn, news + jobs, now)
        save_signals(conn, items)
        return {"count": len(items), "items": items}
    finally:
        conn.close()


async def run_startups_products(
    db_path: str = "data/pipeline.db",
    *,
    target: int = 1000,
) -> dict:
    config = load_config()
    conn = init_db(db_path)
    try:
        async with AsyncFetcher(config, db=conn) as fetcher:
            return await collect_startups_products(
                fetcher,
                conn,
                target=target,
            )
    finally:
        conn.close()


async def run_papers_command(
    db_path: str = "data/pipeline.db",
    *,
    target: int | None = None,
) -> dict:
    config = load_config()
    conn = init_db(db_path)
    try:
        async with AsyncFetcher(config, db=conn) as fetcher:
            summary = await run_papers(
                fetcher,
                conn,
                config=config,
                target=target,
            )
            return summary.to_dict()
    finally:
        conn.close()


def run_export_command(
    db_path: str = "data/pipeline.db",
    *,
    sheet_id: str | None = None,
    sheet_title: str = "AI Data Ingestion",
) -> str:
    conn = init_storage_db(db_path)
    try:
        return export_sheets(
            conn,
            sheet_id=sheet_id or os.environ.get("GOOGLE_SHEET_ID"),
            sheet_title=sheet_title,
        )
    finally:
        conn.close()


def run_audit_command(
    db_path: str = "data/pipeline.db",
    *,
    sample_size: int = 30,
) -> bool:
    conn = init_storage_db(db_path)
    try:
        checks = run_audit(conn, sample_size=sample_size)
        return print_audit_report(checks)
    finally:
        conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="main.py")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("signals")
    startups_products = sub.add_parser("startups-products")
    startups_products.add_argument("--target", type=int, default=1000)
    papers = sub.add_parser("papers")
    papers.add_argument("--target", type=int, default=None)
    export_parser = sub.add_parser("export")
    export_parser.add_argument("--sheet-id")
    export_parser.add_argument("--sheet-title", default="AI Data Ingestion")
    audit_parser = sub.add_parser("audit")
    audit_parser.add_argument("--sample-size", type=int, default=30)
    args = parser.parse_args()
    if args.command == "signals":
        result = asyncio.run(run_signals())
        print(json.dumps(result, indent=2, default=str))
    elif args.command == "startups-products":
        result = asyncio.run(run_startups_products(target=args.target))
        print(json.dumps(result, indent=2, default=str))
    elif args.command == "papers":
        result = asyncio.run(run_papers_command(target=args.target))
        print(json.dumps(result, indent=2))
    elif args.command == "export":
        url = run_export_command(
            sheet_id=args.sheet_id,
            sheet_title=args.sheet_title,
        )
        log.info("Sheet URL: %s", url)
    elif args.command == "audit":
        passed = run_audit_command(sample_size=args.sample_size)
        if not passed:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
