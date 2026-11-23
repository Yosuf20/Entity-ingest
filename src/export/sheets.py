"""Google Sheets export for collected pipeline records."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger(__name__)

TAB_NAMES = (
    "Startups",
    "Products",
    "Research Papers",
    "Jobs",
    "News",
    "Entity Mapping Log",
)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _json_record(row: sqlite3.Row) -> dict[str, Any]:
    record_json = row["record_json"] if "record_json" in row.keys() else None
    if record_json:
        try:
            value = json.loads(record_json)
            if isinstance(value, dict):
                return value
        except json.JSONDecodeError:
            pass
    return {}


def _flatten(record: dict[str, Any], overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Flatten nested source/content fields into sheet column names."""
    source = record.get("source") or {}
    content = record.get("content") or {}
    flattened: dict[str, Any] = {
        "schemaVersion": record.get("schemaVersion"),
        "recordType": record.get("recordType"),
        "source_name": source.get("name"),
        "source_url": source.get("url"),
        "collectedAt": record.get("collectedAt"),
    }
    for key, value in content.items():
        if isinstance(value, (dict, list)):
            flattened[key] = json.dumps(value, ensure_ascii=False)
        else:
            flattened[key] = value
    for key, value in (overrides or {}).items():
        flattened[key] = value
    return flattened


def _startup_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "startups"):
        return []
    rows = conn.execute(
        "SELECT source_url, collected_at, record_json FROM startups ORDER BY source_url"
    ).fetchall()
    return [
        _flatten(
            _json_record(row),
            {
                "source_name": (_json_record(row).get("source") or {}).get("name"),
                "source_url": row["source_url"],
                "collectedAt": row["collected_at"],
            },
        )
        for row in rows
    ]


def _product_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "products"):
        return []
    rows = conn.execute(
        "SELECT source_url, collected_at, record_json FROM products ORDER BY source_url"
    ).fetchall()
    result = []
    for row in rows:
        record = _json_record(row)
        result.append(
            _flatten(
                record,
                {
                    "source_name": (record.get("source") or {}).get("name"),
                    "source_url": row["source_url"],
                    "collectedAt": row["collected_at"],
                },
            )
        )
    return result


def _paper_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "papers"):
        return []
    rows = conn.execute(
        """
        SELECT arxiv_id, title, authors, paper_url, pdf_url, abstract,
               published_date, github_url, github_stars,
               github_stars_fetched_at, github_404, comment, journal_ref,
               source_url, collected_at
        FROM papers
        ORDER BY arxiv_id
        """
    ).fetchall()
    result = []
    for row in rows:
        authors = row["authors"]
        try:
            authors_value = json.loads(authors) if isinstance(authors, str) else authors
        except json.JSONDecodeError:
            authors_value = authors
        result.append({
            "schemaVersion": "1.0",
            "recordType": "RESEARCH_PAPER",
            "source_name": "arXiv",
            "source_url": row["source_url"] or row["paper_url"],
            "collectedAt": row["collected_at"],
            "arxiv_id": row["arxiv_id"],
            "title": row["title"],
            "authors": json.dumps(authors_value, ensure_ascii=False),
            "paper_url": row["paper_url"],
            "pdf_url": row["pdf_url"],
            "abstract": row["abstract"],
            "published_date": row["published_date"],
            "github_url": row["github_url"],
            "github_stars": row["github_stars"],
            "github_stars_fetched_at": row["github_stars_fetched_at"],
            "github_404": bool(row["github_404"]),
            "comment": row["comment"],
            "journal_ref": row["journal_ref"],
        })
    return result


def _job_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "job_signals"):
        return []
    rows = conn.execute(
        """
        SELECT id, title, url, published, date_method, date_confidence,
               full_text, extracted_at, company, is_remote, role_family,
               source_url, feed_url, collectedAt
        FROM job_signals
        ORDER BY id
        """
    ).fetchall()
    return [
        {
            "schemaVersion": "1.0",
            "recordType": "JOB",
            "source_name": "Jobs",
            "source_url": row["source_url"] or row["url"],
            "collectedAt": row["collectedAt"],
            "id": row["id"],
            "title": row["title"],
            "url": row["url"],
            "date": row["published"],
            "date_method": row["date_method"],
            "date_confidence": row["date_confidence"],
            "full_text": row["full_text"],
            "extracted_at": row["extracted_at"],
            "company": row["company"],
            "is_remote": bool(row["is_remote"]),
            "role_family": row["role_family"],
            "feed_url": row["feed_url"],
        }
        for row in rows
    ]


def _news_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "news_signals"):
        return []
    rows = conn.execute(
        """
        SELECT id, title, url, published, date_method, date_confidence,
               full_text, extracted_at, source_url, feed_url, collectedAt
        FROM news_signals
        ORDER BY id
        """
    ).fetchall()
    return [
        {
            "schemaVersion": "1.0",
            "recordType": "NEWS",
            "source_name": "News",
            "source_url": row["source_url"] or row["url"],
            "collectedAt": row["collectedAt"],
            "id": row["id"],
            "title": row["title"],
            "url": row["url"],
            "date": row["published"],
            "date_method": row["date_method"],
            "date_confidence": row["date_confidence"],
            "full_text": row["full_text"],
            "extracted_at": row["extracted_at"],
            "feed_url": row["feed_url"],
        }
        for row in rows
    ]


def _entity_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "entity_log"):
        return []
    rows = conn.execute(
        """
        SELECT raw, canonical, method, score, resolved_at
        FROM entity_log
        ORDER BY resolved_at, raw
        """
    ).fetchall()
    return [
        {
            "schemaVersion": "1.0",
            "recordType": "ENTITY_MAPPING",
            "source_name": "Entity Resolver",
            "source_url": row["raw"],
            "collectedAt": row["resolved_at"],
            "raw": row["raw"],
            "canonical": row["canonical"],
            "method": row["method"],
            "score": row["score"],
            "resolved_at": row["resolved_at"],
        }
        for row in rows
    ]


def collect_sheet_data(conn: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    """Collect all six tabs from SQLite."""
    return {
        "Startups": _startup_rows(conn),
        "Products": _product_rows(conn),
        "Research Papers": _paper_rows(conn),
        "Jobs": _job_rows(conn),
        "News": _news_rows(conn),
        "Entity Mapping Log": _entity_rows(conn),
    }


def _columns(rows: Iterable[dict[str, Any]], preferred: list[str]) -> list[str]:
    present: set[str] = set()
    for row in rows:
        present.update(row.keys())
    remaining = sorted(present - set(preferred))
    return preferred + remaining


def export_sheets(
    conn: sqlite3.Connection,
    *,
    credentials_path: str | Path | None = None,
    sheet_id: str | None = None,
    sheet_title: str = "AI Data Ingestion",
) -> str:
    """Export exactly six tabs and return the spreadsheet URL."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as exc:
        raise RuntimeError("gspread and google-auth must be installed") from exc

    credential_value = (
        credentials_path
        if credentials_path is not None
        else os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    )
    if not credential_value:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is required")
    credential_file = Path(credential_value)
    sheet_key = sheet_id or os.environ.get("GOOGLE_SHEET_ID")
    if not credential_file.is_file():
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON credential file does not exist")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(str(credential_file), scopes=scopes)
    client = gspread.authorize(credentials)
    if sheet_key:
        spreadsheet = client.open_by_key(sheet_key)
    else:
        spreadsheet = client.create(sheet_title)

    data = collect_sheet_data(conn)
    preferred = [
        "schemaVersion", "recordType", "source_name", "source_url",
        "collectedAt",
    ]
    for tab_name in TAB_NAMES:
        rows = data[tab_name]
        columns = _columns(rows, preferred)
        values = [columns]
        values.extend([
            ["" if row.get(column) is None else row.get(column) for column in columns]
            for row in rows
        ])
        try:
            worksheet = spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=tab_name,
                rows=max(100, len(values)),
                cols=max(20, len(columns)),
            )
        worksheet.batch_clear(["A1:ZZZ10000"])
        worksheet.update(
            values,
            "A1",
            value_input_option="RAW",
        )

    for worksheet in list(spreadsheet.worksheets()):
        if worksheet.title not in TAB_NAMES:
            spreadsheet.del_worksheet(worksheet)
    url = spreadsheet.url
    log.info("Google Sheets export complete: %s", url)
    return url
