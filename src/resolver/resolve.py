from __future__ import annotations

import json
import logging
import sqlite3
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from rapidfuzz import fuzz

LOGGER = logging.getLogger(__name__)
SEED_PATH = Path(__file__).with_name("seed_startups.json")
LEGAL_SUFFIXES = {
    "inc", "ltd", "llc", "corp", "co", "gmbh", "labs", "technologies",
}


def normalize(name: str) -> str:
    text = unicodedata.normalize("NFKC", name or "").lower()
    kept = "".join(char if char.isalnum() else " " for char in text)
    words = kept.split()
    while words and words[-1] in LEGAL_SUFFIXES:
        words.pop()
    return " ".join(words)


class EntityResolver:
    def __init__(
        self,
        seed_path: str | Path = SEED_PATH,
        *,
        threshold: float = 90,
        margin: float = 5,
    ) -> None:
        self.threshold = float(threshold)
        self.margin = float(margin)
        self.seed = json.loads(Path(seed_path).read_text(encoding="utf-8"))

        self.canonical_by_normalized: dict[str, str] = {}
        self.alias_by_normalized: dict[str, str] = {}
        for entry in self.seed:
            canonical = entry["canonical"]
            self.canonical_by_normalized[normalize(canonical)] = canonical
            for alias in entry.get("aliases", []):
                self.alias_by_normalized[normalize(alias)] = canonical

        self._fuzzy_candidates = list(
            dict.fromkeys(
                [*self.canonical_by_normalized.keys(), *self.alias_by_normalized.keys()]
            )
        )

    def resolve(self, raw: str) -> tuple[str, str | None, str, float]:
        text = raw if isinstance(raw, str) else str(raw or "")
        normalized = normalize(text)
        canonical = self.canonical_by_normalized.get(normalized)
        if canonical is not None:
            return text, canonical, "exact", 100.0

        canonical = self.alias_by_normalized.get(normalized)
        if canonical is not None:
            return text, canonical, "alias", 100.0

        if not normalized:
            LOGGER.info("resolver unresolved raw=%r method=unresolved score=0", text)
            return text, None, "UNRESOLVED", 0.0

        input_words = normalized.split()
        scores_by_canonical: dict[str, float] = {}
        for candidate in self._fuzzy_candidates:
            if candidate:
                score = float(fuzz.WRatio(normalized, candidate))
                canonical_name = (
                    self.canonical_by_normalized.get(candidate)
                    or self.alias_by_normalized.get(candidate)
                )
                if canonical_name:
                    scores_by_canonical[canonical_name] = max(
                        score, scores_by_canonical.get(canonical_name, 0.0)
                    )

        scored = sorted(
            ((score, canonical_name) for canonical_name, score in scores_by_canonical.items()),
            key=lambda item: (-item[0], item[1].lower()),
        )

        if not scored:
            return text, None, "UNRESOLVED", 0.0

        scored.sort(key=lambda item: (-item[0], item[1].lower()))
        best_score, best_name = scored[0]
        second_score = scored[1][0] if len(scored) > 1 else 0.0
        margin = best_score - second_score
        best_normalized = normalize(best_name)
        best_words = best_normalized.split()

        extra_words = len(input_words) > len(best_words)
        length_difference = abs(len(normalized) - len(best_normalized))
        plural_variant = (
            normalized == best_normalized + "s"
            or normalized == best_normalized + "es"
        )
        if (
            best_score >= self.threshold
            and margin >= self.margin
            and not extra_words
            and length_difference <= 1
            and not plural_variant
        ):
            LOGGER.info(
                "resolver raw=%r canonical=%r method=fuzzy score=%.2f margin=%.2f",
                text, best_name, best_score, margin,
            )
            return text, best_name, "fuzzy", round(best_score, 2)

        LOGGER.info(
            "resolver unresolved raw=%r method=unresolved score=%.2f margin=%.2f",
            text, best_score, margin,
        )
        return text, None, "UNRESOLVED", round(best_score, 2)


def init_entity_log(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entity_log (
            raw TEXT PRIMARY KEY,
            canonical TEXT,
            method TEXT NOT NULL,
            score REAL NOT NULL,
            resolved_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def resolve_and_log(
    conn: sqlite3.Connection,
    raw: str,
    resolver: EntityResolver | None = None,
) -> tuple[str, str | None, str, float]:
    init_entity_log(conn)
    existing = conn.execute(
        "SELECT canonical, method, score FROM entity_log WHERE raw = ?",
        (raw,),
    ).fetchone()
    if existing is not None:
        return raw, existing["canonical"], existing["method"], float(existing["score"])

    resolver = resolver or EntityResolver()
    result = resolver.resolve(raw)
    conn.execute(
        """
        INSERT OR IGNORE INTO entity_log
            (raw, canonical, method, score, resolved_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            result[0],
            result[1],
            result[2],
            result[3],
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()
    return result


def export_entity_log(conn: sqlite3.Connection) -> list[dict]:
    init_entity_log(conn)
    rows = conn.execute(
        """
        SELECT raw, canonical, method, score, resolved_at
        FROM entity_log
        ORDER BY resolved_at, raw
        """
    ).fetchall()
    return [dict(row) for row in rows]


def bulk_resolve(
    conn: sqlite3.Connection,
    names: Iterable[str],
    resolver: EntityResolver | None = None,
) -> list[tuple[str, str | None, str, float]]:
    resolver = resolver or EntityResolver()
    init_entity_log(conn)
    results = [resolve_and_log(conn, name, resolver) for name in names]
    return results
