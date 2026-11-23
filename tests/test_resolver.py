from __future__ import annotations

import sqlite3
import time

import pytest

from src.resolver.resolve import (
    EntityResolver,
    bulk_resolve,
    export_entity_log,
    init_entity_log,
    normalize,
    resolve_and_log,
)


@pytest.fixture
def resolver() -> EntityResolver:
    return EntityResolver()


@pytest.mark.parametrize(
    "raw",
    ["OpenAI", "OpenAI Inc.", "OpenAI, Inc.", "Open AI", "openai"],
)
def test_openai_variants_resolve(resolver: EntityResolver, raw: str):
    assert resolver.resolve(raw)[1] == "OpenAI"


@pytest.mark.parametrize(
    "raw",
    [
        "Coherent",
        "Anthropics Ltd",
        "Mistral Systems",
        "Perplexity Foods",
    ],
)
def test_false_positive_traps_do_not_merge(resolver: EntityResolver, raw: str):
    assert resolver.resolve(raw)[1] is None
    assert resolver.resolve(raw)[2] == "UNRESOLVED"


def test_unknown_company_is_unresolved(resolver: EntityResolver):
    raw = "Totally Unknown Robotics"
    result = resolver.resolve(raw)
    assert result == (raw, None, "UNRESOLVED", result[3])


def test_cohere_is_not_coherent(resolver: EntityResolver):
    assert resolver.resolve("Cohere")[1] == "Cohere"
    assert resolver.resolve("Coherent")[1] is None


def test_unicode_and_case_variations(resolver: EntityResolver):
    assert resolver.resolve("ＯＰＥＮＡＩ")[1] == "OpenAI"
    assert resolver.resolve("OPENAI")[1] == "OpenAI"
    assert resolver.resolve("cOHeRe")[1] == "Cohere"


def test_normalize():
    assert normalize("  OpenAI, INC. ") == "openai"
    assert normalize("AI21 Labs") == "ai21"
    assert normalize("Weights & Biases") == "weights biases"


def test_idempotent_log_row_and_export():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_entity_log(conn)

    first = resolve_and_log(conn, "OpenAI")
    second = resolve_and_log(conn, "OpenAI")

    assert first == second
    assert conn.execute("SELECT COUNT(*) FROM entity_log").fetchone()[0] == 1
    export = export_entity_log(conn)
    assert len(export) == 1
    assert export[0]["raw"] == "OpenAI"
    assert export[0]["canonical"] == "OpenAI"
    conn.close()


def test_bulk_resolve_1000_names_is_fast():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    names = [f"OpenAI variant {i}" for i in range(1000)]

    started = time.perf_counter()
    results = bulk_resolve(conn, names)
    elapsed = time.perf_counter() - started

    assert len(results) == 1000
    assert elapsed < 5.0
    assert conn.execute("SELECT COUNT(*) FROM entity_log").fetchone()[0] == 1000
    conn.close()
