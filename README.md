# AI Data Ingestion Pipeline — LLM Resilience Layer

> **Phase III**: Multi-tier LLM fallback chain with retry, rate-limiting,
> circuit breaking, text chunking, hallucination grounding, and SQLite
> persistence for failed records.

---

## Project Structure

```
src/
  config.py          # Loads config.yaml → typed Pydantic-Settings models
  schemas.py         # Pydantic models: Startup, Product, Paper, Job, News
  llm/
    errors.py        # Exception hierarchy
    retry.py         # Async exponential back-off with jitter
    ratelimit.py     # Async token-bucket rate limiter
    breaker.py       # Circuit breaker (CLOSED / OPEN / HALF_OPEN)
    chunker.py       # Token-budget text compressor (tiktoken)
    providers.py     # GeminiProvider, GroqProvider, DeepSeekProvider, MockProvider
    orchestrator.py  # Fallback chain driver with grounding + DB persistence
  storage/
    db.py            # SQLite — `pending_extractions` table
tests/
  test_retry.py
  test_ratelimit.py
  test_breaker.py
  test_orchestrator.py
config.yaml
.env.example
requirements.txt
```

---

## Setup

### 1. Create a virtual environment (recommended)
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux / macOS
source .venv/bin/activate
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure API keys
```bash
cp .env.example .env
# Edit .env and fill in your keys
```

**Keys are read exclusively from environment variables — never stored in `config.yaml`.**

---

## Running the Tests

```bash
pytest -v
```

All tests use `MockProvider` and injectable sleep/clock — **no real API calls,
no real sleeping**. The full suite completes in well under a second.

### Startup dataset attribution

The Y Combinator startup dataset is sourced from the MIT-licensed community
repository [`akshaybhalotia/yc_company_scraper`](https://github.com/akshaybhalotia/yc_company_scraper).
The repository's `LICENSE` is MIT. Records provide Y Combinator company slugs;
this pipeline derives the attribution URL as
`https://www.ycombinator.com/companies/<slug>`.

### Hugging Face product organization rule

Organization ownership is checked with
`https://huggingface.co/api/organizations/<name>/overview`. A successful
organization response allows models for that author to be collected; personal
accounts that do not resolve as organizations are skipped. Model pages remain
the canonical `source.url`, and the organization name is stored as
`startupName`.

---

## Key Design Decisions

### Hallucination prevention (hard rule)
Every string field extracted by an LLM is verified as a normalised substring of
the source text. Numeric fields must appear verbatim. Any result that fails this
grounding check is discarded and the next provider is tried. `source_url` is
stamped onto every record so provenance is always traceable.

### Provider fallback chain
`Gemini Flash → Groq (Llama 3) → DeepSeek`. The order is configurable in
`config.yaml`. Each provider wraps HTTP calls and maps status codes to typed
exceptions (`RateLimited`, `PayloadTooLarge`, `ProviderDown`, `BadOutput`).

### Retry strategy
Exponential back-off: `delay = min(cap, base × 2^attempt) × jitter(0.5–1.5)`.
If the provider supplies a `Retry-After` header, that value replaces the
computed base. `BadOutput` and `PayloadTooLarge` are never retried.

### Rate limiting
In-process async token bucket (one per provider). After a 429, `penalize()`
idles the whole bucket so concurrent tasks stop piling up requests.
> **Production note** (in `ratelimit.py`): replace with a Redis-backed
> implementation shared across worker processes/pods.

### Circuit breaker
Opens after 5 consecutive failures, cools down for 60 s, then allows one
HALF_OPEN trial request. Timing is injectable so tests run deterministically.

### SQLite persistence
When every provider is exhausted, the raw text, schema name, source URL, and a
`next_attempt_at` timestamp are written to `pending_extractions`. No record is
silently dropped.

### Observability
`Orchestrator.summary()` returns per-provider and total counters:
`rate_limited`, `shrinks`, `failures`, `grounding_failed`, `success`, `skipped`.
All events are logged via the standard `logging` module (no `print` statements).
