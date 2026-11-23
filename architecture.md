# Architecture for 500,000-Record Multi-Source Collection

This document describes a target scale architecture. It distinguishes the currently implemented SQLite prototype from infrastructure that would be needed for hundreds of thousands of records and horizontal workers.

## 1. Scale: queue, source adapters, sharding, and checkpointing

The current prototype fetches sources asynchronously and pages a few categories or datasets locally. It is suitable for a single process and SQLite, but SQLite write contention and the process-local queue would become bottlenecks at 500,000 startups, products, and papers.

The target design uses sharded seeds and stateless workers:

```text
Seed registry / config
  -> partitioned seed queue
  -> stateless fetch workers
  -> parse/normalize workers
  -> dedupe/idempotency layer
  -> primary store + downstream indexes
```

- **Source adapters in config:** each source has a URL/base, type (API/RSS/dataset), pagination strategy, robots policy, domain rate limit, parser adapter, timeout, and retry policy. Adapters can be registered independently of worker code.
- **Queue:** Redis Streams or a managed queue provides work units, leases, and visibility timeout. Dead-letter queues retain poison messages.
- **Sharded seeds:** split YC-style startup lists, Hugging Face organization/model cursors, paper date ranges, RSS feeds, job boards, and API cursors across deterministic shards. Workers do not own a global file cursor.
- **Autoscaling:** scale fetch workers on queue depth and stalled-lease age; scale parser workers on CPU/input backlog. Domain-specific worker pools prevent a busy source from consuming every worker slot.
- **Checkpointing:** persist high-water marks and pagination cursors per adapter/source. On restart, resume from a checkpoint plus an idempotent page range rather than replaying the whole collection.
- **Backpressure:** bounded in-memory queues and per-domain semaphores protect sources; each worker acknowledges a message only after a durable idempotent upsert.

Checkpointing should include source, shard, cursor, page, ETag/If-Modified-Since state, and last successful timestamp.

**Current implementation:** `AsyncFetcher` provides async concurrency, per-domain limits, robots.txt checks, retries, raw-page cache, and resumable database insert behavior. Papers, startups, and products are local; distributed queue, autoscaling, and source configuration sharding are not implemented.

## 2. 413/429 handling at scale

The implemented LLM pattern is useful at small scale:

- per-provider async token buckets;
- Retry-After or exponential backoff with 0.5–1.5 jitter;
- four 413 shrink attempts at 60% text steps;
- provider fallback chain;
- CLOSED/OPEN/HALF_OPEN circuit breakers;
- grounding and schema validation;
- `pending_extractions` rows for exhausted LLM work.

For 500,000 records, move each provider control into shared infrastructure:

- **Redis token buckets per provider and sub-key:** use Lua scripts or Redis Cell so all workers share RPM and concurrent-request limits. Key buckets by provider, tenant/API key, model, and region.
- **Token counting:** count both input and estimated output tokens from provider-specific tokenizers or conservative model limits. Reject or split before sending, and track actual usage for accounting.
- **Adaptive chunking:** binary-search or adaptive chunk sizing against each provider's 413/limit response. Preserve headings, titles, and high-density paragraphs; keep source spans for grounding.
- **Circuit breakers:** propagate breaker state through Redis or a service registry. Open only a provider/model key, not unrelated sources.
- **Fallback chain:** try provider 1, then provider 2, with 413 handled as a same-provider shrink and 429 handled as provider pause/fallback.
- **Dead-letter retry queue:** failed work remains in a retry queue until a next attempt. It should carry schema, canonical URL, content hash, attempt count, provider state, next-available time, and terminal error. Avoid immediate redelivery.

**Observed local behavior:** no LLM 429 or 413 events were used in the latest papers run. The papers pipeline collected records deterministically and made no LLM calls.

**Trade-offs:** Redis coordination improves fairness and cross-node consistency but adds latency and an operational dependency. Provider backoff must respect headers before applying jitter.

## 3. Freshness: exactly-once across nodes

The prototype canonicalizes URLs and hashes normalized text. The signal pipeline persists hashes in SQLite `seen_items`; papers use a unique `arxiv_id`; startups and products use canonical `source_url` keys. The SQLite `INSERT OR IGNORE` behavior is idempotent for reruns.

Across multiple nodes, use:

1. Canonicalize source URLs: remove tracking parameters, normalize scheme/host, lower-case host, and preserve required path data.
2. Compute a content hash from the canonical URL plus a normalized text representation.
3. Atomically claim each item with Redis `SET key value NX PX <lease>` or insert the canonical/content key into Postgres.
4. Perform one idempotent upsert: `INSERT ... ON CONFLICT (canonical_key) DO UPDATE ...` or update by source-specific unique key.
5. Update raw bytes, enriched fields, and audit timestamps in a transaction. Reprocessing the same item must not create a second record.
6. Distinguish verified 24-hour timestamps from inferred “new since last run” items.

Use a canonical key and a content hash separately. A canonical URL prevents duplicate versions of an item; a content hash detects substantive changes. If content changes, update the existing record and preserve provenance and history as needed. PostgreSQL unique constraints provide durable exactly-once semantics after process crashes; Redis NX is a fast lease mechanism but should not be the only source of truth.

**Current implementation:** SQLite uniqueness is enforced for paper IDs, startup/product URLs, and signal IDs. There is no distributed lease or cross-node transaction.

## 4. Storage: Postgres, JSONB, pgvector, and a graph

The prototype uses SQLite tables plus JSON `record_json` columns for startup/product shapes. The target primary store is PostgreSQL:

- relational tables for typed IDs, timestamps, source URLs, status, attempt counts, and foreign keys;
- JSONB for versioned provider/source payloads and optional extra fields;
- `pgvector` embeddings for semantic search and duplicate/similarity detection;
- source-event tables for raw payload, normalized payload, and provenance.

Postgres should maintain unique constraints on canonical URL, source ID (for example arXiv ID), content hash, and external entity IDs. JSONB supports evolution while typed columns preserve queryability and constraints.

Relations such as founder → startup, startup → product, and product → paper should be either:

- **Postgres edge tables:** simplest operations, ACID transactions, strong joins, and lower infrastructure cost; recommended as the primary graph representation.
- **Neo4j:** preferable only if deep, variable-hop traversals are a core product feature.

The suggested target relationship:

```text
Person --FOUNDED--> Startup --MAKES--> Product
                  Startup --RELATED_TO--> Paper
```

Neo4j can ingest Postgres IDs into nodes and relationships, but the database should remain the source of truth. Alternatively, an `entity_edges(src_id, dst_id, relation, confidence, source)` table in Postgres gives graph traversal sufficient for moderate depths and simpler backup/recovery.

Trade-offs:

- SQLite: easy and atomic for one process, but weak for concurrent writers and node-scale deployment.
- Postgres: durable, queryable, transactional, JSONB/pgvector capable, and horizontally scaled mainly through read replicas/partitions.
- Neo4j: strong traversal ergonomics, but another store to synchronize and secure.
- Redis: fast coordination/cache only; not the primary durable store.

## Anti-bot and licensed-data boundaries (Phase V)

Collection should prefer APIs and RSS feeds, then robots.txt-compliant static pages. The existing fetcher already keeps robots.txt checks for every requested host and logs source failures.

A later browser tier could use Playwright asynchronously only for explicitly allowed, JS-rendered pages. It should:

- reuse a bounded session pool and per-domain cookies;
- respect robots.txt and each site's minimum interval;
- stop or back off on 403/429/automation challenges;
- use no CAPTCHA evasion or identity spoofing;
- use human-readable audit logs for URLs, timestamps, and source policy.

For Cloudflare, Datadome, or similarly protected sources, the defensible approach is a managed unblocking product or licensed provider when the source permits it, not proxy rotation or attempts to evade CAPTCHAs. Managed providers expose policy-compliant data access and billing controls. Proxy pools should remain separate from the core pipeline and use only authorized data contracts.

## Real local run evidence

From the existing SQLite database:

- startups: 5,929 rows;
- products: 1,000 rows;
- papers: 1,000 rows;
- news signals: 20 rows;
- job signals: 17 rows.

The latest completed target-20 papers run reported:

```text
fetched=27 with_repo=9 stars_resolved=20
404s=0 dupes_skipped=11 elapsed=8.844s
```

No LLM provider fallback or 429 occurred in that run. The measured Hugging Face daily-page repo shares ranged from 62.5% to 76.9%; across 237 papers in ten probed calls, 159 included a repository URL (67.1%). The run fetched 16 papers on the current day, with 11 repository links, then resumed across historical dates. Empty weekend dates were skipped as no-result pages rather than used as source exhaustion.
