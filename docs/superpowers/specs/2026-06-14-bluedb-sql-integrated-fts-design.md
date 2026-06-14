# Spec B — SQL-integrated full-text search

**Date:** 2026-06-14
**Status:** Design (awaiting review)
**Depends on:** [Spec A — HTTP surface split & write-path](2026-06-14-bluedb-http-write-path-design.md)
(the index DDL lands on Spec A's DDL surface; the live-segment tap sits on the commit path)

## 1. Context & directive

Full-text search must be **SQL-integrated** — it works *like PostgreSQL FTS or
Elasticsearch, but on the same object storage* — **not** a separately-exposed search
API. You create a full-text index on a text column and search it through SQL (a
relevance predicate in `WHERE`, combinable with normal structured filters / `ORDER BY` /
pagination). The tantivy index (splits) lives in the **same object-storage substrate** as
the table, so there is **no separate search cluster and no ETL/sync** — the operational
pain of Elasticsearch.

**Why tantivy.** Plain SQL gives `LIKE` — no relevance ranking, no
tokenization/stemming, no phrase/fuzzy, and a full scan. tantivy is a Lucene-class engine
(BM25, analyzers, phrase/boolean/fuzzy), and via the vendored `quickwit-directories` read
path it runs **directly on the bucket** (lazy hotcache, range-fetched splits). So bluedb
gets Elasticsearch-grade search **without running Elasticsearch.**

**Driver.** Parity with **input-table-v2 (Atlas)**: text search + structured filters +
sorting + pagination on the same table.

### 1.1 Current state (verified)

- `bluedb-sql` has **zero FTS hooks** (only doc-comment mentions). Predicates are
  gluesql's hardcoded path, which we already extend via the `Planner::plan` hook.
- `bluedb-fts` is a **standalone BM25 engine**: `split/indexer/open/manifest/search/
  tombstones/writer/merge/gc`, with paginated search, highlighting, and tantivy-level
  structured filters. It indexes **documents**, with no link to SQL rows.
- `bluedb-engine` composes them as **two side-by-side facades** (`rest_sql` and
  `FtsIndex`) — **there is no bridge.**

So this is **design + build**, not docs over an existing feature.

## 2. Locked decisions (from the design dialogue)

| Decision | Choice | Rationale |
|---|---|---|
| Source of truth | **Model A** — SQL is canonical; FTS is a maintained index | no dual-write/consistency tax; fork-free via the Planner hook; exact Postgres UX |
| Index maintenance | **sync-on-commit tap → in-memory live (NRT) segment**; background seal to durable splits | the write path waits on durability, not CPU, so an in-memory tap is ~free |
| Read-your-writes | **guaranteed, cluster-wide, transparent** | active node serves all traffic and owns the live segment; matches SQL's existing RYW scope |
| Failover | new active **replays the live segment from the SQL watermark**; fresh `MATCH` gated until caught up | CP, zero loss (SQL is source of truth) |
| Cross-region | **out of RYW scope** | async warm standby (RPO>0), bounded staleness by design |
| Seal trigger | **PRAGMA** (`fts_seal_interval` / `fts_seal_max_docs`) | caps memory + failover-replay time; tunable per-DB |

The Elasticsearch model (denormalize filterable columns into the index, answer the whole
query in tantivy) was **rejected** as the foundation: it duplicates data, has heavier
maintenance, and stops SQL being the engine. It remains a possible later optimization for
high-text-selectivity / low-filter-selectivity queries.

## 3. Goals / non-goals

**Goals**

1. `CREATE FULLTEXT INDEX` (via Spec A's DDL surface) on a text column, with an analyzer.
2. The **PostgreSQL FTS surface** — `to_tsvector(cfg, col) @@ to_tsquery(q)` /
   `plainto_tsquery` / `websearch_to_tsquery`, `ts_rank(...)` for `ORDER BY`, combinable
   with structured filters and `LIMIT/OFFSET`. Copy the Postgres way so it's familiar and
   portable.
3. **Read-your-writes** on the index: after `COMMIT`, a subsequent `MATCH` sees the row —
   with no synchronous split flush (write path stays fast).
4. REST parity: `?col=fts.<query>` lowers to the predicate, combinable with the existing
   filter/order/pagination DSL — the input-table-v2 surface.
5. Correct update/delete semantics (tombstones reflected in search immediately).

**Non-goals**

- Cross-region real-time search (bounded-staleness by design).
- The Elasticsearch denormalization model (deferred optimization).
- Exposing tantivy as a direct/standalone search API (explicitly rejected).
- A persistent FTS query cache.

## 4. Architecture

Four pieces; only the bridge + live segment + tap are new. `bluedb-fts` (engine) and the
Planner hook already exist.

### 4.1 Index definition (DDL)

`POST /schema/tables/{t}/fulltext-indexes` `{column, analyzer}` (Spec A DDL surface).
Persisted in the schema-as-data registry alongside the table. Defines: the indexed
column, the analyzer (keyword / stemming / whitespace — `bluedb-fts` already supports
per-field analyzers), and the FTS doc identity (the SQL row's primary key, or the
`SeqAllocator` rowid for PK-less tables — so update/delete tombstones align with SQL rows).

### 4.2 Write path — sync-on-commit tap → in-memory live segment

- The SQL `StoreMut` commit path emits, for any committed row touching an FTS-indexed
  column, a `(pk, indexed-text, op)` event to an in-process **live indexer** on the
  active node — **synchronously, before `COMMIT` returns.**
- The live indexer maintains an **in-memory tantivy segment** plus a **tombstone set**
  (an UPDATE = tombstone old pk + add new doc; a DELETE = tombstone). In-memory, so
  ~µs–sub-ms; **no object-store round trip** — negligible against the ~100 ms (or 25 ms)
  SQL flush wait (ties to Spec A's throughput contract).
- A **background seal** (on the `fts_seal_interval` / `fts_seal_max_docs` PRAGMA) folds the
  live segment into a durable tantivy split, publishes it to the `bluedb-fts` manifest,
  and resets the live segment. This heavy work is **off the ack path.**

### 4.3 Query path — PostgreSQL surface, rewritten in the shim (fork-free)

The query uses the genuine Postgres FTS syntax:

```sql
SELECT id, title
FROM docs
WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')   -- relevance predicate
  AND status = 'open'                                                       -- ordinary structured filter
ORDER BY ts_rank(to_tsvector('english', body), plainto_tsquery('invoice overdue')) DESC
LIMIT 20;
```

**Parser feasibility (verified).** sqlparser 0.52 **tokenizes `@@`** (`Token::AtAt` →
`BinaryOperator::AtAt`), and `to_tsvector` / `to_tsquery` / `plainto_tsquery` /
`websearch_to_tsquery` / `ts_rank` parse as ordinary function calls — so the surface is
syntactically accepted. gluesql's *translate/executor* does **not** understand `@@` or the
`tsvector`/`tsquery` types, so we intercept these constructs in our pre-execution shim
(the same place we rewrite set-ops, comma-joins, and coercions) and the `Planner::plan`
pass, rewriting them into the tantivy plan. The query string is a bound `$N` param
(injection-proof per Spec A).

The rewrite:
1. Detects `to_tsvector(cfg, col) @@ <tsquery-fn>(q)` and `ts_rank(to_tsvector(cfg,col), …)`.
   `cfg` (`'english'`) selects the analyzer; the `*_tsquery` variant selects query parsing
   (`to_tsquery` = boolean/operators, `plainto_tsquery` = AND of terms,
   `websearch_to_tsquery` = web-search syntax).
2. Runs the search over **durable splits (minus tombstones) ∪ the in-memory live segment**
   via an `FtsSearcher` trait → `(pk, score)` for the top candidates (over-fetching a
   window so structured filters don't under-fill a page).
3. Rewrites the `@@` predicate → `pk IN (<candidate pks>)` and threads the score so
   `ts_rank` resolves and `ORDER BY` / `LIMIT` work in GlueSQL as normal.

GlueSQL then applies the structured filters, ordering, and pagination over the candidate
rows — and the `status = 'open'` filter can itself ride a **B-tree secondary index**
(§4.5). **No GlueSQL executor changes** — same shim/Planner machinery we already own.

### 4.4 REST mapping

`GET /tables/docs?body=fts.invoice%20overdue&status=eq.open&order=rank.desc&limit=20`
lowers to the SQL in §4.3. `fts.<query>` is a new `bluedb-rest` operator that emits the
`to_tsvector(cfg, col) @@ plainto_tsquery($q)` predicate (query string as a bound param);
`order=rank.desc` maps to `ORDER BY ts_rank(...) DESC`. The structured filters
(`status=eq.open`) and pagination use the existing DSL unchanged.

### 4.5 Indexing model — B-tree (exists) vs FTS/GIN (this spec)

bluedb-sql already has **B-tree-style secondary indexes** for regular columns:
`CREATE/DROP INDEX`, order-preserving prefix-free value encoding (so they serve equality
lookups, **range** scans, and `ORDER BY`), maintained through the txn overlay, and used by
the planner (`plan_index`). Single-column.

The FTS index in this spec is the **GIN analog** — a separate index *type* (tantivy splits
+ live segment) for text relevance, declared via `CREATE FULLTEXT INDEX` / the DDL surface,
queried via `@@`. The two compose: in an FTS query, the relevance predicate hits the FTS
index and the structured filters (`status = 'open'`) hit the B-tree secondary index. So
"Postgres way" holds on both axes — B-tree for structured, GIN for full-text.

## 5. Read-your-writes — flow & guarantee

```
INSERT/UPDATE/DELETE → SQL commit
   └─ tap indexes (pk, text) into the in-memory live segment + tombstones  [synchronous]
COMMIT returns
   └─ MATCH query on the active node searches splits ∪ live segment (− tombstones)
        → sees the just-committed row              [read-your-writes]
… background …
   seal interval → live segment → durable split → manifest publish → live reset
```

**Scope.** RYW is guaranteed on the **active node**, which in active-passive HA serves
**all** client traffic — so RYW is **cluster-wide and transparent** (clients never reason
about which node they hit). This matches the consistency contract SQL already has in the
cluster. The passive is a warm failover standby, not a live read replica. Cross-region is
bounded-staleness and out of RYW scope.

**Failover (CP).** On promotion, the new active rebuilds its live segment by **replaying
SQL rows committed after the last published split watermark** (SQL is source of truth →
zero loss). Fresh `MATCH` is briefly **gated until the replay catches up** — preserving
RYW across failover rather than serving a stale index. Replay size is bounded by the seal
interval.

## 6. Write-path impact

- In-memory tokenize+index on the synchronous commit (proportional to text size,
  sub-ms for normal rows). **No second durable round trip.** Against Spec A's ~100 ms (or
  25 ms) flush wait, this is negligible — the throughput numbers are essentially unchanged.
- The heavy split build is **async** (background seal).
- Backpressure: the live segment *is* the buffer; it's bounded by the seal trigger
  (memory cap + failover-replay cap). No separate queue.

## 7. Components & isolation

- `bluedb-fts` — **unchanged** engine (append/seal/search/tombstones/merge/gc).
- **New: `LiveSegment`** — in-memory tantivy index + tombstone set on the active node;
  `index(pk, text)`, `tombstone(pk)`, `search(query, k) -> [(pk, score)]`, `seal() ->
  Split`. Independently testable in-memory.
- **New: commit tap** — a hook in `bluedb-sql`'s `StoreMut` commit emitting indexed-column
  changes for FTS-indexed tables.
- **New: SQL↔FTS bridge** — a shim/`Planner::plan` pass detecting the `@@` predicate +
  `to_tsvector`/`*_tsquery`/`ts_rank` constructs and rewriting via an injected
  `FtsSearcher` (which fans the search over splits ∪ live).
- `bluedb-engine` — wires the `LiveSegment` + `FtsSearcher` into the SQL connection and
  drives the background seal scheduler (it already has the `FtsIndex` compaction scheduler
  pattern to follow).

## 8. Testing

- **RYW:** insert → `@@` query in the same connection sees the row with no explicit flush.
- **Filter + rank + pagination:** `… @@ … AND eq-filter ORDER BY ts_rank(…) LIMIT/OFFSET`
  returns the right ranked, filtered, paged set (including the over-fetch-avoids-underfill
  case).
- **Tombstones:** update changes which rows match; delete removes a match — immediately.
- **Seal round-trip:** after a seal, reopen and search → results unchanged (live → split
  is transparent).
- **Failover replay:** promote a standby with un-sealed writes → after replay, no matches
  are lost; fresh `MATCH` is gated until caught up.
- **Analyzer:** stemming/keyword/whitespace behave per the index definition.
- **Injection:** the query string is a bound `$N` param end-to-end.

## 9. Open questions

- **Predicate syntax — resolved to the Postgres surface** (`@@` + `to_tsvector` /
  `*_tsquery` / `ts_rank`); sqlparser 0.52 parses all of it. **Remaining sub-question:**
  whether gluesql's `translate` *errors* on `BinaryOperator::AtAt`/unknown functions (→ we
  must rewrite at the **pre-parse string** level, before gluesql parses) or tolerates them
  far enough to reach our `Planner::plan` hook (→ rewrite on the typed AST). Determines
  which shim stage does the rewrite; verify against gluesql 0.19 `translate`.
- **Score threading mechanism.** How the per-pk score reaches `ts_rank`/`ORDER BY` after
  the `pk IN (…)` rewrite — candidates: a derived `VALUES (pk, score)` join, an injected
  `CASE pk WHEN … THEN score` expression, or a scalar-subquery. Pick by what GlueSQL plans
  efficiently.
- **Over-fetch window** sizing for filter-under-fill (fixed multiple of `LIMIT`, or
  iterative widening until the page fills).
- **Multi-column / multiple FTS indexes** per table (v1 may restrict to one indexed column
  per `@@` predicate).
