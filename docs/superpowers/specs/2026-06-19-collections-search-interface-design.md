# Collections Search — Elasticsearch-style Search Interface (Design)

**Status:** Design · **Date:** 2026-06-19

**Sub-project B** of bluedb's document + search capability. Sub-project **A** (the
MongoDB-style `/collections` document API) is implemented and merged to `dev`
(PR #23). B adds full-text search *over those collection documents*.

## Summary

A `/collections/{c}/search` HTTP surface that gives bluedb an **Elasticsearch-shaped
search API** — an ES Query-DSL subset (`match`/`term`/`bool`/`range`/`match_phrase`/
`exists`), BM25 relevance scoring, and an ES-style hits envelope — by translating
ES query shapes onto bluedb's existing **tantivy** full-text engine (`bluedb-fts`).
There is **no new search engine and no Elasticsearch wire protocol**: a stateless
translation layer over the mature FTS that bluedb already has (BM25, per-field
analyzers, durable segments, read-your-writes).

## Decisions (locked during brainstorming)

- **API-shape compatibility, not the ES wire/REST protocol.** ES Query-DSL-shaped
  JSON over bluedb's own HTTP endpoint; native ES clients / Kibana do **not**
  connect. Rationale: captures the ergonomic value (developers think in the ES
  Query DSL) without emulating Elasticsearch's vast REST surface or chasing its
  version churn. Consistent with the collections (MQL-shape) decision.
- **Familiar 80% subset, not a high-fidelity port** — a documented subset; a
  compatibility matrix states what is supported.
- **Search targets `/collections` documents** — an ES "index" ≈ a bluedb
  collection; you index and search the documents' fields. (The existing
  Postgres-style `@@`/`ts_rank` full-text surface over SQL-table columns is
  unchanged and out of scope here.)
- **Reuse `bluedb-fts` wholesale** — its `IndexMapping` (per-field analyzers),
  BM25 scoring, durable segments + tombstones/merge/GC, and commit-tap
  read-your-writes are the engine. B is a translation + wiring layer.

## Goals / Non-goals

**Goals (v1):** declare a per-collection search mapping; index mapped document
fields into tantivy keyed by `_id`; maintain the index on insert/update/delete;
`search` with the common ES query types; BM25 scoring; ES-style hits envelope
with `from`/`size`, `sort`, `_source` filtering, and highlights; per-tenant
scoping; read-your-writes (inherited from `bluedb-fts`).

**Non-goals (v1):** the Elasticsearch wire/REST protocol and Kibana
compatibility; aggregations / facets; fuzzy, wildcard, and prefix queries
(trigram-accelerated later); suggesters / autocomplete; nested and geo queries;
`scroll` / Point-in-Time; the percolator; cross-index (multi-collection) search;
runtime fields / scripting.

## Architecture

```
client ──ES Query DSL JSON──▶ POST /collections/{c}/search   (bluedb-server handler)
                                       │
                                       ▼
                              bluedb-search            ← new crate: ES DSL ⇄ tantivy
                                ├─ model   (ES request/response types)
                                ├─ query   (DSL → tantivy `Query`)
                                └─ hits    (tantivy results → ES hits envelope)
                                       │
                                       ▼
                                 bluedb-fts            ← engine: BM25, analyzers,
                                                          durable segments, RYW
                              (search index keyed by collection `_id`)

writes: insert/update/delete on a collection  ──▶ collections write path
        (already maintains derived cols / multikey side tables)
              └─ if the collection has a search mapping: extract the mapped
                 fields from `doc` and upsert/delete the tantivy doc by `_id`
```

`bluedb-search` is a **stateless translation layer** (ES DSL ⇄ tantivy + ES
models), unit-testable in isolation. The `/search` and `/searchIndex` handlers in
`bluedb-server` are thin: authorize, resolve tenant, translate, execute via
`bluedb-fts`, shape the response. The indexing side hooks the **existing
collections write path** (the same place that maintains derived columns and
multikey side tables today). No changes to the FTS engine itself.

## 1. Search-index / mapping model

A collection becomes searchable by declaring a mapping:

```
POST /collections/{c}/searchIndex
{ "fields": { "title": {"analyzer": "english"},
              "body":  {"analyzer": "english"},
              "tag":   {"type": "keyword"} } }
```

This maps **directly onto `bluedb-fts`'s `IndexMapping`** (per-field analyzer;
`keyword` → the raw/untokenized tokenizer; default analyzer is `english`). Only
declared fields are indexed — ES-faithful, with per-field analyzer control. The
tantivy document is keyed by the collection's `_id` (the FTS engine already keys
documents by the table's primary key). The mapping is persisted per
collection/tenant (a small registry, mirroring how the gateway tracks its other
index metadata).

## 2. Indexing pipeline + freshness

On `insert` / `update` / `delete` of a collection document, the gateway — for a
collection that has a search mapping — extracts the mapped fields' text from
`doc` and **upserts or deletes the tantivy document keyed by `_id`**. This hooks
the **same collections write path** that already maintains derived columns and
multikey side tables, so there is one consistent write pipeline. `bluedb-fts`'s
commit-tap provides **read-your-writes** over durable segments, so a search
reflects a just-written document immediately (inherited, not rebuilt).
`createSearchIndex` backfills existing documents.

## 3. Query DSL → tantivy

`POST /collections/{c}/search` carries an ES Query-DSL subset, lowered to a
tantivy `Query` by `bluedb-search::query`:

| ES query | tantivy lowering |
|---|---|
| `match` | analyzed terms over a field → `BooleanQuery` (default OR; `operator:and` → Must) |
| `match_phrase` | `PhraseQuery` |
| `term` | exact term (raw) |
| `range` (`gt`/`gte`/`lt`/`lte`) | `RangeQuery` |
| `bool` | `BooleanQuery` — `must`→Must, `should`→Should, `must_not`→MustNot, `filter`→Must but contributes no score |
| `exists` | field-presence query |

A query naming an unmapped field, or an unsupported query type, returns a clear
error (consistent with the documented subset) rather than silently matching
nothing.

## 4. Scoring + results (ES hits envelope)

tantivy computes BM25 scores. The response is ES-shaped:

```json
{ "took": 3,
  "hits": { "total": {"value": 2, "relation": "eq"},
            "max_score": 1.83,
            "hits": [ {"_index": "<coll>", "_id": "...", "_score": 1.83,
                       "_source": { ...document... },
                       "highlight": {"body": ["…<em>matched</em>…"]} } ] } }
```

- **Pagination:** `from` / `size` (ES-style).
- **Sort:** default `_score` descending; or by a field (`sort: [{field: "asc"}]`).
- **`_source`:** the re-inflated document, fetched from the collection by `_id`;
  `_source: false` or a field list trims it.
- **Highlights:** via tantivy's `SnippetGenerator` (v1-lite — the mapped text
  fields named in `highlight`).

## 5. API surface

| Method · path | Body | Scope |
|---|---|---|
| `POST /collections/{c}/searchIndex` | `{fields: {…}}` (declare/replace mapping) | `schema:admin` |
| `GET /collections/{c}/searchIndex` | — (describe the mapping) | `data:read` |
| `POST /collections/{c}/search` | ES search request (`query`, `from`, `size`, `sort`, `highlight`, `_source`) | `data:read` |

Per-tenant via `X-Bluedb-Tenant`; writes/DDL gated to the active writer, as
everywhere else.

## 6. Components & file layout

- **`crates/bluedb-search`** (new): pure translation — `model` (ES request/response
  structs), `query` (DSL → tantivy `Query`), `hits` (tantivy results → ES
  envelope). Depends on `tantivy` + `bluedb-fts` types; unit-testable without I/O.
- **`crates/bluedb-server`**: thin `/search` + `/searchIndex` handlers; search-index
  maintenance hooked into the collections write path; execution via `bluedb-fts`.
- **Reused unchanged:** `bluedb-fts` (engine, `IndexMapping`, BM25, durability,
  RYW) and `bluedb-collections` (document storage, write path, `_id`).

## 7. Testing & the compatibility matrix

- **Unit (`bluedb-search`):** DSL → tantivy `Query` translation (assert the query
  shape per ES type); hits-envelope shaping.
- **Integration (embedded server):** declare a search mapping → index documents →
  `search` with `match`/`bool`/`range`/`match_phrase` → assert hits, ordering by
  `_score`, read-your-writes (search a just-inserted doc with no seal), and
  highlights.
- **Compatibility matrix:** a published doc page listing every supported query
  type, mapping option, and result feature — and the notable unsupported ones.

## Open questions / future work

- Trigram-accelerated `wildcard`/`prefix` (bluedb already has a trigram index).
- Aggregations / facets (tantivy fast fields).
- Vector / kNN search (a later, separate capability).
- ES wire-protocol + Kibana compatibility (a much larger, separate effort).
- Cross-collection (multi-index) search.
