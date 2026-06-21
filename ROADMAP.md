# bluedb — Production Readiness Roadmap

Current state: the **foundational layers are done** — FTS (M1), SQL (M2),
engine + HTTP service (M3), and the M4 single-writer **machinery** (election +
self-fencing). Six follow-on tracks are merged to `dev`:

- **Two-tier SQL surface** — `/sql` (read-your-writes transactional, index-only
  reads + writes with `RETURNING`) and `/query` (HTAP analytical over DataFusion
  + the Iceberg mirror).
- **SQL-integrated full-text search** — `@@`/`ts_rank` rewrites to `pk IN (…)`,
  live-segment NRT, trigram-accelerated `LIKE`.
- **Double-entry ledger** (`bluedb-ledger`) — TigerBeetle-parity data plane.
- **Apache Iceberg lakehouse mirror** (`bluedb-lakehouse`) — the four v1 spikes shipped.
- **Evidence substrate** (`bluedb-evidence`) — verifiable chains + native graph.
- **Collections + search API** — a MongoDB-style document surface
  (`/collections/*`) over the same substrate.

`cargo test --workspace` is green (~1,000 tests); `cargo clippy --workspace` is
clean (modulo a known `AppError` size lint); the docs site builds `--strict`
clean and is published at **bluecopa.github.io/bluedb**. A black-box UAT suite
gates the build (the full release profile is **1,659 cases**, currently **GO** —
see Verification; PRs run a 99-case `core` smoke profile). The tri-cloud
object store is exercised end-to-end (S3/Azure via emulators, **GCS against real
GCS**); Jepsen re-verified the post-group-commit write path on the live 3-node
cluster (2026-06-16).

Foundations working today:
- ✅ `BlobStore` seam + `SlateDbBlobStore` (slatedb 0.13); durability proven across `Db` reopen; `BlobStoreMut` write seam; `ChunkedBlobStore` large-value layer.
- ✅ Vendored Quickwit read path (Bundle/Storage/Hot/Caching directories) on the tantivy fork, bridged to `BlobStore`.
- ✅ FTS: real indexer, lazy hotcache open, split manifest, multi-split BM25 search; logical deletes (generation-scoped tombstones), incremental append, same-id update, and merge/compaction.
- ✅ SQL: GlueSQL `Store`/`StoreMut` + real `Transaction` (overlay + `DbSnapshot` + atomic `WriteBatch`, snapshot isolation), secondary indexes, tenant-namespaced keyspace, and a schema-as-data registry.
- ✅ `bluedb-rest`: PostgREST-style DSL → SQL translation.
- ✅ Apache-2.0 attribution/NOTICE for vendored code.

Items below marked ✅ are done; unchecked items remain. Ordered by build-order milestone; cross-cutting tracks must advance alongside.

---

## M1 — FTS engine — ✅ complete

- [x] Indexing pipeline: documents → tantivy index → split (`indexer` module).
- [x] Incremental indexing (`writer::IndexWriter::append` — build a new split + append to the manifest; `generation` bumped per write).
- [x] Split manifest/catalog (`manifest` module — `SplitMeta`/`Manifest`).
- [x] Multi-split search (merged descending-score ranking with deterministic tie-break).
- [x] Merge / compaction policy (`merge::Compactor` — re-index live docs from N splits into one, generation-scoped liveness, returns superseded keys for GC).
- [x] Deletes & updates (`tombstones` — generation-scoped deletes so a same-id update is a true in-place replace; `multi_split_search_filtered` applies the scope + last-write-wins dedup).
- [x] Lazy reads + hotcache (`open::open_split_lazy` + `SplitBlobDirectory`: real hotcache via vendored `write_hotcache`, `HotDirectory`→`CachingDirectory`→`StorageDirectory`, range-fetch).
- [x] Schema/field mapping + per-field analyzers (`mapping` module; stemming/keyword fields at query time).
- [x] GC executor (`gc` module — `gc_keys` + `gc_orphaned_splits`; pure retention policies). *(Wiring the compaction policy to an automatic scheduler/loop is an ops concern for M3, not a library gap.)*
- [x] Compaction trigger policy (`policy::CompactionPolicy`).
- [x] Query features: pagination, highlighting (`SnippetGenerator`), FTS combined with structured filters (`BooleanQuery`).

## M2 — SQL pillar (GlueSQL over SlateDB) — ✅ complete

- [x] GlueSQL `Store`/`StoreMut` over SlateDB (`bluedb-sql`). Order-preserving key encoding; schemaless tables supported. Pinned `gluesql-core =0.19.0`.
- [x] Secondary indexes (`Index`/`IndexMut`): order-preserving, prefix-free value segment; maintained through the txn overlay on every INSERT/UPDATE/DELETE; `CREATE INDEX` back-fills, `DROP INDEX` purges.
- [x] Transactions / isolation: write-buffer overlay + point-in-time `DbSnapshot` reads + atomic `WriteBatch` commit. Snapshot isolation, read-your-own-writes, all-or-nothing commit, true ROLLBACK (incl. index entries).
- [x] Schema-as-data registry + write-time validation (`SchemaRegistry`; `validate_row`).
- [x] PostgREST-DSL → SQL translation (`bluedb-rest`; identifier allow-listing + literal escaping).
- [x] Multi-tenant key prefixing for the SQL keyspace (`Keyspace`; `SlateDbStorage::new_for_tenant`).
- [x] Uniqueness enforcement — gluesql enforces PRIMARY KEY (O(1) `fetch_data`) and `UNIQUE`-column (O(n) `scan_data`) constraints. **bluedb adds `CREATE UNIQUE INDEX` support** (stripped to a column-level UNIQUE constraint + a registry the guardrail consults — gluesql drops UNIQUE from `CREATE INDEX` natively).
- [x] Cross-statement isolation under concurrent connections (`Database` vends connections sharing one `Db` + a write lease; explicit transactions are serializable; reads are snapshot-isolated and lock-free).
- [x] **Views** — `CREATE VIEW` / `DROP VIEW` via `view_rewrite`; view references are inlined on reads (`cte::inline_views`), on both the `/sql` transactional path and the `/query` analytical path.

> **Scope note:** GlueSQL is OLTP/row-oriented — the transactional facts store, **not** analytics. Analytics is served by the in-process DataFusion front door (`bluedb-query`, over the Iceberg mirror — see the two-tier split below), **not** an external DuckDB/DuckLake.

## M3 — Engine & service — ✅ complete

- [x] `bluedb-engine` crate — REST→SQL execution (`rest_sql`) + the `FtsIndex` facade (ingest, update, delete, search, `maybe_compact`, `spawn_compaction_scheduler`).
- [x] The Rust service binary (`bluedb-server`): an HTTP/REST API over `bluedb-engine` (axum) — PostgREST-style CRUD (`/tables/{table}`), the two-tier `/sql` + `/query`, structured DDL (`/schema/*`), the ledger (`/ledger/*`), evidence (`/evidence/*`), graph (`/graph/*`), collections (`/collections/*`), an Iceberg REST catalog (`/catalog/v1/*`), and `/health`. `build_app(state) -> Router` is testable via `oneshot`; `main` opens the `Db` and serves.
- ~~PyO3 bindings / `fx_api` embedding / maturin~~ — **dropped** (2026-06-14): no Python embedding; the HTTP service is the integration surface.

## M4 — High availability

The single-writer **machinery** (election, self-fencing, fencing tokens) and the
service-level **gating + control API** are built in `bluedb-ha` +
`bluedb-server`. The **Postgres lease provider** is built and wired (a real
external arbiter); only the K8s/NATS lease variants and the cross-region
orchestration remain.

- [x] Single-writer election + self-fencing (`bluedb-ha`): `WriterController` `promote`/`demote`, background renewal, `is_active` self-fence within `safety_margin`. Monotonic `epoch` fencing token composes with SlateDB's `writer_epoch` CAS.
- [x] Service wiring: writes gated to the active writer (→ `503` when passive); reads always served (standby = read-only). `GET /admin/status`, `POST /admin/promote` (→ `409` if held), `POST /admin/demote`.
- [x] FTS search needs no coordination/replication — reads ungated (stateless); only the writer + FTS indexer/compactor are the coordinated single writer.
- [x] **Postgres `LeaseProvider`** (`crates/bluedb-ha/src/postgres.rs`) — atomic upsert + fencing-token epoch; wired in `main.rs`; Jepsen-tested.
- [ ] **K8s `Lease` / NATS KV lease providers** — the `LeaseProvider` trait + in-memory + Postgres impls exist; these two external-store variants need their clients + cluster testing. *(deployment layer.)*
- [ ] **Active-passive multi-region** (RPO > 0): object-store cross-region replication, gated promotion after "waiting out" the old lease, consistency-aware recovery via SlateDB checkpoints on promotion. Orchestrated by an external controller driving `/admin/promote`+`/admin/demote`. *(deployment layer + a checkpoint-recovery hook to add.)*
- [ ] **Failback** — reverse replication + re-promote, via the same `/admin` control surface. *(runbook/ops.)*

---

## Two-tier SQL surface — `/sql` + `/query`

The read path is split into two named surfaces with separate performance
contracts. `POST /sql` is the **transactional, read-your-writes** surface: a
`SELECT` filtered by the primary key or a secondary index is served direct from
fresh SlateDB state at lookup latency, and any read that would need an
analytical scan is rejected with `400 NO_INDEX` (the error names the index to
create). Writes stay here, now with `INSERT`/`UPDATE`/`DELETE … RETURNING`.
`POST /query` is the **HTAP analytical** surface — the DataFusion front door over
the Iceberg mirror ∪ the unsealed CDC tail: joins, aggregates, window functions,
set operations, JSON paths, recursive CTEs (below), and arbitrary non-indexed
filters/sorts. See `docs/sql/query-guardrail.md` for the cost model.

- [x] `/sql` transactional read-your-writes — guarded GlueSQL path; scan/sort guardrail; `NO_INDEX` reject with index-creation hint.
- [x] `/query` analytical front door — DataFusion over Iceberg ∪ unsealed tail; `X-Bluedb-Min-Watermark` freshness gate; read-your-writes on the writer, bounded-stale on a replica.
- [x] `INSERT`/`UPDATE`/`DELETE … RETURNING` on `/sql` (clause stripped, write runs, affected rows read back; DELETE captures pre-state; INSERT derives `WHERE pk IN (…)` from the parsed VALUES).
- [x] `SET default_null_order = 'nulls_first'|'nulls_last'` per-session, per-tenant on `/sql` (intercepted, stored on the `Database`, applied to ORDER BY after the FTS rewrite).
- [x] `GET /tables/{t}` auto-routes: PK/index filters → transactional fast path; non-indexed/JSON-path filters → analytical.
- [x] Scan/sort guardrail exempts `ORDER BY` over a PK point set (equality/IN-list), so a full-text `ORDER BY ts_rank(...)` is allowed (the `@@` rewrites to a bounded `pk IN (...)` set).
- [x] **Recursive CTEs (`WITH RECURSIVE`)** — `enable_recursive_ctes` turned on in the analytical `SessionContext`; runs on `/query` (the GlueSQL `/sql` path can't inline recursion). Bounded by the query's own terminator (`WHERE n < k`). Tests: `recursive_cte_returns_the_recursed_rows` (pure recursion + adjacency-list tree walk), `recursive_cte_accepts_bound_parameter`.
- [ ] REST `?col=fts.<query>` DSL operator on `/tables` *(deferred)*.

## SQL-integrated full-text search (Spec B) — ✅ built

- [x] **B1** — pre-parse rewrite of the Postgres FTS surface (`to_tsvector(…) @@ *_tsquery(…)`, `ts_rank`) → `pk IN (…)` + a rank-preserving `CASE` ordering.
- [x] **B2a** — in-memory tantivy `LiveSegment`: real BM25, NRT read-your-writes, update + tombstones, tsquery-kind → tantivy-query translation.
- [x] **B2c** — SQL commit tap (`CommitObserver`/`RowChange`) → `FtsEngine` maintains the live index → read-your-writes through SQL.
- [x] **B2b** — `CREATE FULLTEXT INDEX` via `POST /schema/.../fulltext-indexes` + `@@`/`ts_rank` over `/sql` (RYW over HTTP).
- [x] **B4** — durable tier: union searcher (live ∪ durable splits) + background seal + persisted index-definition registry + reopen on restart + seal scheduler.
- [x] **B3 (part 1)** — trigram index + correctness-gated `col LIKE '%lit%'` acceleration.
- [ ] **B3 (part 2)** — regex `~`/`~*`/`!~` (designed; gluesql has no regex engine, so it needs an in-rewrite candidate-value fetch + Rust `regex` verify).
- [ ] Cross-node FTS **failover replay** of the un-sealed live tail from the SQL watermark *(HA-M4 — one process owns the index today)*.

## Collections + search API (`/collections/*`) — ✅ built

A MongoDB-style document surface over the same substrate, served columnar via
DataFusion. Documents are stored under a `_id` PK; aggregation pipelines run on
the analytical engine. Routes: `insert`, `find`, `update`, `delete`, `aggregate`,
`count`, `createIndex`, `searchIndex` (GET + POST), `search`.

- [x] CRUD + `$lookup` join + aggregation pipeline stages (`$match`/`$sort`/`$limit`/`$skip`/`$project`/`$group`/`$lookup`/`$count`).
- [x] Single-field + compound + multikey indexes; dotted (nested) paths rejected (index paths and `$lookup` join keys).
- [x] BM25 search (`/collections/{coll}/search`) over a search index, with exists/regex query kinds.
- [x] Honors `X-Bluedb-Min-Watermark` freshness.

## Double-entry ledger (`bluedb-ledger`) — phases A–H ✅ complete

- [x] **A–G** — TigerBeetle data-plane parity: typed `Account`/`Transfer` (u128), all flags, the full named result-code set in TB's exact validation order, two-phase transfers + apply-time timeout expiry, linked chains, balancing, closing, imported events, `id_already_failed` semantics. ~112 engine tests.
- [x] **H** — atomic SQL projection (rows dual-written in the same `WriteBatch`) + `/ledger/{accounts,transfers}` batched create + lookups. u128/u64 cross the wire as **decimal strings** (precision-safe; documented).
- [ ] Run the Jepsen `ledger` workload on a **live 3-node cluster** against the current group-commit write path. *(The generic write path is cluster-verified via the `set` workload; the ledger-specific conservation workload still needs the schema-regime harness port.)*

## Lakehouse mirror (`bluedb-lakehouse`) — Iceberg CDC mirror — v1 ✅ complete

Continuously mirrors bluedb tables into Apache **Iceberg** in the same bucket so
external warehouses can join them — self-authored commit on published
`iceberg-rust` 0.9.1 (no fork, no `unsafe`), full CRUD via equality deletes,
event-driven seal. The four v1-limitation spike items are shipped:

- [x] Core mirror — durable CDC log (exactly-once), gluesql→Iceberg type mapping, self-authored manifests → snapshot → `metadata.json`, event-driven debounced seal, read-only Iceberg REST catalog, `PRAGMA lakehouse_mirror` opt-out. Cross-engine read verified by DuckDB's Iceberg extension.
- [x] #1 Multiple namespaces / multi-tenancy — per-`(tenant,table)` CDC + `LakehouseManager`.
- [x] #2 Composite primary keys — surrogate `__bluedb_pk` over the bluedb-sql rewrite seam; PK-range pushdown.
- [x] #3 Schema-evolution reconciliation — field-ids from stable colcat slots; `ALTER` ADD/DROP/RENAME self-authors an `add_current_schema` commit; **reconciles by column name** (not id) so an ALTER-then-RENAME reads correctly. DROP of a key column rejected.
- [x] #4 Incremental bin-packed compaction — minor pass bin-packs small files; periodic major pass reclaims delete files.
- [ ] Not yet reconciled into the mirror: column **type** changes and adding a `LIST`/`MAP` column to a materialized table.

---

## Evidence substrate (`bluedb-evidence`) — ✅ complete

- [x] Append-only verifiable evidence chains: server-assigned dense gap-free `seq`, idempotency, durability-before-ack; RFC 6962 Merkle (digest + inclusion + consistency proofs); erasure (redaction crypto-shred); HTTP `/evidence/*`; multi-tenant. **Jepsen-validated** (`evidence` workload).
- [x] Native graph store: directed weighted typed edges, append-with-edges, traversal (`reachable`, `widest_path`), atomic edge rewire (`Graph::mutate`), drop-graph; HTTP `/graph/*`. **Delete/mutate report actual change counts** (a missing edge → `deleted:0`).
- [x] Hardening (v1.1): O(log N) proofs (persisted node store); parallel BFS frontier; KMS-only digest signing (ES256 STHs via HashiCorp Vault Transit; no in-process keys) for non-repudiation.
- [x] Snapshot-consistent traversal — every traversal pins one MVCC `ReadView`. **Jepsen-validated** (`graph` workload).
- [x] **Inclusion proof for an unknown seq returns 404** (not 400), matching the redact/hard-delete contract.
- [ ] **External anchoring / witnessing** — level-3 equivocation defense (gossip STHs between consumers, or anchor `{size, root_hash}` outside bluedb).

---

## Cross-cutting (advance alongside every milestone)

### Storage / SlateDB
- [x] Durability round-trip proven (`Db` reopen); WAL on by default.
- [x] `Db` lifecycle management.
- [ ] SlateDB settings tuned per workload — `flush_interval` done (`BLUEDB_FLUSH_INTERVAL_MS`); block cache / Foyer + L0 SST size remain.
- [x] `get_range` KV caveat — `ChunkedBlobStore` splits large values. *(opt-in layer; not yet wired under the FTS/SQL read paths.)*

### Robustness
- [ ] Typed error taxonomy at lib boundaries (replace stringly `anyhow`/`unwrap`/`expect`).
- [ ] Handle corrupt/partial splits, missing keys, version/format mismatches.
- [ ] Input validation — identifier/type allow-listing + param-only done (injection-proof); query timeouts, memory/result-size caps, backpressure remain.

### Observability
- [ ] Tracing via OpenTelemetry (plain `tracing` exists; OTel export not wired).
- [ ] Real metrics export (no `/metrics` endpoint / Prometheus wiring yet): cache hit rate, split counts, index/query/ingest latencies.
- [ ] Structured logging; health/readiness probes (`/health` exists; readiness does not).

### Testing & quality
- [ ] Restore property tests + edge cases (empty index, huge split, concurrent read/write, corrupt data, reopen-durability).
- [x] Integration tests against real S3/GCS/Azure (`objstore_emulators.rs`): S3 (MinIO) + Azure (Azurite) via emulators; **GCS against real GCS**.
- [ ] Benchmarks: index throughput, query latency, rebuild time, memory.
- [ ] Fuzz the split parser.
- [x] HA chaos/failover tests — real Jepsen suite: `set` × {none, kill, partition, mix, skew} all `:valid? true` / lost-count 0; `evidence` + `graph` workloads live-validated. *(Remaining: port `list-append`/`counter`/`unique`/`ledger` to the schema regime; load/soak tests.)*
- [x] **Black-box UAT** — an HTTP suite gates the build. The full release profile is **1,659 cases**, currently **GO** (1,659/1,659, rev `3cf8b0d`, 2026-06-21 — the 99-case `core` smoke profile runs on PRs). Covers SQL (transactional + analytical), REST, collections, search, ledger, evidence, graph, HA.
- [ ] **CI gaps** — `.github/workflows/` has `deny.yml` (cargo-deny), `docs.yml`, `uat.yml` (server build + UAT), `testkit-wheels.yml`. **Missing**: a workflow that runs `cargo test --workspace`, `cargo clippy -D warnings`, and `cargo fmt --check` on PRs.

### Security & multi-tenancy
- [ ] Tenant isolation (keyspace boundaries; no cross-tenant reads).
- [ ] Encryption at rest (object-store SSE) + in transit.
- [ ] AuthZ — per-route bearer-token scopes + `/admin/sql` audit done (Spec A3c); real-IdP integration + structured audit log remain.
- [ ] Data retention / deletion (compliance), tied to the hot-window strategy.

### Maintenance / supply chain
- [ ] Vendored-fork update process: document the tantivy fork-rev pin + Quickwit vendor snapshot; procedure to bump and forward-port.
- [ ] `cargo deny`/`audit` for vulns + license compliance; lockfile discipline.
- [x] **Docs site** — an MkDocs Material site is published at **bluecopa.github.io/bluedb** (`mkdocs.yml` + the `docs/` tree on `dev`); builds `--strict` clean. rustdoc API docs + usage examples remain.

### Ops / deployment
- [x] **Dockerfile** — multi-stage build of `bluedb-server` exists.
- [ ] **K8s manifests / Helm chart** — `docs/deployment/kubernetes.md` is illustrative; no committed manifest set or Helm chart yet. *(deployment layer.)*
- [ ] Config/settings management (env-driven).
- [ ] Backup/restore + DR runbooks.
- [ ] Migration path for existing input-table-v2 data.
- [ ] Capacity & cost model (object-store request volume, compute).
