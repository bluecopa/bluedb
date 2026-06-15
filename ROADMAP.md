# bluedb — Production Readiness Roadmap

Current state: **M1 (FTS), M2 (SQL), M3 (engine + HTTP service), and the M4 single-writer core are complete**, and five follow-on tracks have merged to `dev`: **HTTP surface & write-path hardening (Spec A)**, **SQL-integrated full-text search (Spec B)**, the **double-entry ledger** (`bluedb-ledger`), the **Apache Iceberg lakehouse mirror** (`bluedb-lakehouse` — all four v1 spike items shipped), and the **evidence substrate** (`bluedb-evidence` — verifiable chains + native graph) — see the dedicated sections below. `cargo test --workspace` is green (~900 tests); `cargo clippy --workspace --all-targets` clean; the docs site builds `--strict` clean. **Verification (2026-06-16):** Jepsen re-run on the live 3-node cluster against the **post-group-commit** write path — `set` workload × {none, kill, partition, mix, skew} all `:valid? true`, lost-count 0; and the **tri-cloud object store** is exercised end-to-end (S3/Azure via emulators, **GCS against real GCS**). Remaining is the **M4 deployment layer** (concrete lease store + cross-region replication/orchestration) plus the scoped follow-ups flagged per section (FTS regex `~`, the ledger-specific cluster-Jepsen workload, evidence external anchoring). Foundations working today:
- ✅ `BlobStore` seam + `SlateDbBlobStore` (slatedb 0.13); durability proven across `Db` reopen; `BlobStoreMut` write seam; `ChunkedBlobStore` large-value layer.
- ✅ Vendored Quickwit read path (Bundle/Storage/Hot/Caching directories) on the tantivy fork, bridged to `BlobStore`.
- ✅ FTS: real indexer, lazy hotcache open, split manifest, multi-split BM25 search; **logical deletes (generation-scoped tombstones), incremental append, same-id update, and merge/compaction** (re-index live docs, physically drop the dead).
- ✅ SQL: GlueSQL `Store`/`StoreMut` + **real `Transaction` (overlay + `DbSnapshot` + atomic `WriteBatch`, snapshot isolation), secondary indexes (`Index`/`IndexMut`, order-preserving entry encoding), tenant-namespaced keyspace, and a schema-as-data registry**.
- ✅ `bluedb-rest`: PostgREST-style DSL → SQL translation (input-table-v2 parity surface).
- ✅ Apache-2.0 attribution/NOTICE for vendored code.

Items below marked ✅ are done; unchecked items remain. Ordered by build-order milestone; cross-cutting tracks must advance alongside.

---

## M1 — FTS engine to usable

- [x] Indexing pipeline: documents → tantivy index → split (`indexer` module — `Indexer` / `build_split{,_with_hotcache}`).
- [x] Incremental indexing (`writer::IndexWriter::append` — build a new split + append to the manifest, existing splits untouched; `generation` bumped per write).
- [x] Split **manifest/catalog** (`manifest` module — `SplitMeta`/`Manifest`, load/store over the substrate).
- [x] Multi-split search (`search` module — `multi_split_search`, merged descending-score ranking with deterministic tie-break).
- [x] Merge / compaction policy (`merge::Compactor` — re-index live docs from N splits into one, generation-scoped liveness, returns superseded keys for GC).
- [x] Deletes & updates (`tombstones` — **generation-scoped** deletes so a same-id update is a true in-place replace: tombstone old at gen g, re-append at gen > g; `multi_split_search_filtered` applies the scope + last-write-wins dedup).
- [x] **Lazy reads + hotcache** (`open::open_split_lazy` + `SplitBlobDirectory`: real hotcache via vendored `write_hotcache`, `HotDirectory`→`CachingDirectory`→`StorageDirectory`, range-fetch — never `get_all`).
- [x] Schema/field mapping + per-field analyzers (`mapping` module — `IndexMapping`/`FieldMapping`/`Analyzer{Raw,Default,EnStem,Whitespace}`: builds a tantivy `Schema` and registers the needed tokenizers on a lazily-opened split so stemming/keyword fields work at query time).
- [x] **GC executor** (`gc` module — `gc_keys` deletes the compactor's `superseded_keys`; `gc_orphaned_splits` lists `splits/` via `BlobStoreMut::scan_prefix`, diffs the current manifest, deletes the unreferenced; pure retention policies `splits_below_generation`/`expired_by_time`).
- [x] Compaction trigger policy (`policy::CompactionPolicy` — `should_compact`/`plan_compaction` over split-count + tombstone-ratio thresholds; v1 plan = full compaction). *(Wiring it to an automatic scheduler/loop is an ops concern for M3, not a library gap.)*
- [x] Query features: pagination (`multi_split_search_paginated`), highlighting (`highlight` via `SnippetGenerator`), and FTS combined with structured filters (`multi_split_search_with_filter` + `Filter{Term,U64Range,I64Range}` AND-ed via a `BooleanQuery`).

## M2 — SQL pillar (GlueSQL over SlateDB) — **complete**

- [x] Implement GlueSQL `Store`/`StoreMut` over SlateDB (`bluedb-sql`). Order-preserving key encoding via `Key::to_cmp_be_bytes()`; schemaless tables supported. Pinned `gluesql-core =0.19.0`.
- [x] Secondary indexes (`Index`/`IndexMut`): index entries in a `TAG_INDEX` keyspace with an **order-preserving, prefix-free** value segment (so string-column range/ORDER-BY scans sort by content, not encoded length); maintained through the txn overlay on every INSERT/UPDATE/DELETE; `CREATE INDEX` back-fills, `DROP INDEX` purges.
- [x] Transactions / isolation on SlateDB's single-writer model: write-buffer **overlay** + point-in-time **`DbSnapshot`** reads + atomic **`WriteBatch`** commit. Snapshot isolation, read-your-own-writes, all-or-nothing commit, true ROLLBACK (incl. index entries). `begin(true)` returns `false` inside an open `BEGIN` so statements don't auto-commit.
- [x] Schema-as-data registry + write-time validation (`SchemaRegistry`: list/get/register schemas without DDL; `validate_row` checks count/type/NOT-NULL; schemaless tables accept any row).
- [x] PostgREST-DSL → SQL translation layer (`bluedb-rest`: filters/operators/order/limit/offset + INSERT/UPDATE/DELETE, identifier allow-listing + literal escaping; input-table-v2 parity surface).
- [x] Multi-tenant key prefixing for the SQL keyspace (`Keyspace` length-prefixes a tenant namespace before every key; `SlateDbStorage::new_for_tenant`).
- [x] Uniqueness enforcement — verified: gluesql's executor enforces PRIMARY KEY (O(1) `fetch_data`) and `UNIQUE`-column (O(n) `scan_data`) constraints through our `Store`, correctly even inside a transaction (`tests/uniqueness.rs`). Note: gluesql has no `CREATE UNIQUE INDEX` and does not route uniqueness through secondary indexes — they accelerate query predicates only; the UNIQUE-column check is an O(n) table scan per write.
- [x] Cross-statement isolation under concurrent connections (`Database` vends connections sharing one `Db` + a **write lease**; explicit transactions are serializable — `BEGIN` takes the lease and snapshots under it, so concurrent read-modify-write transactions cannot lose an update — while reads are snapshot-isolated and lock-free. `tests/isolation.rs`, incl. an 8-way concurrent-increment test).
- [ ] (Scope note: GlueSQL is OLTP/row-oriented — for the transactional facts store, **not** analytics. Heavy analytics stays in DuckDB/DuckLake.)
- [ ] (Remaining follow-up, not a blocker: wire `bluedb-rest`'s generated SQL into `bluedb-sql` execution end-to-end. Autocommit statements are atomic + snapshot-isolated but not serialized against each other — use an explicit `BEGIN..COMMIT` for atomic read-modify-write under concurrency.)

## M3 — Engine & service — **complete**

- [x] **`bluedb-engine`** crate — unifies the pillars behind one facade the service wraps:
  - REST→SQL execution (`rest_sql`): a `bluedb-rest` DSL request → SQL → `bluedb-sql` `Glue::execute` → rows; `RestError` vs SQL error kept distinct in `EngineError`. *(carried from M2 follow-ups — done.)*
  - `FtsIndex` facade — ingest (`append`), in-place `update`, generation-scoped `delete`, `search`, and a policy-driven `maybe_compact` (load manifest+tombstones → `CompactionPolicy` → `Compactor` → persist → `gc_keys`) + `spawn_compaction_scheduler` background loop. Manifest read-modify-write serialized by an in-process write lock. *(carried from M1 follow-ups — done.)*
- [x] The Rust service binary (`bluedb-server`): an **HTTP/REST API** over `bluedb-engine` (axum) — PostgREST-style CRUD (`GET`/`POST`/`PATCH`/`DELETE` over `/tables/{table}`, filters/order/limit in the query string, JSON bodies), a `/sql` endpoint (later split by Spec A into a parameterized `/sql` + an off-by-default `/admin/sql` — see the Spec A section), and `/health`. Each request draws a fresh isolated connection from a shared `Database`; `EngineError` maps to HTTP status (REST/SQL → 400, infra → 500). `build_app(state) -> Router` is testable via `oneshot` (no socket); `main` opens the `Db` (local FS or in-memory via env) and serves.
- ~~PyO3 bindings / `fx_api` embedding / maturin~~ — **dropped** (2026-06-14): no Python embedding; the HTTP service is the integration surface.

## M4 — High availability

The single-writer **machinery** (election, self-fencing, fencing tokens) and the
service-level **gating + control API** are built in `bluedb-ha` + `bluedb-server`;
the parts that need external systems (a concrete shared lease store, object-store
cross-region replication, the failover orchestrator) are scoped as the
deployment layer behind the `LeaseProvider` seam.

- [x] Single-writer **election** + self-fencing (`bluedb-ha`): `WriterController` `promote`/`demote`, a background renewal loop, and an `is_active` gate that self-fences within `safety_margin` of lease expiry (and immediately on a lost renew). A monotonic `epoch` fencing token rides on every handover and composes with SlateDB's own `writer_epoch` CAS fencing (the storage backstop). Deterministically tested via `TestClock`.
- [x] Service wiring (`bluedb-server`): writes (`POST/PATCH/DELETE /tables`, `POST /sql`) are gated to the active writer (→ `503` when passive); reads are always served (standby = read-only). `GET /admin/status`, `POST /admin/promote` (→ `409` if held elsewhere), `POST /admin/demote`. Standalone binary auto-promotes; `BLUEDB_START_PASSIVE` starts read-only.
- [x] FTS search needs no coordination/replication — confirmed: reads are ungated (stateless); only the writer (and the FTS indexer/compactor) is the coordinated single writer the lease fences.
- [ ] **Concrete `LeaseProvider`** for real multi-node HA — a Postgres lease row (the HA arbiter), a Kubernetes `Lease`, or a NATS KV bucket. The trait + an in-memory impl exist; these need the external store (+ its client deps) and a cluster to test. *(deployment layer.)*
- [ ] **Active-passive multi-region** (RPO > 0): object-store cross-region replication (bucket config), gated promotion after "waiting out" the old lease to avoid cross-bucket split-brain, and consistency-aware recovery via SlateDB checkpoints on promotion. Orchestrated by an external operator/controller driving `/admin/promote`+`/admin/demote` (no Python/Temporal-in-fx_worker — that path was dropped). *(deployment layer + a checkpoint-recovery hook to add.)*
- [ ] **Failback** — reverse replication + re-promote, via the same `/admin` control surface. *(runbook/ops.)*

## HTTP surface & write-path hardening (Spec A) — **complete**

- [x] **A1** — param-only data plane: `bluedb-rest` emits `$N` placeholders + a typed `Param` vec, the engine binds via `execute_with_params` — **injection-proof by construction**; array INSERT → server-wrapped `BEGIN..COMMIT` batch.
- [x] **A2** — `flush_interval` knob (`BLUEDB_FLUSH_INTERVAL_MS`, default 25 ms; an open-time SlateDB `Settings` field, strong durability always — relaxed durability rejected).
- [x] **A3a** — `/sql` = one **parameterized** non-DDL statement; `/admin/sql` = arbitrary SQL (off by default, audited). Closes the old unauthenticated arbitrary-`/sql` exposure.
- [x] **A3b** — `/schema/*` structured JSON → validated DDL (identifier + type-keyword allow-lists; the client authors no SQL → injection-proof).
- [x] **A3c** — per-route **bearer-token authz** scopes (`data:read`/`data:write`/`data:query`/`schema:admin`/`superuser`); enforced when `BLUEDB_AUTHZ_TOKENS` is set, open otherwise.
- [x] **A4** — HTTP/2 (h2c) so one connection multiplexes many concurrent in-flight writes.

## SQL-integrated full-text search (Spec B) — **built** (`feat/http-surface-and-fts`)

- [x] **B1** — pre-parse rewrite of the Postgres FTS surface (`to_tsvector(…) @@ *_tsquery(…)`, `ts_rank`) → `pk IN (…)` + a rank-preserving `CASE` ordering (gluesql `translate` rejects `@@`, so the rewrite runs before it) + the `FtsSearcher` seam.
- [x] **B2a** — in-memory tantivy `LiveSegment`: real BM25, NRT read-your-writes, update (delete-term-then-add) + tombstones, tsquery-kind → tantivy-query translation.
- [x] **B2c** — SQL **commit tap** (`CommitObserver`/`RowChange` in `bluedb-sql`, fires after the durable write) → `FtsEngine` maintains the live index → **read-your-writes through SQL**.
- [x] **B2b** — `CREATE FULLTEXT INDEX` via `POST /schema/.../fulltext-indexes` + `@@`/`ts_rank` over `/sql` (RYW over HTTP).
- [x] **B4** — durable tier: union searcher (live ∪ durable splits, live's covered-set masks stale hits) + background **seal** (live→split) + persisted index-definition **registry** + **reopen** on restart + seal scheduler + server lifecycle wiring (`BLUEDB_FTS_SEAL_INTERVAL_MS`, bound on promote).
- [x] **B3 (part 1)** — trigram index (pre-trigramized via the built-in whitespace analyzer; reuses the FTS machinery) + correctness-gated `col LIKE '%lit%'` acceleration (`pk IN` prefilter + gluesql's `LIKE` as the exact verify).
- [ ] **B3 (part 2)** — regex `~`/`~*`/`!~` (designed; gluesql has no regex engine, so it needs an in-rewrite candidate-value fetch + Rust `regex` verify).
- [ ] Cross-node FTS **failover replay** of the un-sealed live tail from the SQL watermark *(HA-M4 — one process owns the index today)*.
- [ ] REST `?col=fts.<query>` DSL operator on `/tables` *(deferred)*.

## Double-entry ledger (`bluedb-ledger`) — phases A–H **complete**

- [x] **A–G** — TigerBeetle data-plane parity: typed `Account`/`Transfer` (u128), all flags, the full named result-code set in TB's exact validation order, two-phase transfers (pending/post/void) + apply-time timeout expiry, linked chains, balancing, closing, imported events, `id_already_failed` semantics. 107 engine tests.
- [x] **H** — atomic **SQL projection** (rows dual-written into the SAME `WriteBatch` as the canonical postcard records, via `bluedb_sql::ProjectedTable`) + `/ledger/{accounts,transfers}` batched create (per-item result codes, u128 as JSON strings) + lookups + a Jepsen `ledger` workload (conservation Σdebits=Σcredits + accounting bounds).
- [ ] Run the Jepsen `ledger` workload on a **live 3-node cluster** against the current group-commit write path. *(The generic write path is now cluster-verified post-group-commit via the `set` workload — see Verification below; the ledger-specific conservation workload still needs the schema-regime harness port: it must create its tables with a PK, and `list-append`/`ledger` reads must not assume insertion order under IOT clustering.)*

## Lakehouse mirror (`bluedb-lakehouse`) — Iceberg CDC mirror — **v1 complete**

Continuously mirrors bluedb tables into Apache **Iceberg** in the same bucket so external warehouses (BigQuery/Databricks/Snowflake/Trino/DuckDB) can join them — self-authored commit on published `iceberg-rust` 0.9.1 (no fork, no `unsafe`), full CRUD via equality deletes, event-driven seal. Core merged PR #3; the four v1-limitation spike items are all shipped:

- [x] **Core mirror** — durable CDC log (exactly-once into the data `WriteBatch`), gluesql→Iceberg type mapping, self-authored data/delete manifests → snapshot → `metadata.json`, event-driven debounced seal, read-only Iceberg **REST catalog** (`/catalog/v1/*`), `PRAGMA lakehouse_mirror` opt-out, server-wired over object-store FileIO. Cross-engine read verified by DuckDB's Iceberg extension.
- [x] **#1 Multiple namespaces / multi-tenancy** (PR #4) — per-`(tenant,table)` CDC + one engine per tenant + `LakehouseManager`; `X-Bluedb-Tenant` header → per-tenant Iceberg namespace + `tenant:<name>` authz.
- [x] **#2 Composite primary keys** (PR #5) — surrogate `__bluedb_pk` (order-preserving component encoding) over the bluedb-sql rewrite seam; PK-range pushdown; components mirrored as ordinary prunable columns with an Iceberg sort order.
- [x] **#3 Schema-evolution reconciliation** (PR #6) — Iceberg field-ids derived from stable colcat slots; `ALTER` ADD/DROP/RENAME self-authors an `add_current_schema` commit before the next snapshot; DROP of a key column rejected.
- [x] **#4 Incremental bin-packed compaction** (PR #7) — minor pass bin-packs small data files via a scoped merge-on-read snapshot (reuses iceberg-rust's reader, materializes deletes, preserves survivor sequence numbers); periodic major pass reclaims delete files; target size via `PRAGMA lakehouse_target_file_bytes`.
- [ ] Not yet reconciled into the mirror: column **type** changes and adding a `LIST`/`MAP` column to a materialized table.

---

## Evidence substrate (`bluedb-evidence`) — **complete** (merged, PR #8); hardening pending PR

- [x] Append-only **verifiable evidence chains**: server-assigned dense gap-free `seq`, idempotency, durability-before-ack, per-chain verified/plain mode; **RFC 6962 Merkle** (digest + inclusion + consistency proofs, client-side verification); **erasure** (redaction crypto-shred keeping `leaf_hash` + plain-chain hard-delete); HTTP `/evidence/*`; multi-tenant.
- [x] Native **graph store**: directed weighted typed edges with out/in adjacency, **append-with-edges** (graph = reproducible projection of the chain), edges API, **traversal** (`reachable`, `widest_path`), drop-graph; HTTP `/graph/*`.
- [x] **Hardening (v1.1 — `feat/evidence-hardening`, reviewed-clean, pending PR):** **O(log N)** inclusion/consistency proofs (persisted complete-subtree node store); **parallel** BFS frontier expansion in `reachable`; **KMS-only digest signing** (ES256 Signed Tree Heads — HashiCorp Vault Transit; **no in-process keys** — bluedb holds only a Vault token + key name, the in-process signer is a test fixture only) for **non-repudiation**, with key rotation delegated to the KMS (the signing key version, `key_version`, is surfaced on signed responses).
- [ ] **External anchoring / witnessing** — level-3 equivocation defense (gossip STHs between consumers, or anchor `{size, root_hash}` outside bluedb); snapshot-consistent traversal; redaction of `type`/`at`.

---

## Cross-cutting (advance alongside every milestone)

### Storage / SlateDB
- [x] Durability round-trip proven: reopen the `Db` and read after restart (`durability_survives_reopen` over `LocalFileSystem`). Note: WAL is on by default (no `wal_disable` feature); a bare put-then-drop is **not** guaranteed durable — an explicit `flush()`/`close()` is required for determinism.
- [x] `Db` lifecycle management (`SlateDbBlobStore::open`/`open_local`/`open_in_memory`/`flush`/`shutdown`).
- [ ] SlateDB settings tuned per workload — **`flush_interval` done** (Spec A2: `BLUEDB_FLUSH_INTERVAL_MS`, default 25 ms); block cache / Foyer + L0 SST size remain.
- [x] Fix the `get_range` KV caveat — `ChunkedBlobStore` splits large values across ordered keys + manifest; range reads fetch only overlapping chunks. *(opt-in layer; not yet wired under the FTS/SQL read paths.)*

### Robustness
- [ ] Typed error taxonomy at lib boundaries (replace stringly `anyhow`/`unwrap`/`expect`).
- [ ] Handle corrupt/partial splits, missing keys, version/format mismatches.
- [ ] Input validation — **identifier/type allow-listing + param-only (injection-proof) done** (Spec A1/A3); query timeouts, memory/result-size caps, backpressure remain.

### Observability
- [ ] Tracing via OpenTelemetry (align with fx-runtime `core.observalibity`).
- [ ] Real metrics export (wire the `CacheMetrics` stub → Prometheus): cache hit rate, split counts, index/query/ingest latencies.
- [ ] Structured logging; health/readiness probes.

### Testing & quality
- [ ] Restore property tests + edge cases (empty index, huge split, concurrent read/write, corrupt data, reopen-durability).
- [x] Integration tests against real S3/GCS/Azure (not just `InMemory`) — `objstore_emulators.rs`: a real SlateDB round-trip (write → close → reopen → read, incl. conditional-put) per backend. **S3** (MinIO) + **Azure** (Azurite) via the emulator stack; **GCS** against **real GCS** (`gcs_real_round_trip`, env-gated — object_store's GCS XML API isn't fully served by local emulators).
- [ ] Benchmarks: index throughput, query latency, rebuild time, memory — validate the ~100M-rows/year + rebuild-budget assumptions.
- [ ] Fuzz the split parser.
- [x] HA chaos/failover tests — real Jepsen suite (`jepsen/`): leader-aware client + docker-CLI nemesis; `set` workload × {none, kill, partition, mix, skew} all `:valid? true` / lost-count 0 on the live 3-node cluster, **re-verified on the post-group-commit write path** (2026-06-16). *(Remaining: port `list-append`/`counter`/`unique`/`ledger` to the schema regime; load/soak tests.)*
- [ ] CI: build + test + `clippy -D warnings` + `fmt --check` + `cargo deny`.

### Security & multi-tenancy
- [ ] Tenant isolation (keyspace boundaries; no cross-tenant reads).
- [ ] Encryption at rest (object-store SSE) + in transit.
- [ ] AuthZ — **per-route bearer-token scopes + `/admin/sql` audit done** (Spec A3c); real-IdP integration with fx-runtime auth + structured audit log remain.
- [ ] Data retention / deletion (compliance), tied to the hot-window strategy.

### Maintenance / supply chain
- [ ] Vendored-fork update process: document the tantivy fork-rev pin + Quickwit vendor snapshot; procedure to bump and forward-port the 7 directory files.
- [ ] `cargo deny`/`audit` for vulns + license compliance; lockfile discipline.
- [ ] Docs — README/ROADMAP cover all tracks; an **MkDocs Material site exists** (`docs.yml` + content on `sql-conformance-harness`) but the build workflow + `docs/` tree must land on `dev` for `bluecopa.github.io/bluedb` to publish (Pages source is `dev:/`, never built). rustdoc API docs + usage examples remain.

### Ops / deployment
- [ ] Dockerfile; K8s manifests (Deployment, topology-spread, Lease RBAC).
- [ ] Config/settings management (env-driven).
- [ ] Backup/restore + DR runbooks.
- [ ] Migration path for existing input-table-v2 data.
- [ ] Capacity & cost model (object-store request volume, compute).
