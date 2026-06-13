# bluedb — Production Readiness Roadmap

Current state: **M2 (SQL pillar) is complete; M1 FTS is feature-complete bar GC + query niceties** (`cargo test --workspace` ~117 tests; `cargo clippy --workspace --all-targets` clean). Working today:
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
- [ ] Schema/field mapping from a table's searchable columns; per-field analyzers / tokenizers / language config. *(schema is passed in; no per-field analyzer config layer yet.)*
- [ ] **GC executor** for the superseded/orphaned splits compaction reports (the policy + key list exist; wiring the actual deletes via `BlobStoreMut` + a hot-window retention strategy remains).
- [ ] Compaction trigger policy / scheduler (when to compact — count/size/tombstone-ratio thresholds; today compaction is a manual call).
- [ ] Query features: pagination, highlighting, FTS predicates combined with structured filters.

## M2 — SQL pillar (GlueSQL over SlateDB) — **complete**

- [x] Implement GlueSQL `Store`/`StoreMut` over SlateDB (`bluedb-sql`). Order-preserving key encoding via `Key::to_cmp_be_bytes()`; schemaless tables supported. Pinned `gluesql-core =0.19.0`.
- [x] Secondary indexes (`Index`/`IndexMut`): index entries in a `TAG_INDEX` keyspace with an **order-preserving, prefix-free** value segment (so string-column range/ORDER-BY scans sort by content, not encoded length); maintained through the txn overlay on every INSERT/UPDATE/DELETE; `CREATE INDEX` back-fills, `DROP INDEX` purges.
- [x] Transactions / isolation on SlateDB's single-writer model: write-buffer **overlay** + point-in-time **`DbSnapshot`** reads + atomic **`WriteBatch`** commit. Snapshot isolation, read-your-own-writes, all-or-nothing commit, true ROLLBACK (incl. index entries). `begin(true)` returns `false` inside an open `BEGIN` so statements don't auto-commit.
- [x] Schema-as-data registry + write-time validation (`SchemaRegistry`: list/get/register schemas without DDL; `validate_row` checks count/type/NOT-NULL; schemaless tables accept any row).
- [x] PostgREST-DSL → SQL translation layer (`bluedb-rest`: filters/operators/order/limit/offset + INSERT/UPDATE/DELETE, identifier allow-listing + literal escaping; input-table-v2 parity surface).
- [x] Multi-tenant key prefixing for the SQL keyspace (`Keyspace` length-prefixes a tenant namespace before every key; `SlateDbStorage::new_for_tenant`).
- [ ] (Scope note: GlueSQL is OLTP/row-oriented — for the transactional facts store, **not** analytics. Heavy analytics stays in DuckDB/DuckLake.)
- [ ] (Follow-ups, not blockers: wire `bluedb-rest` output into `bluedb-sql` execution; secondary-index *uniqueness* enforcement; cross-statement isolation under concurrent connections — today single-writer per `Db`.)

## M3 — Service & integration

- [ ] The Rust service binary: gRPC/HTTP data + control API (ingest, query, `promote`/`demote`/`health`).
- [ ] PyO3 bindings (`bluedb-py`) with `pyo3-async-runtimes` (Tokio↔asyncio) so `fx_api`'s async endpoints don't block the event loop.
- [ ] `fx_api` integration: v2-compatible endpoints, Pusher change-event parity, multi-tenant routing.
- [ ] maturin build wired into the uv/hatchling workspace; CI wheel artifacts.

## M4 — High availability (design → implementation)

- [ ] Single-writer election (K8s Lease or NATS) + SlateDB `writer_epoch` fencing (intra-region, RPO 0).
- [ ] Self-fencing writer (halt on lease loss before TTL).
- [ ] External arbiter (HA multi-AZ Postgres lease row) for cross-region.
- [ ] Active-passive multi-region: bucket cross-region replication, gated promotion, consistency-aware recovery (SlateDB checkpoints), Temporal failover saga in `fx_worker`.
- [ ] Failback procedure (reverse replication, re-promote).
- [ ] Confirm FTS search needs no coordination/replication (stateless readers); only the indexer is a coordinated writer.

---

## Cross-cutting (advance alongside every milestone)

### Storage / SlateDB
- [x] Durability round-trip proven: reopen the `Db` and read after restart (`durability_survives_reopen` over `LocalFileSystem`). Note: WAL is on by default (no `wal_disable` feature); a bare put-then-drop is **not** guaranteed durable — an explicit `flush()`/`close()` is required for determinism.
- [x] `Db` lifecycle management (`SlateDbBlobStore::open`/`open_local`/`open_in_memory`/`flush`/`shutdown`).
- [ ] SlateDB settings tuned per workload (block cache / Foyer, flush interval, L0 SST size).
- [x] Fix the `get_range` KV caveat — `ChunkedBlobStore` splits large values across ordered keys + manifest; range reads fetch only overlapping chunks. *(opt-in layer; not yet wired under the FTS/SQL read paths.)*

### Robustness
- [ ] Typed error taxonomy at lib boundaries (replace stringly `anyhow`/`unwrap`/`expect`).
- [ ] Handle corrupt/partial splits, missing keys, version/format mismatches.
- [ ] Input validation; query timeouts; memory/result-size caps; backpressure.

### Observability
- [ ] Tracing via OpenTelemetry (align with fx-runtime `core.observalibity`).
- [ ] Real metrics export (wire the `CacheMetrics` stub → Prometheus): cache hit rate, split counts, index/query/ingest latencies.
- [ ] Structured logging; health/readiness probes.

### Testing & quality
- [ ] Restore property tests + edge cases (empty index, huge split, concurrent read/write, corrupt data, reopen-durability).
- [ ] Integration tests against real S3/GCS/Azure (not just `InMemory`).
- [ ] Benchmarks: index throughput, query latency, rebuild time, memory — validate the ~100M-rows/year + rebuild-budget assumptions.
- [ ] Fuzz the split parser.
- [ ] HA chaos/failover tests; load/soak tests.
- [ ] CI: build + test + `clippy -D warnings` + `fmt --check` + `cargo deny`.

### Security & multi-tenancy
- [ ] Tenant isolation (keyspace boundaries; no cross-tenant reads).
- [ ] Encryption at rest (object-store SSE) + in transit.
- [ ] AuthZ integration with fx-runtime auth; audit logging.
- [ ] Data retention / deletion (compliance), tied to the hot-window strategy.

### Maintenance / supply chain
- [ ] Vendored-fork update process: document the tantivy fork-rev pin + Quickwit vendor snapshot; procedure to bump and forward-port the 7 directory files.
- [ ] `cargo deny`/`audit` for vulns + license compliance; lockfile discipline.
- [ ] rustdoc API docs, usage examples, architecture docs.

### Ops / deployment
- [ ] Dockerfile; K8s manifests (Deployment, topology-spread, Lease RBAC).
- [ ] Config/settings management (env-driven).
- [ ] Backup/restore + DR runbooks.
- [ ] Migration path for existing input-table-v2 data.
- [ ] Capacity & cost model (object-store request volume, compute).
