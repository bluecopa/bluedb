# bluedb — Production Readiness Roadmap

Current state: **M1 FTS core + the M2 SQL pillar exist and pass tests** (`cargo test --workspace`, 45 tests; `cargo clippy --workspace --all-targets` clean). Working today:
- ✅ `BlobStore` seam + `SlateDbBlobStore` (slatedb 0.13); durability proven across `Db` reopen; `BlobStoreMut` write seam; `ChunkedBlobStore` large-value layer.
- ✅ Vendored Quickwit read path (Bundle/Storage/Hot/Caching directories) on the tantivy fork, bridged to `BlobStore`.
- ✅ FTS: real indexer (docs→index→split), real hotcache + lazy range-fetch open, split manifest/catalog, multi-split BM25 search with merged ranking.
- ✅ SQL: GlueSQL `Store`/`StoreMut` over SlateDB — order-preserving key encoding, CREATE/INSERT/SELECT-WHERE/UPDATE/DELETE/ORDER BY, schemaless tables.
- ✅ Apache-2.0 attribution/NOTICE for vendored code.

Items below marked ✅ landed in the M1+M2 parallel build; unchecked items remain. Ordered by build-order milestone; cross-cutting tracks must advance alongside.

---

## M1 — FTS engine to usable

- [x] Indexing pipeline: documents → tantivy index → split (`indexer` module — `Indexer` / `build_split{,_with_hotcache}`).
- [ ] Schema/field mapping from a table's searchable columns; per-field analyzers / tokenizers / language config. *(schema is passed in; no per-field analyzer config layer yet.)*
- [ ] Incremental indexing (add docs over time without full rebuild). *(each `build` is a fresh index.)*
- [x] Split **manifest/catalog** (`manifest` module — `SplitMeta`/`Manifest`, load/store over the substrate).
- [x] Multi-split search (`search` module — `multi_split_search`, merged descending-score ranking with deterministic tie-break).
- [ ] Merge / compaction policy (bound split count and query fan-out).
- [ ] Deletes & updates (tombstones + merge).
- [x] **Lazy reads + hotcache** (`open::open_split_lazy` + `SplitBlobDirectory`: real hotcache via vendored `write_hotcache`, `HotDirectory`→`CachingDirectory`→`StorageDirectory`, range-fetch — never `get_all`).
- [ ] GC of orphaned / superseded / expired splits (incl. the hot-window retention strategy).
- [ ] Query features: pagination, highlighting, FTS predicates combined with structured filters.

## M2 — SQL pillar (GlueSQL over SlateDB)

- [x] Implement GlueSQL `Store`/`StoreMut` over SlateDB (`bluedb-sql`: `fetch_schema`/`fetch_all_schemas`/`fetch_data`/`scan_data` + `insert_schema`/`delete_schema`/`append_data`/`insert_data`/`delete_data`). Order-preserving key encoding via `Key::to_cmp_be_bytes()`; schemaless tables supported. Pinned `gluesql-core =0.19.0`.
- [ ] Secondary indexes (GlueSQL `Index` trait). *(stubbed — uses gluesql "not supported" defaults.)*
- [ ] Transactions / isolation mapped onto SlateDB's single-writer model (the genuine hard design question). *(currently autocommit no-op; `BEGIN`/`COMMIT`/rollback not real.)*
- [ ] Schema-as-data registry + write-time validation (keeps typed-schema UX without DDL).
- [ ] PostgREST-DSL → SQL translation layer (input-table-v2 API parity).
- [ ] Multi-tenant key prefixing for the SQL keyspace. *(key encoding has table+pk namespacing; tenant prefix not yet layered in.)*
- [ ] (Scope note: GlueSQL is OLTP/row-oriented — for the transactional facts store, **not** analytics. Heavy analytics stays in DuckDB/DuckLake.)

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
