# bluedb — Production Readiness Roadmap

Current state: a **validated spike**. Working today (`cargo test -p bluedb-fts`, 21 tests):
- ✅ `BlobStore` seam + `SlateDbBlobStore` (slatedb 0.13).
- ✅ Vendored Quickwit read path (Bundle/Storage/Hot/Caching directories) on the tantivy fork, bridged to `BlobStore`.
- ✅ `split::pack_split` (split writer) and an end-to-end BM25 query over a split stored in SlateDB.
- ✅ Apache-2.0 attribution/NOTICE for vendored code.

Everything below is **not done**. Ordered by build-order milestone; cross-cutting tracks must advance alongside.

---

## M1 — FTS engine to usable

- [ ] Indexing pipeline: documents → tantivy index → split (not just `pack_split`).
- [ ] Schema/field mapping from a table's searchable columns; per-field analyzers / tokenizers / language config.
- [ ] Incremental indexing (add docs over time without full rebuild).
- [ ] Split **manifest/catalog**: which splits exist per tenant/index, with byte ranges, generation, doc count, time window.
- [ ] Multi-split search (open + query across a tenant's splits; merge ranked results).
- [ ] Merge / compaction policy (bound split count and query fan-out).
- [ ] Deletes & updates (tombstones + merge).
- [ ] **Lazy reads + hotcache**: write a real hotcache (`write_hotcache`), open via `HotDirectory` over `StorageDirectory`, range-fetch footer+hotcache instead of reading the whole split into memory.
- [ ] GC of orphaned / superseded / expired splits (incl. the hot-window retention strategy).
- [ ] Query features: pagination, highlighting, FTS predicates combined with structured filters.

## M2 — SQL pillar (GlueSQL over SlateDB) — *not started*

- [ ] Implement GlueSQL `Store`/`StoreMut` over SlateDB (`get`/`put`/`delete`/ordered `scan`); key encoding (table + pk → key), values, schemaless tables.
- [ ] Secondary indexes (GlueSQL `Index` trait).
- [ ] Transactions / isolation mapped onto SlateDB's single-writer model (the genuine hard design question).
- [ ] Schema-as-data registry + write-time validation (keeps typed-schema UX without DDL).
- [ ] PostgREST-DSL → SQL translation layer (input-table-v2 API parity).
- [ ] Multi-tenant key prefixing for the SQL keyspace.
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
- [ ] Durability round-trip proven: reopen the `Db` and read after restart (current tests put+get in one instance — memtable only).
- [ ] `Db` lifecycle management (open/close, background flush/compaction, graceful shutdown).
- [ ] SlateDB settings tuned per workload (block cache / Foyer, flush interval, L0 SST size).
- [ ] Fix the `get_range` KV caveat (chunk large values across keys, or footer-range fetch).

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
