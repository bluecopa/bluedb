# bluedb

Object-storage-native data substrate, in Rust. Durable state lives in object
storage (S3 / GCS / Azure Blob) via **SlateDB**; compute is **stateless and
horizontally scalable**. Spun out of `fx-runtime` design work as its own project
so it can be embedded (PyO3) into `fx-runtime` services and/or run as a service.

## Why this exists

The driver is enterprise **DDL-clearance pain** → wanting **one schema-flexible
storage substrate** that is object-storage-native and tri-cloud (S3/ABS/GCS),
instead of per-table DDL migrations on Postgres. bluedb is the first concrete
slice of that substrate.

## Architecture (target)

| Layer | Choice |
|-------|--------|
| **Storage foundation** | SlateDB (LSM on object storage). Single-writer safety via SlateDB's `writer_epoch` + object-store compare-and-set fencing. |
| **Ingest** | Stateless producers → object storage → manifest-backed queue → single consumer (WarpStream / OpenData-`buffer` style). Decouples write throughput from the single writer. |
| **SQL** | GlueSQL over a SlateDB-backed `Store` (schemaless tables ⇒ no DDL, no migration, no clearance). |
| **Full-text (BM25)** | tantivy + a **vendored copy of Quickwit's `quickwit-directories` read path** over object storage. Index = tantivy *splits* in object storage + our own manifest. **Search is stateless/horizontal; only the indexer is a coordinated writer.** |

## High availability

- **Intra-region:** K8s Lease leader election + SlateDB epoch fencing → RPO 0, automatic failover.
- **Multi-region:** active-passive, warm standby, RPO > 0. Gated promotion via a Temporal saga (runs in `fx-runtime`'s `fx_worker`). Arbiter = HA multi-AZ Postgres lease row (or human break-glass). Cross-bucket split-brain is prevented by the arbiter + "wait out the old lease", since CAS fencing stops at the bucket boundary.
- **For FTS specifically:** search needs no coordination or replication (each region reads its own object-storage replica); only the split-indexer follows the active-passive path.

## Crates

- `bluedb-storage` — the object-store **seam**: a minimal read-only `BlobStore` trait (async read-byte-range) + the additive `BlobStoreMut` write seam (put/delete/ordered scan-prefix), `SlateDbBlobStore` (slatedb 0.13) with lifecycle helpers, and `ChunkedBlobStore` (large values split across ordered keys). The vendored Quickwit `Storage` adapter sits on top of `BlobStore`.
- `bluedb-fts` — BM25 full-text search over object storage: `tantivy` + the vendored `quickwit-directories` read path, hosted on `bluedb-storage`. Indexer (docs→split), split manifest/catalog, multi-split merged search, lazy hotcache open (range-fetch, no whole-split load), and the full **index lifecycle**: incremental append, generation-scoped logical deletes (so a same-id update is a true in-place replace), and merge/compaction that physically drops dead docs.
- `bluedb-sql` — SQL over the substrate: GlueSQL `Store`/`StoreMut` on SlateDB. Order-preserving key encoding (schema/data/index namespaces, `Key::to_cmp_be_bytes()` keys), schemaless tables, CREATE/INSERT/SELECT/UPDATE/DELETE/ORDER BY, **real transactions** (overlay + `DbSnapshot` + atomic `WriteBatch`, snapshot isolation, true ROLLBACK), **secondary indexes** (`CREATE/DROP INDEX`, index-backed scans), **tenant-namespaced** keyspace, and a **schema-as-data registry**.
- `bluedb-rest` — PostgREST-style query DSL → SQL translation (filters/operators/order/limit/offset + INSERT/UPDATE/DELETE), with identifier allow-listing and literal escaping. The input-table-v2 API-parity surface; self-contained (no dependency on `bluedb-sql`).
- _(planned)_ `bluedb-buffer` (ingest), `bluedb-server` (the Rust service), `bluedb-py` (PyO3 bindings for `fx_api`).

## Provenance & licensing

bluedb's own code is **Apache-2.0**. `crates/bluedb-fts/vendor/` will hold a
minimal copy of **`quickwit-directories` + a thin `quickwit-storage` slice**,
copied from [`quickwit-oss/quickwit@main`](https://github.com/quickwit-oss/quickwit)
which is **Apache-2.0** (relicensed from AGPL after the Datadog acquisition).

- Per-file Apache-2.0 headers are preserved verbatim; see `crates/bluedb-fts/vendor/NOTICE`.
- **Do NOT** depend on the crates.io `quickwit-*` packages — they are stale
  `v0.3.0` (2022) and **AGPL**. Copy from `main` only.

## Status

**The M2 SQL pillar is complete and M1 FTS is feature-complete** (bar GC + query
niceties). `cargo test --workspace` is green — **~117 tests** — and `cargo clippy
--workspace --all-targets` is clean.

- **FTS (`bluedb-fts`):** vendored read path on the tantivy fork (`6270552`),
  `impl<B: BlobStore> Storage for B` bridge; a real **indexer**; a real **hotcache** +
  **lazy open** that range-fetches footer+hotcache and never loads the whole split
  (proven by a counting blob store: `get_all` == 0); a split **manifest/catalog**;
  **multi-split** merged BM25 search; and the full **index lifecycle** —
  incremental append, **generation-scoped tombstones** (a same-id update is a true
  in-place replace, proven end-to-end), and **merge/compaction** that physically
  drops dead docs and reports superseded splits for GC.
- **SQL (`bluedb-sql`):** GlueSQL `Store`/`StoreMut` over SlateDB
  (`gluesql-core =0.19.0`); order-preserving key encoding. **Real transactions** —
  write-buffer overlay + point-in-time `DbSnapshot` + atomic `WriteBatch` commit
  (snapshot isolation, read-your-own-writes, true ROLLBACK incl. index entries).
  **Secondary indexes** (`CREATE/DROP INDEX`, order-preserving prefix-free value
  encoding so string range/ORDER-BY scans sort by content not length, maintained
  through the txn overlay). **Tenant-namespaced** keyspace. A **schema-as-data
  registry** with write-time validation.
- **REST (`bluedb-rest`):** PostgREST-style DSL → SQL translation — filters/operators
  (`eq,neq,gt,gte,lt,lte,like,ilike,in,is`, `not.` negation), `select`/`order`/
  `limit`/`offset`, INSERT/UPDATE/DELETE; identifier allow-listing + literal escaping.
- **Storage (`bluedb-storage`):** `SlateDbBlobStore` over `slatedb` 0.13. **Durability
  proven across `Db` reopen**; `BlobStoreMut` write seam; lifecycle helpers;
  `ChunkedBlobStore` for large values.

See [ROADMAP.md](ROADMAP.md) for what's checked off and what remains.

**Next (still open):** wire `bluedb-rest` output into `bluedb-sql` execution; a GC
executor for compaction's superseded-split list + a compaction trigger policy;
per-field FTS analyzers; query niceties (pagination/highlighting); then **M3** —
the service binary and `fx_api` integration via PyO3 (`pyo3-async-runtimes`,
Tokio↔asyncio).
