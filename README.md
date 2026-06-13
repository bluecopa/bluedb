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
- `bluedb-fts` — BM25 full-text search over object storage: `tantivy` + the vendored `quickwit-directories` read path, hosted on `bluedb-storage`. Has the indexer (docs→split), a split manifest/catalog, multi-split merged search, and lazy hotcache-based open (range-fetch, no whole-split load).
- `bluedb-sql` — SQL over the substrate: GlueSQL `Store`/`StoreMut` on SlateDB. Order-preserving key encoding (schema/data namespaces, `Key::to_cmp_be_bytes()` PKs), schemaless tables, CREATE/INSERT/SELECT/UPDATE/DELETE/ORDER BY. Autocommit only (no real transactions/secondary indexes yet).
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

**M1 FTS core + the M2 SQL pillar both work end to end.** `cargo test --workspace`
is green — **45 tests** — and `cargo clippy --workspace --all-targets` is clean.

- **FTS (`bluedb-fts`, 29 tests):** vendored read path (7 directory files + trimmed
  `Storage`/`error`/`ByteRangeCache`/`VersionedComponent`/`BundleStorageFileOffsets`
  + `CacheMetrics` stub) on the tantivy fork (`6270552`), `impl<B: BlobStore> Storage for B`
  bridge; a real **indexer** (docs → tantivy index → split); a real **hotcache** + **lazy
  open** that range-fetches footer+hotcache and never loads the whole split
  (`open_split_lazy`, proven by a counting blob store: `get_all` calls == 0); a split
  **manifest/catalog**; and **multi-split** merged BM25 search.
- **SQL (`bluedb-sql`, 9 tests):** GlueSQL `Store`/`StoreMut` over SlateDB
  (`gluesql-core =0.19.0`). Order-preserving key encoding (schema vs data namespaces,
  length-prefixed table + `Key::to_cmp_be_bytes()` PK ⇒ `ORDER BY pk` falls out of the
  byte-ordered scan). CREATE/INSERT/SELECT-WHERE/UPDATE/DELETE and schemaless tables
  all work, driven through `Glue::execute`.
- **Storage (`bluedb-storage`, 7 tests):** `SlateDbBlobStore` over `slatedb` 0.13
  (re-exports its own `object_store`, so no version skew). **Durability proven across
  `Db` reopen** on a `LocalFileSystem` store; `BlobStoreMut` write seam (put/delete/
  ordered scan-prefix); lifecycle helpers (`open`/`open_local`/`open_in_memory`/
  `flush`/`shutdown`); `ChunkedBlobStore` for large values.

See [ROADMAP.md](ROADMAP.md) for what's checked off and what remains.

**Next (still open):** real transactions/isolation on SlateDB's single-writer model;
secondary indexes; merge/compaction + deletes for FTS; incremental indexing; wiring
the chunked layer under the FTS/SQL read paths; and M3 — wire into `fx_api` via PyO3
(`pyo3-async-runtimes`, Tokio↔asyncio).
