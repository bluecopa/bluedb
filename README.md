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

- `bluedb-storage` — the object-store **seam**: a minimal `BlobStore` trait (async read-byte-range) + SlateDB / `object_store` implementations. The vendored Quickwit `Storage` adapter sits on top of this.
- `bluedb-fts` — BM25 full-text search over object storage: `tantivy` + the vendored `quickwit-directories` read path, hosted on `bluedb-storage`.
- _(planned)_ `bluedb-buffer` (ingest), `bluedb-sql` (GlueSQL), `bluedb-server` (the Rust service), `bluedb-py` (PyO3 bindings for `fx_api`).

## Provenance & licensing

bluedb's own code is **Apache-2.0**. `crates/bluedb-fts/vendor/` will hold a
minimal copy of **`quickwit-directories` + a thin `quickwit-storage` slice**,
copied from [`quickwit-oss/quickwit@main`](https://github.com/quickwit-oss/quickwit)
which is **Apache-2.0** (relicensed from AGPL after the Datadog acquisition).

- Per-file Apache-2.0 headers are preserved verbatim; see `crates/bluedb-fts/vendor/NOTICE`.
- **Do NOT** depend on the crates.io `quickwit-*` packages — they are stale
  `v0.3.0` (2022) and **AGPL**. Copy from `main` only.

## Status

**Full BM25 search over the SlateDB substrate works end to end.** `cargo test -p
bluedb-fts` is green — **21 tests**: 18 vendored-module unit tests, the read-path
seam (`tests/read_path.rs`, 2), and the end-to-end query (`tests/query.rs`, 1):
build a tantivy index → `split::pack_split` → `put` into **SlateDB** → fetch via
`SlateDbBlobStore` → open with `BundleDirectory` → BM25 queries return correct
hits (`revenue`→1, `ledger`→1, `financial`→2, miss→0).

Done:
- vendored read path (7 directory files + trimmed `Storage`/`error`/`ByteRangeCache`/
  `VersionedComponent`/`BundleStorageFileOffsets` + `CacheMetrics` stub) on the
  tantivy fork (`6270552`); `impl<B: BlobStore> Storage for B` bridge.
- `SlateDbBlobStore` (`crates/bluedb-storage`) — the substrate backend, over
  `slatedb` 0.13 (which re-exports its own `object_store`, so no version skew).
  The standalone object_store backend was removed.
- `split::pack_split` (`crates/bluedb-fts`) — the indexer's split *writer*
  (`[files][meta][meta-len][hotcache][hotcache-len]`, empty hotcache).

**Next:** (1) Lazy reads for *large* splits. SlateDB is a KV store (whole-value
get/put), so `BlobStore::get_range` currently fetches the whole value and slices
it — fine for whole-split reads, wasteful at scale. Either keep the split as one
object and range-fetch footer+hotcache, or chunk the split across keys.
(2) the split **manifest** (which splits exist per tenant). (3) merge/compaction.
(4) wire into `fx_api` via PyO3 (`pyo3-async-runtimes`, Tokio↔asyncio).
