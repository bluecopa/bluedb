//! `bluedb-fts` — BM25 full-text search over object storage.
//!
//! Engine = [`tantivy`] (the BM25 implementation). The read path — opening a
//! tantivy index that lives in object storage and searching it with byte-range
//! caching — is a **minimal vendored copy of Quickwit's `quickwit-directories`**
//! (Apache-2.0), hosted on [`bluedb_storage::BlobStore`] instead of Quickwit's
//! own `quickwit-storage`/`quickwit-common`/AWS stack.
//!
//! ## Plan
//! - `vendor/` ← copy the ~7 `quickwit-directories` files + a thin slice of
//!   `quickwit-storage` types (`Storage` trait read subset, `error`,
//!   `ByteRangeCache`, `VersionedComponent`, `BundleStorageFileOffsets`) and the
//!   `Uri` / `SPLIT_FIELDS_FILE_NAME` bits from `quickwit-common`.
//! - Stub `CacheMetrics` (~30 lines of `AtomicU64`) to drop `quickwit-config` +
//!   `quickwit-metrics`.
//! - Implement the (trimmed, read-only) `Storage` trait over `BlobStore`.
//!
//! Still to build beyond the read path: index/write (build tantivy index →
//! bundle into a split → put to object storage), a split **manifest**
//! (our own, in Postgres or SlateDB), and a merge/compaction policy. Search is
//! stateless/horizontal; only the indexer is a coordinated writer.

// Vendored Quickwit read path (Apache-2.0). See `vendor/NOTICE`. Each file
// keeps its upstream license header; adaptations are annotated `// Adapted for
// bluedb: ...`. The trimmed `Storage` trait carries a blanket
// `impl<B: bluedb_storage::BlobStore> vendor::Storage for B`, so any
// `BlobStore` (SlateDB, object_store, ...) can back a `StorageDirectory`.
/// Split *writer* — the indexer's packing step. Produces splits the vendored
/// [`vendor::BundleDirectory`] can open.
pub mod split;

#[path = "../vendor/mod.rs"]
pub mod vendor;
