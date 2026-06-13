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

/// Indexing pipeline — build a tantivy index from documents, pack it into a
/// split (with or without a real hotcache).
pub mod indexer;

/// Lazy split open — range-fetch only the footer + hotcache, then serve reads
/// on demand against the split blob (no `get_all`).
pub mod open;

/// Split **manifest** — the serde-serializable catalog of an index's splits.
pub mod manifest;

/// Multi-split search — run a BM25 query across N splits and merge top-K.
pub mod search;

/// Logical deletes — the tombstone set over immutable splits.
pub mod tombstones;

/// Incremental indexing — append a new split to an existing index without
/// rebuilding, and express updates as tombstone-then-append.
pub mod writer;

/// Merge / compaction — fold N splits into one, physically dropping tombstoned
/// docs, to bound split count and query fan-out.
pub mod merge;

#[path = "../vendor/mod.rs"]
pub mod vendor;

use tantivy::schema::{Field, Value};
use tantivy::TantivyDocument;

/// Render an optional stored string/u64 value into the canonical doc-id string.
///
/// Doc-ids are always strings; a u64 primary key is canonicalized to its
/// decimal rendering so the same value tombstones consistently whether the id
/// field is a `STRING` or a `U64` (`FAST`/`INDEXED`) field. Prefers a string
/// value if present, else a u64, else `None`.
pub fn doc_id_string(as_str: Option<&str>, as_u64: Option<u64>) -> Option<String> {
    if let Some(s) = as_str {
        return Some(s.to_string());
    }
    as_u64.map(|n| n.to_string())
}

/// Canonical doc-id string for a u64 primary key. Use this to build the id you
/// pass to [`tombstones::Tombstones::delete_doc_at`] when your id field is a u64.
pub fn id_of_u64(id: u64) -> String {
    id.to_string()
}

/// The designated **doc-id field**: a STORED field whose value uniquely
/// identifies a document across the logical index.
///
/// Indexing/searching the lifecycle path needs to name a document independently
/// of its physical `DocAddress` (which is per-split and unstable across
/// rebuild/merge). The caller designates one stored field as the id and the
/// engine reads its value at query/merge time. The field MUST be `STORED` and
/// present (single-valued) on every document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdField(pub Field);

impl IdField {
    /// Wrap a tantivy [`Field`] as the doc-id field.
    pub fn new(field: Field) -> Self {
        Self(field)
    }

    /// The underlying tantivy field.
    pub fn field(&self) -> Field {
        self.0
    }

    /// Extract this document's id as the canonical string, or `None` if the
    /// field has no stored string/u64 value on `doc`.
    pub fn extract(&self, doc: &TantivyDocument) -> Option<String> {
        let v = doc.get_first(self.0)?;
        doc_id_string(v.as_str(), v.as_u64())
    }
}

impl From<Field> for IdField {
    fn from(f: Field) -> Self {
        Self(f)
    }
}
