//! `bluedb-storage` — the object-store **seam**.
//!
//! Everything above this crate (the vendored Quickwit `quickwit-directories`
//! read path, the FTS engine, the GlueSQL store) talks to object storage
//! through one narrow trait: "give me bytes `N..M` of this object". That is the
//! exact coupling point we found when tracing `quickwit-directories` — its
//! `Storage` dependency reduces to async byte-range reads + a byte-range cache.
//!
//! Implement [`BlobStore`] over SlateDB (or the `object_store` crate) and the
//! whole read path sits on top, statelessly, in any region.

use std::ops::Range;

use anyhow::Result;
use bytes::Bytes;

/// Read-only access to immutable objects in object storage.
///
/// Implementors: a SlateDB-backed store, or a thin `object_store` wrapper.
/// The vendored Quickwit `Storage` adapter will be expressed in terms of this.
#[async_trait::async_trait]
pub trait BlobStore: Send + Sync + 'static {
    /// Read the half-open byte range `range` of the object at `path`.
    async fn get_range(&self, path: &str, range: Range<usize>) -> Result<Bytes>;

    /// Read the entire object at `path`.
    async fn get_all(&self, path: &str) -> Result<Bytes>;

    /// Length of the object at `path`, in bytes.
    async fn len(&self, path: &str) -> Result<usize>;
}

/// Mutating access to a blob store — the **write seam**.
///
/// This is intentionally a *separate* trait from [`BlobStore`]: the read path
/// (`bluedb-fts`'s vendored Quickwit `Storage` adapter) only ever needs the
/// frozen read methods, so write capability is additive and opt-in. A backend
/// that can mutate (e.g. [`SlateDbBlobStore`]) implements both.
///
/// `scan_prefix` returns entries in ascending key order — SlateDB's `scan`
/// is an ordered range over the keyspace, so prefix scans come back sorted.
#[async_trait::async_trait]
pub trait BlobStoreMut: BlobStore {
    /// Write (or overwrite) the object at `path`.
    async fn put(&self, path: &str, bytes: Bytes) -> Result<()>;

    /// Delete the object at `path`. Deleting a missing key is a no-op.
    async fn delete(&self, path: &str) -> Result<()>;

    /// Return all `(path, bytes)` pairs whose path starts with `prefix`,
    /// in ascending path order.
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Bytes)>>;
}

mod slatedb_backend;
pub use slatedb_backend::SlateDbBlobStore;

mod lifecycle;

mod chunked;
pub use chunked::ChunkedBlobStore;
