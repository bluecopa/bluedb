//! `Db` lifecycle helpers for [`SlateDbBlobStore`].
//!
//! `bluedb-storage` exposes one frozen read trait ([`crate::BlobStore`]). To
//! actually *stand up* a backing store you need to open a SlateDB [`Db`] over
//! some `object_store::ObjectStore`, and to shut it down cleanly you must flush
//! before drop. These thin constructors centralize that.
//!
//! ## Durability note (SlateDB 0.13)
//!
//! With the crate's default features (`["aws", "foyer"]`, *not* `wal_disable`),
//! SlateDB keeps the WAL enabled and auto-flushes on a 100ms `flush_interval`.
//! A write is durable to object storage only once it has been flushed. To make
//! durability deterministic across a process restart, call [`SlateDbBlobStore`]
//! [`shutdown`](SlateDbBlobStore::shutdown) (which `close()`s the `Db`, flushing
//! memtables to L0 so the next open does not even need WAL replay), or
//! [`flush`](SlateDbBlobStore::flush) if you want to keep the handle open.

use std::path::Path as FsPath;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;

use crate::SlateDbBlobStore;

impl SlateDbBlobStore {
    /// Open a [`SlateDbBlobStore`] at `db_path` over an arbitrary object store.
    ///
    /// `db_path` is SlateDB's *logical* root path inside `object_store` (e.g.
    /// `"bluedb"`), not a filesystem path.
    pub async fn open(
        db_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let path = db_path.into();
        let db = Db::open(path.clone(), object_store)
            .await
            .map_err(|err| anyhow!("slatedb open failed for {path}: {err}"))?;
        Ok(Self::new(Arc::new(db)))
    }

    /// Open a [`SlateDbBlobStore`] backed by a persistent local-filesystem
    /// object store rooted at `dir`. `dir` must already exist.
    ///
    /// This is the on-disk backend used by the durability round-trip test and
    /// by single-node dev: data written here survives a process restart once
    /// flushed (see the module docs).
    pub async fn open_local(dir: impl AsRef<FsPath>) -> Result<Self> {
        let dir = dir.as_ref();
        let object_store = LocalFileSystem::new_with_prefix(dir)
            .map_err(|err| anyhow!("local object store at {}: {err}", dir.display()))?;
        Self::open("bluedb", Arc::new(object_store)).await
    }

    /// Open a [`SlateDbBlobStore`] backed by an in-memory object store.
    ///
    /// Convenience for tests/dev. Nothing persists past process exit, and (since
    /// the object store itself is ephemeral) reopening a *fresh* `InMemory` will
    /// not see prior writes — use [`open_local`](Self::open_local) to prove
    /// durability across reopen.
    pub async fn open_in_memory() -> Result<Self> {
        Self::open("bluedb", Arc::new(InMemory::new())).await
    }

    /// Flush outstanding writes to object storage, keeping the handle open.
    ///
    /// After this returns `Ok`, every prior `put`/`delete` is durable: a fresh
    /// `Db` opened at the same path will observe them.
    pub async fn flush(&self) -> Result<()> {
        self.db()
            .flush()
            .await
            .map_err(|err| anyhow!("slatedb flush failed: {err}"))
    }

    /// Gracefully shut down: flush memtables to L0 and close the `Db`.
    ///
    /// Consumes the wrapper. If other clones of the underlying `Arc<Db>` are
    /// still alive this still issues the close on the shared handle; callers who
    /// need a hard guarantee should hold the sole reference.
    pub async fn shutdown(self) -> Result<()> {
        self.db()
            .close()
            .await
            .map_err(|err| anyhow!("slatedb close failed: {err}"))
    }
}
