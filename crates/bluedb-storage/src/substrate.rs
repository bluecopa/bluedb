//! [`Substrate`] — the SlateDB handle a node is currently bound to.
//!
//! In the HA model a node is one of two things:
//! - the **active writer** — holds a writer [`Db`] (reads *and* writes, owns the
//!   `writer_epoch`);
//! - a **read replica** — holds a read-only [`DbReader`] that follows the
//!   writer's manifest and serves reads, taking no write epoch.
//!
//! Both expose the same read surface (`get`, ranged `scan`), so the storage
//! layers above (`SlateDbBlobStore` here, `SlateDbStorage` in `bluedb-sql`)
//! read through a `Substrate` regardless of role. Write/lifecycle operations
//! require the writer and error on a replica via [`Substrate::require_writer`].

use std::ops::Bound;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use slatedb::config::ScanOptions;
use slatedb::{Db, DbIterator, DbReader};

/// The read/write handle a node is bound to: a writer [`Db`] or a read-only
/// [`DbReader`].
#[derive(Clone)]
pub enum Substrate {
    /// Active writer: full read+write access, owns the `writer_epoch`.
    Writer(Arc<Db>),
    /// Read replica: read-only, follows the writer's manifest.
    Reader(Arc<DbReader>),
}

impl Substrate {
    /// Bind to a writer database.
    pub fn writer(db: Arc<Db>) -> Self {
        Self::Writer(db)
    }

    /// Bind to a read-only replica.
    pub fn reader(reader: Arc<DbReader>) -> Self {
        Self::Reader(reader)
    }

    /// Is this the active writer?
    pub fn is_writer(&self) -> bool {
        matches!(self, Self::Writer(_))
    }

    /// The writer handle, or an error if this node is a read-only replica.
    /// Write and lifecycle paths funnel through here so a replica can never
    /// mutate the database.
    pub fn require_writer(&self) -> Result<&Arc<Db>> {
        match self {
            Self::Writer(db) => Ok(db),
            Self::Reader(_) => Err(anyhow!(
                "operation requires the active writer; this node is a read-only replica"
            )),
        }
    }

    /// Point read of `key`, from the writer or the replica.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        match self {
            Self::Writer(db) => db.get(key).await.map_err(|err| anyhow!("slatedb get: {err}")),
            Self::Reader(reader) => reader.get(key).await.map_err(|err| anyhow!("slatedb reader get: {err}")),
        }
    }

    /// Ordered scan of the half-open range `[start, end)` (or `[start, ..)` when
    /// `end` is `None`), from the writer or the replica.
    pub async fn scan_range(&self, start: &[u8], end: Option<&[u8]>) -> Result<DbIterator> {
        let lower = Bound::Included(start.to_vec());
        let upper = match end {
            Some(end) => Bound::Excluded(end.to_vec()),
            None => Bound::Unbounded,
        };
        let range = (lower, upper);
        match self {
            Self::Writer(db) => db
                .scan_with_options(range, &ScanOptions::default())
                .await
                .map_err(|err| anyhow!("slatedb scan: {err}")),
            Self::Reader(reader) => reader
                .scan_with_options(range, &ScanOptions::default())
                .await
                .map_err(|err| anyhow!("slatedb reader scan: {err}")),
        }
    }

    /// Close the underlying handle (writer or reader).
    pub async fn close(&self) -> Result<()> {
        match self {
            Self::Writer(db) => db.close().await.map_err(|err| anyhow!("slatedb close: {err}")),
            Self::Reader(reader) => reader.close().await.map_err(|err| anyhow!("slatedb reader close: {err}")),
        }
    }
}
