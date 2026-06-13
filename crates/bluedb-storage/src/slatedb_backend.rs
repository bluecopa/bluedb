//! A SlateDB-backed [`BlobStore`] — bluedb's real substrate backend.
//!
//! SlateDB is an LSM key-value store on object storage. It exposes whole-value
//! `get`/`put`, so [`BlobStore::get_range`] fetches the whole value and slices
//! it. (A range read of one key is therefore O(value size). For large objects
//! we'll later chunk values across keys; splits are read whole today anyway.)
//!
//! The store reads through a [`Substrate`] — a writer [`Db`] on the active node
//! or a read-only [`DbReader`] on a replica — so the read path is identical in
//! either role. Mutations ([`BlobStoreMut`]) require the writer and error on a
//! replica.

use std::ops::Range;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use slatedb::{Db, DbReader};

use crate::substrate::Substrate;
use crate::{BlobStore, BlobStoreMut};

/// A [`BlobStore`] backed by a SlateDB [`Substrate`] (writer or read replica).
#[derive(Clone)]
pub struct SlateDbBlobStore {
    substrate: Substrate,
}

impl SlateDbBlobStore {
    /// Wrap an already-open writer SlateDB database.
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            substrate: Substrate::writer(db),
        }
    }

    /// Wrap a read-only [`DbReader`] replica.
    pub fn from_reader(reader: Arc<DbReader>) -> Self {
        Self {
            substrate: Substrate::reader(reader),
        }
    }

    /// Wrap an arbitrary [`Substrate`].
    pub fn from_substrate(substrate: Substrate) -> Self {
        Self { substrate }
    }

    /// The substrate this store reads/writes through.
    pub(crate) fn substrate(&self) -> &Substrate {
        &self.substrate
    }

    /// Is this store backed by the active writer?
    pub fn is_writer(&self) -> bool {
        self.substrate.is_writer()
    }

    async fn get_value(&self, path: &str) -> Result<Bytes> {
        let value = self
            .substrate
            .get(path.as_bytes())
            .await?
            .ok_or_else(|| anyhow!("object not found: {path}"))?;
        Ok(Bytes::copy_from_slice(value.as_ref()))
    }
}

#[async_trait::async_trait]
impl BlobStore for SlateDbBlobStore {
    async fn get_range(&self, path: &str, range: Range<usize>) -> Result<Bytes> {
        // SlateDB stores whole values; slice after fetching.
        Ok(self.get_value(path).await?.slice(range))
    }

    async fn get_all(&self, path: &str) -> Result<Bytes> {
        self.get_value(path).await
    }

    async fn len(&self, path: &str) -> Result<usize> {
        Ok(self.get_value(path).await?.len())
    }
}

#[async_trait::async_trait]
impl BlobStoreMut for SlateDbBlobStore {
    async fn put(&self, path: &str, bytes: Bytes) -> Result<()> {
        self.substrate
            .require_writer()?
            .put(path.as_bytes(), bytes.as_ref())
            .await
            .map_err(|err| anyhow!("slatedb put failed for {path}: {err}"))?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.substrate
            .require_writer()?
            .delete(path.as_bytes())
            .await
            .map_err(|err| anyhow!("slatedb delete failed for {path}: {err}"))?;
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Bytes)>> {
        let mut iter = self
            .substrate
            .require_writer()?
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|err| anyhow!("slatedb scan_prefix failed for {prefix}: {err}"))?;

        let mut out = Vec::new();
        while let Some(kv) = iter
            .next()
            .await
            .map_err(|err| anyhow!("slatedb scan iteration failed for {prefix}: {err}"))?
        {
            let key = String::from_utf8(kv.key.to_vec())
                .map_err(|err| anyhow!("non-utf8 key under prefix {prefix}: {err}"))?;
            out.push((key, kv.value));
        }
        Ok(out)
    }
}
