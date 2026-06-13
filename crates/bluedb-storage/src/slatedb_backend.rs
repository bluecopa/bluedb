//! A SlateDB-backed [`BlobStore`] — bluedb's real substrate backend.
//!
//! SlateDB is an LSM key-value store on object storage. It exposes whole-value
//! `get`/`put`, so [`BlobStore::get_range`] fetches the whole value and slices
//! it. (A range read of one key is therefore O(value size). For large objects
//! we'll later chunk values across keys; splits are read whole today anyway.)

use std::ops::Range;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use slatedb::Db;

use crate::{BlobStore, BlobStoreMut};

/// A [`BlobStore`] backed by a SlateDB [`Db`].
#[derive(Clone)]
pub struct SlateDbBlobStore {
    db: Arc<Db>,
}

impl SlateDbBlobStore {
    /// Wrap an already-open SlateDB database.
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    /// Borrow the underlying SlateDB handle (for lifecycle ops like flush/close).
    pub(crate) fn db(&self) -> &Arc<Db> {
        &self.db
    }

    async fn get_value(&self, path: &str) -> Result<Bytes> {
        let value = self
            .db
            .get(path.as_bytes())
            .await
            .map_err(|err| anyhow!("slatedb get failed for {path}: {err}"))?
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
        self.db
            .put(path.as_bytes(), bytes.as_ref())
            .await
            .map_err(|err| anyhow!("slatedb put failed for {path}: {err}"))?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.db
            .delete(path.as_bytes())
            .await
            .map_err(|err| anyhow!("slatedb delete failed for {path}: {err}"))?;
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Bytes)>> {
        let mut iter = self
            .db
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
