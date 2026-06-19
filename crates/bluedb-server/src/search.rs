//! Elasticsearch-shaped search over `/collections` documents. Thin handlers +
//! per-(tenant,collection) tantivy index wiring; all ES-DSL translation lives in
//! the pure `bluedb-search` crate.

use std::collections::HashMap;
use std::sync::Arc;

use bluedb_engine::FtsIndex;
use bluedb_fts::policy::CompactionPolicy;
use bluedb_search::mapping::SearchSchema;
use bluedb_storage::{SlateDbBlobStore, Substrate};
use tokio::sync::RwLock;

use crate::{AppError, AppState};

/// Tenant-isolated index id. One shared SlateDB Db, tenant-prefixed blob keys.
pub(crate) fn index_id(tenant: &str, coll: &str) -> String {
    format!("search/{tenant}/{coll}")
}

/// Build a (read-or-write) `FtsIndex` for a compiled mapping over a blob store.
pub(crate) fn build_fts_index(
    blob: Arc<SlateDbBlobStore>,
    tenant: &str,
    coll: &str,
    ss: &SearchSchema,
) -> FtsIndex {
    FtsIndex::new(
        index_id(tenant, coll),
        blob,
        ss.schema.clone(),
        ss.id_field,
        CompactionPolicy::default(),
    )
}

/// A read-only blob store over the currently-bound substrate (writer OR reader).
/// Used by the search read path so it works on any node with a bound DB.
pub(crate) async fn search_blob(state: &AppState) -> Result<Arc<SlateDbBlobStore>, AppError> {
    let guard = state.db_read().await;
    let db = guard
        .as_ref()
        .ok_or_else(|| AppError::service_unavailable("no database bound on this node"))?;
    Ok(Arc::new(SlateDbBlobStore::from_substrate(db.substrate())))
}

/// Owns the writer-side, shared, write-serialized `FtsIndex` handles. Swapped on
/// promote/demote like `FtsEngine`. `blob == None` means "not the active writer".
pub(crate) struct SearchEngine {
    blob: Option<Arc<SlateDbBlobStore>>,
    indexes: RwLock<HashMap<(String, String), Arc<FtsIndex>>>,
}

impl SearchEngine {
    pub(crate) fn empty() -> Arc<Self> {
        Arc::new(Self { blob: None, indexes: RwLock::new(HashMap::new()) })
    }

    pub(crate) fn new_durable(substrate: Substrate) -> Arc<Self> {
        Arc::new(Self {
            blob: Some(Arc::new(SlateDbBlobStore::from_substrate(substrate))),
            indexes: RwLock::new(HashMap::new()),
        })
    }

    pub(crate) fn writer_blob(&self) -> Option<Arc<SlateDbBlobStore>> {
        self.blob.clone()
    }

    /// Get-or-create the shared, write-serialized index handle for (tenant, coll).
    pub(crate) async fn index_for(
        &self,
        tenant: &str,
        coll: &str,
        ss: &SearchSchema,
    ) -> Result<Arc<FtsIndex>, AppError> {
        let blob = self
            .blob
            .clone()
            .ok_or_else(|| AppError::service_unavailable("search writes require the active writer"))?;
        let key = (tenant.to_string(), coll.to_string());
        {
            let r = self.indexes.read().await;
            if let Some(idx) = r.get(&key) {
                return Ok(idx.clone());
            }
        }
        let mut w = self.indexes.write().await;
        let idx = w
            .entry(key)
            .or_insert_with(|| Arc::new(build_fts_index(blob, tenant, coll, ss)))
            .clone();
        Ok(idx)
    }

    /// Forget a cached handle (e.g. after the mapping is replaced).
    pub(crate) async fn forget(&self, tenant: &str, coll: &str) {
        self.indexes
            .write()
            .await
            .remove(&(tenant.to_string(), coll.to_string()));
    }

    /// Snapshot of all cached (tenant, coll) keys (for the compaction sweep).
    pub(crate) async fn cached_keys(&self) -> Vec<(String, String)> {
        self.indexes.read().await.keys().cloned().collect()
    }
}

/// Build an HTTP error from a `bluedb_search::SearchError` (ES-style: 400 for
/// client errors, 500 for internal).
pub(crate) fn search_err(e: bluedb_search::SearchError) -> AppError {
    use bluedb_search::SearchError::*;
    match e {
        Other(_) => AppError::internal(e.to_string()),
        _ => AppError::bad_request(e.to_string()),
    }
}
