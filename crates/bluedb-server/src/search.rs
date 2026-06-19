//! Elasticsearch-shaped search over `/collections` documents. Thin handlers +
//! per-(tenant,collection) tantivy index wiring; all ES-DSL translation lives in
//! the pure `bluedb-search` crate.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use bluedb_engine::FtsIndex;
use bluedb_fts::policy::CompactionPolicy;
use bluedb_rest::Param;
use bluedb_search::mapping::{FieldKindInfo, SearchSchema, ID_FIELD};
use bluedb_search::model::MappingSpec;
use bluedb_storage::{SlateDbBlobStore, Substrate};
use serde_json::Value;
use tokio::sync::RwLock;

use crate::{authz::Scope, AppError, AppState};

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

// ---------------------------------------------------------------------------
// Search-mapping registry
// ---------------------------------------------------------------------------

/// Per-tenant table that records each collection's mapping JSON.
const SEARCH_CONFIG_TABLE: &str = "__bluedb_search_config";

/// Global table (in the DEFAULT_TENANT keyspace) recording every tenant that
/// has at least one search mapping, so the background sweep can enumerate them.
const SEARCH_TENANTS_TABLE: &str = "__bluedb_search_tenants";

async fn ensure_search_config_registry(state: &AppState, tenant: &str) -> Result<(), AppError> {
    crate::collections::run_ddl(
        state,
        tenant,
        &format!("CREATE TABLE IF NOT EXISTS {SEARCH_CONFIG_TABLE} (collection TEXT PRIMARY KEY, mapping TEXT);"),
    )
    .await
}

async fn ensure_search_tenants_registry(state: &AppState) -> Result<(), AppError> {
    crate::collections::run_ddl(
        state,
        bluedb_sql::DEFAULT_TENANT,
        &format!("CREATE TABLE IF NOT EXISTS {SEARCH_TENANTS_TABLE} (tenant TEXT PRIMARY KEY);"),
    )
    .await
}

/// Persist (replace) the mapping JSON for a collection.
pub(crate) async fn upsert_mapping(
    state: &AppState,
    tenant: &str,
    coll: &str,
    mapping: &MappingSpec,
) -> Result<(), AppError> {
    ensure_search_config_registry(state, tenant).await?;
    let json = serde_json::to_string(mapping)
        .map_err(|e| AppError::internal(format!("serialize mapping: {e}")))?;
    crate::collections::run_write(
        state,
        tenant,
        &format!("DELETE FROM {SEARCH_CONFIG_TABLE} WHERE collection = $1;"),
        &[Param::Str(coll.to_string())],
    )
    .await?;
    crate::collections::run_write(
        state,
        tenant,
        &format!("INSERT INTO {SEARCH_CONFIG_TABLE} (collection, mapping) VALUES ($1, $2);"),
        &[Param::Str(coll.to_string()), Param::Str(json)],
    )
    .await?;
    register_search_tenant(state, tenant).await
}

async fn register_search_tenant(state: &AppState, tenant: &str) -> Result<(), AppError> {
    ensure_search_tenants_registry(state).await?;
    let _ = crate::collections::run_write(
        state,
        bluedb_sql::DEFAULT_TENANT,
        &format!("INSERT INTO {SEARCH_TENANTS_TABLE} (tenant) VALUES ($1);"),
        &[Param::Str(tenant.to_string())],
    )
    .await;
    Ok(())
}

/// Load a collection's mapping spec, if any.
pub(crate) async fn get_mapping(
    state: &AppState,
    tenant: &str,
    coll: &str,
) -> Result<Option<MappingSpec>, AppError> {
    let rows = match crate::collections::run_read_routed_for_mutation(
        state,
        tenant,
        &format!("SELECT mapping FROM {SEARCH_CONFIG_TABLE} WHERE collection = $1;"),
        &[Value::String(coll.to_string())],
    )
    .await
    {
        Ok(rows) => rows,
        Err(_) => return Ok(None),
    };
    let Some(row) = rows.into_iter().next() else { return Ok(None) };
    let json = row.get("mapping").and_then(Value::as_str).unwrap_or("");
    if json.is_empty() {
        return Ok(None);
    }
    let spec: MappingSpec = serde_json::from_str(json)
        .map_err(|e| AppError::internal(format!("parse stored mapping: {e}")))?;
    Ok(Some(spec))
}

/// All collections (per tenant) that have a search mapping (for the sweep).
pub(crate) async fn list_mapped_collections(
    state: &AppState,
    tenant: &str,
) -> Result<Vec<String>, AppError> {
    let rows = match crate::collections::run_read_routed_for_mutation(
        state,
        tenant,
        &format!("SELECT collection FROM {SEARCH_CONFIG_TABLE};"),
        &[],
    )
    .await
    {
        Ok(rows) => rows,
        Err(_) => return Ok(vec![]),
    };
    Ok(rows
        .into_iter()
        .filter_map(|r| r.get("collection").and_then(Value::as_str).map(str::to_string))
        .collect())
}

/// All tenants that have at least one search mapping.
pub(crate) async fn list_search_tenants(state: &AppState) -> Result<Vec<String>, AppError> {
    let rows = match crate::collections::run_read_routed_for_mutation(
        state,
        bluedb_sql::DEFAULT_TENANT,
        &format!("SELECT tenant FROM {SEARCH_TENANTS_TABLE};"),
        &[],
    )
    .await
    {
        Ok(rows) => rows,
        Err(_) => return Ok(vec![]),
    };
    Ok(rows
        .into_iter()
        .filter_map(|r| r.get("tenant").and_then(Value::as_str).map(str::to_string))
        .collect())
}

// ---------------------------------------------------------------------------
// Document → tantivy conversion
// ---------------------------------------------------------------------------

/// Build a tantivy document from a collection document JSON for a compiled
/// mapping. Returns `None` if the document has no `_id` field.
pub(crate) fn doc_to_tantivy(
    ss: &SearchSchema,
    spec: &MappingSpec,
    doc: &Value,
) -> Option<tantivy::TantivyDocument> {
    let id = doc.get(ID_FIELD).and_then(Value::as_str)?;
    let mut td = tantivy::TantivyDocument::default();
    let id_resolved = ss.field(ID_FIELD)?;
    td.add_text(id_resolved.field, id);
    for (name, _fspec) in &spec.fields {
        let Some(resolved) = ss.field(name) else { continue };
        let Some(v) = doc.get(name) else { continue };
        match resolved.kind {
            FieldKindInfo::Text(_) | FieldKindInfo::Keyword => {
                if let Some(s) = v.as_str() {
                    td.add_text(resolved.field, s);
                } else if !v.is_null() {
                    td.add_text(resolved.field, &v.to_string());
                }
            }
            FieldKindInfo::Integer => {
                if let Some(n) = v.as_i64() {
                    td.add_i64(resolved.field, n);
                }
            }
        }
    }
    Some(td)
}

// ---------------------------------------------------------------------------
// Write-path search-index maintenance
// ---------------------------------------------------------------------------

/// After an insert/update batch: (re)index the given documents if the collection
/// has a search mapping. `docs` carry their `_id`. No-op if no mapping / not writer.
pub(crate) async fn maintain_on_upsert(
    state: &AppState,
    tenant: &str,
    coll: &str,
    docs: &[Value],
) -> Result<(), AppError> {
    if docs.is_empty() {
        return Ok(());
    }
    let Some(spec) = get_mapping(state, tenant, coll).await? else { return Ok(()) };
    let ss = bluedb_search::mapping::compile(&spec).map_err(search_err)?;
    let engine = state.search().await;
    let idx = engine.index_for(tenant, coll, &ss).await?;

    let mut ids = Vec::with_capacity(docs.len());
    let mut tdocs = Vec::with_capacity(docs.len());
    for doc in docs {
        if let Some(td) = doc_to_tantivy(&ss, &spec, doc) {
            if let Some(id) = doc.get(ID_FIELD).and_then(Value::as_str) {
                ids.push(id.to_string());
            }
            tdocs.push(td);
        }
    }
    if tdocs.is_empty() {
        return Ok(());
    }
    idx.update(ids, tdocs)
        .await
        .map_err(|e| AppError::internal(format!("index upsert: {e}")))?;
    Ok(())
}

/// After a delete: drop the given ids from the search index if a mapping exists.
pub(crate) async fn maintain_on_delete(
    state: &AppState,
    tenant: &str,
    coll: &str,
    ids: &[String],
) -> Result<(), AppError> {
    if ids.is_empty() {
        return Ok(());
    }
    let Some(spec) = get_mapping(state, tenant, coll).await? else { return Ok(()) };
    let ss = bluedb_search::mapping::compile(&spec).map_err(search_err)?;
    let engine = state.search().await;
    let idx = engine.index_for(tenant, coll, &ss).await?;
    idx.delete(ids.iter().cloned())
        .await
        .map_err(|e| AppError::internal(format!("index delete: {e}")))?;
    Ok(())
}

/// Read a `doc` cell as a JSON object, accepting both a JSON string (the
/// pre-`run_read_routed` shape) and an already-parsed object. The `find`
/// handler notes that `run_read_routed` re-inflates the `doc` column, so in
/// practice it arrives as an object, but we guard both for safety.
fn read_doc_value(cell: Option<&Value>) -> Option<Value> {
    match cell {
        Some(Value::String(s)) => serde_json::from_str(s).ok(),
        Some(v @ Value::Object(_)) => Some(v.clone()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

/// `POST /collections/{coll}/searchIndex` — declare/replace a search mapping
/// and backfill existing documents into the tantivy index.
pub(crate) async fn create_search_index(
    State(state): State<crate::AppState>,
    headers: HeaderMap,
    Path(coll): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;

    let spec: MappingSpec = serde_json::from_value(body)
        .map_err(|e| AppError::bad_request(format!("bad mapping: {e}")))?;
    let ss = bluedb_search::mapping::compile(&spec).map_err(search_err)?;

    upsert_mapping(&state, &tenant, &coll, &spec).await?;
    let engine = state.search().await;
    engine.forget(&tenant, &coll).await;
    let idx = engine.index_for(&tenant, &coll, &ss).await?;

    // Backfill existing documents from the collection table.
    let rows = crate::collections::run_read_routed_for_mutation(
        &state,
        &tenant,
        &format!("SELECT doc FROM \"{coll}\";"),
        &[],
    )
    .await
    .unwrap_or_default();

    let mut tdocs = Vec::with_capacity(rows.len());
    for row in &rows {
        if let Some(doc) = read_doc_value(row.get("doc")) {
            if let Some(td) = doc_to_tantivy(&ss, &spec, &doc) {
                tdocs.push(td);
            }
        }
    }
    let backfilled = tdocs.len();
    if !tdocs.is_empty() {
        idx.append(tdocs)
            .await
            .map_err(|e| AppError::internal(format!("backfill index: {e}")))?;
    }

    Ok(Json(serde_json::json!({"acknowledged": true, "backfilled": backfilled})))
}

/// `GET /collections/{coll}/searchIndex` — describe the persisted mapping.
pub(crate) async fn get_search_index(
    State(state): State<crate::AppState>,
    headers: HeaderMap,
    Path(coll): Path<String>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    match get_mapping(&state, &tenant, &coll).await? {
        Some(spec) => {
            let mut m = serde_json::Map::new();
            m.insert(coll.clone(), serde_json::json!({"mappings": spec}));
            Ok(Json(Value::Object(m)))
        }
        None => Err(AppError::not_found(format!(
            "no search mapping for collection [{coll}]"
        ))),
    }
}
