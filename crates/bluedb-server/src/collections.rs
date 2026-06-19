//! Document-oriented collections API.
//!
//! A collection is a table `coll(_id TEXT PRIMARY KEY, doc JSON)` managed
//! transparently: the table is created on first insert (`CREATE TABLE IF NOT
//! EXISTS`), and each document is assigned a 24-character hex `_id` if it
//! doesn't already carry one.
//!
//! ## Endpoints
//! - `POST /collections/{coll}/insert`              — bulk-insert documents
//! - `POST /collections/{coll}/find`                — query documents (MQL filter / sort / limit)
//! - `POST /collections/{coll}/createIndex`         — add a gateway-maintained JSON-path index

use std::collections::HashSet;

use axum::extract::{Path, State};
use axum::Json;
use gluesql_core::prelude::Glue;
use gluesql_core::store::Store;
use serde::Deserialize;
use serde_json::{json, Value};

use bluedb_engine::rest_sql;
use bluedb_collections::{
    filter::parse_filter,
    index::{derive_value, derived_col, valid_path},
    project::apply_projection,
};

use crate::{authz::Scope, run_read_routed, schema::ident, AppError, AppState};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Ensure the backing table `coll(_id TEXT PRIMARY KEY, doc JSON)` exists for
/// `tenant`. Uses `CREATE TABLE IF NOT EXISTS` — GlueSQL supports it (the
/// bluedb-sql projection layer uses the same idiom), so this is safe to call
/// on every insert without any prior existence check.
async fn ensure_collection(state: &AppState, tenant: &str, coll: &str) -> Result<(), AppError> {
    state.require_active()?;
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {coll} (_id TEXT PRIMARY KEY, doc JSON);"
    );
    let mut glue = Glue::new(state.connection_serialized(tenant).await?);
    rest_sql::execute_sql(&mut glue, &sql, &[], true).await?;
    Ok(())
}

/// Run a single parameterized write statement (`INSERT`, `UPDATE`) through the
/// FTS commit-observer path so the live index is maintained. Mirrors the write
/// branch of `exec_sql`.
async fn run_write(
    state: &AppState,
    tenant: &str,
    sql: &str,
    params: &[bluedb_rest::Param],
) -> Result<(), AppError> {
    let mut glue = Glue::new(state.connection_serialized(tenant).await?);
    state.fts().await.execute_fts(&mut glue, sql, params).await?;
    Ok(())
}

/// Run a DDL statement (ALTER TABLE, CREATE INDEX) with `allow_arbitrary = true`.
/// DDL does not go through the FTS rewriter (which would reject it via the DML
/// guard). Uses a serialized connection for write ordering.
async fn run_ddl(
    state: &AppState,
    tenant: &str,
    sql: &str,
) -> Result<(), AppError> {
    state.require_active()?;
    let mut glue = Glue::new(state.connection_serialized(tenant).await?);
    rest_sql::execute_sql(&mut glue, sql, &[], true).await?;
    Ok(())
}

/// Return the set of JSON paths that have a gateway-maintained derived column
/// (`__cidx_<path>`) for `coll` in `tenant`. Used by `insert` and `find` to
/// keep derived columns in sync and to route queries through the index.
///
/// Introspects the table's `column_defs` from the SlateDB schema, filters for
/// names starting with `__cidx_`, then strips the prefix to reconstruct the
/// path (dots were turned into underscores — this is lossy, so callers use the
/// returned paths to build a `HashSet<String>` and check membership via
/// `derived_col(path)` rather than reversing). For lookup correctness we return
/// the paths exactly as stored in the column names (underscores only) — callers
/// call `derived_col(path_from_filter)` and check if that column name is present.
///
/// Returns an empty set if the table doesn't exist yet.
async fn indexed_col_names(
    state: &AppState,
    tenant: &str,
    coll: &str,
) -> Result<HashSet<String>, AppError> {
    let storage = state.connection(tenant).await?;
    let schema = match Store::fetch_schema(&storage, coll).await {
        Ok(Some(s)) => s,
        Ok(None) => return Ok(HashSet::new()),
        Err(e) => return Err(AppError::internal(format!("fetch schema: {e}"))),
    };
    let cols: HashSet<String> = schema
        .column_defs
        .unwrap_or_default()
        .into_iter()
        .filter_map(|c| {
            c.name
                .strip_prefix("__cidx_")
                .map(|_| c.name.clone())
        })
        .collect();
    Ok(cols)
}

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// Body for `POST /collections/{coll}/createIndex`.
///
/// ```json
/// {"keys": {"status": 1}, "options": {"unique": false}}
/// ```
#[derive(Deserialize)]
pub(crate) struct CreateIndexRequest {
    pub keys: serde_json::Map<String, Value>,
    #[serde(default)]
    pub options: CreateIndexOptions,
}

#[derive(Deserialize, Default)]
pub(crate) struct CreateIndexOptions {
    #[serde(default)]
    pub unique: bool,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `POST /collections/{coll}/insert` — insert documents into a collection.
///
/// Body: `{"documents": [{...}, ...]}`. Each document receives a generated
/// 24-char hex `_id` if it doesn't already carry one. The backing table is
/// created implicitly on first insert. Returns
/// `{"insertedIds": [...], "insertedCount": N}`.
///
/// If the collection already has gateway-maintained JSON-path index columns
/// (`__cidx_<path>`), their values are extracted from each document and
/// included in the INSERT so the secondary index stays current.
pub(crate) async fn insert(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(coll): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;

    let coll = ident(&coll)?.to_string();

    let docs_val = body
        .get("documents")
        .ok_or_else(|| AppError::bad_request("missing 'documents' field"))?;
    let docs_arr = docs_val
        .as_array()
        .ok_or_else(|| AppError::bad_request("'documents' must be an array"))?;
    if docs_arr.is_empty() {
        return Ok(Json(json!({ "insertedIds": [], "insertedCount": 0 })));
    }

    // Ensure the backing table exists (idempotent).
    ensure_collection(&state, &tenant, &coll).await?;

    // Fetch the set of derived-column names (may be empty on a fresh collection).
    let dcols = indexed_col_names(&state, &tenant, &coll).await?;

    let mut inserted_ids: Vec<String> = Vec::with_capacity(docs_arr.len());

    for raw_doc in docs_arr {
        let mut doc = raw_doc.clone();
        // ensure_id requires a mutable JSON object.
        if !doc.is_object() {
            return Err(AppError::bad_request("each document must be a JSON object"));
        }
        let id = bluedb_collections::model::ensure_id(&mut doc);

        // Serialize the whole document (including _id) as the `doc` column value.
        let doc_text = doc.to_string();

        // Build INSERT with optional derived-column values for each index.
        // Column order: _id, doc, [__cidx_<path>, ...]
        let mut col_names = vec!["_id".to_string(), "doc".to_string()];
        let mut params: Vec<bluedb_rest::Param> = vec![
            bluedb_rest::Param::Str(id.clone()),
            bluedb_rest::Param::Str(doc_text),
        ];

        for dcol in &dcols {
            // Strip "__cidx_" to get the path (dots became underscores — we use the
            // column name as the path key since we stored it that way).
            let path = dcol.strip_prefix("__cidx_").unwrap_or(dcol);
            col_names.push(dcol.clone());
            match derive_value(&doc, path) {
                Some(v) => params.push(bluedb_rest::Param::Str(v)),
                None => params.push(bluedb_rest::Param::Null),
            }
        }

        let placeholders: Vec<String> = (1..=params.len()).map(|i| format!("${i}")).collect();
        let sql = format!(
            "INSERT INTO {coll} ({cols}) VALUES ({vals});",
            cols = col_names.join(", "),
            vals = placeholders.join(", "),
        );
        run_write(&state, &tenant, &sql, &params).await?;
        inserted_ids.push(id);
    }

    Ok(Json(json!({
        "insertedIds": inserted_ids,
        "insertedCount": inserted_ids.len(),
    })))
}

/// `POST /collections/{coll}/find` — query documents with an MQL filter.
///
/// Body: `{"filter": {...}, "projection": {...}, "sort": {field: 1|-1}, "limit": N, "skip": N}`.
/// All fields are optional. For equality filters on indexed fields the derived
/// column (`__cidx_<path>`) is used — GlueSQL serves this via its secondary
/// index (read-your-writes fast path, no DataFusion round-trip needed). For
/// non-indexed fields the filter uses `doc->>'field'` which may be guardrail-
/// rejected and routed to DataFusion.
///
/// Returns `{"documents": [...]}`.
pub(crate) async fn find(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(coll): Path<String>,
    Json(req): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    // Reads are allowed anywhere — no `require_active()`.

    let coll = ident(&coll)?.to_string();

    // Parse the MQL filter.
    let empty = json!({});
    let filter_val = req.get("filter").unwrap_or(&empty);
    let filter = parse_filter(filter_val)
        .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;

    // Build the set of indexed paths: for each filter path, check whether
    // `derived_col(path)` exists as a column in the table. This avoids the
    // lossy dot→underscore round-trip (we let `derived_col` do the mapping and
    // check membership against the live column set).
    let dcol_names = indexed_col_names(&state, &tenant, &coll).await.unwrap_or_default();
    let indexed: HashSet<String> = collect_filter_paths(&filter)
        .into_iter()
        .filter(|p| dcol_names.contains(&derived_col(p)))
        .collect();

    // Build the WHERE clause and collect bound parameters.
    let mut params: Vec<Value> = Vec::new();
    let where_sql = filter.to_sql_indexed(&mut params, &indexed);

    // Build SELECT with optional ORDER BY / LIMIT / OFFSET.
    let mut sql = format!("SELECT doc FROM {coll} WHERE {where_sql}");

    // ORDER BY — `{"field": 1}` → ASC, `{"field": -1}` → DESC.
    if let Some(sort_obj) = req.get("sort").and_then(Value::as_object) {
        if !sort_obj.is_empty() {
            let mut order_parts: Vec<String> = Vec::new();
            for (field, dir_val) in sort_obj {
                let dir = if dir_val.as_i64().unwrap_or(1) < 0 { "DESC" } else { "ASC" };
                let col_expr = if field == "_id" {
                    "_id".to_string()
                } else {
                    format!("(doc->>'{}') ", field.replace('\'', "''"))
                };
                order_parts.push(format!("{col_expr} {dir}"));
            }
            sql.push_str(" ORDER BY ");
            sql.push_str(&order_parts.join(", "));
        }
    }

    // LIMIT / OFFSET — server-controlled integers, safe to inline.
    if let Some(limit) = req.get("limit").and_then(Value::as_u64) {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(skip) = req.get("skip").and_then(Value::as_u64) {
        sql.push_str(&format!(" OFFSET {skip}"));
    }

    let rows = run_read_routed(&state, &tenant, &sql, &params).await?;

    // Extract and project the `doc` column from each row.
    let projection = req.get("projection").cloned().unwrap_or(empty);
    let mut docs: Vec<Value> = Vec::with_capacity(rows.len());
    for row in rows {
        // The `doc` column is stored as JSON text; `run_read_routed` re-inflates
        // it (via select_to_json / reinflate_rows), so `doc` should already be a
        // JSON object. Handle the string fallback for safety.
        let doc = match row.get("doc") {
            Some(Value::String(s)) => serde_json::from_str::<Value>(s)
                .unwrap_or_else(|_| Value::String(s.clone())),
            Some(v) => v.clone(),
            None => continue,
        };
        docs.push(apply_projection(&doc, &projection));
    }

    Ok(Json(json!({ "documents": docs })))
}

/// `POST /collections/{coll}/createIndex` — create a gateway-maintained JSON-path index.
///
/// Body: `{"keys": {"<path>": 1}, "options": {"unique": false}}`.
/// Only the first key in `keys` is used (compound indexes are not supported).
///
/// Steps:
/// 1. `ALTER TABLE {coll} ADD COLUMN __cidx_<path> TEXT` (skipped if already present).
/// 2. Backfill from existing rows (`UPDATE ... SET __cidx_<path> = $1 WHERE _id = $2`).
/// 3. `CREATE [UNIQUE] INDEX cidx_{coll}_{path_with_underscores} ON {coll} (__cidx_<path>)`.
///
/// Returns `{"name": "<index_name>"}`.
pub(crate) async fn create_index(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(coll): Path<String>,
    Json(req): Json<CreateIndexRequest>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;

    let coll = ident(&coll)?.to_string();

    if req.keys.is_empty() {
        return Err(AppError::bad_request("'keys' must not be empty"));
    }

    // Take the first key as the path to index.
    let (path, _) = req.keys.iter().next().unwrap();
    let path = path.clone();

    if !valid_path(&path) {
        return Err(AppError::bad_request(format!("invalid index field path: {path:?}")).with_code("PARSE_ERROR"));
    }

    let dcol = derived_col(&path);
    let unique_kw = if req.options.unique { "UNIQUE " } else { "" };
    let index_name = format!(
        "cidx_{coll}_{}",
        path.replace('.', "_")
    );

    // Ensure the collection table exists first (createIndex before any insert).
    ensure_collection(&state, &tenant, &coll).await?;

    // Step 1: Add the derived column if it doesn't already exist.
    let existing_cols = indexed_col_names(&state, &tenant, &coll).await?;
    if !existing_cols.contains(&dcol) {
        let alter_sql = format!("ALTER TABLE {coll} ADD COLUMN {dcol} TEXT;");
        run_ddl(&state, &tenant, &alter_sql).await?;
    }

    // Step 2: Backfill existing rows.
    // Read all existing _id + doc pairs (unindexed scan — only at createIndex time).
    let select_sql = format!("SELECT _id, doc FROM {coll} WHERE TRUE;");
    let rows = run_read_routed(&state, &tenant, &select_sql, &[]).await?;
    for row in rows {
        let id = match row.get("_id") {
            Some(Value::String(s)) => s.clone(),
            _ => continue,
        };
        let doc: Value = match row.get("doc") {
            Some(Value::String(s)) => serde_json::from_str(s).unwrap_or(Value::Null),
            Some(v) => v.clone(),
            None => continue,
        };
        if let Some(val) = derive_value(&doc, &path) {
            let update_sql = format!("UPDATE {coll} SET {dcol} = $1 WHERE _id = $2;");
            let params = vec![
                bluedb_rest::Param::Str(val),
                bluedb_rest::Param::Str(id),
            ];
            run_write(&state, &tenant, &update_sql, &params).await?;
        }
    }

    // Step 3: Create the secondary index on the derived column — skip if an
    // index with this exact name already exists (idempotent createIndex).
    let storage = state.connection(&tenant).await?;
    let schema = Store::fetch_schema(&storage, &coll)
        .await
        .map_err(|e| AppError::internal(format!("fetch schema: {e}")))?;
    let index_exists = schema.map_or(false, |s| s.indexes.iter().any(|i| i.name == index_name));
    if !index_exists {
        let create_idx_sql = format!(
            "CREATE {unique_kw}INDEX {index_name} ON {coll} ({dcol});"
        );
        run_ddl(&state, &tenant, &create_idx_sql).await?;
    }

    Ok(Json(json!({ "name": index_name })))
}

/// Collect all leaf paths referenced by a `Filter` (for `Cmp` nodes only).
/// Used to build the indexed-path set for `find`.
fn collect_filter_paths(filter: &bluedb_collections::filter::Filter) -> Vec<String> {
    use bluedb_collections::filter::Filter;
    match filter {
        Filter::True => vec![],
        Filter::Cmp { path, .. } => vec![path.clone()],
        Filter::And(v) | Filter::Or(v) => v.iter().flat_map(collect_filter_paths).collect(),
        Filter::Not(f) => collect_filter_paths(f),
    }
}
