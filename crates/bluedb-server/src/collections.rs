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
//! - `POST /collections/{coll}/update`              — read-modify-write (+ upsert)
//! - `POST /collections/{coll}/delete`              — delete by filter

use std::collections::HashSet;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use gluesql_core::prelude::Glue;
use gluesql_core::store::Store;
use serde::Deserialize;
use serde_json::{json, Value};

use bluedb_engine::rest_sql;
use bluedb_collections::{
    error::MqlError,
    filter::parse_filter,
    index::{derive_value, derived_col, valid_path},
    model::ensure_id,
    project::apply_projection,
    update::apply_update,
};

use crate::{authz::Scope, run_read_routed, schema::ident, AppError, AppState};

// ---------------------------------------------------------------------------
// MongoDB-shaped error for the /collections surface
// ---------------------------------------------------------------------------

/// MongoDB-shaped error: `{"ok":0,"code":<mongo_code>,"codeName":<name>,"errmsg":<msg>}`.
///
/// All `/collections` handlers return this instead of the generic `AppError`
/// so clients that speak the MongoDB wire protocol get a familiar error shape.
pub(crate) struct CollError(StatusCode, serde_json::Value);

impl IntoResponse for CollError {
    fn into_response(self) -> Response {
        (self.0, Json(self.1)).into_response()
    }
}

impl From<AppError> for CollError {
    fn from(e: AppError) -> Self {
        let (code, code_name): (i32, &'static str) = match e.error_code() {
            Some("UNIQUE_VIOLATION") => (11000, "DuplicateKey"),
            Some("NOT_FOUND")        => (26,    "NamespaceNotFound"),
            Some("PARSE_ERROR") | Some("TYPE_MISMATCH") | Some("NO_INDEX") => (2, "BadValue"),
            _                        => (8,     "UnknownError"),
        };
        let body = json!({
            "ok": 0,
            "code": code,
            "codeName": code_name,
            "errmsg": e.message(),
        });
        CollError(e.status(), body)
    }
}

impl From<MqlError> for CollError {
    fn from(e: MqlError) -> Self {
        let body = json!({
            "ok": 0,
            "code": e.mongo_code(),
            "codeName": e.mongo_code_name(),
            "errmsg": e.to_string(),
        });
        CollError(StatusCode::BAD_REQUEST, body)
    }
}

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

/// INSERT a complete document into `coll`, setting `_id`, `doc`, and every
/// `__cidx_*` column. Shared by `insert` (per doc) and `upsert` (single doc).
///
/// The caller must have already called `ensure_collection` and must pass `dcols`
/// (from `indexed_col_names`) so the derived columns are populated atomically
/// with the main row. `doc` must already contain `_id`.
async fn write_full_doc(
    state: &AppState,
    tenant: &str,
    coll: &str,
    doc: &Value,
    dcols: &HashSet<String>,
) -> Result<(), AppError> {
    let id = doc
        .get("_id")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::internal("write_full_doc: doc missing _id"))?
        .to_string();
    let doc_text = doc.to_string();

    let mut col_names = vec!["_id".to_string(), "doc".to_string()];
    let mut params: Vec<bluedb_rest::Param> = vec![
        bluedb_rest::Param::Str(id),
        bluedb_rest::Param::Str(doc_text),
    ];

    for dcol in dcols {
        let path = dcol.strip_prefix("__cidx_").unwrap_or(dcol);
        col_names.push(dcol.clone());
        match derive_value(doc, path) {
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
    run_write(state, tenant, &sql, &params).await
}

/// UPDATE a row's `doc` and all `__cidx_*` columns for the given `_id`.
/// Used by the `update` handler after applying an in-memory MQL update to
/// the document so the secondary indexes stay consistent.
async fn rewrite_doc_row(
    state: &AppState,
    tenant: &str,
    coll: &str,
    id: &str,
    doc: &Value,
    dcols: &HashSet<String>,
) -> Result<(), AppError> {
    let doc_text = doc.to_string();

    // SET doc = $1, __cidx_a = $2, ... WHERE _id = $N
    let mut set_clauses = vec!["doc = $1".to_string()];
    let mut params: Vec<bluedb_rest::Param> = vec![bluedb_rest::Param::Str(doc_text)];

    for dcol in dcols {
        let path = dcol.strip_prefix("__cidx_").unwrap_or(dcol);
        let idx = params.len() + 1;
        set_clauses.push(format!("{dcol} = ${idx}"));
        match derive_value(doc, path) {
            Some(v) => params.push(bluedb_rest::Param::Str(v)),
            None => params.push(bluedb_rest::Param::Null),
        }
    }

    let id_idx = params.len() + 1;
    params.push(bluedb_rest::Param::Str(id.to_string()));

    let sql = format!(
        "UPDATE {coll} SET {sets} WHERE _id = ${id_idx};",
        sets = set_clauses.join(", "),
    );
    run_write(state, tenant, &sql, &params).await
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

/// Body for `POST /collections/{coll}/update`.
///
/// ```json
/// {"filter": {"_id": "..."}, "update": {"$set": {"age": 37}}, "multi": false, "upsert": false}
/// ```
#[derive(Deserialize)]
pub(crate) struct UpdateRequest {
    pub filter: Value,
    pub update: Value,
    #[serde(default)]
    pub multi: bool,
    #[serde(default)]
    pub upsert: bool,
}

/// Body for `POST /collections/{coll}/delete`.
///
/// ```json
/// {"filter": {"status": "inactive"}, "multi": false}
/// ```
#[derive(Deserialize)]
pub(crate) struct DeleteRequest {
    pub filter: Value,
    #[serde(default)]
    pub multi: bool,
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
) -> Result<Json<Value>, CollError> {
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
            return Err(AppError::bad_request("each document must be a JSON object").into());
        }
        let id = ensure_id(&mut doc);
        write_full_doc(&state, &tenant, &coll, &doc, &dcols).await?;
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
) -> Result<Json<Value>, CollError> {
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
) -> Result<Json<Value>, CollError> {
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;

    let coll = ident(&coll)?.to_string();

    if req.keys.is_empty() {
        return Err(AppError::bad_request("'keys' must not be empty").into());
    }

    // Take the first key as the path to index.
    let (path, _) = req.keys.iter().next().unwrap();
    let path = path.clone();

    if !valid_path(&path) {
        return Err(AppError::bad_request(format!("invalid index field path: {path:?}")).with_code("PARSE_ERROR").into());
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
    let index_exists = schema.is_some_and(|s| s.indexes.iter().any(|i| i.name == index_name));
    if !index_exists {
        let create_idx_sql = format!(
            "CREATE {unique_kw}INDEX {index_name} ON {coll} ({dcol});"
        );
        run_ddl(&state, &tenant, &create_idx_sql).await?;
    }

    Ok(Json(json!({ "name": index_name })))
}

/// `POST /collections/{coll}/update` — read-modify-write with optional upsert.
///
/// Body: `{"filter": {...}, "update": {...}, "multi": bool, "upsert": bool}`.
/// Applies the MQL update operator (or replacement) to each matched document,
/// then rewrites the row (doc + all `__cidx_*` columns) so secondary indexes
/// stay current. With `multi: false` (default) only the first match is updated.
/// With `upsert: true`, inserts a new document when nothing matches.
///
/// Returns `{"matchedCount": N, "modifiedCount": N, "upsertedId": <str>|null}`.
pub(crate) async fn update(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(coll): Path<String>,
    Json(req): Json<UpdateRequest>,
) -> Result<Json<Value>, CollError> {
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;

    let coll = ident(&coll)?.to_string();

    let filter = parse_filter(&req.filter)
        .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;

    // Fetch index columns once — used for both the SELECT routing and the UPDATE.
    let dcols = indexed_col_names(&state, &tenant, &coll).await.unwrap_or_default();

    // Build the set of indexed paths for query routing.
    let indexed: HashSet<String> = collect_filter_paths(&filter)
        .into_iter()
        .filter(|p| dcols.contains(&derived_col(p)))
        .collect();

    let mut params: Vec<Value> = Vec::new();
    let where_sql = filter.to_sql_indexed(&mut params, &indexed);

    let mut read_sql = format!("SELECT _id, doc FROM {coll} WHERE {where_sql}");
    if !req.multi {
        read_sql.push_str(" LIMIT 1");
    }

    let rows = run_read_routed(&state, &tenant, &read_sql, &params)
        .await
        .unwrap_or_default();

    let matched = rows.len();
    let mut modified: usize = 0;

    for row in &rows {
        let id = match row.get("_id") {
            Some(Value::String(s)) => s.clone(),
            _ => continue,
        };
        let mut doc: Value = match row.get("doc") {
            Some(Value::String(s)) => serde_json::from_str(s).unwrap_or(Value::Null),
            Some(v) => v.clone(),
            None => continue,
        };
        let before = doc.clone();
        apply_update(&mut doc, &req.update)
            .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;
        if doc != before {
            rewrite_doc_row(&state, &tenant, &coll, &id, &doc, &dcols).await?;
            modified += 1;
        }
    }

    let mut upserted_id: Value = Value::Null;
    if matched == 0 && req.upsert {
        ensure_collection(&state, &tenant, &coll).await?;
        let mut doc = json!({});
        apply_update(&mut doc, &req.update)
            .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;
        let id = ensure_id(&mut doc);
        write_full_doc(&state, &tenant, &coll, &doc, &dcols).await?;
        upserted_id = json!(id);
    }

    Ok(Json(json!({
        "matchedCount": matched,
        "modifiedCount": modified,
        "upsertedId": upserted_id,
    })))
}

/// `POST /collections/{coll}/delete` — delete documents matching a filter.
///
/// Body: `{"filter": {...}, "multi": bool}`. With `multi: false` (default)
/// deletes at most one document. Deleting a row automatically removes all
/// `__cidx_*` column values — no extra maintenance needed.
///
/// Returns `{"deletedCount": N}`.
pub(crate) async fn delete(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(coll): Path<String>,
    Json(req): Json<DeleteRequest>,
) -> Result<Json<Value>, CollError> {
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;

    let coll = ident(&coll)?.to_string();

    let filter = parse_filter(&req.filter)
        .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;

    let dcols = indexed_col_names(&state, &tenant, &coll).await.unwrap_or_default();

    let indexed: HashSet<String> = collect_filter_paths(&filter)
        .into_iter()
        .filter(|p| dcols.contains(&derived_col(p)))
        .collect();

    let mut params: Vec<Value> = Vec::new();
    let where_sql = filter.to_sql_indexed(&mut params, &indexed);

    // Read the matching _id values first so we know the count and can scope
    // the DELETE precisely when multi=false. GlueSQL DELETE does not support
    // LIMIT, so we delete by the specific _id set we collected.
    let read_sql = format!("SELECT _id FROM {coll} WHERE {where_sql}");
    let rows = run_read_routed(&state, &tenant, &read_sql, &params)
        .await
        .unwrap_or_default();

    let to_delete: Vec<String> = rows
        .iter()
        .filter_map(|r| r.get("_id").and_then(Value::as_str).map(str::to_owned))
        .take(if req.multi { usize::MAX } else { 1 })
        .collect();

    let deleted_count = to_delete.len();

    for id in &to_delete {
        let del_sql = format!("DELETE FROM {coll} WHERE _id = $1;");
        run_write(
            &state,
            &tenant,
            &del_sql,
            &[bluedb_rest::Param::Str(id.clone())],
        )
        .await?;
    }

    Ok(Json(json!({ "deletedCount": deleted_count })))
}

/// `POST /collections/{coll}/aggregate` — run a MongoDB aggregation pipeline.
///
/// Body: `{"pipeline": [ {"$match": {...}}, {"$group": {...}}, {"$sort": {...}}, ... ]}`.
/// The pipeline is built directly as a DataFusion `DataFrame` over the tenant's
/// Iceberg mirror (`bluedb_collections::pipeline::apply_pipeline`), so a stage
/// reads the **sealed** snapshot — callers seal (or wait for the seal cadence)
/// before aggregating freshly-written data.
///
/// Supported stages: `$match`, `$sort`, `$limit`, `$skip`, `$count`, `$group`
/// (`$sum`/`$avg`/`$min`/`$max`/`$count`), `$project`/`$addFields`/`$set`,
/// `$lookup`, `$unwind`. An unknown stage is a 400 (`PARSE_ERROR`).
///
/// Returns `{"documents": [...]}` — each result row as a JSON object.
pub(crate) async fn aggregate(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(coll): Path<String>,
    Json(req): Json<Value>,
) -> Result<Json<Value>, CollError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    // Reads are allowed anywhere — no `require_active()`.

    let coll = ident(&coll)?.to_string();

    let stages = req
        .get("pipeline")
        .and_then(|p| p.as_array())
        .cloned()
        .ok_or_else(|| AppError::bad_request("aggregate requires a `pipeline` array"))?;

    // Resolve the tenant's analytical engine (Iceberg mirror via DataFusion).
    let engine = state
        .lakehouse()
        .await
        .ok_or_else(|| AppError::internal("analytical engine unavailable"))?
        .engine_for(&tenant)
        .await
        .map_err(|e| AppError::internal(format!("get lakehouse engine: {e}")))?;

    let ctx = bluedb_query::session_with_catalog(engine)
        .await
        .map_err(|e| AppError::internal(format!("build analytical context: {e}")))?;

    let df = bluedb_collections::pipeline::apply_pipeline(&ctx, &coll, &stages)
        .await
        .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| AppError::bad_request(format!("aggregate: {e}")))?;

    let rows = crate::record_batches_to_json(&batches);
    Ok(Json(json!({ "documents": rows })))
}

/// `POST /collections/{coll}/count` — count documents matching a filter.
///
/// Body: `{"filter": {...}}`. All fields are optional; an empty filter counts
/// every document. For equality filters on indexed fields the derived column
/// (`__cidx_<path>`) is used — same index-aware routing as `find`. Non-indexed
/// fields go through `doc->>'field'` which may be guardrail-rejected and routed
/// to DataFusion (requires a prior seal for freshly-written data).
///
/// Returns `{"count": N}`.
pub(crate) async fn count(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(coll): Path<String>,
    Json(req): Json<Value>,
) -> Result<Json<Value>, CollError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    // Reads are allowed anywhere — no `require_active()`.

    let coll = ident(&coll)?.to_string();

    let filter = parse_filter(req.get("filter").unwrap_or(&json!({})))
        .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;

    // Resolve indexed paths — same logic as `find`.
    let dcol_names = indexed_col_names(&state, &tenant, &coll).await.unwrap_or_default();
    let indexed: HashSet<String> = collect_filter_paths(&filter)
        .into_iter()
        .filter(|p| dcol_names.contains(&derived_col(p)))
        .collect();

    let mut params: Vec<Value> = Vec::new();
    let where_sql = filter.to_sql_indexed(&mut params, &indexed);

    let sql = format!("SELECT COUNT(*) AS n FROM {coll} WHERE {where_sql}");

    let rows = run_read_routed(&state, &tenant, &sql, &params).await?;

    // The alias "n" is how GlueSQL surfaces it; DataFusion may surface it
    // differently. Extract robustly: try "n" first, then fall back to the
    // first value in the row object.
    let n: i64 = rows
        .first()
        .and_then(|r| {
            // Try the alias directly.
            if let Some(v) = r.get("n").and_then(|v| v.as_i64()) {
                return Some(v);
            }
            // Try the first value in the row (DataFusion may use a generated name).
            r.as_object()
                .and_then(|m| m.values().next())
                .and_then(|v| v.as_i64())
        })
        .unwrap_or(0);

    Ok(Json(json!({ "count": n })))
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
