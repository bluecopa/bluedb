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

use std::collections::HashMap;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use gluesql_core::ast::DataType as GlueDataType;
use gluesql_core::prelude::Glue;
use gluesql_core::store::Store;
use serde::Deserialize;
use serde_json::{json, Value};

use bluedb_engine::rest_sql;
use bluedb_collections::{
    error::MqlError,
    filter::{parse_filter, sort_accessor},
    index::{
        compound_col, compound_key, derive_typed_value, derived_col, encode_compound,
        index_sql_type, infer_index_type, valid_path, IndexType,
    },
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

/// Like [`run_read_routed`] but treats "collection not yet visible in the
/// analytical engine" as an empty result rather than an error. This is the
/// correct semantic for the `update`/`delete` pre-read: if the collection has
/// never been sealed into the Iceberg mirror, the analytical path can't plan the
/// query (`table not found`), but that just means there are no rows to match —
/// upsert should still proceed. Genuine I/O errors (storage failures, etc.) are
/// still propagated via `?`.
async fn run_read_routed_for_mutation(
    state: &AppState,
    tenant: &str,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, AppError> {
    match run_read_routed(state, tenant, sql, params).await {
        Ok(rows) => Ok(rows),
        Err(ref e)
            if e.message().contains("planning SQL")
                || e.message().contains("table not found") =>
        {
            // The collection has no Iceberg snapshot yet (never sealed) → 0 matches.
            Ok(vec![])
        }
        Err(e) => Err(e),
    }
}

/// Return a map from derived-column name (`__cidx_<path>`) to [`IndexType`] for
/// every gateway-maintained single-field index column in `coll` for `tenant`.
///
/// The column's GlueSQL `DataType` is mapped to `IndexType`:
/// - `Float` / `Float32` / any integer variant → `Number`
/// - `Boolean`                                 → `Bool`
/// - anything else (incl. `Text`)              → `Text`
///
/// Returns an empty map if the table doesn't exist yet.
/// Note: compound index columns (`__cidxm_*`) are excluded here — use
/// [`compound_col_defs`] to retrieve them.
async fn indexed_col_types(
    state: &AppState,
    tenant: &str,
    coll: &str,
) -> Result<HashMap<String, IndexType>, AppError> {
    let storage = state.connection(tenant).await?;
    let schema = match Store::fetch_schema(&storage, coll).await {
        Ok(Some(s)) => s,
        Ok(None) => return Ok(HashMap::new()),
        Err(e) => return Err(AppError::internal(format!("fetch schema: {e}"))),
    };
    let map: HashMap<String, IndexType> = schema
        .column_defs
        .unwrap_or_default()
        .into_iter()
        .filter_map(|c| {
            // Only single-field index columns (prefix __cidx_ but NOT __cidxm_).
            if !c.name.starts_with("__cidx_") || c.name.starts_with("__cidxm_") {
                return None;
            }
            let idx_type = glue_data_type_to_index_type(&c.data_type);
            Some((c.name, idx_type))
        })
        .collect();
    Ok(map)
}

/// Return the list of compound index columns in `coll` for `tenant`, as
/// `(col_name, component_paths)` pairs.  The column name has the `__cidxm_`
/// prefix stripped and the remainder split on `__` to recover the component
/// paths (with `_` restored — note: this is exact only for top-level fields
/// without dots in their names, which is v1 scope for compound indexes).
///
/// Returns an empty vec if the table doesn't exist yet.
async fn compound_col_defs(
    state: &AppState,
    tenant: &str,
    coll: &str,
) -> Result<Vec<(String, Vec<String>)>, AppError> {
    let storage = state.connection(tenant).await?;
    let schema = match Store::fetch_schema(&storage, coll).await {
        Ok(Some(s)) => s,
        Ok(None) => return Ok(vec![]),
        Err(e) => return Err(AppError::internal(format!("fetch schema: {e}"))),
    };
    let defs: Vec<(String, Vec<String>)> = schema
        .column_defs
        .unwrap_or_default()
        .into_iter()
        .filter_map(|c| {
            let suffix = c.name.strip_prefix("__cidxm_")?;
            // Split on __ to recover component paths (top-level only in v1).
            let paths: Vec<String> = suffix.split("__").map(str::to_owned).collect();
            if paths.len() < 2 {
                return None;
            }
            Some((c.name, paths))
        })
        .collect();
    Ok(defs)
}

/// Map a GlueSQL [`DataType`] to the [`IndexType`] used for filter routing.
///
/// Numeric columns: `INT` (`DataType::Int` → `Value::I64`) is the type we
/// store for `Number` indexes.  FLOAT can't be serialized as an index key in
/// bluedb-sql; DECIMAL stores but `evaluate_cmp(Decimal, I64)` returns None
/// in GlueSQL 0.19.  All integer variants and Float32/Float are also mapped to
/// `Number` for completeness (e.g. a column created via a legacy DDL path).
fn glue_data_type_to_index_type(dt: &GlueDataType) -> IndexType {
    match dt {
        GlueDataType::Int
        | GlueDataType::Int8
        | GlueDataType::Int16
        | GlueDataType::Int32
        | GlueDataType::Int128
        | GlueDataType::Uint8
        | GlueDataType::Uint16
        | GlueDataType::Uint32
        | GlueDataType::Uint64
        | GlueDataType::Uint128
        | GlueDataType::Float
        | GlueDataType::Float32
        | GlueDataType::Decimal => IndexType::Number,
        GlueDataType::Boolean => IndexType::Bool,
        _ => IndexType::Text,
    }
}

/// INSERT a complete document into `coll`, setting `_id`, `doc`, every
/// `__cidx_*` (single-field) column, and every `__cidxm_*` (compound) column.
/// Shared by `insert` (per doc) and `upsert` (single doc).
///
/// The caller must have already called `ensure_collection` and must pass `dcols`
/// (from `indexed_col_types`) so the derived columns are populated atomically
/// with the main row. Each derived column is populated with a typed value
/// (FLOAT/BOOLEAN/TEXT) matching the column's declared [`IndexType`].
/// Compound columns (`__cidxm_*`) in `cdefs` are always TEXT (NUL-joined key).
/// `doc` must already contain `_id`.
async fn write_full_doc(
    state: &AppState,
    tenant: &str,
    coll: &str,
    doc: &Value,
    dcols: &HashMap<String, IndexType>,
    cdefs: &[(String, Vec<String>)],
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

    // Single-field index columns.
    for (dcol, &idx_type) in dcols {
        let path = dcol.strip_prefix("__cidx_").unwrap_or(dcol);
        col_names.push(dcol.clone());
        match derive_typed_value(doc, path, idx_type) {
            Some(v) => params.push(json_value_to_param(&v)),
            None => params.push(bluedb_rest::Param::Null),
        }
    }

    // Compound index columns — always TEXT (NUL-joined key).
    for (ccol, paths) in cdefs {
        col_names.push(ccol.clone());
        let key = compound_key(doc, paths);
        params.push(bluedb_rest::Param::Str(key));
    }

    let placeholders: Vec<String> = (1..=params.len()).map(|i| format!("${i}")).collect();
    let sql = format!(
        "INSERT INTO {coll} ({cols}) VALUES ({vals});",
        cols = col_names.join(", "),
        vals = placeholders.join(", "),
    );
    run_write(state, tenant, &sql, &params).await
}

/// UPDATE a row's `doc`, all `__cidx_*` (single-field), and all `__cidxm_*`
/// (compound) columns for the given `_id`.  Used by the `update` handler after
/// applying an in-memory MQL update to the document so all secondary indexes
/// stay consistent.
async fn rewrite_doc_row(
    state: &AppState,
    tenant: &str,
    coll: &str,
    id: &str,
    doc: &Value,
    dcols: &HashMap<String, IndexType>,
    cdefs: &[(String, Vec<String>)],
) -> Result<(), AppError> {
    let doc_text = doc.to_string();

    // SET doc = $1, __cidx_a = $2, ... WHERE _id = $N
    let mut set_clauses = vec!["doc = $1".to_string()];
    let mut params: Vec<bluedb_rest::Param> = vec![bluedb_rest::Param::Str(doc_text)];

    // Single-field index columns.
    for (dcol, &idx_type) in dcols {
        let path = dcol.strip_prefix("__cidx_").unwrap_or(dcol);
        let idx = params.len() + 1;
        set_clauses.push(format!("{dcol} = ${idx}"));
        match derive_typed_value(doc, path, idx_type) {
            Some(v) => params.push(json_value_to_param(&v)),
            None => params.push(bluedb_rest::Param::Null),
        }
    }

    // Compound index columns — always TEXT.
    for (ccol, paths) in cdefs {
        let idx = params.len() + 1;
        set_clauses.push(format!("{ccol} = ${idx}"));
        let key = compound_key(doc, paths);
        params.push(bluedb_rest::Param::Str(key));
    }

    let id_idx = params.len() + 1;
    params.push(bluedb_rest::Param::Str(id.to_string()));

    let sql = format!(
        "UPDATE {coll} SET {sets} WHERE _id = ${id_idx};",
        sets = set_clauses.join(", "),
    );
    run_write(state, tenant, &sql, &params).await
}

/// Convert a typed JSON value (as returned by [`derive_typed_value`]) to the
/// appropriate [`bluedb_rest::Param`] variant for a parameterized SQL statement.
///
/// `derive_typed_value` returns exactly the JSON type that matches the column:
/// - `Number` column → `Value::Number` → prefer `Param::Int` (i64); fractional
///   floats map to `Param::Null` because the column type is `INT` and GlueSQL
///   cannot coerce a fractional float into an integer without data loss.
/// - `Bool`   column → `Value::Bool`   → `Param::Bool`
/// - `Text`   column → `Value::String` → `Param::Str`
fn json_value_to_param(v: &Value) -> bluedb_rest::Param {
    match v {
        Value::Bool(b) => bluedb_rest::Param::Bool(*b),
        Value::Number(n) => {
            // Prefer integer representation so the param type matches the INT
            // column type exactly.
            //
            // Whole-valued floats like `5.0` also canonicalize to `Int` so a
            // doc stored as `{"qty":5}` and one stored as `{"qty":5.0}` unify
            // on the same index entry — and a query `{qty:5}` also finds
            // a stored `5.0` document and vice-versa.
            //
            // Truly fractional floats (e.g. `3.14`) cannot be stored in an INT
            // column without data loss. `derive_typed_value` will have returned
            // `None` for them (→ NULL stored) and `value_matches_type` will
            // not have routed the query here; but as a belt-and-suspenders
            // guard, map them to `Null` so the INT column gets no entry.
            if let Some(i) = n.as_i64() {
                bluedb_rest::Param::Int(i)
            } else if let Some(f) = n.as_f64() {
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    bluedb_rest::Param::Int(f as i64)
                } else {
                    bluedb_rest::Param::Null // truly fractional → no INT entry
                }
            } else {
                bluedb_rest::Param::Null
            }
        }
        Value::String(s) => bluedb_rest::Param::Str(s.clone()),
        Value::Null => bluedb_rest::Param::Null,
        // Arrays/objects should not appear from derive_typed_value for our index
        // types, but fall back to their JSON text form for safety.
        other => bluedb_rest::Param::Str(other.to_string()),
    }
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
    /// Optional explicit column type hint: `"number"`, `"bool"` / `"boolean"`,
    /// or `"string"`. When absent, the type is inferred from existing documents.
    #[serde(rename = "type")]
    pub index_type_hint: Option<String>,
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

    // Fetch the map of derived-column names → IndexType (may be empty on a fresh collection).
    let dcols = indexed_col_types(&state, &tenant, &coll).await?;
    // Fetch compound index column definitions.
    let cdefs = compound_col_defs(&state, &tenant, &coll).await?;

    let mut inserted_ids: Vec<String> = Vec::with_capacity(docs_arr.len());

    for raw_doc in docs_arr {
        let mut doc = raw_doc.clone();
        // ensure_id requires a mutable JSON object.
        if !doc.is_object() {
            return Err(AppError::bad_request("each document must be a JSON object").into());
        }
        let id = ensure_id(&mut doc);
        write_full_doc(&state, &tenant, &coll, &doc, &dcols, &cdefs).await?;
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

    // Build the map of indexed paths: derived column name → IndexType.
    // The filter's to_sql_indexed takes this map keyed by derived column name.
    let col_types = indexed_col_types(&state, &tenant, &coll).await.unwrap_or_default();
    let cdefs = compound_col_defs(&state, &tenant, &coll).await.unwrap_or_default();

    // Build the WHERE clause and collect bound parameters.
    // Try the compound fast path first; fall through to single-field indexed routing.
    let mut params: Vec<Value> = Vec::new();
    let where_sql = if let Some(sql) = try_compound_match(&filter, &cdefs, &mut params) {
        sql
    } else {
        let indexed: HashMap<String, IndexType> = collect_filter_paths(&filter)
            .into_iter()
            .filter_map(|p| {
                let dcol = derived_col(&p);
                col_types.get(&dcol).map(|&t| (dcol, t))
            })
            .collect();
        filter.to_sql_indexed(&mut params, &indexed)
    };

    // Build SELECT with optional ORDER BY / LIMIT / OFFSET.
    let mut sql = format!("SELECT doc FROM {coll} WHERE {where_sql}");

    // ORDER BY — `{"field": 1}` → ASC, `{"field": -1}` → DESC.
    if let Some(sort_obj) = req.get("sort").and_then(Value::as_object) {
        if !sort_obj.is_empty() {
            let mut order_parts: Vec<String> = Vec::new();
            for (field, dir_val) in sort_obj {
                let dir = if dir_val.as_i64().unwrap_or(1) < 0 { "DESC" } else { "ASC" };
                let col_expr = sort_accessor(field);
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
/// Body: `{"keys": {"<path>": 1}, "options": {"unique": false, "type": "number"}}`.
///
/// **Single-field** (`keys` has 1 entry): the derived column type is determined by:
/// 1. `options.type` hint: `"number"` → `INT`, `"bool"`/`"boolean"` → `BOOLEAN`, `"string"` → `TEXT`.
/// 2. Otherwise: infer from existing documents.
///
/// **Compound** (`keys` has >1 entries): always creates a `TEXT` column
/// (`__cidxm_<a>__<b>`) storing the NUL-joined component values.  The type hint
/// is ignored (compound keys are always TEXT — equality only in v1).
///
/// Steps:
/// 1. `ALTER TABLE {coll} ADD COLUMN {derived_col} {type}` (skipped if already present).
/// 2. Backfill existing rows.
/// 3. `CREATE [UNIQUE] INDEX {name} ON {coll} ({derived_col})`.
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

    // Ensure the collection table exists first (createIndex before any insert).
    ensure_collection(&state, &tenant, &coll).await?;

    let unique_kw = if req.options.unique { "UNIQUE " } else { "" };

    // -----------------------------------------------------------------------
    // Compound index: keys.len() > 1
    // -----------------------------------------------------------------------
    if req.keys.len() > 1 {
        // Validate and collect paths in insertion order.
        let paths: Vec<String> = req.keys.keys().cloned().collect();
        for p in &paths {
            if !valid_path(p) {
                return Err(AppError::bad_request(format!(
                    "invalid compound index field path: {p:?}"
                ))
                .with_code("PARSE_ERROR")
                .into());
            }
        }

        let dcol = compound_col(&paths);
        let index_name = format!(
            "cidxm_{coll}_{}",
            paths.iter().map(|p| p.replace('.', "_")).collect::<Vec<_>>().join("_")
        );

        // Step 1: Add the TEXT column if not present.
        let existing_cdefs = compound_col_defs(&state, &tenant, &coll).await?;
        let already_exists = existing_cdefs.iter().any(|(c, _)| c == &dcol);
        if !already_exists {
            let alter_sql = format!("ALTER TABLE {coll} ADD COLUMN {dcol} TEXT;");
            run_ddl(&state, &tenant, &alter_sql).await?;
        }

        // Step 2: Backfill existing rows.
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
            let key = compound_key(&doc, &paths);
            let update_sql = format!("UPDATE {coll} SET {dcol} = $1 WHERE _id = $2;");
            let params = vec![
                bluedb_rest::Param::Str(key),
                bluedb_rest::Param::Str(id),
            ];
            run_write(&state, &tenant, &update_sql, &params).await?;
        }

        // Step 3: Create the index.
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

        return Ok(Json(json!({ "name": index_name })));
    }

    // -----------------------------------------------------------------------
    // Single-field index: keys.len() == 1 (original behavior unchanged)
    // -----------------------------------------------------------------------

    // Take the first key as the path to index.
    let (path, _) = req.keys.iter().next().unwrap();
    let path = path.clone();

    if !valid_path(&path) {
        return Err(AppError::bad_request(format!("invalid index field path: {path:?}")).with_code("PARSE_ERROR").into());
    }

    let dcol = derived_col(&path);
    let index_name = format!(
        "cidx_{coll}_{}",
        path.replace('.', "_")
    );

    // Step 1: Add the derived column if it doesn't already exist.
    let existing_col_types = indexed_col_types(&state, &tenant, &coll).await?;
    if !existing_col_types.contains_key(&dcol) {
        // Resolve the index type from hint or by sampling existing docs.
        let idx_type = if let Some(hint) = req.options.index_type_hint.as_deref() {
            match hint.to_ascii_lowercase().as_str() {
                "number" => IndexType::Number,
                "bool" | "boolean" => IndexType::Bool,
                "string" | "text" => IndexType::Text,
                other => {
                    return Err(AppError::bad_request(format!(
                        "unknown index type hint {other:?}; expected \"number\", \"bool\", or \"string\""
                    ))
                    .with_code("PARSE_ERROR")
                    .into());
                }
            }
        } else {
            // Read existing docs and sample the field's values.
            let select_sql = format!("SELECT doc FROM {coll} WHERE TRUE;");
            let rows = run_read_routed(&state, &tenant, &select_sql, &[]).await?;
            let sample_values: Vec<serde_json::Value> = rows
                .iter()
                .filter_map(|row| {
                    let doc: Value = match row.get("doc") {
                        Some(Value::String(s)) => serde_json::from_str(s).ok()?,
                        Some(v) => v.clone(),
                        None => return None,
                    };
                    // Navigate to the field; skip absent fields.
                    let mut cur = &doc;
                    for part in path.split('.') {
                        cur = cur.get(part)?;
                    }
                    if cur.is_null() {
                        None
                    } else {
                        Some(cur.clone())
                    }
                })
                .collect();
            infer_index_type(&sample_values)
        };

        let sql_type = index_sql_type(idx_type);
        let alter_sql = format!("ALTER TABLE {coll} ADD COLUMN {dcol} {sql_type};");
        run_ddl(&state, &tenant, &alter_sql).await?;
    }

    // Re-fetch type map so we know the column's type for backfill (whether we
    // just added it or it already existed).
    let col_type_map = indexed_col_types(&state, &tenant, &coll).await?;
    let idx_type = col_type_map.get(&dcol).copied().unwrap_or(IndexType::Text);

    // Step 2: Backfill existing rows with typed values.
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
        if let Some(val) = derive_typed_value(&doc, &path, idx_type) {
            let update_sql = format!("UPDATE {coll} SET {dcol} = $1 WHERE _id = $2;");
            let params = vec![
                json_value_to_param(&val),
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
    let dcols = indexed_col_types(&state, &tenant, &coll).await.unwrap_or_default();
    let cdefs = compound_col_defs(&state, &tenant, &coll).await.unwrap_or_default();

    // Build the typed indexed map for query routing.
    let indexed: HashMap<String, IndexType> = collect_filter_paths(&filter)
        .into_iter()
        .filter_map(|p| {
            let dcol = derived_col(&p);
            dcols.get(&dcol).map(|&t| (dcol, t))
        })
        .collect();

    let mut params: Vec<Value> = Vec::new();
    let where_sql = filter.to_sql_indexed(&mut params, &indexed);

    let mut read_sql = format!("SELECT _id, doc FROM {coll} WHERE {where_sql}");
    if !req.multi {
        read_sql.push_str(" LIMIT 1");
    }

    let rows = run_read_routed_for_mutation(&state, &tenant, &read_sql, &params).await?;

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
            rewrite_doc_row(&state, &tenant, &coll, &id, &doc, &dcols, &cdefs).await?;
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
        write_full_doc(&state, &tenant, &coll, &doc, &dcols, &cdefs).await?;
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

    let dcols = indexed_col_types(&state, &tenant, &coll).await.unwrap_or_default();

    let indexed: HashMap<String, IndexType> = collect_filter_paths(&filter)
        .into_iter()
        .filter_map(|p| {
            let dcol = derived_col(&p);
            dcols.get(&dcol).map(|&t| (dcol, t))
        })
        .collect();

    let mut params: Vec<Value> = Vec::new();
    let where_sql = filter.to_sql_indexed(&mut params, &indexed);

    // Read the matching _id values first so we know the count and can scope
    // the DELETE precisely when multi=false. GlueSQL DELETE does not support
    // LIMIT, so we delete by the specific _id set we collected.
    let read_sql = format!("SELECT _id FROM {coll} WHERE {where_sql}");
    let rows = run_read_routed_for_mutation(&state, &tenant, &read_sql, &params).await?;

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

    let json_cols = vec!["doc".to_string()];
    let rows = crate::reinflate_rows(crate::record_batches_to_json(&batches), &json_cols);
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
    let col_types = indexed_col_types(&state, &tenant, &coll).await.unwrap_or_default();
    let cdefs = compound_col_defs(&state, &tenant, &coll).await.unwrap_or_default();

    let mut params: Vec<Value> = Vec::new();
    let where_sql = if let Some(sql) = try_compound_match(&filter, &cdefs, &mut params) {
        sql
    } else {
        let indexed: HashMap<String, IndexType> = collect_filter_paths(&filter)
            .into_iter()
            .filter_map(|p| {
                let dcol = derived_col(&p);
                col_types.get(&dcol).map(|&t| (dcol, t))
            })
            .collect();
        filter.to_sql_indexed(&mut params, &indexed)
    };

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

/// Try to match a filter against the available compound indexes.
///
/// Returns `Some((sql_fragment, params))` when the filter's full-key equality
/// is covered by a compound index (all component paths present as `$eq`
/// predicates).  The SQL fragment uses the compound column with a single
/// `= $N` predicate; any equality predicates that are NOT covered by the
/// chosen compound index are appended as additional AND clauses rendered
/// via `to_sql` (JSON accessor, analytical fallback if needed).
///
/// Returns `None` when no compound index covers the filter.
fn try_compound_match(
    filter: &bluedb_collections::filter::Filter,
    cdefs: &[(String, Vec<String>)],
    params: &mut Vec<Value>,
) -> Option<String> {
    if cdefs.is_empty() {
        return None;
    }
    // Get the top-level equality map — only pure-eq filters qualify.
    let eq_map = filter.eq_map()?;
    if eq_map.is_empty() {
        return None;
    }

    // Find the first compound index whose component paths are all in the eq map.
    let (ccol, paths) = cdefs.iter().find(|(_, paths)| {
        paths.iter().all(|p| eq_map.contains_key(p))
    })?;

    // Build the compound key value from the equality map (component order = index order).
    let parts: Vec<String> = paths
        .iter()
        .map(|p| {
            let v = &eq_map[p];
            match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            }
        })
        .collect();
    let compound_val = encode_compound(&parts);

    let placeholder = {
        params.push(Value::String(compound_val));
        format!("${}", params.len())
    };

    let mut clauses = vec![format!("{ccol} = {placeholder}")];

    // Any leftover equality predicates not covered by the compound index.
    let covered: std::collections::HashSet<&str> = paths.iter().map(|s| s.as_str()).collect();
    for (p, v) in &eq_map {
        if !covered.contains(p.as_str()) {
            use bluedb_collections::filter::{Filter, Cmp};
            let leftover = Filter::Cmp { path: p.clone(), op: Cmp::Eq, value: v.clone() };
            clauses.push(format!("({})", leftover.to_sql(params)));
        }
    }

    Some(clauses.join(" AND "))
}
