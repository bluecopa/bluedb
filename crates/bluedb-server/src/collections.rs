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
use std::time::{SystemTime, UNIX_EPOCH};

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
    id::new_object_id,
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

// ---------------------------------------------------------------------------
// Multikey (array-field) index helpers
// ---------------------------------------------------------------------------

/// The name of the multikey side table for a collection field.
///
/// `{coll}__mk_{field}` (dots in `field` → underscores).  The convention is
/// parallel to `derived_col` for single-field indexes but uses a separate table
/// rather than a derived column, because one row per array element is needed.
fn multikey_side_table(coll: &str, field: &str) -> String {
    format!("{coll}__mk_{}", field.replace('.', "_"))
}

/// Create the multikey side table for `(coll, field)` if it doesn't exist yet,
/// including the secondary index on `val`.
///
/// Schema: `(rid TEXT PRIMARY KEY, _id TEXT, val TEXT)` with `INDEX(val)`.
/// - `rid` is a generated unique id for each (element, parent) pair.
/// - `_id` is the owning document's `_id`.
/// - `val` is the element rendered as text (same as `derive_value` would produce).
///
/// The `val` index is what makes the element-membership find index-served: the
/// rewrite `_id IN (SELECT _id FROM {side} WHERE val = $1)` runs the subquery's
/// `WHERE val = $1` against this index (a point lookup on the side table), so the
/// query guardrail keeps it on the GlueSQL fast path instead of routing it to a
/// DataFusion full scan. Creating it here (rather than only at `createIndex`
/// time) also backfills the index onto any side table that predates this change,
/// since both `createIndex` and the write path call through here.
///
/// Both the `CREATE TABLE` and the `CREATE INDEX` are idempotent: the table uses
/// `IF NOT EXISTS`, and the index is created only when the side table's schema
/// does not already carry it (GlueSQL has no `CREATE INDEX IF NOT EXISTS` and
/// errors on a duplicate index name).
async fn ensure_multikey_side_table(
    state: &AppState,
    tenant: &str,
    side: &str,
) -> Result<(), AppError> {
    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {side} (rid TEXT PRIMARY KEY, _id TEXT, val TEXT);"
    );
    run_ddl(state, tenant, &create_table).await?;

    // Create INDEX(val) once. Stable name: `{side}_val`.
    let index_name = format!("{side}_val");
    let storage = state.connection(tenant).await?;
    let schema = Store::fetch_schema(&storage, side)
        .await
        .map_err(|e| AppError::internal(format!("fetch schema: {e}")))?;
    let index_exists = schema.is_some_and(|s| s.indexes.iter().any(|i| i.name == index_name));
    if !index_exists {
        let create_idx = format!("CREATE INDEX {index_name} ON {side} (val);");
        run_ddl(state, tenant, &create_idx).await?;
    }
    Ok(())
}

/// Return the list of multikey side-table field names for `coll`.
///
/// Discovers them by listing all tables whose name matches the pattern
/// `{coll}__mk_*` and stripping the prefix. Returns `(side_table_name,
/// field_name)` pairs in arbitrary order.
///
/// Uses `Store::fetch_all_schemas` to enumerate tables for the tenant.
/// Returns an empty vec if the collection has no multikey indexes.
async fn multikey_side_tables(
    state: &AppState,
    tenant: &str,
    coll: &str,
) -> Result<Vec<(String, String)>, AppError> {
    let storage = state.connection(tenant).await?;
    let prefix = format!("{coll}__mk_");
    let all_schemas = Store::fetch_all_schemas(&storage)
        .await
        .map_err(|e| AppError::internal(format!("fetch all schemas: {e}")))?;
    let result: Vec<(String, String)> = all_schemas
        .into_iter()
        .filter_map(|s| {
            let field = s.table_name.strip_prefix(&prefix)?.to_owned();
            Some((s.table_name, field))
        })
        .collect();
    Ok(result)
}

/// Insert side-table rows for one document's array field.
///
/// For each element in `arr`, inserts one row `(rid, doc_id, elem_text)` into
/// `side`. Elements are rendered as text: strings unquoted, other scalars via
/// `to_string()`. Non-scalar elements (nested arrays/objects) are rendered as
/// their JSON text form.
async fn insert_multikey_elements(
    state: &AppState,
    tenant: &str,
    side: &str,
    doc_id: &str,
    arr: &[Value],
) -> Result<(), AppError> {
    for elem in arr {
        let val = match elem {
            Value::String(s) => s.clone(),
            Value::Null => continue, // nulls are not indexed
            other => other.to_string(),
        };
        let rid = new_object_id();
        let sql = format!("INSERT INTO {side} (rid, _id, val) VALUES ($1, $2, $3);");
        let params = &[
            bluedb_rest::Param::Str(rid),
            bluedb_rest::Param::Str(doc_id.to_string()),
            bluedb_rest::Param::Str(val),
        ];
        run_write(state, tenant, &sql, params).await?;
    }
    Ok(())
}

/// Delete all side-table entries for a document `_id` from every multikey side
/// table of `coll`.
async fn delete_multikey_for_doc(
    state: &AppState,
    tenant: &str,
    coll: &str,
    doc_id: &str,
) -> Result<(), AppError> {
    let sides = multikey_side_tables(state, tenant, coll).await?;
    for (side, _field) in sides {
        let sql = format!("DELETE FROM {side} WHERE _id = $1;");
        run_write(state, tenant, &sql, &[bluedb_rest::Param::Str(doc_id.to_string())]).await?;
    }
    Ok(())
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
/// Multikey side tables (from `mk_sides`) receive one row per array element.
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

    let id_str = doc
        .get("_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    let placeholders: Vec<String> = (1..=params.len()).map(|i| format!("${i}")).collect();
    let sql = format!(
        "INSERT INTO {coll} ({cols}) VALUES ({vals});",
        cols = col_names.join(", "),
        vals = placeholders.join(", "),
    );
    run_write(state, tenant, &sql, &params).await?;

    // Maintain multikey side tables — one row per array element (or scalar) per field.
    // Note: `field` here is the suffix after `{coll}__mk_`, dots replaced by
    // underscores. For top-level fields (no dots in path), `field` is the
    // exact key name in the document; for nested paths this would require extra
    // metadata. V1 supports top-level multikey fields only.
    let mk_sides = multikey_side_tables(state, tenant, coll).await?;
    for (side, field) in &mk_sides {
        match doc.get(field.as_str()) {
            Some(Value::Array(arr)) => {
                insert_multikey_elements(state, tenant, side, &id_str, arr).await?;
            }
            // FIX M5: a scalar (non-array, non-null) value is also indexed as a
            // single entry — matches MongoDB multikey semantics.
            Some(v) if !v.is_null() => {
                let singleton = [v.clone()];
                insert_multikey_elements(state, tenant, side, &id_str, &singleton).await?;
            }
            _ => {}
        }
    }

    Ok(())
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
    run_write(state, tenant, &sql, &params).await?;

    // Maintain multikey side tables: delete old entries, re-insert from updated doc.
    // `field` is the suffix after `{coll}__mk_`, with dots replaced by underscores.
    // For top-level fields (v1 scope), `field` is the exact document key.
    let mk_sides = multikey_side_tables(state, tenant, coll).await?;
    for (side, field) in &mk_sides {
        // Delete old elements for this document.
        let del_sql = format!("DELETE FROM {side} WHERE _id = $1;");
        run_write(state, tenant, &del_sql, &[bluedb_rest::Param::Str(id.to_string())]).await?;
        // Re-insert new elements (array or scalar — FIX M5).
        match doc.get(field.as_str()) {
            Some(Value::Array(arr)) => {
                insert_multikey_elements(state, tenant, side, id, arr).await?;
            }
            Some(v) if !v.is_null() => {
                let singleton = [v.clone()];
                insert_multikey_elements(state, tenant, side, id, &singleton).await?;
            }
            _ => {}
        }
    }

    Ok(())
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
    /// When present, turns this into a TTL index: documents expire N seconds
    /// after the epoch timestamp stored in the indexed field.
    #[serde(rename = "expireAfterSeconds")]
    pub expire_after_seconds: Option<i64>,
    /// Explicit multikey opt-in. When `true`, the index is created as a multikey
    /// side table regardless of what the sampled values look like. When absent,
    /// multikey is auto-detected from existing documents (if any sampled value is
    /// an array, the index is multikey).
    #[serde(default)]
    pub multikey: bool,
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
    let mk_sides = multikey_side_tables(&state, &tenant, &coll).await.unwrap_or_default();

    // Build the WHERE clause and collect bound parameters.
    // Priority: multikey single-equality → compound → single-field indexed.
    //
    // Multikey element-match: for a filter `{field: "x"}` where `field` has a
    // multikey side table, rewrite to:
    //   _id IN (SELECT _id FROM {side} WHERE val = $1)
    //
    // The `_id IN (SELECT ... WHERE val = $1)` shape IS recognized by the query
    // guardrail as index-served (PK membership + the side table's INDEX(val)), so
    // it stays on the GlueSQL transactional fast path — a point lookup on the side
    // table, not a DataFusion full scan. See `bluedb_sql::guardrail`
    // (`in_subquery_hits_index`) and `ensure_multikey_side_table` (which creates
    // the `val` index this relies on).
    //
    // Only a single-equality-on-a-multikey-field is rewritten here; other shapes
    // (e.g. `$in` on a multikey field, mixed predicates) fall through to the
    // existing routing and are handled by the JSON accessor / DataFusion path.
    let mut params: Vec<Value> = Vec::new();
    let where_sql = if let Some(mk_sql) = try_multikey_match(&filter, &mk_sides, &coll, &mut params) {
        mk_sql
    } else if let Some(sql) = try_compound_match(&filter, &cdefs, &mut params) {
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
            // FIX I4: reject dotted (nested) paths in compound indexes. The
            // column-name round-trip `__cidxm_a__b` conflates `a.b` with a
            // top-level field named `a_b`; nested-path support in compound
            // indexes is deferred to v2.
            if p.contains('.') {
                return Err(AppError::bad_request(
                    "nested (dotted) paths are not supported for compound indexes in v1"
                )
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
    // Single-field index: keys.len() == 1
    // -----------------------------------------------------------------------

    // Take the first key as the path to index.
    let (path, _) = req.keys.iter().next().unwrap();
    let path = path.clone();

    if !valid_path(&path) {
        return Err(AppError::bad_request(format!("invalid index field path: {path:?}")).with_code("PARSE_ERROR").into());
    }

    // Read existing docs to sample the field (needed for multikey detection and backfill).
    let select_sql = format!("SELECT _id, doc FROM {coll} WHERE TRUE;");
    let existing_rows = run_read_routed(&state, &tenant, &select_sql, &[]).await?;

    // Collect sample values (non-null field values) for type inference / multikey detection.
    let sample_values: Vec<Value> = existing_rows
        .iter()
        .filter_map(|row| {
            let doc: Value = match row.get("doc") {
                Some(Value::String(s)) => serde_json::from_str(s).ok()?,
                Some(v) => v.clone(),
                None => return None,
            };
            let mut cur = &doc;
            for part in path.split('.') {
                cur = cur.get(part)?;
            }
            if cur.is_null() { None } else { Some(cur.clone()) }
        })
        .collect();

    // Detect multikey: explicit opt-in OR any sampled value is an array.
    let is_multikey = req.options.multikey
        || sample_values.iter().any(|v| v.is_array());

    if is_multikey {
        // -----------------------------------------------------------------------
        // Multikey index: create a side table {coll}__mk_{field}.
        //
        // Element-membership find (field:"x" on an array field) is served via:
        //   SELECT doc FROM {coll} WHERE _id IN (SELECT _id FROM side WHERE val=$1)
        // The side table carries INDEX(val) (created by ensure_multikey_side_table),
        // and the guardrail recognises the `_id IN (SELECT ... WHERE val=$1)` shape
        // as index-served, so this stays on the GlueSQL fast path (a point lookup
        // on the side table) — not a DataFusion full scan.
        //
        // FIX I4: reject dotted (nested) paths in multikey indexes. The side-table
        // name `{coll}__mk_a_b` conflates `a.b` with a top-level field named `a_b`,
        // and `write_full_doc` uses `doc.get(field)` (literal key, not path nav).
        // Nested multikey support is deferred to v2.
        // -----------------------------------------------------------------------
        if path.contains('.') {
            return Err(AppError::bad_request(
                "nested (dotted) paths are not supported for multikey indexes in v1"
            )
            .with_code("PARSE_ERROR")
            .into());
        }
        let side = multikey_side_table(&coll, &path);
        let index_name = format!("mk_{coll}_{}", path.replace('.', "_"));

        // Step 1: Create the side table (idempotent).
        ensure_multikey_side_table(&state, &tenant, &side).await?;

        // Step 2: Backfill existing docs.
        for row in &existing_rows {
            let id = match row.get("_id") {
                Some(Value::String(s)) => s.clone(),
                _ => continue,
            };
            let doc: Value = match row.get("doc") {
                Some(Value::String(s)) => serde_json::from_str(s).unwrap_or(Value::Null),
                Some(v) => v.clone(),
                None => continue,
            };
            let mut cur = &doc;
            let mut found = true;
            for part in path.split('.') {
                match cur.get(part) {
                    Some(v) => cur = v,
                    None => { found = false; break; }
                }
            }
            if found {
                // Clear any prior entries first (idempotent backfill).
                let del_sql = format!("DELETE FROM {side} WHERE _id = $1;");
                run_write(&state, &tenant, &del_sql, &[bluedb_rest::Param::Str(id.clone())]).await?;
                match cur {
                    Value::Array(arr) => {
                        insert_multikey_elements(&state, &tenant, &side, &id, arr).await?;
                    }
                    // FIX M5: a scalar value is indexed as a single entry.
                    v if !v.is_null() => {
                        let singleton = [v.clone()];
                        insert_multikey_elements(&state, &tenant, &side, &id, &singleton).await?;
                    }
                    _ => {}
                }
            }
        }

        return Ok(Json(json!({ "name": index_name })));
    }

    // -----------------------------------------------------------------------
    // Regular (non-multikey) single-field index: derived column approach.
    // -----------------------------------------------------------------------

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
    for row in &existing_rows {
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

    // Step 4 (TTL only): register the expiry config in __bluedb_ttl.
    if let Some(secs) = req.options.expire_after_seconds {
        if secs < 0 {
            return Err(AppError::bad_request(
                "expireAfterSeconds must be >= 0",
            )
            .with_code("PARSE_ERROR")
            .into());
        }
        // Register the per-tenant TTL config and record this tenant in the
        // global registry so the sweep loop picks it up on every tick.
        ensure_ttl_registry(&state, &tenant).await?;
        upsert_ttl_config(&state, &tenant, &coll, &path, secs).await?;
        register_ttl_tenant(&state, &tenant).await?;
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
        // Clean up multikey side-table entries before deleting the main row.
        delete_multikey_for_doc(&state, &tenant, &coll, id).await?;

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
    let mut rows = crate::reinflate_rows(crate::record_batches_to_json(&batches), &json_cols);

    // `$lookup` produces its `as` column as a `List<Utf8>` of foreign-document
    // JSON texts (the serializer renders it as a JSON array of strings). Re-inflate
    // each `as` field's elements into JSON objects so the client gets
    // `customer: [{"_id":"c1","name":"Ada"}]`, not `["{\"_id\":...}"]`. A no-match
    // left row arrives as a NULL `as` cell → normalized to an empty array `[]`.
    let lookup_as_fields = lookup_as_field_names(&stages);
    if !lookup_as_fields.is_empty() {
        reinflate_lookup_arrays(&mut rows, &lookup_as_fields);
    }

    Ok(Json(json!({ "documents": rows })))
}

/// Collect the `as` output-field names of every `$lookup` stage in a pipeline.
fn lookup_as_field_names(stages: &[Value]) -> Vec<String> {
    stages
        .iter()
        .filter_map(|stage| {
            let lookup = stage.as_object()?.get("$lookup")?.as_object()?;
            lookup.get("as").and_then(Value::as_str).map(str::to_owned)
        })
        .collect()
}

/// Re-inflate each `$lookup` `as` field in `rows` from an array of JSON-text
/// strings (the `List<Utf8>` serialization) into an array of JSON objects.
///
/// - Each string element is parsed into a JSON value; an element that fails to
///   parse is left as-is.
/// - JSON `null` elements are dropped (defensive — the `array_agg` FILTER already
///   excludes them).
/// - A `null` `as` cell (no-match left row, a SQL NULL list) becomes `[]`.
fn reinflate_lookup_arrays(rows: &mut Value, fields: &[String]) {
    let Some(arr) = rows.as_array_mut() else { return };
    for row in arr {
        let Some(map) = row.as_object_mut() else { continue };
        for field in fields {
            match map.get(field) {
                // The serialized list: parse each string element, drop nulls.
                Some(Value::Array(elems)) => {
                    let inflated: Vec<Value> = elems
                        .iter()
                        .filter_map(|e| match e {
                            Value::Null => None,
                            Value::String(s) => match serde_json::from_str::<Value>(s) {
                                Ok(Value::Null) => None,
                                Ok(v) => Some(v),
                                Err(_) => Some(e.clone()),
                            },
                            other => Some(other.clone()),
                        })
                        .collect();
                    map.insert(field.clone(), Value::Array(inflated));
                }
                // No-match left row: NULL list cell → empty array.
                Some(Value::Null) => {
                    map.insert(field.clone(), Value::Array(Vec::new()));
                }
                _ => {}
            }
        }
    }
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

// ---------------------------------------------------------------------------
// TTL index support
// ---------------------------------------------------------------------------

/// The per-tenant metadata table that tracks TTL configurations.
const TTL_TABLE: &str = "__bluedb_ttl";

/// Global registry (lives in the DEFAULT_TENANT keyspace) that records every
/// tenant name that has at least one TTL index. The sweep loop reads this once
/// per tick and calls [`sweep_all_ttl`] for each registered tenant.
///
/// Schema: `tenant TEXT PRIMARY KEY`
///
/// Durability: the table is a regular SlateDB-backed GlueSQL table in the
/// DEFAULT_TENANT keyspace, so it survives restarts automatically.
const TTL_TENANTS_TABLE: &str = "__bluedb_ttl_tenants";

/// Ensure the `__bluedb_ttl(collection TEXT PRIMARY KEY, field TEXT, seconds
/// INTEGER)` metadata table exists for `tenant`.
async fn ensure_ttl_registry(state: &AppState, tenant: &str) -> Result<(), AppError> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {TTL_TABLE} \
         (collection TEXT PRIMARY KEY, field TEXT, seconds INTEGER);"
    );
    run_ddl(state, tenant, &sql).await
}

/// Upsert a TTL config `(collection, field, seconds)` into the registry.
async fn upsert_ttl_config(
    state: &AppState,
    tenant: &str,
    coll: &str,
    field: &str,
    seconds: i64,
) -> Result<(), AppError> {
    // GlueSQL has no ON CONFLICT; delete-then-insert is idempotent for a PRIMARY KEY table.
    let del_sql = format!("DELETE FROM {TTL_TABLE} WHERE collection = $1;");
    run_write(state, tenant, &del_sql, &[bluedb_rest::Param::Str(coll.to_string())]).await?;
    let ins_sql = format!(
        "INSERT INTO {TTL_TABLE} (collection, field, seconds) VALUES ($1, $2, $3);"
    );
    run_write(state, tenant, &ins_sql, &[
        bluedb_rest::Param::Str(coll.to_string()),
        bluedb_rest::Param::Str(field.to_string()),
        bluedb_rest::Param::Int(seconds),
    ])
    .await
}

/// Ensure the `__bluedb_ttl_tenants(tenant TEXT PRIMARY KEY)` table exists in
/// the DEFAULT_TENANT keyspace (the global TTL tenant registry).
async fn ensure_ttl_tenants_registry(state: &AppState) -> Result<(), AppError> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {TTL_TENANTS_TABLE} (tenant TEXT PRIMARY KEY);"
    );
    run_ddl(state, bluedb_sql::DEFAULT_TENANT, &sql).await
}

/// Register `tenant` in the global TTL tenant registry so it gets swept.
/// Idempotent: re-registering an already-present tenant is a no-op.
async fn register_ttl_tenant(state: &AppState, tenant: &str) -> Result<(), AppError> {
    ensure_ttl_tenants_registry(state).await?;
    // DELETE-then-INSERT is idempotent for a PRIMARY KEY table (GlueSQL has no ON CONFLICT).
    let del = format!("DELETE FROM {TTL_TENANTS_TABLE} WHERE tenant = $1;");
    run_write(state, bluedb_sql::DEFAULT_TENANT, &del, &[bluedb_rest::Param::Str(tenant.to_string())]).await?;
    let ins = format!("INSERT INTO {TTL_TENANTS_TABLE} (tenant) VALUES ($1);");
    run_write(state, bluedb_sql::DEFAULT_TENANT, &ins, &[bluedb_rest::Param::Str(tenant.to_string())]).await
}

/// Return all tenants that have at least one TTL index registered.
/// Returns an empty list if the global registry table doesn't exist yet (no
/// TTL index has ever been created on this cluster node).
pub(crate) async fn list_ttl_tenants(state: &AppState) -> Result<Vec<String>, AppError> {
    let sql = format!("SELECT tenant FROM {TTL_TENANTS_TABLE} WHERE TRUE;");
    let rows = match run_read_routed(state, bluedb_sql::DEFAULT_TENANT, &sql, &[]).await {
        Ok(r) => r,
        Err(ref e)
            if e.message().contains("planning SQL")
                || e.message().contains("table not found") =>
        {
            return Ok(vec![]);
        }
        Err(e) => return Err(e),
    };
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            row.get("tenant")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect())
}

/// Delete documents in `coll` whose TTL `field` value + `seconds` <= `now_epoch`.
///
/// - A numeric field value is treated as an epoch in **seconds**.
/// - A string field value is parsed as RFC 3339 / ISO-8601; parsing failure → skip.
/// - A missing or null field → skip (document never expires).
///
/// `now_epoch` is injected so callers (tests) can control time deterministically.
/// Returns the number of documents deleted.
pub(crate) async fn sweep_ttl(
    state: &AppState,
    tenant: &str,
    coll: &str,
    field: &str,
    seconds: i64,
    now_epoch: i64,
) -> Result<usize, AppError> {
    // Read all docs — the TTL sweep is infrequent (60s cadence) and the scan is
    // necessary because an expired doc by definition has no dedicated index.
    let sql = format!("SELECT _id, doc FROM {coll} WHERE TRUE;");
    let rows = match run_read_routed(state, tenant, &sql, &[]).await {
        Ok(r) => r,
        // Collection not yet sealed / not in Iceberg → fall through to the
        // GlueSQL path. If the table truly doesn't exist, we get 0 rows.
        Err(ref e)
            if e.message().contains("planning SQL")
                || e.message().contains("table not found") =>
        {
            vec![]
        }
        Err(e) => return Err(e),
    };

    let mut deleted = 0usize;
    for row in &rows {
        let id = match row.get("_id") {
            Some(Value::String(s)) => s.clone(),
            _ => continue,
        };
        let doc: Value = match row.get("doc") {
            Some(Value::String(s)) => serde_json::from_str(s).unwrap_or(Value::Null),
            Some(v) => v.clone(),
            None => continue,
        };

        // Navigate to the TTL field (simple single-level name for now).
        let field_val = doc.get(field);
        let field_epoch: i64 = match field_val {
            None | Some(Value::Null) => continue,
            Some(Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    i
                } else if let Some(f) = n.as_f64() {
                    f as i64
                } else {
                    continue;
                }
            }
            Some(Value::String(s)) => {
                // Try RFC 3339 / ISO-8601 via chrono.
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                    dt.timestamp()
                } else {
                    // Try date-only "YYYY-MM-DD".
                    if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        use chrono::TimeZone;
                        chrono::Utc.from_utc_datetime(&nd.and_hms_opt(0, 0, 0).unwrap_or_default()).timestamp()
                    } else {
                        continue; // unparseable → skip
                    }
                }
            }
            _ => continue, // arrays, objects → skip
        };

        if field_epoch + seconds <= now_epoch {
            let del_sql = format!("DELETE FROM {coll} WHERE _id = $1;");
            run_write(state, tenant, &del_sql, &[bluedb_rest::Param::Str(id)]).await?;
            deleted += 1;
        }
    }

    Ok(deleted)
}

/// Read every TTL config from `__bluedb_ttl` for `tenant` and sweep each
/// collection using the current wall-clock time.  Called by the background task.
pub(crate) async fn sweep_all_ttl(state: &AppState, tenant: &str) -> Result<(), AppError> {
    // FIX M6: Do not sweep on a node that is no longer the active writer.
    // A node losing its lease must not issue DELETEs — this could race with the
    // new writer and corrupt data. The scheduler loop is aborted on demote, but
    // there is a window between the last tick and the abort where `is_writer()`
    // may have already flipped. Guard defensively here.
    if !state.is_writer() {
        return Ok(());
    }

    // If the TTL registry table doesn't exist yet, nothing to sweep.
    let sql = format!("SELECT collection, field, seconds FROM {TTL_TABLE} WHERE TRUE;");
    let rows = match run_read_routed(state, tenant, &sql, &[]).await {
        Ok(r) => r,
        Err(ref e)
            if e.message().contains("planning SQL")
                || e.message().contains("table not found") =>
        {
            return Ok(());
        }
        Err(e) => return Err(e),
    };

    let now_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    for row in &rows {
        let coll = match row.get("collection").and_then(Value::as_str) {
            Some(s) => s.to_owned(),
            None => continue,
        };
        let field = match row.get("field").and_then(Value::as_str) {
            Some(s) => s.to_owned(),
            None => continue,
        };
        let seconds: i64 = match row.get("seconds") {
            Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
            _ => continue,
        };
        if let Err(e) = sweep_ttl(state, tenant, &coll, &field, seconds, now_epoch).await {
            eprintln!("bluedb-server: TTL sweep {tenant}/{coll}: {:?}", e);
        }
    }
    Ok(())
}

/// Sweep TTL indexes for **all registered tenants**.
///
/// Called by the background scheduler on the active writer. Reads the global
/// tenant registry (`__bluedb_ttl_tenants` in the DEFAULT_TENANT keyspace) and
/// calls [`sweep_all_ttl`] for each tenant that has a TTL index configured.
///
/// The `is_writer()` guard inside [`sweep_all_ttl`] provides defence-in-depth
/// against a demote that races the loop between the registry read and the sweep.
pub(crate) async fn sweep_all_tenants_ttl(state: &AppState) -> Result<(), AppError> {
    if !state.is_writer() {
        return Ok(());
    }
    let tenants = list_ttl_tenants(state).await?;
    for tenant in tenants {
        if let Err(e) = sweep_all_ttl(state, &tenant).await {
            eprintln!("bluedb-server: TTL sweep for tenant '{tenant}': {:?}", e);
        }
    }
    Ok(())
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

/// Try to match a **single equality** filter against a multikey side table.
///
/// For a filter `{field: scalar}` (i.e. `Filter::Cmp{ op: Eq, value: scalar }`)
/// where `field` has a multikey side table in `mk_sides`, returns:
///
/// ```sql
/// _id IN (SELECT _id FROM {side} WHERE val = $N)
/// ```
///
/// This is the element-membership path: `find {tags:"x"}` on an array
/// field `tags:["x","y"]` correctly matches (the side table stores one row per
/// element — or one row for a scalar value).
///
/// **Fast path:** the side table carries `INDEX(val)` (see
/// [`ensure_multikey_side_table`]), and the guardrail accepts the
/// `_id IN (SELECT _id FROM {side} WHERE val = $N)` shape as index-served (PK
/// membership + the indexed subquery scan), so this stays on the GlueSQL
/// transactional fast path — read-your-writes, no DataFusion round-trip.
///
/// Only the **single-equality-on-a-multikey-field** shape is handled here. Other
/// shapes (multiple predicates, `$in`, `$ne`, etc. on a multikey field) fall
/// through to the existing routing (JSON accessor / DataFusion).
///
/// Returns `None` when the filter doesn't match this pattern.
fn try_multikey_match(
    filter: &bluedb_collections::filter::Filter,
    mk_sides: &[(String, String)],
    _coll: &str,
    params: &mut Vec<Value>,
) -> Option<String> {
    use bluedb_collections::filter::{Cmp, Filter};

    if mk_sides.is_empty() {
        return None;
    }

    // Only a single top-level equality node qualifies.
    let (path, value) = match filter {
        Filter::Cmp { path, op: Cmp::Eq, value } => (path.as_str(), value),
        _ => return None,
    };

    // Check if this path has a multikey side table.
    let (side, _) = mk_sides
        .iter()
        .find(|(_, field)| field == &path.replace('.', "_"))?;

    // Render the scalar as text (the side table stores val as TEXT).
    let val_text = match value {
        Value::String(s) => s.clone(),
        Value::Null => return None, // null is not indexed
        other => other.to_string(),
    };

    let placeholder = {
        params.push(Value::String(val_text));
        format!("${}", params.len())
    };

    // GlueSQL supports `IN (SELECT ...)` subqueries (verified via rewrite.rs).
    // `side` is the exact side-table name discovered from the storage schema.
    Some(format!("_id IN (SELECT _id FROM {side} WHERE val = {placeholder})"))
}
