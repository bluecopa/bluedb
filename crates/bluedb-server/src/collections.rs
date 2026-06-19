//! Document-oriented collections API.
//!
//! A collection is a table `coll(_id TEXT PRIMARY KEY, doc JSON)` managed
//! transparently: the table is created on first insert (`CREATE TABLE IF NOT
//! EXISTS`), and each document is assigned a 24-character hex `_id` if it
//! doesn't already carry one.
//!
//! ## Endpoints
//! - `POST /collections/{coll}/insert` — bulk-insert documents

use axum::extract::{Path, State};
use axum::Json;
use gluesql_core::prelude::Glue;
use serde_json::{json, Value};

use bluedb_engine::rest_sql;

use crate::{authz::Scope, schema::ident, AppError, AppState};

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

/// Run a single parameterized write statement (`INSERT`) through the FTS
/// commit-observer path so the live index is maintained. Mirrors the write
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

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `POST /collections/{coll}/insert` — insert documents into a collection.
///
/// Body: `{"documents": [{...}, ...]}`. Each document receives a generated
/// 24-char hex `_id` if it doesn't already carry one. The backing table is
/// created implicitly on first insert. Returns
/// `{"insertedIds": [...], "insertedCount": N}`.
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

        let sql = format!("INSERT INTO {coll} (_id, doc) VALUES ($1, $2);");
        let params = vec![
            bluedb_rest::Param::Str(id.clone()),
            bluedb_rest::Param::Str(doc_text),
        ];
        run_write(&state, &tenant, &sql, &params).await?;
        inserted_ids.push(id);
    }

    Ok(Json(json!({
        "insertedIds": inserted_ids,
        "insertedCount": inserted_ids.len(),
    })))
}
