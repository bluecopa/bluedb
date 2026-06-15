//! Read-only **Iceberg REST Catalog** (`/catalog/v1/*`) so external warehouses
//! (BigQuery, Databricks, Snowflake) can discover and load the mirrored tables
//! using the standard Iceberg REST protocol, then read the Parquet straight from
//! object storage.
//!
//! v1 is read-only and single-namespace (`default`): table creation/mutation
//! happens through bluedb's SQL/REST surface and is reflected here automatically
//! by the seal loop. Every route requires `data:read` and is served by the
//! active writer (which holds the mirror engine).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde_json::{json, Value};

use bluedb_lakehouse::LakehouseEngine;

use crate::{authz, AppError, AppState};

/// The active mirror engine, or `503` on a passive node.
async fn engine(state: &AppState) -> Result<Arc<LakehouseEngine>, AppError> {
    state.lakehouse().await.ok_or_else(|| AppError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        message: "lakehouse catalog unavailable (node is not the active writer)".into(),
    })
}

/// `GET /catalog/v1/config` — catalog defaults/overrides (empty for v1).
pub(crate) async fn config(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    Ok(Json(json!({ "defaults": {}, "overrides": {} })))
}

/// `GET /catalog/v1/namespaces` — the single mirror namespace.
pub(crate) async fn list_namespaces(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    let eng = engine(&state).await?;
    Ok(Json(json!({ "namespaces": [[eng.namespace()]] })))
}

/// `GET /catalog/v1/namespaces/{ns}` — namespace metadata (no properties in v1).
pub(crate) async fn get_namespace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    Ok(Json(json!({ "namespace": [ns], "properties": {} })))
}

/// `GET /catalog/v1/namespaces/{ns}/tables` — the mirrored tables.
pub(crate) async fn list_tables(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    let eng = engine(&state).await?;
    let tables = eng
        .list_iceberg_tables()
        .await
        .map_err(|e| AppError::internal(format!("list tables: {e}")))?;
    let identifiers: Vec<Value> = tables
        .into_iter()
        .map(|name| json!({ "namespace": [ns.clone()], "name": name }))
        .collect();
    Ok(Json(json!({ "identifiers": identifiers })))
}

/// `GET /catalog/v1/namespaces/{ns}/tables/{table}` — `loadTable`: the current
/// metadata location + metadata JSON the warehouse reads.
pub(crate) async fn load_table(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((_ns, table)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    let eng = engine(&state).await?;
    match eng
        .table_metadata_json(&table)
        .await
        .map_err(|e| AppError::internal(format!("load table: {e}")))?
    {
        Some((location, metadata)) => Ok(Json(json!({
            "metadata-location": location,
            "metadata": metadata,
            "config": {},
        }))),
        None => Err(AppError::not_found(format!(
            "table '{table}' is not in the lakehouse mirror"
        ))),
    }
}
