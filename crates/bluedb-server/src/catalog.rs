//! Read-only **Iceberg REST Catalog** (`/catalog/v1/*`) so external warehouses
//! (BigQuery, Databricks, Snowflake) can discover and load the mirrored tables
//! using the standard Iceberg REST protocol, then read the Parquet straight from
//! object storage.
//!
//! Multi-tenant: each tenant publishes under its own Iceberg namespace
//! (`namespace == tenant`, the default tenant maps to `default`). Every route
//! requires `data:read`, and the per-namespace routes additionally require the
//! token to be authorized for that namespace's tenant — so a tenant's token can
//! discover only its own tables. Served by the active writer (which holds the
//! mirror manager).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use serde_json::{json, Value};

use bluedb_lakehouse::{tenant_for_namespace, LakehouseManager};

use crate::{authz, AppError, AppState};

/// The active mirror manager, or `503` on a passive node.
async fn manager(state: &AppState) -> Result<Arc<LakehouseManager>, AppError> {
    state.lakehouse().await.ok_or_else(|| {
        AppError::service_unavailable(
            "lakehouse catalog unavailable (node is not the active writer)",
        )
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

/// `GET /catalog/v1/namespaces` — the mirror namespaces this token may see.
pub(crate) async fn list_namespaces(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    let mgr = manager(&state).await?;
    let namespaces: Vec<Value> = mgr
        .namespaces()
        .await
        .into_iter()
        // Only namespaces whose tenant this token is authorized for.
        .filter(|ns| {
            state
                .authorize_tenant(&headers, &tenant_for_namespace(ns))
                .is_ok()
        })
        .map(|ns| json!([ns]))
        .collect();
    Ok(Json(json!({ "namespaces": namespaces })))
}

/// `GET /catalog/v1/namespaces/{ns}` — namespace metadata (no properties in v1).
pub(crate) async fn get_namespace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    state.authorize_tenant(&headers, &tenant_for_namespace(&ns))?;
    Ok(Json(json!({ "namespace": [ns], "properties": {} })))
}

/// `GET /catalog/v1/namespaces/{ns}/tables` — the mirrored tables in `ns`.
pub(crate) async fn list_tables(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    state.authorize_tenant(&headers, &tenant_for_namespace(&ns))?;
    let mgr = manager(&state).await?;
    let Some(eng) = mgr.engine_for_namespace(&ns).await else {
        return Err(AppError::not_found(format!(
            "namespace '{ns}' is not mirrored"
        )));
    };
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
    Path((ns, table)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    state.authorize_tenant(&headers, &tenant_for_namespace(&ns))?;
    let mgr = manager(&state).await?;
    let Some(eng) = mgr.engine_for_namespace(&ns).await else {
        return Err(AppError::not_found(format!(
            "namespace '{ns}' is not mirrored"
        )));
    };
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
            "table '{ns}.{table}' is not in the lakehouse mirror"
        ))),
    }
}
