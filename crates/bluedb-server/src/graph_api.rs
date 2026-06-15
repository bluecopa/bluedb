//! `/graph/*` — HTTP surface for the native graph store (edge maintenance).
//! Traversal endpoints (`reachable`, `widest-path`) are a later plan.

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use bluedb_evidence::{EdgeRef, EdgeUpsert, Merge};

use crate::evidence_api::map_evidence_err;
use crate::{authz::Scope, AppError, AppState};

#[derive(Deserialize)]
pub(crate) struct UpsertBody {
    edges: Vec<UpsertEdge>,
    #[serde(default)]
    merge: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct UpsertEdge {
    src: String,
    dst: String,
    weight: i64,
    #[serde(default, rename = "type")]
    etype: String,
}

#[derive(Deserialize)]
pub(crate) struct DeleteBody {
    edges: Vec<DeleteEdge>,
}

#[derive(Deserialize)]
pub(crate) struct DeleteEdge {
    src: String,
    dst: String,
    #[serde(default, rename = "type")]
    etype: String,
}

/// `PUT /graph/{graph}/edges` — upsert edges. `merge` in {"set","max"} (default set).
pub async fn upsert_edges(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(graph): Path<String>,
    Json(body): Json<UpsertBody>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    let merge = match body.merge.as_deref() {
        None | Some("set") => Merge::Set,
        Some("max") => Merge::Max,
        Some(other) => {
            return Err(AppError::bad_request(format!(
                "unknown merge mode '{other}' (want 'set' or 'max')"
            )))
        }
    };
    let edges: Vec<EdgeUpsert> = body
        .edges
        .into_iter()
        .map(|e| EdgeUpsert { src: e.src, dst: e.dst, weight: e.weight, etype: e.etype })
        .collect();
    let n = edges.len();
    state.graph(&tenant).await?.upsert(&graph, &edges, merge).await.map_err(map_evidence_err)?;
    Ok(Json(json!({ "graph": graph, "upserted": n })))
}

/// `DELETE /graph/{graph}/edges` — delete edges by identity.
pub async fn delete_edges(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(graph): Path<String>,
    Json(body): Json<DeleteBody>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    let edges: Vec<EdgeRef> = body
        .edges
        .into_iter()
        .map(|e| EdgeRef { src: e.src, dst: e.dst, etype: e.etype })
        .collect();
    let n = edges.len();
    state.graph(&tenant).await?.delete(&graph, &edges).await.map_err(map_evidence_err)?;
    Ok(Json(json!({ "graph": graph, "deleted": n })))
}
