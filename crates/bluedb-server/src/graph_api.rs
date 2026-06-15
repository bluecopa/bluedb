//! `/graph/*` — HTTP surface for the native graph store: edge maintenance
//! (`PUT`/`DELETE /graph/{graph}/edges`) plus read-only traversal
//! (`POST /graph/{graph}/reachable`, `POST /graph/{graph}/widest-path`).

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

/// `DELETE /graph/{graph}` — drop an entire graph (all its edges). `data:write`.
pub async fn drop_graph(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(graph): Path<String>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    let dropped = state.graph(&tenant).await?.drop_graph(&graph).await.map_err(map_evidence_err)?;
    Ok(Json(json!({ "graph": graph, "dropped": dropped })))
}

#[derive(Deserialize)]
pub(crate) struct ReachableBody {
    from: Vec<String>,
    #[serde(default)]
    floor: Option<i64>,
    #[serde(default)]
    directed: Option<bool>,
}

/// `POST /graph/{graph}/reachable` → `{ graph, nodes: [...] }`.
pub async fn reachable(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(graph): Path<String>,
    Json(body): Json<ReachableBody>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let floor = body.floor.unwrap_or(i64::MIN);
    let directed = body.directed.unwrap_or(true);
    let nodes = state
        .graph(&tenant)
        .await?
        .reachable(&graph, &body.from, floor, directed)
        .await
        .map_err(map_evidence_err)?;
    Ok(Json(json!({ "graph": graph, "nodes": nodes })))
}

#[derive(Deserialize)]
pub(crate) struct WidestPathBody {
    from: String,
    to: String,
    #[serde(default)]
    directed: Option<bool>,
}

/// `POST /graph/{graph}/widest-path` → `{ connected, bottleneck? }`.
pub async fn widest_path(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(graph): Path<String>,
    Json(body): Json<WidestPathBody>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let directed = body.directed.unwrap_or(true);
    let wp = state
        .graph(&tenant)
        .await?
        .widest_path(&graph, &body.from, &body.to, directed)
        .await
        .map_err(map_evidence_err)?;
    let mut out = json!({ "connected": wp.connected });
    if let Some(b) = wp.bottleneck {
        out["bottleneck"] = json!(b);
    }
    Ok(Json(out))
}
