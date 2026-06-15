//! End-to-end HTTP contract for the native graph store edge-maintenance API
//! (`PUT`/`DELETE /graph/{graph}/edges`) plus the hard-delete `retract_edges`
//! query flag. Asserts status codes + JSON only — graph CONTENT correctness is
//! covered by crate-level integration tests in `bluedb-evidence`.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use bluedb_ha::{LeaseProvider, LocalLeaseProvider, SystemClock, WriterController};
use bluedb_server::{authz::Authz, build_app, AppState};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use tower::ServiceExt;

const TTL: Duration = Duration::from_secs(30);
const MARGIN: Duration = Duration::from_secs(5);

async fn promoted(authz: Option<Authz>) -> (AppState, Router) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arc::new(WriterController::new(
        "test-node",
        Arc::new(LocalLeaseProvider::new()) as Arc<dyn LeaseProvider>,
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ));
    let mut state = AppState::new(store, "bluedb", writer);
    if let Some(a) = authz {
        state = state.with_authz(a);
    }
    state.promote().await.expect("promote");
    let app = build_app(state.clone());
    (state, app)
}

/// Call with an optional tenant header and optional bearer token.
async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    tenant: Option<&str>,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(t) = tenant {
        builder = builder.header("x-bluedb-tenant", t);
    }
    if let Some(tok) = token {
        builder = builder.header("authorization", format!("Bearer {tok}"));
    }
    let request = match body {
        Some(v) => builder
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&v).unwrap()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    (status, json)
}

#[tokio::test]
async fn graph_edge_upsert_and_delete_contract() {
    // Open mode (no authz): tenant header drives isolation only.
    let (_, app) = promoted(None).await;

    // 1. Upsert one edge (default merge = set) → 200, upserted:1.
    let (s, body) = call(
        &app,
        "PUT",
        "/graph/g/edges",
        Some("acme"),
        None,
        Some(json!({ "edges": [{ "src": "A", "dst": "B", "weight": 5 }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "upsert: {s} {body}");
    assert_eq!(body["graph"], "g", "graph echoed: {body}");
    assert_eq!(body["upserted"], 1, "upserted count: {body}");

    // 2a. Re-upsert same identity with merge=max → 200, upserted:1.
    let (s, body) = call(
        &app,
        "PUT",
        "/graph/g/edges",
        Some("acme"),
        None,
        Some(json!({ "edges": [{ "src": "A", "dst": "B", "weight": 9 }], "merge": "max" })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "upsert max: {s} {body}");
    assert_eq!(body["upserted"], 1, "upserted count max: {body}");

    // 2b. Re-upsert with explicit merge=set (lower weight) → 200 (idempotent success).
    let (s, body) = call(
        &app,
        "PUT",
        "/graph/g/edges",
        Some("acme"),
        None,
        Some(json!({ "edges": [{ "src": "A", "dst": "B", "weight": 1 }], "merge": "set" })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "upsert set: {s} {body}");
    assert_eq!(body["upserted"], 1, "upserted count set: {body}");

    // 3. Unknown merge mode → 400.
    let (s, body) = call(
        &app,
        "PUT",
        "/graph/g/edges",
        Some("acme"),
        None,
        Some(json!({ "edges": [{ "src": "A", "dst": "B", "weight": 1 }], "merge": "bogus" })),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "bogus merge: {s} {body}");

    // 4. Delete the edge by identity → 200, deleted:1.
    let (s, body) = call(
        &app,
        "DELETE",
        "/graph/g/edges",
        Some("acme"),
        None,
        Some(json!({ "edges": [{ "src": "A", "dst": "B" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "delete: {s} {body}");
    assert_eq!(body["graph"], "g", "graph echoed on delete: {body}");
    assert_eq!(body["deleted"], 1, "deleted count: {body}");
}

#[tokio::test]
async fn graph_edge_auth_negatives() {
    // acmero is bound to tenant acme with read-only scope; root is superuser.
    let authz = Authz::parse_env(
        "acmero=data:read,tenant:acme;acmerw=data:write,tenant:acme;root=superuser",
    )
    .unwrap();
    let (_, app) = promoted(Some(authz)).await;

    let edge_body = json!({ "edges": [{ "src": "A", "dst": "B", "weight": 5 }] });

    // 5a. PUT without data:write scope (read-only token) → 403.
    let (s, body) = call(
        &app,
        "PUT",
        "/graph/g/edges",
        Some("acme"),
        Some("acmero"),
        Some(edge_body.clone()),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "read-only token on upsert: {s} {body}");

    // 5b. PUT with a token whose tenant scope doesn't match → 403.
    let (s, body) = call(
        &app,
        "PUT",
        "/graph/g/edges",
        Some("globex"),
        Some("acmerw"),
        Some(edge_body.clone()),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "acme token on globex tenant: {s} {body}");

    // Sanity: the write token DOES work on its own tenant.
    let (s, body) = call(
        &app,
        "PUT",
        "/graph/g/edges",
        Some("acme"),
        Some("acmerw"),
        Some(edge_body),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "acme write token on acme tenant: {s} {body}");
    assert_eq!(body["upserted"], 1, "upserted count: {body}");
}

#[tokio::test]
async fn hard_delete_retract_edges_flag_over_http() {
    // Open mode; tenant header drives isolation only.
    let (_, app) = promoted(None).await;

    // Plain (unverified) chain so hard-delete is allowed.
    let (s, body) = call(
        &app,
        "PUT",
        "/evidence/p",
        Some("acme"),
        None,
        Some(json!({ "verified": false })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "create plain chain: {s} {body}");

    // Append TWO entries so each delete targets a real seq. Note: the append
    // HTTP handler does NOT parse `edges` from the body (it builds
    // `EntryInput { edges: Vec::new(), .. }`), so these entries carry no edges.
    // This test therefore asserts the `retract_edges` FLAG PLUMBING only
    // (status + echoed JSON field); actual edge retraction is covered by the
    // crate-level integration test `hard_delete_retracts_edges_by_default`.
    for i in 0..2u32 {
        let (s, body) = call(
            &app,
            "POST",
            "/evidence/p/entries",
            Some("acme"),
            None,
            Some(json!({
                "events": [{ "type": "ev", "payload_b64": B64.encode(format!("q{i}").as_bytes()) }]
            })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "append p[{i}]: {s} {body}");
    }

    // Explicit ?retract_edges=false → 200, echoed retract_edges:false.
    let (s, body) = call(
        &app,
        "DELETE",
        "/evidence/p/entries/1?retract_edges=false",
        Some("acme"),
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "hard-delete retract=false: {s} {body}");
    assert_eq!(body["deleted"], true, "deleted: {body}");
    assert_eq!(body["retract_edges"], false, "retract_edges echoed false: {body}");

    // Default (no query param) → 200, echoed retract_edges:true.
    let (s, body) = call(
        &app,
        "DELETE",
        "/evidence/p/entries/2",
        Some("acme"),
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "hard-delete default: {s} {body}");
    assert_eq!(body["deleted"], true, "deleted: {body}");
    assert_eq!(body["retract_edges"], true, "retract_edges echoed true: {body}");
}
