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
async fn http_drop_graph_clears_edges() {
    let (_, app) = promoted(None).await;
    let (s, _b) = call(&app, "PUT", "/graph/g/edges", Some("acme"), None, Some(json!({
        "edges": [
            {"src":"A","dst":"B","weight":5},
            {"src":"B","dst":"C","weight":3}
        ]
    }))).await;
    assert_eq!(s, StatusCode::OK);

    let (s, body) = call(&app, "DELETE", "/graph/g", Some("acme"), None, None).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["dropped"], 2);

    // After drop, reachable from A sees only the seed (no edges left).
    let (s, body) = call(&app, "POST", "/graph/g/reachable", Some("acme"), None, Some(json!({ "from": ["A"] }))).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["nodes"], json!(["A"]));
}

/// Drop is `data:write`: a read-only token → 403; a tenant-mismatched write
/// token → 403 (mirrors `graph_edge_auth_negatives`).
#[tokio::test]
async fn http_drop_graph_auth_negatives() {
    let authz = Authz::parse_env(
        "acmero=data:read,tenant:acme;acmerw=data:write,tenant:acme;root=superuser",
    )
    .unwrap();
    let (_, app) = promoted(Some(authz)).await;

    // Read-only token on the drop route → 403.
    let (s, body) = call(&app, "DELETE", "/graph/g", Some("acme"), Some("acmero"), None).await;
    assert_eq!(s, StatusCode::FORBIDDEN, "read-only token on drop: {s} {body}");

    // Write token whose tenant scope doesn't match the header → 403.
    let (s, body) = call(&app, "DELETE", "/graph/g", Some("globex"), Some("acmerw"), None).await;
    assert_eq!(s, StatusCode::FORBIDDEN, "acme token on globex tenant: {s} {body}");

    // Sanity: the write token works on its own tenant (empty graph → dropped:0).
    let (s, body) = call(&app, "DELETE", "/graph/g", Some("acme"), Some("acmerw"), None).await;
    assert_eq!(s, StatusCode::OK, "acme write token on acme tenant: {s} {body}");
    assert_eq!(body["dropped"], 0, "dropped count: {body}");
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

    // Append TWO entries so each delete targets a real seq. These entries omit
    // the optional `edges` field, so they carry no edges. This test therefore
    // asserts the `retract_edges` FLAG PLUMBING only (status + echoed JSON
    // field); actual edge retraction is covered by the crate-level integration
    // test `hard_delete_retracts_edges_by_default`.
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

/// append-with-edges over HTTP: edges are framed into the verified `leaf_hash`,
/// so the digest reflects them. A chain appended WITH an edge has a different
/// `root_hash` than the same event appended WITHOUT, and matches a third chain
/// given the SAME edge. (Verified chains auto-create on first append.)
#[tokio::test]
async fn http_append_with_edges_affects_digest() {
    // Open mode; tenant header drives isolation only.
    let (_, app) = promoted(None).await;

    // Same base64 payload for all three so ONLY the edge differs.
    let payload_b64 = B64.encode(b"p");
    let edge = json!({ "graph": "lin", "src": "A", "dst": "B", "weight": 5 });

    // chain "with" — one event carrying an edge.
    let (s, body) = call(
        &app,
        "POST",
        "/evidence/with/entries",
        Some("acme"),
        None,
        Some(json!({
            "events": [{ "type": "t", "payload_b64": payload_b64, "edges": [edge.clone()] }]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "append with-edge: {s} {body}");

    // chain "without" — identical event, NO edges.
    let (s, body) = call(
        &app,
        "POST",
        "/evidence/without/entries",
        Some("acme"),
        None,
        Some(json!({
            "events": [{ "type": "t", "payload_b64": payload_b64 }]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "append without-edge: {s} {body}");

    // chain "same" — identical event WITH the same edge as "with".
    let (s, body) = call(
        &app,
        "POST",
        "/evidence/same/entries",
        Some("acme"),
        None,
        Some(json!({
            "events": [{ "type": "t", "payload_b64": payload_b64, "edges": [edge.clone()] }]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "append same-edge: {s} {body}");

    // GET each digest → {size:1, root_hash:"<64-hex>"}.
    let digest = |chain: &'static str| {
        let app = app.clone();
        async move {
            let (s, body) = call(
                &app,
                "GET",
                &format!("/evidence/{chain}/digest"),
                Some("acme"),
                None,
                None,
            )
            .await;
            assert_eq!(s, StatusCode::OK, "digest {chain}: {s} {body}");
            assert_eq!(body["size"], 1, "digest {chain} size: {body}");
            body["root_hash"].as_str().unwrap().to_string()
        }
    };
    let with = digest("with").await;
    let without = digest("without").await;
    let same = digest("same").await;

    // edge changed the leaf hash ⇒ different root.
    assert_ne!(with, without, "edge must change root_hash: with={with} without={without}");
    // same edge ⇒ same hash.
    assert_eq!(with, same, "same edge must yield same root_hash: with={with} same={same}");
}

/// append edge parse-error negatives → 400.
#[tokio::test]
async fn http_append_edge_parse_errors() {
    let (_, app) = promoted(None).await;
    let payload_b64 = B64.encode(b"p");

    // Edge missing 'graph' → 400.
    let (s, body) = call(
        &app,
        "POST",
        "/evidence/c/entries",
        Some("acme"),
        None,
        Some(json!({
            "events": [{
                "type": "t",
                "payload_b64": payload_b64,
                "edges": [{ "src": "A", "dst": "B", "weight": 5 }]
            }]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "edge missing graph: {s} {body}");

    // Edge with unknown 'op' → 400.
    let (s, body) = call(
        &app,
        "POST",
        "/evidence/c/entries",
        Some("acme"),
        None,
        Some(json!({
            "events": [{
                "type": "t",
                "payload_b64": payload_b64,
                "edges": [{ "graph": "lin", "src": "A", "dst": "B", "op": "bogus" }]
            }]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "edge unknown op: {s} {body}");
}

/// Traversal contract: `reachable` (with/without a weight floor) and
/// `widest-path` (connected with bottleneck, disconnected omits bottleneck).
#[tokio::test]
async fn http_reachable_and_widest_path() {
    let (_, app) = promoted(None).await;
    let (s, _b) = call(
        &app,
        "PUT",
        "/graph/g/edges",
        Some("acme"),
        None,
        Some(json!({
            "edges": [
                {"src":"A","dst":"B","weight":5},
                {"src":"B","dst":"C","weight":3},
                {"src":"A","dst":"C","weight":1},
                {"src":"C","dst":"D","weight":10}
            ]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, body) = call(
        &app,
        "POST",
        "/graph/g/reachable",
        Some("acme"),
        None,
        Some(json!({ "from": ["A"] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["nodes"], json!(["A", "B", "C", "D"]));

    let (s, body) = call(
        &app,
        "POST",
        "/graph/g/reachable",
        Some("acme"),
        None,
        Some(json!({ "from": ["A"], "floor": 4 })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["nodes"], json!(["A", "B"]));

    let (s, body) = call(
        &app,
        "POST",
        "/graph/g/widest-path",
        Some("acme"),
        None,
        Some(json!({ "from": "A", "to": "D" })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["connected"], true);
    assert_eq!(body["bottleneck"], 3);

    let (s, body) = call(
        &app,
        "POST",
        "/graph/g/widest-path",
        Some("acme"),
        None,
        Some(json!({ "from": "D", "to": "A" })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["connected"], false);
    assert!(body.get("bottleneck").is_none(), "no bottleneck when disconnected: {body}");
}

/// Traversal reads require `data:read`; tenant scope must match the header.
#[tokio::test]
async fn http_traversal_requires_read_scope() {
    // acmero: read-only on acme; acmerw: write on acme; root: superuser.
    let authz = Authz::parse_env(
        "acmero=data:read,tenant:acme;acmerw=data:write,tenant:acme;root=superuser",
    )
    .unwrap();
    let (_, app) = promoted(Some(authz)).await;

    let reach_body = json!({ "from": ["A"] });

    // A token with NO scopes at all → 403 on the read route.
    let (s, body) = call(
        &app,
        "POST",
        "/graph/g/reachable",
        Some("acme"),
        Some("nobody"),
        Some(reach_body.clone()),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "no-scope token on reachable: {s} {body}");

    // A token whose tenant scope doesn't match the header → 403 (mirrors the
    // edge auth test). acmero CAN read, but only on acme — not globex.
    let (s, body) = call(
        &app,
        "POST",
        "/graph/g/reachable",
        Some("globex"),
        Some("acmero"),
        Some(reach_body.clone()),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "acme read token on globex tenant: {s} {body}");

    // Sanity: the read token DOES work on its own tenant.
    let (s, body) = call(
        &app,
        "POST",
        "/graph/g/reachable",
        Some("acme"),
        Some("acmero"),
        Some(reach_body),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "acme read token on acme tenant: {s} {body}");
    // No edges upserted in authz mode → seed only.
    assert_eq!(body["nodes"], json!(["A"]), "seed-only reachable: {body}");
}
