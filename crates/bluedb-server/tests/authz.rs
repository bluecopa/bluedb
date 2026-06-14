//! Authorization enforcement tests.
//!
//! Each test builds an app WITH an `Authz` map configured:
//!   - `rotoken`  → `data:read`
//!   - `rwtoken`  → `data:write`
//!   - `super`    → `superuser`
//!
//! All existing tests (api.rs, schema.rs, http2.rs) build WITHOUT authz → open
//! mode → they keep passing tokenless.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use bluedb_ha::{LeaseProvider, LocalLeaseProvider, SystemClock, WriterController};
use bluedb_server::{authz::{Authz, Scope}, build_app, AppState};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use tower::ServiceExt;

const TTL: Duration = Duration::from_secs(30);
const MARGIN: Duration = Duration::from_secs(5);

fn node(node_id: &str, store: Arc<dyn ObjectStore>, lease: Arc<dyn LeaseProvider>) -> AppState {
    let writer = Arc::new(WriterController::new(node_id, lease, Arc::new(SystemClock), TTL, MARGIN));
    AppState::new(store, "bluedb", writer)
}

/// Build an app with a three-token authz map and admin SQL enabled (for setup).
async fn make_app_authz() -> Router {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut authz = Authz::default();
    authz.insert("rotoken".into(), HashSet::from([Scope::DataRead]));
    authz.insert("rwtoken".into(), HashSet::from([Scope::DataWrite]));
    authz.insert("super".into(), HashSet::from([Scope::Superuser]));

    let state = node("test-node", store, Arc::new(LocalLeaseProvider::new()))
        .with_admin_sql_enabled(true)
        .with_authz(authz);
    state.promote().await.expect("promote");
    build_app(state)
}

/// Send a request and return (status, json_body). Optionally attach a bearer token.
async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    json_body: Option<Value>,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(tok) = token {
        builder = builder.header("authorization", format!("Bearer {tok}"));
    }
    let request = match json_body {
        Some(v) => builder
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&v).unwrap()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, body)
}

// ---------------------------------------------------------------------------
// Case 1: GET /tables/t with NO token → 401
// ---------------------------------------------------------------------------
#[tokio::test]
async fn get_table_no_token_is_401() {
    let app = make_app_authz().await;
    let (status, _) = call(&app, "GET", "/tables/t", None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "no token should yield 401");
}

// ---------------------------------------------------------------------------
// Case 2: GET /tables/t with rotoken → reaches handler (NOT 401/403)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn get_table_with_read_token_reaches_handler() {
    let app = make_app_authz().await;
    // Set up a table first using super token so the read has something to hit.
    let (setup_status, _) = call(
        &app,
        "POST",
        "/admin/sql",
        Some(json!({ "sql": "CREATE TABLE t (id INTEGER PRIMARY KEY);" })),
        Some("super"),
    )
    .await;
    assert_eq!(setup_status, StatusCode::OK, "setup failed");

    let (status, _) = call(&app, "GET", "/tables/t", None, Some("rotoken")).await;
    assert_ne!(status, StatusCode::UNAUTHORIZED, "rotoken should not get 401");
    assert_ne!(status, StatusCode::FORBIDDEN, "rotoken should not get 403");
}

// ---------------------------------------------------------------------------
// Case 3: POST /tables/t (insert) with rotoken → 403 (read-only)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn insert_with_read_token_is_403() {
    let app = make_app_authz().await;
    let (status, _) = call(
        &app,
        "POST",
        "/tables/t",
        Some(json!({ "id": 1 })),
        Some("rotoken"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "rotoken on insert should be 403");
}

// ---------------------------------------------------------------------------
// Case 4: POST /tables/t with rwtoken → reaches handler (NOT 401/403)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn insert_with_write_token_reaches_handler() {
    let app = make_app_authz().await;
    // Create the table first using the super token.
    let (setup_status, _) = call(
        &app,
        "POST",
        "/admin/sql",
        Some(json!({ "sql": "CREATE TABLE t (id INTEGER PRIMARY KEY);" })),
        Some("super"),
    )
    .await;
    assert_eq!(setup_status, StatusCode::OK, "setup failed");

    let (status, _) = call(
        &app,
        "POST",
        "/tables/t",
        Some(json!({ "id": 1 })),
        Some("rwtoken"),
    )
    .await;
    assert_ne!(status, StatusCode::UNAUTHORIZED, "rwtoken on insert should not be 401");
    assert_ne!(status, StatusCode::FORBIDDEN, "rwtoken on insert should not be 403");
}

// ---------------------------------------------------------------------------
// Case 5a: POST /admin/sql with rwtoken → 403
// ---------------------------------------------------------------------------
#[tokio::test]
async fn admin_sql_with_write_token_is_403() {
    let app = make_app_authz().await;
    let (status, _) = call(
        &app,
        "POST",
        "/admin/sql",
        Some(json!({ "sql": "SELECT 1;" })),
        Some("rwtoken"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "rwtoken on /admin/sql should be 403");
}

// ---------------------------------------------------------------------------
// Case 5b: POST /admin/sql with super → reaches handler (NOT 401/403)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn admin_sql_with_super_token_reaches_handler() {
    let app = make_app_authz().await;
    let (status, _) = call(
        &app,
        "POST",
        "/admin/sql",
        Some(json!({ "sql": "CREATE TABLE things (id INTEGER PRIMARY KEY);" })),
        Some("super"),
    )
    .await;
    assert_ne!(status, StatusCode::UNAUTHORIZED, "super on /admin/sql should not be 401");
    assert_ne!(status, StatusCode::FORBIDDEN, "super on /admin/sql should not be 403");
}

// ---------------------------------------------------------------------------
// Case 6: GET /health with no token → 200 (public, unaffected)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn health_is_public() {
    let app = make_app_authz().await;
    let (status, body) = call(&app, "GET", "/health", None, None).await;
    assert_eq!(status, StatusCode::OK, "health should be 200 regardless of authz");
    assert_eq!(body, json!({ "status": "ok" }));
}
