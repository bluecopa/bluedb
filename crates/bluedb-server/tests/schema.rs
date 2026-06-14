//! Integration tests for the structured `/schema` DDL API.
//!
//! Drives the router via `tower::ServiceExt::oneshot`, no socket.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use bluedb_ha::{LeaseProvider, LocalLeaseProvider, SystemClock, WriterController};
use bluedb_server::{build_app, AppState};
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

async fn app() -> Router {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = node("test-node", store, Arc::new(LocalLeaseProvider::new()));
    state.promote().await.expect("promote");
    build_app(state)
}

async fn call(app: &Router, method: &str, uri: &str, json_body: Option<Value>) -> (StatusCode, Value) {
    let builder = Request::builder().method(method).uri(uri);
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
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body)
}

/// 1. Create table via /schema/tables, then DML round-trip confirms it exists.
#[tokio::test]
async fn create_table_and_dml_round_trip() {
    let app = app().await;

    // Create the table via the schema endpoint.
    let (status, body) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "docs",
            "columns": [
                { "name": "id", "type": "INTEGER", "primaryKey": true },
                { "name": "body", "type": "TEXT" }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create table should succeed; body: {body}");

    // Insert a row via the DML surface to confirm the table exists.
    let (status, body) = call(&app, "POST", "/tables/docs", Some(json!({ "id": 1, "body": "x" }))).await;
    assert_eq!(status, StatusCode::OK, "insert should succeed; body: {body}");
    assert_eq!(body, json!({ "inserted": 1 }));

    // Read the row back.
    let (status, body) = call(&app, "GET", "/tables/docs", None).await;
    assert_eq!(status, StatusCode::OK, "select should succeed; body: {body}");
    assert_eq!(body, json!([{ "id": 1, "body": "x" }]));
}

/// 2. Create index on an existing table.
#[tokio::test]
async fn create_index_succeeds() {
    let app = app().await;

    // Create the table first.
    let (status, _) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "docs",
            "columns": [
                { "name": "id", "type": "INTEGER", "primaryKey": true },
                { "name": "body", "type": "TEXT" }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Create an index.
    let (status, body) = call(
        &app,
        "POST",
        "/schema/tables/docs/indexes",
        Some(json!({ "name": "idx_body", "columns": ["body"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create index should succeed; body: {body}");
}

/// 3. Drop an index.
#[tokio::test]
async fn drop_index_succeeds() {
    let app = app().await;

    // Setup: table + index.
    let (status, _) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "docs",
            "columns": [
                { "name": "id", "type": "INTEGER", "primaryKey": true },
                { "name": "body", "type": "TEXT" }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = call(
        &app,
        "POST",
        "/schema/tables/docs/indexes",
        Some(json!({ "name": "idx_body", "columns": ["body"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Drop the index.
    let (status, body) = call(&app, "DELETE", "/schema/tables/docs/indexes/idx_body", None).await;
    assert_eq!(status, StatusCode::OK, "drop index should succeed; body: {body}");
}

/// 4. Drop table makes the table gone.
#[tokio::test]
async fn drop_table_removes_table() {
    let app = app().await;

    // Create table + one row.
    let (status, _) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "docs",
            "columns": [
                { "name": "id", "type": "INTEGER", "primaryKey": true },
                { "name": "body", "type": "TEXT" }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = call(&app, "POST", "/tables/docs", Some(json!({ "id": 1, "body": "hi" }))).await;
    assert_eq!(status, StatusCode::OK);

    // Drop the table.
    let (status, body) = call(&app, "DELETE", "/schema/tables/docs", None).await;
    assert_eq!(status, StatusCode::OK, "drop table should succeed; body: {body}");

    // Afterwards, a SELECT on the gone table is a 400 (table not found).
    let (status, _) = call(&app, "GET", "/tables/docs", None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "select on dropped table should fail");
}

/// 5a. Validation: malicious table name is rejected with 400, nothing executed.
#[tokio::test]
async fn validation_rejects_malicious_table_name() {
    let app = app().await;

    let (status, body) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "; DROP TABLE users",
            "columns": [{ "name": "id", "type": "INTEGER", "primaryKey": true }]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "malicious name should be rejected; body: {body}");
    assert!(body.get("error").is_some(), "should have error field; body: {body}");
}

/// 5b. Validation: malicious column type is rejected with 400, nothing executed.
#[tokio::test]
async fn validation_rejects_malicious_column_type() {
    let app = app().await;

    let (status, body) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "safe_table",
            "columns": [{ "name": "id", "type": "TEXT); --", "primaryKey": true }]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "malicious type should be rejected; body: {body}");
    assert!(body.get("error").is_some(), "should have error field; body: {body}");

    // Confirm the table was NOT created (a subsequent SELECT errors with 400, not 200).
    let (status, _) = call(&app, "GET", "/tables/safe_table", None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "table must not have been created");
}
