//! Integration tests for the `/collections` API.

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

async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    json_body: Option<Value>,
) -> (StatusCode, Value) {
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

#[tokio::test]
async fn insert_single_doc_generates_id_and_returns_count() {
    let app = app().await;
    let (status, body) = call(
        &app,
        "POST",
        "/collections/people/insert",
        Some(json!({ "documents": [{ "name": "ada", "age": 36 }] })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    let inserted_count = body["insertedCount"].as_i64().expect("insertedCount missing");
    assert_eq!(inserted_count, 1);

    let ids = body["insertedIds"].as_array().expect("insertedIds missing");
    assert_eq!(ids.len(), 1);

    let id_str = ids[0].as_str().expect("insertedIds[0] is not a string");
    assert_eq!(id_str.len(), 24, "expected 24-char _id, got '{id_str}'");
}

#[tokio::test]
async fn insert_preserves_provided_id() {
    let app = app().await;
    let (status, body) = call(
        &app,
        "POST",
        "/collections/things/insert",
        Some(json!({ "documents": [{ "_id": "my-custom-id", "val": 1 }] })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    let ids = body["insertedIds"].as_array().expect("insertedIds missing");
    assert_eq!(ids[0].as_str().unwrap(), "my-custom-id");
}

#[tokio::test]
async fn insert_multiple_docs_returns_all_ids() {
    let app = app().await;
    let (status, body) = call(
        &app,
        "POST",
        "/collections/items/insert",
        Some(json!({ "documents": [{ "x": 1 }, { "x": 2 }, { "x": 3 }] })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    assert_eq!(body["insertedCount"].as_i64().unwrap(), 3);
    let ids = body["insertedIds"].as_array().unwrap();
    assert_eq!(ids.len(), 3);
    // All generated ids are distinct.
    let id_strs: Vec<&str> = ids.iter().map(|v| v.as_str().unwrap()).collect();
    let unique: std::collections::HashSet<_> = id_strs.iter().collect();
    assert_eq!(unique.len(), 3, "ids should be distinct: {id_strs:?}");
}

#[tokio::test]
async fn insert_empty_documents_returns_zero() {
    let app = app().await;
    let (status, body) = call(
        &app,
        "POST",
        "/collections/empty_test/insert",
        Some(json!({ "documents": [] })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    assert_eq!(body["insertedCount"].as_i64().unwrap(), 0);
}

#[tokio::test]
async fn insert_missing_documents_field_is_400() {
    let app = app().await;
    let (status, _body) = call(
        &app,
        "POST",
        "/collections/people/insert",
        Some(json!({ "docs": [] })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn second_insert_reuses_existing_table() {
    let app = app().await;
    // First insert creates the table.
    let (s1, _) = call(
        &app,
        "POST",
        "/collections/reuse/insert",
        Some(json!({ "documents": [{ "n": 1 }] })),
    )
    .await;
    assert_eq!(s1, StatusCode::OK);

    // Second insert must NOT fail with "table already exists".
    let (s2, body) = call(
        &app,
        "POST",
        "/collections/reuse/insert",
        Some(json!({ "documents": [{ "n": 2 }] })),
    )
    .await;
    assert_eq!(s2, StatusCode::OK, "second insert failed: {body}");
    assert_eq!(body["insertedCount"].as_i64().unwrap(), 1);
}
