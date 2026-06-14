//! FTS over the HTTP surface (Spec B §4.1/§4.3/§5): declare a fulltext index via
//! the `/schema` DDL surface, then a `@@` SELECT on `/sql` sees rows committed by
//! a prior insert with no explicit flush — read-your-writes over HTTP. A delete
//! is reflected immediately too. Drives the router via `oneshot`, no socket.

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

/// An active (promoted) node over a fresh in-memory store, admin SQL on.
async fn make_app(promote: bool) -> Router {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arc::new(WriterController::new(
        "test-node",
        Arc::new(LocalLeaseProvider::new()) as Arc<dyn LeaseProvider>,
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ));
    let state = AppState::new(store, "bluedb", writer).with_admin_sql_enabled(true);
    if promote {
        state.promote().await.expect("promote");
    }
    build_app(state)
}

/// Send a request (JSON body optional) and return `(status, json_body)`.
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
async fn fts_over_http_read_your_writes() {
    let app = make_app(true).await;

    // 1. create the table via the structured DDL surface (A3b).
    let (s, _) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "docs",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": true},
                {"name": "body", "type": "TEXT"}
            ]
        })),
    )
    .await;
    assert!(s.is_success(), "create table: {s}");

    // 2. declare a fulltext index (this increment's new endpoint).
    let (s, body) = call(
        &app,
        "POST",
        "/schema/tables/docs/fulltext-indexes",
        Some(json!({"column": "body", "analyzer": "english"})),
    )
    .await;
    assert!(s.is_success(), "create fulltext index: {s} {body}");

    // 3. insert via /sql — the observed connection maintains the live index.
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "INSERT INTO docs (id, body) VALUES (1, 'quarterly invoice overdue'), (2, 'weather sunny skies')"
        })),
    )
    .await;
    assert!(s.is_success(), "insert: {s}");

    // 4. @@ query over /sql sees the just-committed matching row (RYW over HTTP).
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')"
        })),
    )
    .await;
    assert!(s.is_success(), "@@ select: {s}");
    // exec_sql serializes a single Payload::Select as a JSON array of {label: value}.
    assert_eq!(body, json!([{ "id": 1 }]), "only the matching row id=1");

    // 5. delete row 1 via /sql, re-query → empty (delete reflected immediately).
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "DELETE FROM docs WHERE id = 1"})),
    )
    .await;
    assert!(s.is_success(), "delete: {s}");

    let (s, body2) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')"
        })),
    )
    .await;
    assert!(s.is_success(), "@@ select after delete: {s}");
    assert_eq!(body2, json!([]), "deleted row no longer matches");
}

#[tokio::test]
async fn fulltext_index_on_missing_table_is_400() {
    let app = make_app(true).await;
    let (s, body) = call(
        &app,
        "POST",
        "/schema/tables/ghost/fulltext-indexes",
        Some(json!({"column": "body"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "missing table → 400");
    assert!(body.get("error").is_some(), "error body: {body}");
}
