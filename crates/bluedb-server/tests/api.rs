//! HTTP API tests — drive the router via `tower::ServiceExt::oneshot`, no socket.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use bluedb_server::{build_app, AppState};
use bluedb_sql::Database;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use tower::ServiceExt;

async fn app() -> Router {
    let db = Db::open("bluedb-server-test", Arc::new(InMemory::new()))
        .await
        .expect("open slatedb");
    build_app(AppState::new(Database::new(Arc::new(db))))
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

/// Send raw SQL text to `POST /sql`.
async fn sql(app: &Router, statement: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/sql")
        .body(Body::from(statement.to_owned()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn health_is_ok() {
    let app = app().await;
    let (status, body) = call(&app, "GET", "/health", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "status": "ok" }));
}

#[tokio::test]
async fn full_crud_round_trip() {
    let app = app().await;

    // DDL via the raw /sql endpoint.
    let (status, _) = sql(
        &app,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // INSERT via POST (array of objects).
    let (status, body) = call(
        &app,
        "POST",
        "/tables/users",
        Some(json!([
            { "id": 1, "name": "alice", "age": 30 },
            { "id": 2, "name": "bob", "age": 25 }
        ])),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "inserted": 2 }));

    // SELECT via GET with a PostgREST filter + projection + order.
    let (status, body) = call(
        &app,
        "GET",
        "/tables/users?select=name&age=gt.26&order=name.asc",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([{ "name": "alice" }]), "only alice (age 30 > 26)");

    // PATCH via query filter + JSON assignments.
    let (status, body) = call(
        &app,
        "PATCH",
        "/tables/users?id=eq.2",
        Some(json!({ "name": "BOB" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "updated": 1 }));

    // DELETE via query filter.
    let (status, body) = call(&app, "DELETE", "/tables/users?id=eq.1", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "deleted": 1 }));

    // Final state: only the updated bob remains.
    let (status, body) = call(&app, "GET", "/tables/users?order=id.asc", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([{ "id": 2, "name": "BOB", "age": 25 }]));
}

#[tokio::test]
async fn bad_identifier_is_a_400() {
    let app = app().await;
    // A malicious table name is rejected by the REST translator → 400, not 500.
    let (status, body) = call(&app, "GET", "/tables/users;%20DROP%20TABLE%20x;--", None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.get("error").is_some(), "error body: {body}");
}

#[tokio::test]
async fn unknown_table_select_is_a_400() {
    let app = app().await;
    // Selecting a table that doesn't exist is a SQL error → 400 (client error).
    let (status, _) = call(&app, "GET", "/tables/ghost", None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
