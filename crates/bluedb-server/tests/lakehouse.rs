//! End-to-end: writes over HTTP → CDC → seal → the table shows up in the
//! read-only Iceberg REST Catalog, and `PRAGMA lakehouse_mirror` toggles it.

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

/// A promoted node + a handle to its `AppState` (to force a deterministic seal).
async fn make_state() -> AppState {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arc::new(WriterController::new(
        "test-node",
        Arc::new(LocalLeaseProvider::new()) as Arc<dyn LeaseProvider>,
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ));
    let state = AppState::new(store, "bluedb", writer).with_admin_sql_enabled(true);
    state.promote().await.expect("promote");
    state
}

async fn call(app: &Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let builder = Request::builder().method(method).uri(uri);
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
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, json)
}

#[tokio::test]
async fn http_writes_seal_into_iceberg_and_appear_in_rest_catalog() {
    let state = make_state().await;
    let app = build_app(state.clone());

    // Turn the mirror on (opt-out default) via PRAGMA over /sql.
    let (s, body) = call(&app, "POST", "/sql", Some(json!({"sql": "PRAGMA lakehouse_mirror = on"}))).await;
    assert!(s.is_success(), "pragma on: {s} {body}");

    // Create a table (PK required by the schema regime) + write through /sql.
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

    for (id, body) in [(1, "a"), (2, "b"), (3, "c")] {
        let (s, _) = call(
            &app,
            "POST",
            "/sql",
            Some(json!({"sql": format!("INSERT INTO docs VALUES ({id}, '{body}')")})),
        )
        .await;
        assert!(s.is_success(), "insert {id}: {s}");
    }
    let (s, _) = call(&app, "POST", "/sql", Some(json!({"sql": "DELETE FROM docs WHERE id = 2"}))).await;
    assert!(s.is_success(), "delete: {s}");

    // Force a deterministic seal (the background loop would also fire it).
    state.seal_now().await.expect("seal");

    // REST catalog: the table is discoverable.
    let (s, tables) = call(&app, "GET", "/catalog/v1/namespaces/default/tables", None).await;
    assert!(s.is_success(), "list tables: {s} {tables}");
    let names: Vec<&str> = tables["identifiers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"docs"), "catalog should list docs, got {names:?}");

    // loadTable returns a metadata location + schema with our columns.
    let (s, load) = call(&app, "GET", "/catalog/v1/namespaces/default/tables/docs", None).await;
    assert!(s.is_success(), "load table: {s} {load}");
    assert!(load["metadata-location"].as_str().unwrap().ends_with(".metadata.json"));
    let metadata = load["metadata"].to_string();
    assert!(metadata.contains("\"id\""), "schema has id: {metadata}");
    assert!(metadata.contains("\"body\""), "schema has body: {metadata}");
}

#[tokio::test]
async fn pragma_off_means_no_mirror() {
    let state = make_state().await;
    let app = build_app(state.clone());

    // Opt-in default (off) — and a table is created + written but never mirrored.
    let (s, _) = call(&app, "POST", "/sql", Some(json!({"sql": "PRAGMA lakehouse_mirror = off"}))).await;
    assert!(s.is_success(), "pragma off: {s}");
    let (s, _) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "secret",
            "columns": [{"name": "id", "type": "INTEGER", "primary_key": true}]
        })),
    )
    .await;
    assert!(s.is_success(), "create table: {s}");
    let (s, _) = call(&app, "POST", "/sql", Some(json!({"sql": "INSERT INTO secret VALUES (1)"}))).await;
    assert!(s.is_success(), "insert: {s}");

    state.seal_now().await.unwrap();

    let (s, tables) = call(&app, "GET", "/catalog/v1/namespaces/default/tables", None).await;
    assert!(s.is_success());
    assert!(
        tables["identifiers"].as_array().unwrap().is_empty(),
        "no table should be mirrored when off: {tables}"
    );
}
