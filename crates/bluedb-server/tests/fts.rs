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
async fn durable_engine_serves_a_second_fulltext_index() {
    // After B4-4, `promote` binds a durable, reopened `FtsEngine` over the writer's
    // substrate (empty registry on a fresh in-memory DB → behaves like the old
    // in-memory engine). This proves the durable engine, bound through the
    // promote/demote lifecycle, serves MORE than one index definition end-to-end
    // over HTTP — i.e. the read-lock-clone wiring in `connection*`/`fts()` and the
    // `create_fulltext_index_auto`/`execute_fts` handlers all resolve the same
    // engine instance.
    let app = make_app(true).await;

    // Two tables, each with its own text column + fulltext index.
    for (table, col) in [("invoices", "memo"), ("emails", "subject")] {
        let (s, body) = call(
            &app,
            "POST",
            "/schema/tables",
            Some(json!({
                "name": table,
                "columns": [
                    {"name": "id", "type": "INTEGER", "primary_key": true},
                    {"name": col, "type": "TEXT"}
                ]
            })),
        )
        .await;
        assert!(s.is_success(), "create table {table}: {s} {body}");

        let (s, body) = call(
            &app,
            "POST",
            &format!("/schema/tables/{table}/fulltext-indexes"),
            Some(json!({"column": col, "analyzer": "english"})),
        )
        .await;
        assert!(s.is_success(), "create fulltext index on {table}: {s} {body}");
    }

    // Insert into both tables on the observed connection (maintains both live indexes).
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "INSERT INTO invoices (id, memo) VALUES (1, 'quarterly invoice overdue'), (2, 'paid on time')"
        })),
    )
    .await;
    assert!(s.is_success(), "insert invoices: {s}");
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "INSERT INTO emails (id, subject) VALUES (10, 'overdue payment reminder'), (11, 'welcome aboard')"
        })),
    )
    .await;
    assert!(s.is_success(), "insert emails: {s}");

    // A `@@` query on the SECOND index resolves through the same durable engine.
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "SELECT id FROM emails WHERE to_tsvector('english', subject) @@ plainto_tsquery('overdue payment')"
        })),
    )
    .await;
    assert!(s.is_success(), "@@ on second index: {s} {body}");
    assert_eq!(body, json!([{ "id": 10 }]), "second index returns its matching row");

    // And the first index still works (both defs coexist on one bound engine).
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "SELECT id FROM invoices WHERE to_tsvector('english', memo) @@ plainto_tsquery('invoice overdue')"
        })),
    )
    .await;
    assert!(s.is_success(), "@@ on first index: {s} {body}");
    assert_eq!(body, json!([{ "id": 1 }]), "first index still resolves");
}

#[tokio::test]
async fn trigram_like_over_http_read_your_writes() {
    let app = make_app(true).await;

    // 1. create the table.
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

    // 2. declare a TRIGRAM index (this increment's new endpoint).
    let (s, body) = call(
        &app,
        "POST",
        "/schema/tables/docs/trigram-indexes",
        Some(json!({"column": "body"})),
    )
    .await;
    assert!(s.is_success(), "create trigram index: {s} {body}");

    // 3. insert via /sql — the observed connection maintains the live trigram index.
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "INSERT INTO docs (id, body) VALUES (1, 'quarterly invoice overdue'), (2, 'weather sunny'), (3, 'overdue notice')"
        })),
    )
    .await;
    assert!(s.is_success(), "insert: {s}");

    // 4. LIKE '%overdue%' over /sql is trigram-accelerated AND correct (RYW over HTTP).
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "SELECT id FROM docs WHERE body LIKE '%overdue%'"})),
    )
    .await;
    assert!(s.is_success(), "LIKE select: {s} {body}");
    assert_eq!(body, json!([{ "id": 1 }, { "id": 3 }]), "matching rows 1 and 3");

    // 5. a column with NO trigram index: the engine passes the LIKE through
    //    unchanged, so it would be a full scan — which the guardrail now rejects.
    //    (Trigram-accelerate the column, or filter on the PK / an index, to read.)
    let (s, _) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "notes",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": true},
                {"name": "memo", "type": "TEXT"}
            ]
        })),
    )
    .await;
    assert!(s.is_success(), "create notes: {s}");
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "INSERT INTO notes (id, memo) VALUES (1, 'overdue payment'), (2, 'paid')"})),
    )
    .await;
    assert!(s.is_success(), "insert notes: {s}");
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "SELECT id FROM notes WHERE memo LIKE '%overdue%'"})),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::BAD_REQUEST,
        "LIKE on an un-indexed column is a full scan and must be rejected: {body}"
    );
}

#[tokio::test]
async fn trigram_index_on_missing_table_is_400() {
    let app = make_app(true).await;
    let (s, body) = call(
        &app,
        "POST",
        "/schema/tables/ghost/trigram-indexes",
        Some(json!({"column": "body"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "missing table → 400");
    assert!(body.get("error").is_some(), "error body: {body}");
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
