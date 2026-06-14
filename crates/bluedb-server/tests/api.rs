//! HTTP API tests — drive the router via `tower::ServiceExt::oneshot`, no socket.

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

/// An [`AppState`] over a fresh in-memory store with its own (or a shared)
/// lease provider.
fn node(node_id: &str, store: Arc<dyn ObjectStore>, lease: Arc<dyn LeaseProvider>) -> AppState {
    let writer = Arc::new(WriterController::new(node_id, lease, Arc::new(SystemClock), TTL, MARGIN));
    AppState::new(store, "bluedb", writer)
}

/// Build an app; `promote` decides whether the node starts as the active writer
/// (and thus has a bound database). A non-promoted node is unbound (`503`).
async fn make_app(promote: bool) -> Router {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = node("test-node", store, Arc::new(LocalLeaseProvider::new()));
    if promote {
        state.promote().await.expect("promote");
    }
    build_app(state)
}

/// The common case: an active (writable) node.
async fn app() -> Router {
    make_app(true).await
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

#[tokio::test]
async fn unbound_passive_node_refuses_writes_and_answers_status() {
    let app = make_app(false).await; // passive, never promoted → no bound database

    // Writes are refused with 503 (not the active writer).
    let (status, _) = sql(&app, "CREATE TABLE t (id INTEGER PRIMARY KEY);").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let (status, _) = call(&app, "POST", "/tables/t", Some(json!({ "id": 1 }))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    // Liveness and status always answer.
    let (status, _) = call(&app, "GET", "/health", None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = call(&app, "GET", "/admin/status", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["role"], json!("passive"));
    assert_eq!(body["epoch"], json!(null));
}

#[tokio::test]
async fn failover_new_writer_sees_prior_data_and_can_write() {
    // Two nodes over ONE shared object store + ONE shared lease — an in-process
    // stand-in for a cluster, exercising the role-swap + SlateDB epoch fencing.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let lease: Arc<dyn LeaseProvider> = Arc::new(LocalLeaseProvider::new());
    let a = node("a", store.clone(), lease.clone());
    let b = node("b", store.clone(), lease.clone());

    // a is the writer: create a table + a row.
    a.promote().await.expect("a promotes");
    let app_a = build_app(a.clone());
    assert_eq!(
        sql(&app_a, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);").await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(&app_a, "POST", "/tables/t", Some(json!({ "id": 1, "v": 10 }))).await.1,
        json!({ "inserted": 1 })
    );

    // a steps down (flushing for a clean handoff); b's HA tick takes the freed
    // lease and opens the writer database (bumping the epoch).
    a.demote().await.expect("a demotes");
    b.ha_tick().await;
    let app_b = build_app(b.clone());

    // The NEW writer sees a's committed data...
    let (status, body) = call(&app_b, "GET", "/tables/t?order=id.asc", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([{ "id": 1, "v": 10 }]), "new writer inherited the data");

    // ...and can write.
    assert_eq!(
        call(&app_b, "POST", "/tables/t", Some(json!({ "id": 2, "v": 20 }))).await.1,
        json!({ "inserted": 1 })
    );

    // The demoted node is now a read replica: writes are refused.
    assert_eq!(
        call(&app_a, "POST", "/tables/t", Some(json!({ "id": 3, "v": 30 }))).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn promote_enables_writes_and_demote_disables_them() {
    let app = make_app(false).await; // start passive

    // Promote via the admin endpoint → active, fencing epoch 1.
    let (status, body) = call(&app, "POST", "/admin/promote", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["role"], json!("active"));
    assert_eq!(body["epoch"], json!(1));

    // Writes now succeed.
    let (status, _) = sql(&app, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);").await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = call(&app, "POST", "/tables/t", Some(json!({ "id": 1, "v": 10 }))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "inserted": 1 }));

    // Demote → writes blocked again, but the committed read still works.
    let (status, body) = call(&app, "POST", "/admin/demote", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["role"], json!("passive"));

    let (status, _) = call(&app, "POST", "/tables/t", Some(json!({ "id": 2, "v": 20 }))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    let (status, body) = call(&app, "GET", "/tables/t?order=id.asc", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([{ "id": 1, "v": 10 }]), "reads continue while passive");
}

// --- /ledger/* --------------------------------------------------------------

#[tokio::test]
async fn ledger_create_accounts_transfer_and_lookup() {
    let app = app().await;

    // Two accounts (u128 ids/values as strings).
    let (status, body) = call(
        &app,
        "POST",
        "/ledger/accounts",
        Some(json!([
            { "id": "1", "ledger": 700, "code": 1 },
            { "id": "2", "ledger": 700, "code": 1 }
        ])),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["results"][0]["result"], json!("created"));
    assert_eq!(body["results"][1]["result"], json!("created"));
    assert_eq!(body["results"][0]["id"], json!("1"));

    // A transfer of 100 from 1 -> 2.
    let (status, body) = call(
        &app,
        "POST",
        "/ledger/transfers",
        Some(json!([
            { "id": "10", "debit_account_id": "1", "credit_account_id": "2", "amount": "100", "ledger": 700, "code": 1 }
        ])),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["results"][0]["result"], json!("created"));

    // Canonical lookups reflect the transfer.
    let (status, body) = call(&app, "GET", "/ledger/accounts/1", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["debits_posted"], json!("100"));
    assert_eq!(body["credits_posted"], json!("0"));

    let (status, body) = call(&app, "GET", "/ledger/accounts/2", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["credits_posted"], json!("100"));

    let (status, body) = call(&app, "GET", "/ledger/transfers/10", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["amount"], json!("100"));
    assert_eq!(body["debit_account_id"], json!("1"));
}

#[tokio::test]
async fn ledger_idempotent_and_error_result_codes() {
    let app = app().await;
    call(
        &app,
        "POST",
        "/ledger/accounts",
        Some(json!([{ "id": "1", "ledger": 700, "code": 1 }, { "id": "2", "ledger": 700, "code": 1 }])),
    )
    .await;

    // First transfer created; an identical retry is idempotent (`exists`).
    let xfer = json!([{ "id": "10", "debit_account_id": "1", "credit_account_id": "2", "amount": "100", "ledger": 700, "code": 1 }]);
    let (_, body) = call(&app, "POST", "/ledger/transfers", Some(xfer.clone())).await;
    assert_eq!(body["results"][0]["result"], json!("created"));
    let (_, body) = call(&app, "POST", "/ledger/transfers", Some(xfer)).await;
    assert_eq!(body["results"][0]["result"], json!("exists"));

    // A transfer to a non-existent credit account → the TB result code.
    let (_, body) = call(
        &app,
        "POST",
        "/ledger/transfers",
        Some(json!([{ "id": "11", "debit_account_id": "1", "credit_account_id": "999", "amount": "5", "ledger": 700, "code": 1 }])),
    )
    .await;
    assert_eq!(body["results"][0]["result"], json!("credit_account_not_found"));

    // Unknown account → 404.
    let (status, _) = call(&app, "GET", "/ledger/accounts/12345", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ledger_projection_is_visible_through_sql_and_conserves() {
    let app = app().await;
    call(
        &app,
        "POST",
        "/ledger/accounts",
        Some(json!([{ "id": "1", "ledger": 700, "code": 1 }, { "id": "2", "ledger": 700, "code": 1 }])),
    )
    .await;
    call(
        &app,
        "POST",
        "/ledger/transfers",
        Some(json!([{ "id": "10", "debit_account_id": "1", "credit_account_id": "2", "amount": "100", "ledger": 700, "code": 1 }])),
    )
    .await;

    // The projection is queryable over /sql; u128 columns come back as strings.
    let (status, body) = sql(&app, "SELECT debits_posted FROM ledger_accounts WHERE id = 1").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body, json!([{ "debits_posted": "100" }]));

    // Conservation: total debits == total credits across the ledger.
    let (_, body) = sql(
        &app,
        "SELECT SUM(debits_posted) AS d, SUM(credits_posted) AS c FROM ledger_accounts",
    )
    .await;
    assert_eq!(body[0]["d"], body[0]["c"], "debits and credits must balance: {body}");
}

#[tokio::test]
async fn ledger_writes_refused_on_passive_node() {
    let app = make_app(false).await; // not promoted → unbound
    let (status, _) = call(
        &app,
        "POST",
        "/ledger/transfers",
        Some(json!([{ "id": "1", "debit_account_id": "1", "credit_account_id": "2", "amount": "1", "ledger": 700 }])),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
