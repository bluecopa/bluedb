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
/// `admin_sql` enables `POST /admin/sql` (needed for DDL setup in tests).
async fn make_app_opts(promote: bool, admin_sql: bool) -> Router {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = node("test-node", store, Arc::new(LocalLeaseProvider::new()))
        .with_admin_sql_enabled(admin_sql);
    if promote {
        state.promote().await.expect("promote");
    }
    build_app(state)
}

/// Build an app (promoted + admin SQL enabled for DDL setup).
async fn make_app(promote: bool) -> Router {
    make_app_opts(promote, true).await
}

/// The common case: an active (writable) node with admin SQL enabled for DDL.
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

/// Send SQL (no params) to `POST /admin/sql` (DDL + arbitrary).
async fn sql_admin(app: &Router, statement: &str) -> (StatusCode, Value) {
    call(app, "POST", "/admin/sql", Some(json!({ "sql": statement }))).await
}

/// Like [`call`] but sets `Prefer: return=representation`.
async fn call_prefer(
    app: &Router,
    method: &str,
    uri: &str,
    json_body: Option<Value>,
) -> (StatusCode, Value) {
    let builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("prefer", "return=representation");
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

/// Send a parameterized statement to `POST /sql` (non-DDL only).
#[allow(dead_code)]
async fn sql_dml(app: &Router, statement: &str, params: serde_json::Value) -> (StatusCode, Value) {
    call(app, "POST", "/sql", Some(json!({ "sql": statement, "params": params }))).await
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

    // DDL via /admin/sql.
    let (status, _) = sql_admin(
        &app,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Index the columns we filter/sort on — the guardrail rejects a filter or
    // ORDER BY on a non-indexed column (it would be a full scan / memory sort).
    let (status, _) = sql_admin(&app, "CREATE INDEX users_age ON users (age);").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = sql_admin(&app, "CREATE INDEX users_name ON users (name);").await;
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
async fn json_column_round_trips_as_real_json() {
    let app = app().await;
    let (status, _) =
        sql_admin(&app, "CREATE TABLE docs (id INTEGER PRIMARY KEY, data JSON);").await;
    assert_eq!(status, StatusCode::OK);

    // POST a row whose JSON column holds a real object (today this 400'd as
    // "expected a scalar value").
    let row = json!({ "id": 1, "data": { "k": "v", "n": 3, "tags": [1, 2] } });
    let (status, body) = call(&app, "POST", "/tables/docs", Some(row.clone())).await;
    assert_eq!(status, StatusCode::OK, "insert failed: {body}");

    // GET it back: `data` is a real JSON object, not an escaped string.
    let (status, body) = call(&app, "GET", "/tables/docs?id=eq.1", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([row]));
    // Specifically, it must be an object — not the string "{\"k\":\"v\",...}".
    assert!(body[0]["data"].is_object(), "data should be a JSON object: {body}");
}

#[tokio::test]
async fn arbitrary_and_json_filters_on_tables_route_to_analytical_engine() {
    let app = app().await;

    // Enable the Iceberg mirror so the analytical engine has the tenant's data.
    let (status, _) = call(&app, "POST", "/sql",
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"}))).await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = sql_admin(
        &app,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT, data JSON)",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Insert rows; `label` is NOT indexed, `data` is JSON.
    for (id, label, status_field) in [(1, "alpha", "active"), (2, "beta", "idle"), (3, "alpha", "active")] {
        let (s, body) = call(
            &app,
            "POST",
            "/tables/items",
            Some(json!({"id": id, "label": label, "data": {"status": status_field}})),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "insert {id}: {body}");
    }

    // A filter on the NON-indexed `label` 400s on the GlueSQL fast path (guardrail).
    // It must now route to the analytical engine and return rows (doc ask #4).
    let (status, body) = call(&app, "GET", "/tables/items?label=eq.alpha&order=id.asc", None).await;
    assert_eq!(status, StatusCode::OK, "label filter should route, got: {body}");
    let ids: Vec<i64> = body
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![1, 3], "both alpha rows: {body}");

    // A JSON-path filter also routes; `data` comes back as a real object. `>` is
    // percent-encoded as a conformant HTTP client would send it (`%3E%3E`).
    let (status, body) =
        call(&app, "GET", "/tables/items?data-%3E%3Estatus=eq.active&order=id.asc", None).await;
    assert_eq!(status, StatusCode::OK, "json-path filter should route, got: {body}");
    let rows = body.as_array().expect("rows");
    assert_eq!(rows.len(), 2, "two active rows: {body}");
    assert!(rows[0]["data"].is_object(), "data re-inflated to an object: {body}");
    assert_eq!(rows[0]["data"]["status"], json!("active"));

    // A PK point read still takes the GlueSQL fast path and works unchanged.
    let (status, body) = call(&app, "GET", "/tables/items?id=eq.2", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().expect("rows").len(), 1);
    assert_eq!(body[0]["label"], json!("beta"));
}

#[tokio::test]
async fn prefer_representation_returns_affected_rows() {
    let app = app().await;
    let (status, _) = sql_admin(&app, "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)").await;
    assert_eq!(status, StatusCode::OK);

    // INSERT + representation → the inserted row(s), not a count.
    let (status, body) =
        call_prefer(&app, "POST", "/tables/t", Some(json!({"id": 1, "label": "alpha"}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!([{"id": 1, "label": "alpha"}]), "insert repr: {body}");

    // PATCH + representation → the updated row.
    let (status, body) =
        call_prefer(&app, "PATCH", "/tables/t?id=eq.1", Some(json!({"label": "beta"}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([{"id": 1, "label": "beta"}]), "update repr: {body}");

    // DELETE + representation → the removed row (its pre-delete state).
    let (status, body) = call_prefer(&app, "DELETE", "/tables/t?id=eq.1", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([{"id": 1, "label": "beta"}]), "delete repr: {body}");

    // The row is really gone.
    let (_, body) = call(&app, "GET", "/tables/t?id=eq.1", None).await;
    assert_eq!(body, json!([]), "row should be deleted: {body}");

    // Without the header, the count shape is unchanged.
    let (_, body) = call(&app, "POST", "/tables/t", Some(json!({"id": 2, "label": "g"}))).await;
    assert_eq!(body, json!({"inserted": 1}), "no-prefer insert still returns a count: {body}");
}

#[tokio::test]
async fn unknown_table_select_is_a_404() {
    let app = app().await;
    // Selecting a table that doesn't exist → 404 NOT_FOUND (ask #7 taxonomy).
    let (status, body) = call(&app, "GET", "/tables/ghost", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], json!("NOT_FOUND"), "{body}");
}

#[tokio::test]
async fn errors_carry_structured_codes() {
    let app = app().await;
    let (status, _) = sql_admin(&app, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
    assert_eq!(status, StatusCode::OK);

    // A duplicate primary key → 409 with a stable code, not a parsed-prose 400.
    let (s, _) = call(&app, "POST", "/tables/t", Some(json!({"id": 1, "v": "a"}))).await;
    assert_eq!(s, StatusCode::OK);
    let (status, body) = call(&app, "POST", "/tables/t", Some(json!({"id": 1, "v": "b"}))).await;
    assert_eq!(status, StatusCode::CONFLICT, "dup pk: {body}");
    assert_eq!(body["code"], json!("UNIQUE_VIOLATION"), "{body}");
}

#[tokio::test]
async fn unbound_passive_node_refuses_writes_and_answers_status() {
    let app = make_app(false).await; // passive, never promoted → no bound database

    // Writes are refused with 503 (not the active writer).
    let (status, _) = sql_admin(&app, "CREATE TABLE t (id INTEGER PRIMARY KEY);").await;
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
    let a = node("a", store.clone(), lease.clone()).with_admin_sql_enabled(true);
    let b = node("b", store.clone(), lease.clone());

    // a is the writer: create a table + a row.
    a.promote().await.expect("a promotes");
    let app_a = build_app(a.clone());
    assert_eq!(
        sql_admin(&app_a, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);").await.0,
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
    let (status, _) = sql_admin(&app, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);").await;
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

#[tokio::test]
async fn lease_held_but_writer_db_unbound_reports_passive_and_refuses_writes() {
    // Reproduce the failover window INSIDE `promote()`: the lease is acquired (the
    // controller goes Active) a beat BEFORE the writer `Db` is opened and swapped
    // into `inner.db`. A node in this window is still bound to its pre-failover
    // reader, so it must NOT advertise itself as the active writer — otherwise a
    // routing client (e.g. the Jepsen counter) latches it as leader and serves
    // stale reads from the lagging reader, the root cause of the lost-acked-
    // increment finding under `kill`. "Active" must mean *lease held AND writer
    // Db bound*.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = node("gap", store, Arc::new(LocalLeaseProvider::new()))
        .with_admin_sql_enabled(true);
    // Take the lease at the controller level ONLY — deliberately skip
    // `AppState::promote`, which is what would bind the writer `Db`.
    state.writer().promote().await.expect("acquire lease");
    let app = build_app(state);

    // Despite holding the lease, status downgrades to passive (no writer Db yet).
    let (status, body) = call(&app, "GET", "/admin/status", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["role"],
        json!("passive"),
        "lease held but writer Db unbound must report passive, not active"
    );

    // And writer-gated traffic is refused (`require_active` sees the unbound writer).
    let (status, _) = call(&app, "POST", "/tables/t", Some(json!({ "id": 1, "v": 10 }))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

// --- new surface-enforcement tests ------------------------------------------

/// `POST /sql` must reject DDL (CREATE TABLE) with 400.
#[tokio::test]
async fn sql_rejects_ddl_with_400() {
    let app = app().await;
    let (status, body) =
        call(&app, "POST", "/sql", Some(json!({ "sql": "CREATE TABLE t (id INTEGER PRIMARY KEY);" }))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "DDL on /sql should be 400; body: {body}");
    let err = body.get("error").expect("error field in body");
    assert!(err.as_str().unwrap_or("").contains("not allowed"), "error should mention 'not allowed': {err}");
}

/// `POST /admin/sql` returns 404 when the admin flag is off (default).
#[tokio::test]
async fn admin_sql_disabled_returns_404() {
    // Build an app WITHOUT admin SQL enabled (the default).
    let app = make_app_opts(true, false).await;
    let (status, body) =
        call(&app, "POST", "/admin/sql", Some(json!({ "sql": "CREATE TABLE t (id INTEGER PRIMARY KEY);" }))).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "admin/sql off → 404; body: {body}");
}

/// `POST /admin/sql` when enabled can run DDL successfully.
#[tokio::test]
async fn admin_sql_enabled_runs_ddl() {
    // app() already enables admin SQL.
    let app = app().await;
    let (status, body) =
        sql_admin(&app, "CREATE TABLE things (id INTEGER PRIMARY KEY, label TEXT);").await;
    assert_eq!(status, StatusCode::OK, "DDL on /admin/sql should succeed; body: {body}");
    // Insert a row and read it back to confirm the table exists.
    let (status, _) = call(&app, "POST", "/tables/things", Some(json!({ "id": 1, "label": "hello" }))).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = call(&app, "GET", "/tables/things", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([{ "id": 1, "label": "hello" }]));
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
    let (status, body) =
        sql_dml(&app, "SELECT debits_posted FROM ledger_accounts WHERE id = 1", json!([])).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body, json!([{ "debits_posted": "100" }]));

    // Conservation: total debits == total credits across the ledger.
    let (_, body) = sql_dml(
        &app,
        "SELECT SUM(debits_posted) AS d, SUM(credits_posted) AS c FROM ledger_accounts",
        json!([]),
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
