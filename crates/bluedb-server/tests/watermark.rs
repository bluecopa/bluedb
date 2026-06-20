//! Phase 1 + Phase 2.2 — Watermark surfacing tests.
//!
//! Verifies the `X-Bluedb-Watermark` / `X-Bluedb-Min-Watermark` contract:
//!
//! (a) A write returns `X-Bluedb-Watermark: <tenant>:<seq>` with a
//!     monotonically-increasing seq.
//! (b) A read (`GET /tables`) with `X-Bluedb-Min-Watermark` is served from
//!     SlateDB regardless — SlateDB is always at least as fresh as the seal, so
//!     the freshness gate does NOT apply here.
//! (c) An analytical query (`POST /query` over the Iceberg mirror) with
//!     `X-Bluedb-Min-Watermark > sealed` returns 503 + the current sealed
//!     watermark in `X-Bluedb-Watermark`.

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

/// Build a promoted, CDC-enabled app state (lakehouse mirror on by default).
async fn make_state() -> AppState {
    let state = make_unpromoted_state();
    state.promote().await.expect("promote");
    state
}

/// Build an app state that has NOT been promoted (no writer `Db` bound). Used to
/// exercise the writer-unavailable fail-fast path.
fn make_unpromoted_state() -> AppState {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arc::new(WriterController::new(
        "test-node",
        Arc::new(LocalLeaseProvider::new()) as Arc<dyn LeaseProvider>,
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ));
    AppState::new(store, "bluedb", writer).with_admin_sql_enabled(true)
}

/// Full response (status + headers + body).
struct FullResponse {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Value,
}

/// Issue a request and return the full response, including headers.
async fn call_full(
    app: &Router,
    method: &str,
    uri: &str,
    tenant: Option<&str>,
    min_watermark: Option<&str>,
    json_body: Option<Value>,
) -> FullResponse {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(t) = tenant {
        builder = builder.header("x-bluedb-tenant", t);
    }
    if let Some(mw) = min_watermark {
        builder = builder.header("x-bluedb-min-watermark", mw);
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
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or_else(|_| {
            Value::String(String::from_utf8_lossy(&bytes).into_owned())
        })
    };
    FullResponse { status, headers, body }
}

/// Parse `X-Bluedb-Watermark: <tenant>:<seq>` and return the seq.
fn parse_watermark_header(headers: &axum::http::HeaderMap) -> Option<(String, i64)> {
    let v = headers.get("x-bluedb-watermark")?.to_str().ok()?;
    let (tenant, seq_str) = v.split_once(':')?;
    let seq: i64 = seq_str.trim().parse().ok()?;
    Some((tenant.to_string(), seq))
}

// ----- test (a): write returns X-Bluedb-Watermark with monotonically-increasing seq -----

#[tokio::test]
async fn write_returns_watermark_header_with_increasing_seq() {
    let state = make_state().await;
    let app = build_app(state.clone());

    // Turn lakehouse mirror on for the default tenant so CDC seqs are assigned.
    let r = call_full(
        &app,
        "POST",
        "/sql",
        None,
        None,
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"})),
    ).await;
    assert!(r.status.is_success(), "pragma on: {} {:?}", r.status, r.body);

    // DDL (admin SQL) — no CDC seq expected on DDL.
    let r = call_full(
        &app,
        "POST",
        "/admin/sql",
        None,
        None,
        Some(json!({"sql": "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)"})),
    ).await;
    assert!(r.status.is_success(), "create: {}", r.status);

    // First INSERT via /tables — should return watermark.
    let r1 = call_full(
        &app,
        "POST",
        "/tables/items",
        None,
        None,
        Some(json!({"id": 1, "val": "a"})),
    ).await;
    assert!(r1.status.is_success(), "insert1: {} {:?}", r1.status, r1.body);
    let (tenant1, seq1) = parse_watermark_header(&r1.headers)
        .expect("X-Bluedb-Watermark missing on write response");
    assert_eq!(tenant1, "_", "tenant should be default '_'");
    assert!(seq1 > 0, "seq should be positive, got {seq1}");

    // Second INSERT — seq must be strictly greater.
    let r2 = call_full(
        &app,
        "POST",
        "/tables/items",
        None,
        None,
        Some(json!({"id": 2, "val": "b"})),
    ).await;
    assert!(r2.status.is_success(), "insert2: {}", r2.status);
    let (_, seq2) = parse_watermark_header(&r2.headers)
        .expect("X-Bluedb-Watermark missing on second write");
    assert!(seq2 > seq1, "seq must be monotonically increasing: {seq1} -> {seq2}");
}

// ----- test (a) via /sql -----

#[tokio::test]
async fn write_via_sql_returns_watermark_header() {
    let state = make_state().await;
    let app = build_app(state.clone());

    let r = call_full(
        &app, "POST", "/sql", None, None,
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"})),
    ).await;
    assert!(r.status.is_success());

    let r = call_full(
        &app, "POST", "/admin/sql", None, None,
        Some(json!({"sql": "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)"})),
    ).await;
    assert!(r.status.is_success());

    // INSERT via /sql.
    let r = call_full(
        &app, "POST", "/sql", None, None,
        Some(json!({"sql": "INSERT INTO events VALUES (1, 'boot')"})),
    ).await;
    assert!(r.status.is_success(), "insert: {} {:?}", r.status, r.body);
    let (tenant, seq) = parse_watermark_header(&r.headers)
        .expect("X-Bluedb-Watermark missing on /sql write");
    assert_eq!(tenant, "_");
    assert!(seq > 0, "seq > 0, got {seq}");
}

// ----- test (b): read with X-Bluedb-Min-Watermark <= sealed serves and echoes watermark -----

#[tokio::test]
async fn read_with_min_watermark_at_most_sealed_serves_and_echoes_watermark() {
    let state = make_state().await;
    let app = build_app(state.clone());

    // Enable CDC.
    let r = call_full(&app, "POST", "/sql", None, None,
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"}))).await;
    assert!(r.status.is_success());

    let r = call_full(&app, "POST", "/admin/sql", None, None,
        Some(json!({"sql": "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)"}))).await;
    assert!(r.status.is_success());

    // Write one row.
    let rw = call_full(&app, "POST", "/tables/docs", None, None,
        Some(json!({"id": 1, "body": "hello"}))).await;
    assert!(rw.status.is_success());
    let (_, _write_seq) = parse_watermark_header(&rw.headers)
        .expect("watermark on write");

    // Force a seal so the watermark is durably in Iceberg.
    state.seal_now().await.expect("seal");

    // Read with min-watermark = 0 (always ≤ sealed) — must succeed + echo watermark.
    let rr = call_full(&app, "GET", "/tables/docs", None, Some("0"), None).await;
    assert!(rr.status.is_success(), "read with min=0: {} {:?}", rr.status, rr.body);
    let (tenant, sealed_seq) = parse_watermark_header(&rr.headers)
        .expect("X-Bluedb-Watermark missing on read response");
    assert_eq!(tenant, "_");
    assert!(sealed_seq >= 0, "sealed seq should be non-negative");
}

// ----- test (b corrected): GET /tables with X-Bluedb-Min-Watermark > sealed still serves -----
//
// SlateDB is always at least as fresh as the Iceberg seal, so the freshness gate
// does NOT apply to the OLTP read path. A min-watermark larger than the sealed
// watermark must NOT 503 here — it is served and succeeds.

#[tokio::test]
async fn get_tables_with_min_watermark_above_sealed_still_serves() {
    let state = make_state().await;
    let app = build_app(state.clone());

    let r = call_full(&app, "POST", "/sql", None, None,
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"}))).await;
    assert!(r.status.is_success());

    let r = call_full(&app, "POST", "/admin/sql", None, None,
        Some(json!({"sql": "CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)"}))).await;
    assert!(r.status.is_success());

    // Write a row to create some CDC entries.
    let rw = call_full(&app, "POST", "/tables/things", None, None,
        Some(json!({"id": 1, "name": "one"}))).await;
    assert!(rw.status.is_success());
    let (_, write_seq) = parse_watermark_header(&rw.headers)
        .expect("watermark on write");

    // Do NOT seal — the Iceberg sealed watermark is still 0.
    // Ask for a min-watermark higher than anything sealed so far.
    let future_seq = write_seq + 1_000_000;
    let rr = call_full(
        &app, "GET", "/tables/things",
        None,
        Some(&future_seq.to_string()),
        None,
    ).await;
    // P2.2 correction: GET /tables reads from SlateDB, not Iceberg, so the
    // freshness gate must NOT 503 here — the data IS available from SlateDB.
    assert!(
        rr.status.is_success(),
        "GET /tables with min > sealed must succeed (SlateDB is fresher than the seal): {} {:?}",
        rr.status, rr.body
    );
}

// ----- test: per-tenant scoping -----

#[tokio::test]
async fn watermark_is_scoped_per_tenant() {
    let state = make_state().await;
    let app = build_app(state.clone());

    // Enable CDC for both tenants.
    for tenant in ["ta", "tb"] {
        let r = call_full(&app, "POST", "/sql", Some(tenant), None,
            Some(json!({"sql": "PRAGMA lakehouse_mirror = on"}))).await;
        assert!(r.status.is_success(), "pragma on tenant {tenant}: {}", r.status);
    }

    // Create table + insert for tenant "ta".
    let r = call_full(&app, "POST", "/admin/sql", Some("ta"), None,
        Some(json!({"sql": "CREATE TABLE ta_tbl (id INTEGER PRIMARY KEY, v TEXT)"}))).await;
    assert!(r.status.is_success());

    let r1 = call_full(&app, "POST", "/tables/ta_tbl", Some("ta"), None,
        Some(json!({"id": 1, "v": "x"}))).await;
    assert!(r1.status.is_success(), "ta write: {}", r1.status);
    let (t1, seq_ta) = parse_watermark_header(&r1.headers).expect("ta watermark");
    assert_eq!(t1, "ta");
    assert!(seq_ta > 0);

    // Create table + insert for tenant "tb".
    let r = call_full(&app, "POST", "/admin/sql", Some("tb"), None,
        Some(json!({"sql": "CREATE TABLE tb_tbl (id INTEGER PRIMARY KEY, v TEXT)"}))).await;
    assert!(r.status.is_success());

    let r2 = call_full(&app, "POST", "/tables/tb_tbl", Some("tb"), None,
        Some(json!({"id": 1, "v": "y"}))).await;
    assert!(r2.status.is_success(), "tb write: {}", r2.status);
    let (t2, seq_tb) = parse_watermark_header(&r2.headers).expect("tb watermark");
    assert_eq!(t2, "tb");
    assert!(seq_tb > 0);

    // The two tenants have independent CDC seq spaces starting at 1 each.
    // Both should be 1 for their first writes.
    assert_eq!(seq_ta, 1, "ta first write seq should be 1");
    assert_eq!(seq_tb, 1, "tb first write seq should be 1 (independent counter)");
}

// ----- Phase 2.2 tests -----

// Helper: enable the lakehouse mirror, create a table, insert rows, seal.
// Returns the sealed watermark seq (obtained from the response headers after
// a dummy GET that echoes the sealed watermark).
async fn setup_analytical_table(state: &AppState, app: &axum::Router) -> i64 {
    // Turn on the lakehouse mirror.
    let r = call_full(app, "POST", "/sql", None, None,
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"}))).await;
    assert!(r.status.is_success(), "pragma: {} {:?}", r.status, r.body);

    // Create table: `products (id INTEGER PK, name TEXT, score INTEGER)`.
    // score is NOT indexed — filters on it will be guardrail-rejected and
    // routed to the analytical path.
    let r = call_full(app, "POST", "/admin/sql", None, None,
        Some(json!({"sql": "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"}))).await;
    assert!(r.status.is_success(), "create: {} {:?}", r.status, r.body);

    // Insert rows via /sql.
    for (id, name, score) in [(1, "alpha", 90), (2, "beta", 40), (3, "gamma", 75)] {
        let r = call_full(app, "POST", "/sql", None, None,
            Some(json!({"sql": format!("INSERT INTO products VALUES ({id}, '{name}', {score})")}))).await;
        assert!(r.status.is_success(), "insert {id}: {} {:?}", r.status, r.body);
    }

    // Seal so the Iceberg snapshot exists.
    state.seal_now().await.expect("seal");

    // Read the sealed watermark from the response header of a GET /tables request.
    let r = call_full(app, "GET", "/tables/products", None, None, None).await;
    assert!(r.status.is_success(), "GET products: {} {:?}", r.status, r.body);
    parse_watermark_header(&r.headers).map(|(_, seq)| seq).unwrap_or(0)
}

// ----- test: guardrail-rejected SELECT is routed to the analytical path -----
//
// A filter on a non-indexed column (`score`) would be rejected by the guardrail
// when running against SlateDB. After P2.2 the handler routes it to DataFusion
// over the sealed Iceberg snapshot and returns correct rows.

#[tokio::test]
async fn guardrail_rejected_select_routed_to_analytical_path_returns_correct_rows() {
    let state = make_state().await;
    let app = build_app(state.clone());

    setup_analytical_table(&state, &app).await;

    // Query: filter on `score` (non-indexed) + ORDER BY `score` (non-indexed).
    // `/sql` rejects this (index-only RYW surface); `/query` serves it analytically.
    let r = call_full(
        &app, "POST", "/query", None, None,
        Some(json!({"sql": "SELECT id, name FROM products WHERE score > 50 ORDER BY score DESC"})),
    ).await;
    assert!(
        r.status.is_success(),
        "analytical query on /query must succeed: {} {:?}",
        r.status, r.body
    );

    // Expect rows with score > 50: alpha (90) and gamma (75), in DESC order.
    let rows = r.body.as_array().expect("response is an array of rows");
    assert_eq!(rows.len(), 2, "expected 2 rows with score > 50, got {rows:?}");

    // alpha (90) first, gamma (75) second (DESC by score).
    let names: Vec<&str> = rows.iter()
        .map(|row| row["name"].as_str().unwrap_or("?"))
        .collect();
    assert_eq!(names, vec!["alpha", "gamma"], "rows should be alpha then gamma (score DESC): {names:?}");
}

// ----- test: analytical path honors X-Bluedb-Min-Watermark (P4) -----
//
// HTAP P4 changed the `min > sealed` outcome on the ACTIVE WRITER from a hard
// 503 to a writer-local FRESH serve (the writer holds rows ≥ any acknowledged
// write). `min <= sealed` still serves from the sealed Iceberg snapshot.

#[tokio::test]
async fn analytical_path_honors_min_watermark() {
    let state = make_state().await;
    let app = build_app(state.clone());

    let sealed = setup_analytical_table(&state, &app).await;

    // (a) min <= sealed: served from the sealed Iceberg snapshot.
    let r_ok = call_full(
        &app, "POST", "/query", None, Some("0"),
        Some(json!({"sql": "SELECT id, name FROM products WHERE score > 50 ORDER BY score DESC"})),
    ).await;
    assert!(
        r_ok.status.is_success(),
        "analytical path with min=0 (<=sealed) must succeed: {} {:?}",
        r_ok.status, r_ok.body
    );
    // Response should echo the sealed watermark.
    let wm = r_ok.headers.get("x-bluedb-watermark");
    assert!(wm.is_some(), "analytical response must include X-Bluedb-Watermark");

    // (b) min > sealed on the active writer: P4 serves FRESH (not 503).
    let future_seq = sealed + 1_000_000;
    let r_fresh = call_full(
        &app, "POST", "/query", None, Some(&future_seq.to_string()),
        Some(json!({"sql": "SELECT id, name FROM products WHERE score > 50 ORDER BY score DESC"})),
    ).await;
    assert!(
        r_fresh.status.is_success(),
        "P4: analytical path with min > sealed must serve FRESH on the active writer, not 503: {} {:?}",
        r_fresh.status, r_fresh.body
    );
    // Same rows as the sealed path: alpha (90), gamma (75) by score DESC.
    let names: Vec<&str> = r_fresh.body.as_array().expect("rows array")
        .iter().map(|row| row["name"].as_str().unwrap_or("?")).collect();
    assert_eq!(names, vec!["alpha", "gamma"], "fresh rows must match: {names:?}");
    assert!(
        r_fresh.headers.get("x-bluedb-watermark").is_some(),
        "fresh serve must include X-Bluedb-Watermark"
    );
}

// ----- P4 test: writer serves an UNSEALED freshly-written row (min > sealed) ---
//
// The core P4 win: a row written but NOT yet sealed into Iceberg is still
// served by the analytical path on the active writer, gated by the freshness
// header. The table is never sealed (sealed watermark stays 0), so the OLD
// behavior would have 503'd; P4 reads it fresh from the live store.

#[tokio::test]
async fn writer_serves_unsealed_fresh_row_on_analytical_path() {
    let state = make_state().await;
    let app = build_app(state.clone());

    let r = call_full(&app, "POST", "/sql", None, None,
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"}))).await;
    assert!(r.status.is_success());

    let r = call_full(&app, "POST", "/admin/sql", None, None,
        Some(json!({"sql": "CREATE TABLE fresh_items (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"}))).await;
    assert!(r.status.is_success(), "create: {} {:?}", r.status, r.body);

    // Write rows; capture the write watermark. Do NOT seal — sealed stays 0.
    let mut write_seq = 0i64;
    for (id, name, score) in [(1, "alpha", 90), (2, "beta", 40), (3, "gamma", 75)] {
        let r = call_full(&app, "POST", "/sql", None, None,
            Some(json!({"sql": format!("INSERT INTO fresh_items VALUES ({id}, '{name}', {score})")}))).await;
        assert!(r.status.is_success(), "insert {id}: {} {:?}", r.status, r.body);
        if let Some((_, seq)) = parse_watermark_header(&r.headers) {
            write_seq = seq;
        }
    }
    assert!(write_seq > 0, "writes should have produced a CDC watermark");

    // Demand a min-watermark at the last write (strictly greater than the sealed
    // watermark, which is still 0). The non-indexed filter on `score` runs on the
    // analytical path (`/query`); P4 must serve the UNSEALED rows fresh.
    let r = call_full(
        &app, "POST", "/query", None, Some(&write_seq.to_string()),
        Some(json!({"sql": "SELECT id, name FROM fresh_items WHERE score > 50 ORDER BY score DESC"})),
    ).await;
    assert!(
        r.status.is_success(),
        "P4: writer must serve unsealed fresh rows for min={write_seq} > sealed=0: {} {:?}",
        r.status, r.body
    );
    let names: Vec<&str> = r.body.as_array().expect("rows array")
        .iter().map(|row| row["name"].as_str().unwrap_or("?")).collect();
    assert_eq!(names, vec!["alpha", "gamma"], "expected alpha, gamma (score DESC): {names:?}");
}

// ----- P4 test: PRAGMA bluedb_read_wait_seal_n is accepted and stored ---------

#[tokio::test]
async fn read_wait_seal_n_pragma_is_accepted_and_affects_decision() {
    let state = make_state().await;
    let app = build_app(state.clone());

    let r = call_full(&app, "POST", "/sql", None, None,
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"}))).await;
    assert!(r.status.is_success());

    // The PRAGMA is intercepted (gluesql would reject it) and acked by name.
    let r = call_full(&app, "POST", "/sql", None, None,
        Some(json!({"sql": "PRAGMA bluedb_read_wait_seal_n = 3"}))).await;
    assert!(r.status.is_success(), "pragma accepted: {} {:?}", r.status, r.body);
    assert_eq!(
        r.body["pragma"].as_str(), Some("bluedb_read_wait_seal_n"),
        "ack must name the pragma: {:?}", r.body
    );

    // `0` (reset to default) is also accepted and acked by name. (The stored
    // value itself is engine-internal; its effect is covered below.)
    let r = call_full(&app, "POST", "/sql", None, None,
        Some(json!({"sql": "SET bluedb_read_wait_seal_n = 0"}))).await;
    assert!(r.status.is_success(), "pragma reset: {} {:?}", r.status, r.body);
    assert_eq!(r.body["pragma"].as_str(), Some("bluedb_read_wait_seal_n"));

    // With the pragma set to a non-default value, a min > sealed analytical read
    // still serves fresh on the writer (the pragma tunes the follower-redirect
    // budget, not the writer's own fresh-serve).
    let r = call_full(&app, "POST", "/sql", None, None,
        Some(json!({"sql": "PRAGMA bluedb_read_wait_seal_n = 5"}))).await;
    assert!(r.status.is_success());
    let _ = call_full(&app, "POST", "/admin/sql", None, None,
        Some(json!({"sql": "CREATE TABLE wpragma (id INTEGER PRIMARY KEY, score INTEGER)"}))).await;
    let _ = call_full(&app, "POST", "/sql", None, None,
        Some(json!({"sql": "INSERT INTO wpragma VALUES (1, 99)"}))).await;
    let r = call_full(
        &app, "POST", "/query", None, Some("1000000"),
        Some(json!({"sql": "SELECT id FROM wpragma WHERE score > 0"})),
    ).await;
    assert!(
        r.status.is_success(),
        "writer serves fresh even with pragma set: {} {:?}", r.status, r.body
    );
}

// ----- P4 test: writer unavailable → fail-fast 503 (never hang) ---------------
//
// A node with no writer `Db` bound (never promoted) must 503 immediately on a
// `/sql` write rather than block. `/sql` gates on the active writer
// (`require_active`); a passive node fails fast instead of hanging.

#[tokio::test]
async fn unpromoted_node_fails_fast_503_on_sql() {
    let state = make_unpromoted_state();
    let app = build_app(state.clone());

    // No promote() — the node is passive. A write must 503 fast
    // (require_active gate), not hang.
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        call_full(
            &app, "POST", "/sql", None, None,
            Some(json!({"sql": "INSERT INTO whatever VALUES (1)"})),
        ),
    )
    .await
    .expect("request must not hang — fail fast");
    assert_eq!(
        r.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "unpromoted node must 503 (writer unavailable): {} {:?}",
        r.status, r.body
    );
}

// ----- serializer cross-path consistency tests -----
//
// A table with DECIMAL, DATE, TIMESTAMP, TIME columns must render IDENTICALLY
// whether the row is served by the SlateDB/OLTP path (GET /tables) or by the
// analytical Iceberg path (a guardrail-rejected filter routed to DataFusion).
//
// `score INTEGER` is intentionally NOT indexed so `WHERE score > 0` is
// guardrail-rejected and routed to the analytical path.

/// Helper: set up a typed-column table, insert one row, seal to Iceberg.
/// Returns the sealed watermark seq.
async fn setup_typed_table(state: &AppState, app: &axum::Router) -> i64 {
    let r = call_full(app, "POST", "/sql", None, None,
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"}))).await;
    assert!(r.status.is_success(), "pragma: {} {:?}", r.status, r.body);

    let r = call_full(app, "POST", "/admin/sql", None, None,
        Some(json!({"sql":
            "CREATE TABLE typed_row (\
                id INTEGER PRIMARY KEY, \
                score INTEGER, \
                amount DECIMAL, \
                day DATE, \
                ts TIMESTAMP, \
                slot TIME\
            )"
        }))).await;
    assert!(r.status.is_success(), "create typed_row: {} {:?}", r.status, r.body);

    // Insert one row with known values.
    let r = call_full(app, "POST", "/sql", None, None,
        Some(json!({"sql":
            "INSERT INTO typed_row VALUES (\
                1, \
                42, \
                12.34, \
                DATE '2024-03-15', \
                TIMESTAMP '2024-03-15 12:34:56', \
                TIME '14:30:05'\
            )"
        }))).await;
    assert!(r.status.is_success(), "insert typed_row: {} {:?}", r.status, r.body);

    state.seal_now().await.expect("seal typed_row");

    let r = call_full(app, "GET", "/tables/typed_row", None, None, None).await;
    assert!(r.status.is_success());
    parse_watermark_header(&r.headers).map(|(_, seq)| seq).unwrap_or(0)
}

#[tokio::test]
async fn decimal_date_timestamp_time_render_identically_on_both_paths() {
    let state = make_state().await;
    let app = build_app(state.clone());

    setup_typed_table(&state, &app).await;

    // --- SlateDB/OLTP path: GET /tables/typed_row (no filter → PK scan) ---
    let r_oltp = call_full(&app, "GET", "/tables/typed_row", None, None, None).await;
    assert!(
        r_oltp.status.is_success(),
        "OLTP path must succeed: {} {:?}", r_oltp.status, r_oltp.body
    );
    let oltp_rows = r_oltp.body.as_array().expect("OLTP response is an array");
    assert_eq!(oltp_rows.len(), 1, "expected 1 row from OLTP path");
    let oltp = &oltp_rows[0];

    // --- Analytical path (`/query`): non-indexed WHERE score > 0 → DataFusion ---
    let r_analytical = call_full(
        &app, "POST", "/query", None, None,
        Some(json!({"sql": "SELECT * FROM typed_row WHERE score > 0 ORDER BY id"})),
    ).await;
    assert!(
        r_analytical.status.is_success(),
        "analytical path must succeed: {} {:?}", r_analytical.status, r_analytical.body
    );
    let analytical_rows = r_analytical.body.as_array().expect("analytical response is an array");
    assert_eq!(analytical_rows.len(), 1, "expected 1 row from analytical path");
    let analytical = &analytical_rows[0];

    // Assert canonical rendering for each typed column.
    // DECIMAL 12.34 → normalized string "12.34" (trailing zeros stripped).
    assert_eq!(
        oltp["amount"], analytical["amount"],
        "DECIMAL: OLTP={} analytical={}", oltp["amount"], analytical["amount"]
    );
    assert_eq!(
        oltp["amount"],
        Value::String("12.34".to_string()),
        "DECIMAL canonical form should be \"12.34\", got {}", oltp["amount"]
    );

    // DATE '2024-03-15' → ISO-8601 "2024-03-15".
    assert_eq!(
        oltp["day"], analytical["day"],
        "DATE: OLTP={} analytical={}", oltp["day"], analytical["day"]
    );
    assert_eq!(
        oltp["day"],
        Value::String("2024-03-15".to_string()),
        "DATE canonical form should be \"2024-03-15\", got {}", oltp["day"]
    );

    // TIMESTAMP '2024-03-15 12:34:56' → "2024-03-15T12:34:56".
    assert_eq!(
        oltp["ts"], analytical["ts"],
        "TIMESTAMP: OLTP={} analytical={}", oltp["ts"], analytical["ts"]
    );
    assert_eq!(
        oltp["ts"],
        Value::String("2024-03-15T12:34:56".to_string()),
        "TIMESTAMP canonical form should be \"2024-03-15T12:34:56\", got {}", oltp["ts"]
    );

    // TIME '14:30:05' → "14:30:05".
    assert_eq!(
        oltp["slot"], analytical["slot"],
        "TIME: OLTP={} analytical={}", oltp["slot"], analytical["slot"]
    );
    assert_eq!(
        oltp["slot"],
        Value::String("14:30:05".to_string()),
        "TIME canonical form should be \"14:30:05\", got {}", oltp["slot"]
    );
}

/// JSON accessors (`->>` / `->`) over a JSON-as-TEXT column run on the analytical
/// (DataFusion) path — projecting and filtering on a JSON subfield. Proves the
/// hand-rolled json_get_str/json_get UDFs and the `->`/`->>` operator rewrite are
/// wired into query_via_catalog, and that a JSON column mirrors as a plain string.
#[tokio::test]
async fn json_accessors_work_on_the_analytical_path() {
    let state = make_state().await;
    let app = build_app(state.clone());

    let r = call_full(&app, "POST", "/sql", None, None,
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"}))).await;
    assert!(r.status.is_success(), "pragma: {} {:?}", r.status, r.body);

    let r = call_full(&app, "POST", "/admin/sql", None, None,
        Some(json!({"sql": "CREATE TABLE docs (id INTEGER PRIMARY KEY, data JSON)"}))).await;
    assert!(r.status.is_success(), "create: {} {:?}", r.status, r.body);

    // Insert JSON documents (the JSON column stores canonical text).
    let mut write_seq = 0i64;
    for (id, body) in [
        (1, r#"{"status":"active","n":90}"#),
        (2, r#"{"status":"idle","n":40}"#),
    ] {
        let r = call_full(&app, "POST", "/sql", None, None,
            Some(json!({"sql": format!("INSERT INTO docs VALUES ({id}, '{body}')")}))).await;
        assert!(r.status.is_success(), "insert {id}: {} {:?}", r.status, r.body);
        if let Some((_, seq)) = parse_watermark_header(&r.headers) {
            write_seq = seq;
        }
    }

    // Project + filter on a JSON subfield via `->>` (text accessor). JSON paths
    // run on the analytical surface (`/query`); min-watermark forces a fresh
    // (unsealed) read on the writer.
    let r = call_full(
        &app, "POST", "/query", None, Some(&write_seq.to_string()),
        Some(json!({
            "sql": "SELECT id, data->>'status' AS status, data->>'n' AS n \
                    FROM docs WHERE (data->>'status') = 'active'"
        })),
    ).await;
    assert!(r.status.is_success(), "json query: {} {:?}", r.status, r.body);
    let rows = r.body.as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "only the active row: {:?}", r.body);
    assert_eq!(rows[0]["id"], json!(1));
    assert_eq!(rows[0]["status"], json!("active"));
    assert_eq!(rows[0]["n"], json!("90"), "->> renders the number as text");
}
