//! Phase 1 — Watermark surfacing tests.
//!
//! Verifies the three behaviors of the `X-Bluedb-Watermark` /
//! `X-Bluedb-Min-Watermark` contract:
//!
//! (a) A write returns `X-Bluedb-Watermark: <tenant>:<seq>` with a
//!     monotonically-increasing seq.
//! (b) A read with `X-Bluedb-Min-Watermark ≤ sealed` serves and reports the
//!     sealed watermark in `X-Bluedb-Watermark`.
//! (c) A read with `X-Bluedb-Min-Watermark > sealed` returns 503 + the current
//!     sealed watermark in `X-Bluedb-Watermark`.

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

// ----- test (c): read with X-Bluedb-Min-Watermark > sealed returns 503 -----

#[tokio::test]
async fn read_with_min_watermark_above_sealed_returns_503() {
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
    assert_eq!(
        rr.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "min > sealed should return 503, got {} {:?}", rr.status, rr.body
    );
    // The response should still tell the client what the current sealed watermark is.
    let wm_hdr = rr.headers.get("x-bluedb-watermark");
    assert!(wm_hdr.is_some(), "503 should include X-Bluedb-Watermark indicating current sealed seq");
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
