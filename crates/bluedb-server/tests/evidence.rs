//! End-to-end evidence substrate test: append, head, read_range, tenant isolation.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use bluedb_ha::{LeaseProvider, LocalLeaseProvider, SystemClock, WriterController};
use bluedb_server::{build_app, AppState};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use tower::ServiceExt;

const TTL: Duration = Duration::from_secs(30);
const MARGIN: Duration = Duration::from_secs(5);

async fn promoted() -> (AppState, Router) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arc::new(WriterController::new(
        "test-node",
        Arc::new(LocalLeaseProvider::new()) as Arc<dyn LeaseProvider>,
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ));
    let state = AppState::new(store, "bluedb", writer);
    state.promote().await.expect("promote");
    let app = build_app(state.clone());
    (state, app)
}

async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    tenant: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(t) = tenant {
        builder = builder.header("x-bluedb-tenant", t);
    }
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
        serde_json::from_slice(&bytes).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    (status, json)
}

#[tokio::test]
async fn evidence_append_head_read_and_tenant_isolation() {
    let (_, app) = promoted().await;

    // --- acme: append two events (second has non-UTF-8 payload) ---
    let non_utf8_bytes: Vec<u8> = vec![0xff, 0xfe, 0x00, 0x01];
    let (s, body) = call(
        &app,
        "POST",
        "/evidence/audit/entries",
        Some("acme"),
        Some(json!({
            "events": [
                { "type": "login", "payload_b64": B64.encode(b"hello"), "at": "2026-06-15T10:00:00Z" },
                { "type": "binary_event", "payload_b64": B64.encode(&non_utf8_bytes), "at": "2026-06-15T10:01:00Z" }
            ]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "append acme: {s} {body}");
    assert_eq!(body["base_seq"], 0, "base_seq should be 0: {body}");
    assert_eq!(
        body["seqs"].as_array().unwrap().len(),
        2,
        "should have 2 seqs: {body}"
    );
    assert_eq!(body["seqs"][0], 1, "first seq: {body}");
    assert_eq!(body["seqs"][1], 2, "second seq: {body}");

    // --- acme: GET head ---
    let (s, body) = call(&app, "GET", "/evidence/audit/head", Some("acme"), None).await;
    assert_eq!(s, StatusCode::OK, "head acme: {s} {body}");
    assert_eq!(body["seq"], 2, "head should be 2: {body}");

    // --- acme: GET entries?from=1&to=2 ---
    let (s, body) = call(
        &app,
        "GET",
        "/evidence/audit/entries?from=1&to=2",
        Some("acme"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "read_range acme: {s} {body}");
    let entries = body.as_array().expect("entries should be array");
    assert_eq!(entries.len(), 2, "should have 2 entries: {body}");

    // Verify the non-UTF-8 entry's payload_b64 decodes byte-identically.
    let second = &entries[1];
    let decoded = B64
        .decode(second["payload_b64"].as_str().expect("payload_b64 present"))
        .expect("valid base64");
    assert_eq!(
        decoded, non_utf8_bytes,
        "non-UTF-8 payload must round-trip byte-identically"
    );

    // --- tenant isolation: globex appends to the same chain name ---
    let (s, body) = call(
        &app,
        "POST",
        "/evidence/audit/entries",
        Some("globex"),
        Some(json!({
            "events": [
                { "type": "purchase", "payload_b64": B64.encode(b"order-1"), "at": "2026-06-15T11:00:00Z" }
            ]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "append globex: {s} {body}");
    // globex's 'audit' chain is independent — sequence starts at 1
    assert_eq!(body["seqs"][0], 1, "globex seq should start at 1: {body}");
    assert_eq!(
        body["seqs"].as_array().unwrap().len(),
        1,
        "globex should have 1 seq"
    );
}

#[tokio::test]
async fn evidence_create_chain_explicit() {
    let (_, app) = promoted().await;

    // Explicitly create chain with verified=true.
    let (s, body) = call(
        &app,
        "PUT",
        "/evidence/contracts",
        Some("acme"),
        Some(json!({ "verified": true })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "create_chain: {s} {body}");
    assert_eq!(body["chain"], "contracts");
    assert_eq!(body["verified"], true);

    // Idempotent re-create (same mode) is OK.
    let (s, _) = call(
        &app,
        "PUT",
        "/evidence/contracts",
        Some("acme"),
        Some(json!({ "verified": true })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "idempotent re-create");

    // Mode conflict returns 409.
    let (s, body) = call(
        &app,
        "PUT",
        "/evidence/contracts",
        Some("acme"),
        Some(json!({ "verified": false })),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "mode conflict: {s} {body}");
    assert!(
        body["error"].as_str().unwrap_or("").contains("E_CHAIN_MODE_CONFLICT"),
        "conflict message: {body}"
    );
}

#[tokio::test]
async fn evidence_idempotency() {
    let (_, app) = promoted().await;

    let payload = B64.encode(b"ev1");

    // First append with idem_key.
    let (s, first) = call(
        &app,
        "POST",
        "/evidence/orders/entries",
        Some("acme"),
        Some(json!({
            "events": [{ "type": "created", "payload_b64": payload }],
            "idem_key": "req-001"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "first append: {s} {first}");

    // Replay with same key and same payload → same seqs.
    let (s, replay) = call(
        &app,
        "POST",
        "/evidence/orders/entries",
        Some("acme"),
        Some(json!({
            "events": [{ "type": "created", "payload_b64": payload }],
            "idem_key": "req-001"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "replay: {s} {replay}");
    assert_eq!(first["seqs"], replay["seqs"], "replay must return same seqs");

    // Different payload with same key → 409 IdemConflict.
    let (s, body) = call(
        &app,
        "POST",
        "/evidence/orders/entries",
        Some("acme"),
        Some(json!({
            "events": [{ "type": "created", "payload_b64": B64.encode(b"different") }],
            "idem_key": "req-001"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "idem conflict: {s} {body}");
    assert!(
        body["error"].as_str().unwrap_or("").contains("E_IDEM_CONFLICT"),
        "idem conflict message: {body}"
    );
}

#[tokio::test]
async fn evidence_negative_seq_params_rejected() {
    let (_, app) = promoted().await;

    // ?from=-1 should be rejected with 400.
    let (s, body) = call(
        &app,
        "GET",
        "/evidence/audit/entries?from=-1&to=2",
        Some("acme"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "negative from: {s} {body}");

    // ?to=-1 should also be rejected.
    let (s, body) = call(
        &app,
        "GET",
        "/evidence/audit/entries?from=1&to=-1",
        Some("acme"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "negative to: {s} {body}");

    // ?after=-1 should also be rejected.
    let (s, body) = call(
        &app,
        "GET",
        "/evidence/audit/entries?after=-1",
        Some("acme"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "negative after: {s} {body}");
}
