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
async fn evidence_merkle_and_erasure_e2e() {
    let (_, app) = promoted().await;

    // --- Verified chain "v": append 5 events ---
    for i in 0..5u32 {
        let (s, body) = call(
            &app,
            "POST",
            "/evidence/v/entries",
            Some("tenant1"),
            Some(json!({
                "events": [{ "type": "ev", "payload_b64": B64.encode(format!("p{i}").as_bytes()) }]
            })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "append v[{i}]: {s} {body}");
    }

    // GET /evidence/v/digest → size==5, root_hash is 64 lowercase hex chars.
    let (s, body) = call(&app, "GET", "/evidence/v/digest", Some("tenant1"), None).await;
    assert_eq!(s, StatusCode::OK, "digest: {s} {body}");
    assert_eq!(body["size"], 5, "digest size: {body}");
    let root_hash = body["root_hash"].as_str().expect("root_hash is string").to_string();
    assert_eq!(root_hash.len(), 64, "root_hash must be 64 hex chars: {root_hash}");
    assert!(root_hash.chars().all(|c| c.is_ascii_hexdigit()), "root_hash lowercase hex: {root_hash}");
    let digest_before = root_hash;

    // GET /evidence/v/proof?seq=3 → audit_path non-empty.
    let (s, body) = call(&app, "GET", "/evidence/v/proof?seq=3", Some("tenant1"), None).await;
    assert_eq!(s, StatusCode::OK, "proof: {s} {body}");
    let audit_path = body["audit_path"].as_array().expect("audit_path is array");
    assert!(!audit_path.is_empty(), "audit_path should be non-empty for size=5: {body}");

    // GET /evidence/v/consistency?from=2 → proof present.
    let (s, body) = call(&app, "GET", "/evidence/v/consistency?from=2", Some("tenant1"), None).await;
    assert_eq!(s, StatusCode::OK, "consistency: {s} {body}");
    assert!(body["proof"].as_array().is_some(), "consistency proof field: {body}");

    // POST /evidence/v/entries/3/redact → 200.
    let (s, body) = call(
        &app,
        "POST",
        "/evidence/v/entries/3/redact",
        Some("tenant1"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "redact: {s} {body}");
    assert_eq!(body["redacted"], true, "redact response: {body}");

    // GET /evidence/v/entries?from=1&to=5 → 5 rows; seq=3 has redacted:true and no payload_b64.
    let (s, body) = call(
        &app,
        "GET",
        "/evidence/v/entries?from=1&to=5",
        Some("tenant1"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "entries after redact: {s} {body}");
    let rows = body.as_array().expect("entries array");
    assert_eq!(rows.len(), 5, "5 rows: {body}");
    let row3 = rows.iter().find(|r| r["seq"] == 3).expect("row seq=3");
    assert_eq!(row3["redacted"], true, "seq=3 redacted: {row3}");
    assert!(row3.get("payload_b64").is_none() || row3["payload_b64"].is_null(), "seq=3 no payload_b64: {row3}");

    // GET /evidence/v/head → 5.
    let (s, body) = call(&app, "GET", "/evidence/v/head", Some("tenant1"), None).await;
    assert_eq!(s, StatusCode::OK, "head: {s} {body}");
    assert_eq!(body["seq"], 5, "head still 5: {body}");

    // GET /evidence/v/digest → root_hash unchanged (redaction keeps the digest).
    let (s, body) = call(&app, "GET", "/evidence/v/digest", Some("tenant1"), None).await;
    assert_eq!(s, StatusCode::OK, "digest after redact: {s} {body}");
    assert_eq!(
        body["root_hash"].as_str().unwrap(),
        digest_before,
        "digest must not change after redaction: {body}"
    );

    // --- Plain chain "p": PUT {verified:false}; digest → 400 E_NOT_VERIFIED ---
    let (s, body) = call(
        &app,
        "PUT",
        "/evidence/p",
        Some("tenant1"),
        Some(json!({ "verified": false })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "create plain chain: {s} {body}");

    let (s, body) = call(&app, "GET", "/evidence/p/digest", Some("tenant1"), None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "digest on plain chain: {s} {body}");
    assert!(
        body["error"].as_str().unwrap_or("").contains("E_NOT_VERIFIED"),
        "E_NOT_VERIFIED message: {body}"
    );

    // --- DELETE /evidence/v/entries/1 (verified chain) → 409 E_VERIFIED_NO_DELETE ---
    let (s, body) = call(
        &app,
        "DELETE",
        "/evidence/v/entries/1",
        Some("tenant1"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "hard-delete on verified: {s} {body}");
    assert!(
        body["error"].as_str().unwrap_or("").contains("E_VERIFIED_NO_DELETE"),
        "E_VERIFIED_NO_DELETE message: {body}"
    );

    // --- Plain chain: append 2 events, then hard-delete seq 1 ---
    for i in 0..2u32 {
        let (s, body) = call(
            &app,
            "POST",
            "/evidence/p/entries",
            Some("tenant1"),
            Some(json!({
                "events": [{ "type": "plain", "payload_b64": B64.encode(format!("q{i}").as_bytes()) }]
            })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "append p[{i}]: {s} {body}");
    }

    let (s, body) = call(
        &app,
        "DELETE",
        "/evidence/p/entries/1",
        Some("tenant1"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "hard-delete plain: {s} {body}");
    assert_eq!(body["deleted"], true, "deleted response: {body}");

    // GET /evidence/p/entries → seq 1 absent (gap); seq 2 present.
    let (s, body) = call(&app, "GET", "/evidence/p/entries", Some("tenant1"), None).await;
    assert_eq!(s, StatusCode::OK, "entries after delete: {s} {body}");
    let rows = body.as_array().expect("entries array");
    let seqs: Vec<i64> = rows.iter().map(|r| r["seq"].as_i64().unwrap()).collect();
    assert!(!seqs.contains(&1), "seq 1 must be absent (gap): {seqs:?}");
    assert!(seqs.contains(&2), "seq 2 must remain: {seqs:?}");

    // GET /evidence/p/head → 2 (head unchanged by hard-delete).
    let (s, body) = call(&app, "GET", "/evidence/p/head", Some("tenant1"), None).await;
    assert_eq!(s, StatusCode::OK, "head plain: {s} {body}");
    assert_eq!(body["seq"], 2, "plain head unchanged: {body}");
}

/// An inclusion proof for a `seq` that doesn't exist in the chain is a
/// not-found condition (404), not a bad-request (400). UAT-EVIDENCE-010.
#[tokio::test]
async fn evidence_inclusion_unknown_seq_is_404() {
    let (_, app) = promoted().await;

    // Append one event → chain "p404" has head==1.
    let (s, body) = call(
        &app,
        "POST",
        "/evidence/p404/entries",
        Some("tenant1"),
        Some(json!({
            "events": [{ "type": "ev", "payload_b64": B64.encode(b"x") }]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "append: {s} {body}");

    // seq=99 doesn't exist (size=1) → 404, not 400.
    let (s, body) = call(&app, "GET", "/evidence/p404/proof?seq=99", Some("tenant1"), None).await;
    assert_eq!(
        s,
        StatusCode::NOT_FOUND,
        "unknown proof seq must be 404, not 400: {s} {body}"
    );

    // redact of an unknown seq is already 404 (existing contract) — keep it so.
    let (s, body) = call(
        &app,
        "POST",
        "/evidence/p404/entries/99/redact",
        Some("tenant1"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "redact unknown seq must be 404: {s} {body}");

    // seq=0 (below 1) is a malformed request → stays 400.
    let (s, body) = call(&app, "GET", "/evidence/p404/proof?seq=0", Some("tenant1"), None).await;
    assert_eq!(
        s,
        StatusCode::BAD_REQUEST,
        "seq<1 is a malformed request (400), not 404: {s} {body}"
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
