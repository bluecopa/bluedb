//! End-to-end test of evidence digest signing (Signed Tree Heads).
//!
//! Verifies the full HTTP path with the **test-only** in-process ES256 signer
//! (injected via `AppState::with_local_signer_for_tests` — never a config
//! path): fetch the signed digest + the public key, rebuild the canonical
//! `sth_payload`, and confirm the signature verifies. Also asserts cross-chain
//! replay is rejected and that a node with signing off returns `501`.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use bluedb_evidence::sth_payload;
use bluedb_ha::{LeaseProvider, LocalLeaseProvider, SystemClock, WriterController};
use bluedb_server::{build_app, verify_es256_der, AppState};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

const TTL: Duration = Duration::from_secs(30);
const MARGIN: Duration = Duration::from_secs(5);

fn writer(node: &str) -> Arc<WriterController> {
    Arc::new(WriterController::new(
        node,
        Arc::new(LocalLeaseProvider::new()) as Arc<dyn LeaseProvider>,
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ))
}

/// A promoted node with the **test-only** in-process signer enabled (ephemeral
/// ES256 key). Production never reaches `LocalSigner` from config — `vault` is
/// the only key-bearing config mode — so the e2e injects it via the
/// `#[doc(hidden)]` test seam.
async fn promoted_signing() -> Router {
    use slatedb::object_store::{memory::InMemory, ObjectStore};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = AppState::new(store, "bluedb", writer("sign-node")).with_local_signer_for_tests();
    state.promote().await.expect("promote");
    build_app(state)
}

/// A promoted node with signing **off** (the default).
async fn promoted_no_signing() -> Router {
    use slatedb::object_store::{memory::InMemory, ObjectStore};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = AppState::new(store, "bluedb", writer("nosign-node"));
    state.promote().await.expect("promote");
    build_app(state)
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
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    (status, json)
}

fn hex_to_root(s: &str) -> [u8; 32] {
    assert_eq!(s.len(), 64, "root_hash must be 64 hex chars: {s}");
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).expect("hex");
    }
    out
}

#[tokio::test]
async fn signed_digest_verifies_end_to_end_and_rejects_replay() {
    let app = promoted_signing().await;
    let tenant = "acme";

    // Two distinct verified chains so we can test cross-chain replay.
    for chain in ["chainA", "chainB"] {
        let (s, _) = call(&app, "PUT", &format!("/evidence/{chain}"), Some(tenant), Some(json!({}))).await;
        assert_eq!(s, StatusCode::OK);
        let (s, _) = call(
            &app,
            "POST",
            &format!("/evidence/{chain}/entries"),
            Some(tenant),
            Some(json!({
                "events": [
                    { "type": "e1", "payload_b64": B64.encode(b"one") },
                    { "type": "e2", "payload_b64": B64.encode(b"two") },
                    { "type": "e3", "payload_b64": B64.encode(b"three") }
                ]
            })),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
    }

    // --- fetch the public key ---
    let (s, key) = call(&app, "GET", "/evidence/signing-key", Some(tenant), None).await;
    assert_eq!(s, StatusCode::OK, "signing-key: {key}");
    assert_eq!(key["alg"], "ES256");
    assert!(key["key_version"].is_i64(), "signing-key carries key_version: {key}");
    let pem = key["public_key"].as_str().expect("public_key pem").to_string();
    assert!(pem.contains("BEGIN PUBLIC KEY"), "SPKI PEM: {pem}");

    // --- signed digest for chainA ---
    let (s, sa) = call(&app, "GET", "/evidence/chainA/digest/signed", Some(tenant), None).await;
    assert_eq!(s, StatusCode::OK, "digest/signed A: {sa}");
    assert_eq!(sa["size"], 3, "size: {sa}");
    assert_eq!(sa["alg"], "ES256");
    assert_eq!(sa["key_id"], "local-ephemeral");
    assert!(sa["key_version"].is_i64(), "signed digest carries key_version: {sa}");
    let root_a = sa["root_hash"].as_str().expect("root_hash");
    let ts_a = sa["timestamp"].as_i64().expect("timestamp");
    let sig_a = B64.decode(sa["signature"].as_str().expect("signature")).expect("b64 sig");

    // --- verify end-to-end: rebuild the canonical STH bytes + verify ---
    let payload_a = sth_payload(tenant, "chainA", 3, &hex_to_root(root_a), ts_a);
    assert!(
        verify_es256_der(&pem, &payload_a, &sig_a),
        "chainA signature must verify against the published key"
    );

    // --- cross-chain replay: chainA's signature must NOT verify as a chainB STH ---
    let payload_b_forged = sth_payload(tenant, "chainB", 3, &hex_to_root(root_a), ts_a);
    assert!(
        !verify_es256_der(&pem, &payload_b_forged, &sig_a),
        "chainA signature must NOT verify against chainB's STH bytes"
    );

    // --- and the genuine chainB signature verifies against chainB only ---
    let (s, sb) = call(&app, "GET", "/evidence/chainB/digest/signed", Some(tenant), None).await;
    assert_eq!(s, StatusCode::OK, "digest/signed B: {sb}");
    let root_b = sb["root_hash"].as_str().unwrap();
    let ts_b = sb["timestamp"].as_i64().unwrap();
    let sig_b = B64.decode(sb["signature"].as_str().unwrap()).unwrap();
    let payload_b = sth_payload(tenant, "chainB", 3, &hex_to_root(root_b), ts_b);
    assert!(verify_es256_der(&pem, &payload_b, &sig_b), "chainB signature must verify");
    // chainB's signature must not verify against chainA's bytes either.
    assert!(
        !verify_es256_der(&pem, &payload_a, &sig_b),
        "chainB signature must NOT verify as chainA"
    );
}

#[tokio::test]
async fn signing_off_returns_501() {
    let app = promoted_no_signing().await;
    let tenant = "acme";

    // Create + append so the chain exists (the 501 is independent of chain state).
    let (s, _) = call(&app, "PUT", "/evidence/c", Some(tenant), Some(json!({}))).await;
    assert_eq!(s, StatusCode::OK);
    let (s, _) = call(
        &app,
        "POST",
        "/evidence/c/entries",
        Some(tenant),
        Some(json!({ "events": [{ "type": "e", "payload_b64": B64.encode(b"x") }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, body) = call(&app, "GET", "/evidence/c/digest/signed", Some(tenant), None).await;
    assert_eq!(s, StatusCode::NOT_IMPLEMENTED, "signing off: {body}");

    let (s, body) = call(&app, "GET", "/evidence/signing-key", Some(tenant), None).await;
    assert_eq!(s, StatusCode::NOT_IMPLEMENTED, "signing-key off: {body}");
}
