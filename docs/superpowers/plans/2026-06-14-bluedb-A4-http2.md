# bluedb A4 — HTTP/2 (h2c) Implementation Plan

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Ensure the server speaks HTTP/2 so one client connection can multiplex many concurrent in-flight writes (no large connection pool needed), prove it end-to-end, and document it.

**Finding:** axum 0.8's default features already include `http2`, and the `h2` crate is in `bluedb-server`'s build tree — so `axum::serve(listener, app)` already auto-negotiates HTTP/1.1 and HTTP/2 (h2c, prior-knowledge) over the plaintext listener. A4 makes this explicit + tested + documented. h2-over-TLS (ALPN) is a deployment concern, out of scope.

**Tech Stack:** axum 0.8, hyper/h2, reqwest (dev-dep, h2c client for the test).

---

## Task 1: Make http2 explicit, prove h2c, document

**Files:** `crates/bluedb-server/Cargo.toml`, `crates/bluedb-server/src/main.rs`, `crates/bluedb-server/tests/http2.rs` (new).

- [ ] **Step 1: explicit feature (guard).** In `crates/bluedb-server/Cargo.toml`, pin axum's `http2` feature explicitly so a future `default-features` change can't silently drop it:
```toml
axum          = { version = "0.8", features = ["http2"] }
```
(Functionally a no-op today since it's default; intent + guard.)

- [ ] **Step 2: failing h2c integration test.** Add dev-dep reqwest (h2c client, lean features):
```toml
# in [dev-dependencies]
reqwest = { version = "0.12", default-features = false, features = ["http2"] }
```
Create `crates/bluedb-server/tests/http2.rs`:
```rust
//! Proves the server accepts HTTP/2 (h2c, prior-knowledge) on the plaintext listener.
use std::sync::Arc;

use bluedb_server::build_router; // see Step 3 — expose the Router builder
use slatedb::object_store::memory::InMemory;
use tokio::net::TcpListener;

#[tokio::test]
async fn server_speaks_http2_h2c_on_health() {
    // Bind an ephemeral port and serve the real router in the background.
    let app = build_router_for_test().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // h2c prior-knowledge client (cleartext HTTP/2, no TLS).
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let resp = client.get(format!("http://{addr}/health")).send().await.unwrap();
    assert_eq!(resp.version(), reqwest::Version::HTTP_2);
    assert!(resp.status().is_success());
}

// Build the router with an in-memory store + a passive AppState (enough for /health,
// which does not require the active writer).
async fn build_router_for_test() -> axum::Router {
    let _ = InMemory::new(); // store wiring per the helper added in Step 3
    build_router_with_in_memory_store()
}
```
Note: the exact test-harness construction depends on what `bluedb-server` exposes. The intent: serve the real `Router` over a real socket and assert the response is HTTP/2. If `/health` needs no `AppState` role, build the minimal `AppState` over an `InMemory` store. Adapt Step 3 to expose what the test needs.

- [ ] **Step 3: expose a Router builder for the test.** If `bluedb-server` only constructs its `Router` inside `main()`, refactor the `Router::new().route(...)` construction into a `pub fn build_router(state: AppState) -> Router` in `lib.rs`, and have `main()` call it. Add a test-only constructor for an `AppState` over an `InMemory` object store (e.g. `AppState::new(Arc::new(InMemory::new()), "bluedb", writer)` with a `LocalLeaseProvider`-backed `WriterController`, or expose a `pub fn build_router_with_in_memory_store() -> Router` under `#[cfg(any(test, feature = "test-util"))]`). Keep it minimal — just enough for the test to hit `/health` over h2c.

- [ ] **Step 4: run.** `cargo test -p bluedb-server --test http2 -- --nocapture` → PASS (response is HTTP/2.0). Then `cargo test -p bluedb-server` (all pass) and `cargo build -p bluedb-server` (clean).

- [ ] **Step 5: document + commit.** Add to `main.rs`'s `//!` block:
```
//! The server speaks HTTP/1.1 and HTTP/2 (h2c, prior-knowledge) on the plaintext
//! listener — an h2 client can multiplex many concurrent in-flight writes over one
//! connection. HTTP/2 over TLS (ALPN) is a deployment-layer concern.
```
Commit:
```bash
git add crates/bluedb-server
git commit -m "feat(server): explicit HTTP/2 (h2c) support + end-to-end test"
```

## Fallback
If exposing a test Router/AppState proves disproportionately invasive, OR the reqwest h2c client is fiddly, DON'T force it: keep Step 1 (explicit `http2` feature) + Step 5 (doc), drop the integration test, and report DONE_WITH_CONCERNS noting h2c is provided by axum's `http2` feature (the `h2` crate is in the build tree) but an end-to-end transport test was deferred for harness-complexity reasons. A documented, feature-guaranteed capability beats a brittle test.

## Self-Review
- Spec A §4.6 coverage: HTTP/2 available ✓ (h2c), documented ✓, TLS/ALPN noted as deployment ✓. Multiplexing is a property of h2 itself (provided), not custom code.
