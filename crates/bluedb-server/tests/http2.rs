//! End-to-end HTTP/2 (h2c, prior-knowledge) test.
//!
//! Binds the real Router on a random port and verifies that a reqwest client
//! using `.http2_prior_knowledge()` speaks HTTP/2.0 to `/health`.

use std::sync::Arc;
use std::time::Duration;

use bluedb_ha::{LocalLeaseProvider, SystemClock, WriterController};
use bluedb_server::{build_app, AppState};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use tokio::net::TcpListener;

const TTL: Duration = Duration::from_secs(30);
const MARGIN: Duration = Duration::from_secs(5);

/// Build a promoted AppState over an in-memory object store.
async fn make_state() -> AppState {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arc::new(WriterController::new(
        "test-node",
        Arc::new(LocalLeaseProvider::new()),
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ));
    let state = AppState::new(store, "bluedb", writer);
    state.promote().await.expect("promote");
    state
}

#[tokio::test]
async fn server_speaks_http2_h2c_on_health() {
    let app = build_app(make_state().await);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();

    let resp = client
        .get(format!("http://{addr}/health"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.version(), reqwest::Version::HTTP_2);
    assert!(resp.status().is_success());
}
