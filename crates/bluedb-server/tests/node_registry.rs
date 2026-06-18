//! R1 node-registry wiring on `AppState`.
//!
//! Verifies the discovery seam the later cross-node redirect / affinity routing
//! will consume: an installed [`NodeRegistry`] is reachable via
//! `AppState::node_registry()`, a heartbeat makes a node discoverable, and
//! `url_for(node_id)` resolves it — including resolving *this* node's own
//! `node_id` (the lease holder), which is exactly the `url_for(lease.holder)`
//! call the HA-302 redirect will make.
//!
//! These exercise the `InMemoryNodeRegistry` backend through the server's state
//! (no socket, no Postgres, no cluster — default `cargo test`).

use std::sync::Arc;
use std::time::Duration;

use bluedb_ha::{
    InMemoryNodeRegistry, LeaseProvider, LocalLeaseProvider, NodeRegistry, SystemClock,
    WriterController,
};
use bluedb_server::AppState;
use slatedb::object_store::{memory::InMemory, ObjectStore};

const TTL: Duration = Duration::from_secs(30);
const MARGIN: Duration = Duration::from_secs(5);
const REG_TTL: Duration = Duration::from_secs(60);

/// An app state for `node_id` with an in-memory registry installed.
fn state_with_registry(node_id: &str) -> (AppState, Arc<dyn NodeRegistry>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arc::new(WriterController::new(
        node_id,
        Arc::new(LocalLeaseProvider::new()) as Arc<dyn LeaseProvider>,
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ));
    let registry: Arc<dyn NodeRegistry> = Arc::new(InMemoryNodeRegistry::new(REG_TTL));
    let state = AppState::new(store, "bluedb", writer).with_node_registry(registry.clone());
    (state, registry)
}

#[tokio::test]
async fn node_registry_is_installed_and_returned() {
    let (state, _reg) = state_with_registry("node-a");
    assert!(
        state.node_registry().is_some(),
        "with_node_registry installs a registry reachable via node_registry()"
    );
}

#[tokio::test]
async fn unset_node_registry_is_none() {
    // AppState::new without with_node_registry → no discovery (single-node/tests).
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arc::new(WriterController::new(
        "solo",
        Arc::new(LocalLeaseProvider::new()) as Arc<dyn LeaseProvider>,
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ));
    let state = AppState::new(store, "bluedb", writer);
    assert!(state.node_registry().is_none());
}

#[tokio::test]
async fn heartbeat_then_resolve_via_state_registry() {
    let (state, _reg) = state_with_registry("node-a");
    let reg = state.node_registry().expect("registry installed").clone();

    // Simulate the heartbeat loop registering this node.
    reg.heartbeat("node-a", "http://node-a:8080").await.unwrap();
    // And a peer registering itself.
    reg.heartbeat("node-b", "http://node-b:8080").await.unwrap();

    // Both are discoverable; url_for resolves each.
    let mut live = reg.live_nodes().await.unwrap();
    live.sort();
    assert_eq!(
        live,
        vec![
            ("node-a".to_string(), "http://node-a:8080".to_string()),
            ("node-b".to_string(), "http://node-b:8080".to_string()),
        ]
    );
    assert_eq!(
        reg.url_for("node-b").await.unwrap(),
        Some("http://node-b:8080".to_string())
    );
}

#[tokio::test]
async fn resolves_this_nodes_own_id_for_ha302() {
    // The HA-302 redirect resolves the writer via `url_for(lease.holder)`. Here
    // the writer controller's node_id IS the lease holder once promoted, so the
    // registry must resolve that same id to a URL. This is the exact call shape
    // the redirect will use.
    let (state, _reg) = state_with_registry("writer-1");
    let reg = state.node_registry().expect("registry installed").clone();
    let my_id = state.writer().node_id().to_string();
    assert_eq!(my_id, "writer-1");

    reg.heartbeat(&my_id, "http://writer-1:8080").await.unwrap();
    assert_eq!(
        reg.url_for(&my_id).await.unwrap(),
        Some("http://writer-1:8080".to_string()),
        "url_for(self/lease.holder) resolves — the HA-302 lookup the redirect will perform"
    );
}
