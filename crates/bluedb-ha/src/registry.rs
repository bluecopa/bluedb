//! The **node registry** — discovery of live coordinator nodes and their
//! peer-reachable URLs, keyed by `node_id`.
//!
//! This is the *discovery* half of HA, complementary to (and independent of) the
//! [`LeaseProvider`](crate::LeaseProvider) election. The lease answers *which*
//! `node_id` is the writer (a fencing-token grant); the registry answers *where*
//! a `node_id` can be reached on the peer network, and *which* nodes are live at
//! all.
//! Compose them and a follower can resolve the writer's address —
//! `registry.url_for(lease.holder)` — to forward a write to the active writer, and
//! `live_nodes()` feeds affinity routing across the cluster.
//!
//! The server-side write forwarder consumes the writer lookup; the future
//! affinity hash ring will consume `live_nodes()`. This crate remains just the
//! discovery seam plus its deployment backends.
//!
//! [`NodeRegistry`] is the trait. Three backends pick up where the lease's
//! [`LeaseProvider`] backends leave off, one per deployment model:
//!
//! - [`InMemoryNodeRegistry`] — single-process (and test) backing. A node's
//!   [`heartbeat`](NodeRegistry::heartbeat) upserts `node_id → (url, now)`; a node
//!   is *live* while its last heartbeat is within a TTL. Mirrors
//!   [`LocalLeaseProvider`](crate::LocalLeaseProvider).
//! - `PostgresNodeRegistry` (behind the `postgres` feature) — a shared `bluedb_nodes`
//!   table; `heartbeat` is an upsert, liveness is `last_heartbeat > now - TTL`.
//!   Reuses the lease's Postgres plumbing. Mirrors
//!   [`PostgresLeaseProvider`](crate::PostgresLeaseProvider).
//! - `K8sNodeRegistry` (behind the `kubernetes` feature) — liveness comes from the
//!   Kubernetes API (Ready pods of the coordinator Service), so `heartbeat` is a
//!   documented no-op (readiness *is* the heartbeat).
//!
//! The `node_id` must be the **same** id the [`LeaseProvider`] elects on, so
//! `url_for(lease.holder)` resolves the elected writer. `bluedb-server` derives
//! both from `BLUEDB_NODE_ID`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

/// Discovery of live coordinator nodes and their peer-reachable URLs.
///
/// All three methods key on `node_id` — the SAME id the
/// [`LeaseProvider`](crate::LeaseProvider) elects on — so a caller that knows the
/// writer's `node_id` (from the lease) can resolve its URL here.
#[async_trait]
pub trait NodeRegistry: Send + Sync + 'static {
    /// All currently-live nodes as `(node_id, url)`. "Live" is backend-defined:
    /// within a heartbeat TTL ([`InMemoryNodeRegistry`]/Postgres), or Ready per
    /// the Kubernetes API (K8s). Order is unspecified.
    async fn live_nodes(&self) -> Result<Vec<(String, String)>>;

    /// The peer-reachable URL of `node_id`, or `None` if it is not
    /// currently live. Equivalent to looking `node_id` up in [`Self::live_nodes`].
    async fn url_for(&self, node_id: &str) -> Result<Option<String>>;

    /// Register or refresh this node's liveness, advertising it reachable at
    /// `url`. Backends whose liveness is externally maintained (Kubernetes
    /// readiness) implement this as a **no-op**. A node calls this on a timer
    /// (interval well under the liveness TTL) so it stays in [`Self::live_nodes`].
    async fn heartbeat(&self, node_id: &str, url: &str) -> Result<()>;
}

/// In-memory [`NodeRegistry`] — single-process (and test) backing.
///
/// [`heartbeat`](NodeRegistry::heartbeat) upserts `node_id → (url, last_seen)`;
/// a node is live while `now - last_seen <= ttl`. Correct for one process (the
/// mutex serializes access); it does NOT coordinate across processes — use the
/// Postgres or Kubernetes backend for real multi-node discovery. Mirrors
/// [`LocalLeaseProvider`](crate::LocalLeaseProvider).
///
/// Time is injected via a [`Clock`](crate::Clock) so liveness/TTL expiry is
/// deterministically testable without sleeping (as the lease tests do).
pub struct InMemoryNodeRegistry {
    ttl: Duration,
    clock: std::sync::Arc<dyn crate::Clock>,
    /// `node_id → (url, last_heartbeat_millis)`.
    nodes: Mutex<HashMap<String, (String, i64)>>,
}

impl InMemoryNodeRegistry {
    /// A registry whose entries are live for `ttl` after each heartbeat, using
    /// the wall clock ([`SystemClock`](crate::SystemClock)).
    pub fn new(ttl: Duration) -> Self {
        Self::with_clock(ttl, std::sync::Arc::new(crate::SystemClock))
    }

    /// A registry with an injected clock (tests advance it instead of sleeping).
    pub fn with_clock(ttl: Duration, clock: std::sync::Arc<dyn crate::Clock>) -> Self {
        Self {
            ttl,
            clock,
            nodes: Mutex::new(HashMap::new()),
        }
    }

    fn ttl_millis(&self) -> i64 {
        self.ttl.as_millis() as i64
    }
}

#[async_trait]
impl NodeRegistry for InMemoryNodeRegistry {
    async fn live_nodes(&self) -> Result<Vec<(String, String)>> {
        let now = self.clock.now_millis();
        let cutoff = now - self.ttl_millis();
        let nodes = self.nodes.lock().expect("node registry mutex poisoned");
        Ok(nodes
            .iter()
            .filter(|(_, (_, last))| *last > cutoff)
            .map(|(id, (url, _))| (id.clone(), url.clone()))
            .collect())
    }

    async fn url_for(&self, node_id: &str) -> Result<Option<String>> {
        let now = self.clock.now_millis();
        let cutoff = now - self.ttl_millis();
        let nodes = self.nodes.lock().expect("node registry mutex poisoned");
        Ok(nodes
            .get(node_id)
            .filter(|(_, last)| *last > cutoff)
            .map(|(url, _)| url.clone()))
    }

    async fn heartbeat(&self, node_id: &str, url: &str) -> Result<()> {
        let now = self.clock.now_millis();
        let mut nodes = self.nodes.lock().expect("node registry mutex poisoned");
        nodes.insert(node_id.to_owned(), (url.to_owned(), now));
        Ok(())
    }
}
