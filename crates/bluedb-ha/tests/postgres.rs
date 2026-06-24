//! Conformance + concurrency tests for the Postgres lease arbiter.
//!
//! Gated two ways: the `postgres` crate feature must be on, AND
//! `BLUEDB_TEST_PG_URL` must point at a reachable Postgres (else each test
//! no-ops). Run with, e.g.:
//!
//! ```text
//! BLUEDB_TEST_PG_URL=postgresql://postgres:pw@localhost:5432/postgres \
//!   cargo test -p bluedb-ha --features postgres --test postgres
//! ```
//!
//! Each test uses a unique `resource` key so reruns start from a clean row and
//! concurrent tests don't interfere.

#![cfg(feature = "postgres")]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bluedb_ha::{
    LeaseProvider, NodeRegistry, PostgresLeaseProvider, PostgresNodeRegistry, TestClock,
};

const TTL: Duration = Duration::from_millis(10_000);

fn pg_url() -> Option<String> {
    std::env::var("BLUEDB_TEST_PG_URL").ok()
}

fn unique_resource(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// Connect a provider for a fresh unique resource, or `None` if PG isn't configured.
async fn provider(prefix: &str) -> Option<PostgresLeaseProvider> {
    let url = pg_url()?;
    Some(
        PostgresLeaseProvider::connect(&url, unique_resource(prefix))
            .await
            .expect("connect postgres"),
    )
}

macro_rules! provider_or_skip {
    ($prefix:expr) => {
        match provider($prefix).await {
            Some(p) => p,
            None => {
                eprintln!("skipping: BLUEDB_TEST_PG_URL not set");
                return;
            }
        }
    };
}

#[tokio::test]
async fn election_renew_and_handover() {
    let p = provider_or_skip!("election");

    // a takes the fresh lease at epoch 1.
    let a = p
        .try_acquire("a", TTL, 0)
        .await
        .unwrap()
        .expect("a acquires");
    assert_eq!(a.epoch, 1);
    assert_eq!(a.expires_at_millis, 10_000);

    // b is denied while a's lease is live.
    assert!(p.try_acquire("b", TTL, 1_000).await.unwrap().is_none());

    // a renews (same epoch, extended expiry).
    let renewed = p
        .renew("a", 1, TTL, 2_000)
        .await
        .unwrap()
        .expect("a renews");
    assert_eq!(renewed.epoch, 1);
    assert_eq!(renewed.expires_at_millis, 12_000);

    // After expiry, b takes over with a strictly higher fencing epoch.
    let b = p
        .try_acquire("b", TTL, 13_000)
        .await
        .unwrap()
        .expect("b takes expired lease");
    assert_eq!(b.epoch, 2);

    // a, having lost it, fails to renew → must self-fence.
    assert!(p.renew("a", 1, TTL, 14_000).await.unwrap().is_none());
}

#[tokio::test]
async fn epoch_preserved_on_self_reacquire_bumped_on_handover() {
    let p = provider_or_skip!("epoch");

    assert_eq!(p.try_acquire("a", TTL, 0).await.unwrap().unwrap().epoch, 1);
    // Re-acquiring our own live lease keeps the epoch.
    assert_eq!(
        p.try_acquire("a", TTL, 1_000).await.unwrap().unwrap().epoch,
        1
    );
    // Release, then a genuine handover bumps it.
    p.release("a", 1).await.unwrap();
    assert_eq!(
        p.try_acquire("b", TTL, 2_000).await.unwrap().unwrap().epoch,
        2
    );
}

#[tokio::test]
async fn release_frees_the_lease_immediately() {
    let p = provider_or_skip!("release");
    p.try_acquire("a", TTL, 0).await.unwrap().unwrap();
    p.release("a", 1).await.unwrap();
    // Free now (no need to wait for expiry).
    let b = p
        .try_acquire("b", TTL, 1)
        .await
        .unwrap()
        .expect("b acquires freed lease");
    assert_eq!(b.epoch, 2);
}

#[tokio::test]
async fn concurrent_acquire_grants_exactly_one() {
    let url = match pg_url() {
        Some(u) => u,
        None => {
            eprintln!("skipping: BLUEDB_TEST_PG_URL not set");
            return;
        }
    };
    // Two independent providers (separate connections) racing for ONE resource —
    // the real test of the atomic upsert across connections.
    let resource = unique_resource("race");
    let pa = PostgresLeaseProvider::connect(&url, resource.clone())
        .await
        .unwrap();
    let pb = PostgresLeaseProvider::connect(&url, resource)
        .await
        .unwrap();

    let (ra, rb) = tokio::join!(pa.try_acquire("a", TTL, 0), pb.try_acquire("b", TTL, 0),);
    let granted = [ra.unwrap().is_some(), rb.unwrap().is_some()];
    assert_eq!(
        granted.iter().filter(|g| **g).count(),
        1,
        "exactly one of two concurrent acquirers wins the fresh lease"
    );
}

// --- node registry ----------------------------------------------------------
//
// Gated the same two ways (the `postgres` feature + a reachable `BLUEDB_TEST_PG_URL`).
// Each test uses a unique `node_id` so concurrent runs against the shared
// `bluedb_nodes` table don't interfere, and an injected `TestClock` so TTL
// liveness is deterministic without sleeping.

/// Connect a registry (TTL 10s) on an injected clock, or `None` if PG is unset.
async fn registry(clock: TestClock) -> Option<PostgresNodeRegistry> {
    let url = pg_url()?;
    Some(
        PostgresNodeRegistry::connect_with_clock(&url, TTL, Arc::new(clock))
            .await
            .expect("connect postgres node registry"),
    )
}

macro_rules! registry_or_skip {
    ($clock:expr) => {
        match registry($clock).await {
            Some(r) => r,
            None => {
                eprintln!("skipping: BLUEDB_TEST_PG_URL not set");
                return;
            }
        }
    };
}

#[tokio::test]
async fn registry_heartbeat_live_and_url_for() {
    let clock = TestClock::new(0);
    let reg = registry_or_skip!(clock.clone());
    let id = unique_resource("node");

    assert_eq!(reg.url_for(&id).await.unwrap(), None);
    reg.heartbeat(&id, "http://a:8080").await.unwrap();

    // Live + resolvable within TTL.
    assert_eq!(
        reg.url_for(&id).await.unwrap(),
        Some("http://a:8080".to_string())
    );
    assert!(reg
        .live_nodes()
        .await
        .unwrap()
        .iter()
        .any(|(n, u)| n == &id && u == "http://a:8080"));
}

#[tokio::test]
async fn registry_entry_ages_out_past_ttl() {
    let clock = TestClock::new(0);
    let reg = registry_or_skip!(clock.clone());
    let id = unique_resource("node");
    reg.heartbeat(&id, "http://a:8080").await.unwrap(); // last_heartbeat = 0

    clock.set(9_999);
    assert_eq!(
        reg.url_for(&id).await.unwrap(),
        Some("http://a:8080".to_string())
    );

    clock.set(10_001);
    assert_eq!(reg.url_for(&id).await.unwrap(), None, "aged out past TTL");
    assert!(!reg
        .live_nodes()
        .await
        .unwrap()
        .iter()
        .any(|(n, _)| n == &id));
}

#[tokio::test]
async fn registry_re_heartbeat_refreshes_and_updates_url() {
    let clock = TestClock::new(0);
    let reg = registry_or_skip!(clock.clone());
    let id = unique_resource("node");
    reg.heartbeat(&id, "http://a:8080").await.unwrap(); // t=0

    clock.set(9_000);
    reg.heartbeat(&id, "http://a-v2:8080").await.unwrap(); // refresh + new url, expires 19_000

    clock.set(18_000);
    assert_eq!(
        reg.url_for(&id).await.unwrap(),
        Some("http://a-v2:8080".to_string()),
        "upsert refreshed the window and updated the url"
    );

    clock.set(19_001);
    assert_eq!(reg.url_for(&id).await.unwrap(), None);
}
