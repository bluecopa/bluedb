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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bluedb_ha::{LeaseProvider, PostgresLeaseProvider};

const TTL: Duration = Duration::from_millis(10_000);

fn pg_url() -> Option<String> {
    std::env::var("BLUEDB_TEST_PG_URL").ok()
}

fn unique_resource(prefix: &str) -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
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
    let a = p.try_acquire("a", TTL, 0).await.unwrap().expect("a acquires");
    assert_eq!(a.epoch, 1);
    assert_eq!(a.expires_at_millis, 10_000);

    // b is denied while a's lease is live.
    assert!(p.try_acquire("b", TTL, 1_000).await.unwrap().is_none());

    // a renews (same epoch, extended expiry).
    let renewed = p.renew("a", 1, TTL, 2_000).await.unwrap().expect("a renews");
    assert_eq!(renewed.epoch, 1);
    assert_eq!(renewed.expires_at_millis, 12_000);

    // After expiry, b takes over with a strictly higher fencing epoch.
    let b = p.try_acquire("b", TTL, 13_000).await.unwrap().expect("b takes expired lease");
    assert_eq!(b.epoch, 2);

    // a, having lost it, fails to renew → must self-fence.
    assert!(p.renew("a", 1, TTL, 14_000).await.unwrap().is_none());
}

#[tokio::test]
async fn epoch_preserved_on_self_reacquire_bumped_on_handover() {
    let p = provider_or_skip!("epoch");

    assert_eq!(p.try_acquire("a", TTL, 0).await.unwrap().unwrap().epoch, 1);
    // Re-acquiring our own live lease keeps the epoch.
    assert_eq!(p.try_acquire("a", TTL, 1_000).await.unwrap().unwrap().epoch, 1);
    // Release, then a genuine handover bumps it.
    p.release("a", 1).await.unwrap();
    assert_eq!(p.try_acquire("b", TTL, 2_000).await.unwrap().unwrap().epoch, 2);
}

#[tokio::test]
async fn release_frees_the_lease_immediately() {
    let p = provider_or_skip!("release");
    p.try_acquire("a", TTL, 0).await.unwrap().unwrap();
    p.release("a", 1).await.unwrap();
    // Free now (no need to wait for expiry).
    let b = p.try_acquire("b", TTL, 1).await.unwrap().expect("b acquires freed lease");
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
    let pa = PostgresLeaseProvider::connect(&url, resource.clone()).await.unwrap();
    let pb = PostgresLeaseProvider::connect(&url, resource).await.unwrap();

    let (ra, rb) = tokio::join!(
        pa.try_acquire("a", TTL, 0),
        pb.try_acquire("b", TTL, 0),
    );
    let granted = [ra.unwrap().is_some(), rb.unwrap().is_some()];
    assert_eq!(
        granted.iter().filter(|g| **g).count(),
        1,
        "exactly one of two concurrent acquirers wins the fresh lease"
    );
}
