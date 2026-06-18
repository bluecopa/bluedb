//! [`InMemoryNodeRegistry`] discovery semantics, exercised deterministically
//! with a `TestClock` (no sleeping): heartbeat→live, `url_for`, TTL expiry,
//! multi-node, and re-heartbeat refresh.

use std::sync::Arc;
use std::time::Duration;

use bluedb_ha::{InMemoryNodeRegistry, NodeRegistry, TestClock};

const TTL: Duration = Duration::from_millis(10_000);

/// A registry over a fresh clock at t=0.
fn registry() -> (InMemoryNodeRegistry, TestClock) {
    let clock = TestClock::new(0);
    let reg = InMemoryNodeRegistry::with_clock(TTL, Arc::new(clock.clone()));
    (reg, clock)
}

#[tokio::test]
async fn heartbeat_makes_a_node_live_and_resolvable() {
    let (reg, _clock) = registry();

    // Nothing registered yet.
    assert!(reg.live_nodes().await.unwrap().is_empty());
    assert_eq!(reg.url_for("a").await.unwrap(), None);

    reg.heartbeat("a", "http://a:8080").await.unwrap();

    // Now live + resolvable.
    let live = reg.live_nodes().await.unwrap();
    assert_eq!(live, vec![("a".to_string(), "http://a:8080".to_string())]);
    assert_eq!(reg.url_for("a").await.unwrap(), Some("http://a:8080".to_string()));
    // Unknown node stays unresolvable.
    assert_eq!(reg.url_for("ghost").await.unwrap(), None);
}

#[tokio::test]
async fn entry_past_ttl_is_not_live() {
    let (reg, clock) = registry();
    reg.heartbeat("a", "http://a:8080").await.unwrap(); // last_seen = 0, TTL 10_000

    clock.set(9_999);
    assert_eq!(reg.live_nodes().await.unwrap().len(), 1, "within TTL");
    assert_eq!(reg.url_for("a").await.unwrap(), Some("http://a:8080".to_string()));

    // Strictly past the TTL window → no longer live (and url_for hides it).
    clock.set(10_001);
    assert!(reg.live_nodes().await.unwrap().is_empty(), "past TTL");
    assert_eq!(reg.url_for("a").await.unwrap(), None);
}

#[tokio::test]
async fn re_heartbeat_refreshes_liveness_and_url() {
    let (reg, clock) = registry();
    reg.heartbeat("a", "http://a:8080").await.unwrap(); // last_seen = 0

    // Just before expiry, heartbeat again with a NEW url → window resets + url updates.
    clock.set(9_000);
    reg.heartbeat("a", "http://a-v2:8080").await.unwrap(); // last_seen = 9_000

    // At 18_000 the ORIGINAL window (expires 10_000) is long gone, but the
    // refreshed one (expires 19_000) keeps the node live with the new url.
    clock.set(18_000);
    assert_eq!(
        reg.url_for("a").await.unwrap(),
        Some("http://a-v2:8080".to_string()),
        "re-heartbeat refreshed the window and updated the url"
    );

    // Past the refreshed window → gone.
    clock.set(19_001);
    assert_eq!(reg.url_for("a").await.unwrap(), None);
}

#[tokio::test]
async fn multi_node_tracks_each_independently() {
    let (reg, clock) = registry();
    reg.heartbeat("a", "http://a:8080").await.unwrap(); // t=0
    clock.set(6_000);
    reg.heartbeat("b", "http://b:8080").await.unwrap(); // t=6_000

    let mut live = reg.live_nodes().await.unwrap();
    live.sort();
    assert_eq!(
        live,
        vec![
            ("a".to_string(), "http://a:8080".to_string()),
            ("b".to_string(), "http://b:8080".to_string()),
        ]
    );

    // At 11_000, a's window (expires 10_000) has lapsed but b's (expires 16_000)
    // is still live — independent liveness per node.
    clock.set(11_000);
    let live = reg.live_nodes().await.unwrap();
    assert_eq!(live, vec![("b".to_string(), "http://b:8080".to_string())]);
    assert_eq!(reg.url_for("a").await.unwrap(), None);
    assert_eq!(reg.url_for("b").await.unwrap(), Some("http://b:8080".to_string()));
}
