//! Single-writer election + self-fencing, exercised deterministically with a
//! `TestClock` (no sleeping) and a shared in-memory lease provider.

use std::sync::Arc;
use std::time::Duration;

use bluedb_ha::{HaError, LeaseProvider, LocalLeaseProvider, TestClock, WriterController};

const TTL: Duration = Duration::from_millis(10_000);
const MARGIN: Duration = Duration::from_millis(2_000);

/// Two controllers ("a", "b") over one shared provider + clock.
fn two_nodes() -> (Arc<WriterController>, Arc<WriterController>, TestClock) {
    let provider: Arc<dyn LeaseProvider> = Arc::new(LocalLeaseProvider::new());
    let clock = TestClock::new(0);
    let a = Arc::new(WriterController::new(
        "a",
        provider.clone(),
        Arc::new(clock.clone()),
        TTL,
        MARGIN,
    ));
    let b = Arc::new(WriterController::new(
        "b",
        provider.clone(),
        Arc::new(clock.clone()),
        TTL,
        MARGIN,
    ));
    (a, b, clock)
}

#[tokio::test]
async fn only_one_node_can_be_active() {
    let (a, b, _clock) = two_nodes();

    let lease = a.promote().await.expect("a promotes");
    assert_eq!(lease.epoch, 1);
    assert!(a.is_active());
    assert!(!b.is_active());

    // b cannot promote while a holds a live lease.
    assert!(matches!(
        b.promote().await,
        Err(HaError::LeaseHeldByAnother)
    ));
    assert!(!b.is_active());

    // After a steps down, b can take over — with a higher fencing epoch.
    a.demote().await.expect("a demotes");
    assert!(!a.is_active());
    let lease_b = b.promote().await.expect("b promotes after a demotes");
    assert_eq!(lease_b.epoch, 2, "epoch advances on handover");
    assert!(b.is_active());
}

#[tokio::test]
async fn writer_self_fences_before_lease_expiry() {
    let (a, _b, clock) = two_nodes();
    a.promote().await.unwrap(); // lease [0, 10_000), self-fence margin 2_000

    clock.set(7_000);
    assert!(a.is_active(), "well within validity");

    // Past (expiry - margin) = 8_000, the node self-fences even though the lease
    // technically lasts until 10_000 and renewal hasn't run.
    clock.set(8_500);
    assert!(!a.is_active(), "self-fenced inside the safety margin");

    clock.set(11_000);
    assert!(!a.is_active(), "expired");
}

#[tokio::test]
async fn renewal_extends_validity() {
    let (a, _b, clock) = two_nodes();
    a.promote().await.unwrap(); // [0, 10_000)

    clock.set(3_000);
    assert!(a.renew_once().await.unwrap(), "renew while active"); // now [_, 13_000)

    // Without the renew this would be self-fenced (8_000); with it, still active.
    clock.set(9_000);
    assert!(a.is_active(), "renew pushed expiry out to 13_000");
}

#[tokio::test]
async fn losing_the_lease_self_fences_on_renew() {
    let (a, b, clock) = two_nodes();
    a.promote().await.unwrap(); // a holds [0, 10_000), epoch 1

    // a's lease expires; b legitimately takes over at 11_000 (epoch 2).
    clock.set(11_000);
    let lease_b = b.promote().await.expect("b takes the expired lease");
    assert_eq!(lease_b.epoch, 2);

    // a, none the wiser, tries to renew → discovers it lost the lease → Passive.
    assert!(
        !a.renew_once().await.unwrap(),
        "renew fails: lease was taken"
    );
    assert!(!a.is_active(), "a self-fenced");
    assert_eq!(a.epoch(), None);
    assert!(b.is_active());
}

#[tokio::test]
async fn reacquiring_own_live_lease_keeps_the_epoch() {
    let (a, _b, clock) = two_nodes();
    let first = a.promote().await.unwrap();
    assert_eq!(first.epoch, 1);

    // Re-promoting while still holding a live lease does not bump the epoch...
    clock.set(1_000);
    let again = a.promote().await.unwrap();
    assert_eq!(again.epoch, 1, "no handover ⇒ same fencing token");

    // ...but a genuine handover (after release) does.
    a.demote().await.unwrap();
    let after_handover = a.promote().await.unwrap();
    assert_eq!(after_handover.epoch, 2);
}
