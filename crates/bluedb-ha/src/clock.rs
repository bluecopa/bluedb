//! Injectable clock — so lease expiry / self-fencing is deterministically
//! testable without sleeping.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// A source of "now" in epoch milliseconds.
pub trait Clock: Send + Sync + 'static {
    /// Current time, milliseconds since the Unix epoch.
    fn now_millis(&self) -> i64;
}

/// Wall-clock backed by the system time. The production clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// A manually-advanced clock for tests: lease TTLs, renewal, and self-fencing
/// can be exercised by moving time forward instead of waiting.
#[derive(Debug, Clone)]
pub struct TestClock(Arc<AtomicI64>);

impl TestClock {
    /// A test clock starting at `start_millis`.
    pub fn new(start_millis: i64) -> Self {
        Self(Arc::new(AtomicI64::new(start_millis)))
    }

    /// Move the clock forward by `delta_millis`.
    pub fn advance(&self, delta_millis: i64) {
        self.0.fetch_add(delta_millis, Ordering::SeqCst);
    }

    /// Set the clock to an absolute time.
    pub fn set(&self, millis: i64) {
        self.0.store(millis, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_millis(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}
