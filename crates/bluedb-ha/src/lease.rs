//! The lease abstraction — a time-bounded, **fencing-token-stamped** grant of
//! the single-writer role.
//!
//! Exactly one node may hold the lease at a time. Each time the lease changes
//! hands (a new acquisition after the previous holder's lease expired or was
//! released) the `epoch` — a monotonically increasing **fencing token** —
//! advances. Renewing your own live lease keeps the same epoch. This epoch
//! fences at the *lease* layer (a stale holder can no longer renew). The storage
//! layer is fenced *independently* by SlateDB's own `writer_epoch`, which SlateDB
//! bumps from its persisted manifest on every writer `Db` open — this lease
//! `epoch` is not threaded into SlateDB. The two compose only in that each admits
//! at most one writer; neither counter is derived from the other.
//!
//! [`LeaseProvider`] is the seam. The in-memory [`LocalLeaseProvider`] backs a
//! single process (and the tests). A production deployment implements it over a
//! shared, HA store — a Postgres lease row (`SELECT ... FOR UPDATE` / a
//! conditional `UPDATE`), a Kubernetes `Lease` object, or a NATS KV bucket —
//! without changing anything above this trait.

use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

/// A held lease: who holds it, the fencing-token `epoch`, and when it expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    /// Node id of the holder.
    pub holder: String,
    /// Monotonic fencing token, bumped on every change of holder.
    pub epoch: u64,
    /// Expiry, epoch milliseconds. After this, another node may acquire.
    pub expires_at_millis: i64,
}

/// The pluggable lease store. Implementations must guarantee that at most one
/// holder has an unexpired lease at any wall-clock instant.
#[async_trait]
pub trait LeaseProvider: Send + Sync + 'static {
    /// Acquire (or re-acquire) the lease for `holder`, valid for `ttl` from
    /// `now_millis`. Returns the granted [`Lease`] (epoch bumped on a genuine
    /// change of holder; preserved when `holder` already holds a live lease), or
    /// `None` if a *different* holder currently has an unexpired lease.
    async fn try_acquire(&self, holder: &str, ttl: Duration, now_millis: i64) -> Result<Option<Lease>>;

    /// Extend `holder`'s lease, identified by `epoch`. Returns the refreshed
    /// lease, or `None` if `holder` no longer holds it at `epoch` (lost it — the
    /// caller MUST self-fence). Never bumps the epoch.
    async fn renew(&self, holder: &str, epoch: u64, ttl: Duration, now_millis: i64) -> Result<Option<Lease>>;

    /// Best-effort release of `holder`'s lease at `epoch` (no-op if not held).
    async fn release(&self, holder: &str, epoch: u64) -> Result<()>;
}

fn ttl_millis(ttl: Duration) -> i64 {
    ttl.as_millis() as i64
}

/// In-memory [`LeaseProvider`] — single-process (and test) backing.
///
/// Correct for one process: the mutex serializes acquire/renew/release so the
/// single-holder invariant holds. It does NOT coordinate across processes — use
/// a shared store (Postgres/K8s/NATS) for real multi-node HA.
#[derive(Debug, Default)]
pub struct LocalLeaseProvider {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    lease: Option<Lease>,
    /// Highest epoch ever issued, so a new acquisition always advances.
    last_epoch: u64,
}

impl LocalLeaseProvider {
    /// A fresh, unheld lease store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LeaseProvider for LocalLeaseProvider {
    async fn try_acquire(&self, holder: &str, ttl: Duration, now_millis: i64) -> Result<Option<Lease>> {
        let mut inner = self.inner.lock().expect("lease mutex poisoned");

        // Held by a *different*, still-live holder → denied.
        if let Some(current) = &inner.lease {
            if current.holder != holder && current.expires_at_millis > now_millis {
                return Ok(None);
            }
        }

        // Grant. Preserve the epoch only when we're re-acquiring our OWN still-live
        // lease; otherwise (free / expired / taken-from-an-expired-other) advance.
        let epoch = match &inner.lease {
            Some(current) if current.holder == holder && current.expires_at_millis > now_millis => {
                current.epoch
            }
            _ => inner.last_epoch + 1,
        };
        inner.last_epoch = inner.last_epoch.max(epoch);

        let lease = Lease {
            holder: holder.to_owned(),
            epoch,
            expires_at_millis: now_millis + ttl_millis(ttl),
        };
        inner.lease = Some(lease.clone());
        Ok(Some(lease))
    }

    async fn renew(&self, holder: &str, epoch: u64, ttl: Duration, now_millis: i64) -> Result<Option<Lease>> {
        let mut inner = self.inner.lock().expect("lease mutex poisoned");
        match &inner.lease {
            // Still ours, same epoch, not yet expired → extend.
            Some(current)
                if current.holder == holder
                    && current.epoch == epoch
                    && current.expires_at_millis > now_millis =>
            {
                let lease = Lease {
                    holder: holder.to_owned(),
                    epoch,
                    expires_at_millis: now_millis + ttl_millis(ttl),
                };
                inner.lease = Some(lease.clone());
                Ok(Some(lease))
            }
            // Lost it (expired, or someone else holds it, or epoch moved on).
            _ => Ok(None),
        }
    }

    async fn release(&self, holder: &str, epoch: u64) -> Result<()> {
        let mut inner = self.inner.lock().expect("lease mutex poisoned");
        if let Some(current) = &inner.lease {
            if current.holder == holder && current.epoch == epoch {
                inner.lease = None;
            }
        }
        Ok(())
    }
}
