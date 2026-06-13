//! [`WriterController`] — the self-fencing single-writer state machine.
//!
//! A node is either **Active** (holds the lease; may write) or **Passive**
//! (read-only). `promote` acquires the lease, `demote` releases it, and a
//! background renewal task keeps an Active node's lease fresh. Two independent
//! fences keep at most one writer live:
//!
//! 1. **Election** — [`promote`](WriterController::promote) only succeeds if the
//!    [`LeaseProvider`] grants the lease, so two nodes can't both go Active.
//! 2. **Self-fencing** — [`is_active`](WriterController::is_active) (the gate a
//!    write checks) returns `false` once the clock is within `safety_margin` of
//!    the lease's expiry, *even if the renewal task is wedged*. So a node stops
//!    writing before its lease could be handed to someone else; and a failed
//!    renew (lease lost) immediately flips the node Passive.
//!
//! The lease `epoch` (a fencing token) rides along for the storage layer:
//! SlateDB's own `writer_epoch` CAS fences a stale former writer that slips
//! past the logical gate, so the two layers compose into hard single-writer
//! safety.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use thiserror::Error;
use tokio::task::JoinHandle;

use crate::clock::Clock;
use crate::lease::{Lease, LeaseProvider};

/// A node's writer role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Holds the lease; writes are permitted.
    Active,
    /// Read-only; writes are refused.
    Passive,
}

impl Role {
    /// Lowercase label (`"active"` / `"passive"`) for status/JSON.
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Active => "active",
            Role::Passive => "passive",
        }
    }
}

/// Errors from promote/renew/demote.
#[derive(Debug, Error)]
pub enum HaError {
    /// `promote` failed because a different node holds an unexpired lease.
    #[error("cannot promote: the lease is held by another node")]
    LeaseHeldByAnother,
    /// The underlying [`LeaseProvider`] errored.
    #[error(transparent)]
    Provider(#[from] anyhow::Error),
}

/// A point-in-time view of a controller's role.
#[derive(Debug, Clone)]
pub struct Status {
    /// This node's id.
    pub node_id: String,
    /// Effective role *now* (applies the self-fencing time gate).
    pub role: Role,
    /// Current fencing-token epoch, if Active.
    pub epoch: Option<u64>,
    /// Lease expiry (epoch millis), if Active.
    pub lease_expires_at_millis: Option<i64>,
}

#[derive(Debug)]
struct State {
    role: Role,
    epoch: Option<u64>,
    expires_at_millis: i64,
}

/// Drives one node's writer role over a shared [`LeaseProvider`].
pub struct WriterController {
    node_id: String,
    provider: Arc<dyn LeaseProvider>,
    clock: Arc<dyn Clock>,
    ttl: Duration,
    safety_margin: Duration,
    state: Mutex<State>,
}

impl WriterController {
    /// A new controller, initially **Passive**. `ttl` is the lease lifetime;
    /// `safety_margin` is how long before expiry the node self-fences (must be
    /// `< ttl`).
    pub fn new(
        node_id: impl Into<String>,
        provider: Arc<dyn LeaseProvider>,
        clock: Arc<dyn Clock>,
        ttl: Duration,
        safety_margin: Duration,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            provider,
            clock,
            ttl,
            safety_margin,
            state: Mutex::new(State {
                role: Role::Passive,
                epoch: None,
                expires_at_millis: 0,
            }),
        }
    }

    /// This node's id.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Try to become the Active writer by acquiring the lease. Returns the
    /// granted [`Lease`], or [`HaError::LeaseHeldByAnother`].
    pub async fn promote(&self) -> Result<Lease, HaError> {
        let now = self.clock.now_millis();
        match self.provider.try_acquire(&self.node_id, self.ttl, now).await? {
            Some(lease) => {
                self.set_active(&lease);
                Ok(lease)
            }
            None => Err(HaError::LeaseHeldByAnother),
        }
    }

    /// Renew the lease once. Returns `true` if still Active afterward; `false`
    /// if not Active to begin with, or if the lease was lost (the node
    /// self-fences to Passive).
    pub async fn renew_once(&self) -> Result<bool, HaError> {
        let epoch = match self.stored_active_epoch() {
            Some(epoch) => epoch,
            None => return Ok(false),
        };
        let now = self.clock.now_millis();
        match self.provider.renew(&self.node_id, epoch, self.ttl, now).await? {
            Some(lease) => {
                self.set_active(&lease);
                Ok(true)
            }
            None => {
                // Lost the lease → self-fence.
                self.set_passive();
                Ok(false)
            }
        }
    }

    /// Step down: release the lease (if held) and go Passive.
    pub async fn demote(&self) -> Result<(), HaError> {
        if let Some(epoch) = self.stored_active_epoch() {
            self.provider.release(&self.node_id, epoch).await?;
        }
        self.set_passive();
        Ok(())
    }

    /// Is this node permitted to write *right now*? True only while Active AND
    /// the lease is still valid with at least `safety_margin` to spare. This is
    /// the gate every write must pass — it self-fences a node whose lease is
    /// near expiry even if renewal has stalled.
    pub fn is_active(&self) -> bool {
        let now = self.clock.now_millis();
        let state = self.state.lock().expect("ha state poisoned");
        state.role == Role::Active
            && now < state.expires_at_millis - self.safety_margin.as_millis() as i64
    }

    /// The current fencing-token epoch (only while genuinely Active).
    pub fn epoch(&self) -> Option<u64> {
        if self.is_active() {
            self.state.lock().expect("ha state poisoned").epoch
        } else {
            None
        }
    }

    /// A status snapshot, with the effective (self-fenced) role.
    pub fn status(&self) -> Status {
        let active = self.is_active();
        let state = self.state.lock().expect("ha state poisoned");
        Status {
            node_id: self.node_id.clone(),
            role: if active { Role::Active } else { Role::Passive },
            epoch: if active { state.epoch } else { None },
            lease_expires_at_millis: if active { Some(state.expires_at_millis) } else { None },
        }
    }

    /// Spawn the background renewal loop: every `ttl/3` it renews while Active.
    /// A failed renew self-fences (handled in [`renew_once`]). Runs until the
    /// returned handle is aborted or the process exits.
    pub fn spawn_renewal(self: Arc<Self>) -> JoinHandle<()> {
        let interval = std::cmp::max(self.ttl / 3, Duration::from_millis(1));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                if self.stored_active_epoch().is_some() {
                    let _ = self.renew_once().await;
                }
            }
        })
    }

    // --- state helpers (lock held only briefly, never across an await) -------

    fn set_active(&self, lease: &Lease) {
        let mut state = self.state.lock().expect("ha state poisoned");
        state.role = Role::Active;
        state.epoch = Some(lease.epoch);
        state.expires_at_millis = lease.expires_at_millis;
    }

    fn set_passive(&self) {
        let mut state = self.state.lock().expect("ha state poisoned");
        state.role = Role::Passive;
        state.epoch = None;
        state.expires_at_millis = 0;
    }

    /// The stored epoch if the *stored* role is Active (ignores the time gate —
    /// used by renewal, which is what refreshes an about-to-expire lease).
    fn stored_active_epoch(&self) -> Option<u64> {
        let state = self.state.lock().expect("ha state poisoned");
        match state.role {
            Role::Active => state.epoch,
            Role::Passive => None,
        }
    }
}
