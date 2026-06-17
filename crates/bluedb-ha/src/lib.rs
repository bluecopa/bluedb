//! `bluedb-ha` — single-writer high availability for bluedb.
//!
//! bluedb's substrate (SlateDB on object storage) is **single-writer**: exactly
//! one node may mutate a database at a time, enforced at the storage layer by
//! SlateDB's `writer_epoch` compare-and-set fencing. This crate is the layer
//! *above* that — the election and self-fencing that decide *which* node is the
//! writer, and stop a node from writing once it's no longer the writer.
//!
//! - [`LeaseProvider`] — the seam: a time-bounded, epoch-stamped lease store.
//!   [`LocalLeaseProvider`] backs one process (and the tests); a production
//!   deployment implements the trait over a shared HA store (a Postgres lease
//!   row, a Kubernetes `Lease`, or a NATS KV bucket) with no change above it.
//! - [`WriterController`] — the per-node state machine: `promote` / `demote`, a
//!   background renewal loop, and the [`is_active`](WriterController::is_active)
//!   gate every write checks. It **self-fences** within `safety_margin` of lease
//!   expiry, so it cannot write into a window where another node might be
//!   promoted.
//! - [`Clock`] — injectable time, so expiry / self-fencing is deterministically
//!   testable ([`TestClock`]) without sleeping; [`SystemClock`] in production.
//!
//! ## How it composes (intra-region, RPO 0)
//! K8s elects one writer pod (or the Postgres arbiter grants the lease); that
//! pod `promote`s and serves writes; standby pods stay Passive (read-only,
//! lock-free — readers need no coordination). On writer loss, its lease expires,
//! a standby `promote`s with a higher `epoch`, and SlateDB's CAS fences any
//! stragglers from the old writer. Failover is automatic; no data is lost
//! because both writers share the same object-storage database.
//!
//! ## Multi-region (active-passive, RPO > 0) — deployment layer
//! Cross-region failover (object-store cross-region replication, gated promotion
//! after "waiting out" the old lease to avoid split-brain across buckets, and
//! the orchestration of it) lives in the deployment/ops layer and drives this
//! crate through `promote`/`demote`. It is intentionally *not* code here.

mod clock;
mod controller;
mod lease;
#[cfg(feature = "postgres")]
mod postgres;
mod registry;
#[cfg(feature = "postgres")]
mod registry_postgres;
#[cfg(feature = "kubernetes")]
mod registry_k8s;

pub use clock::{Clock, SystemClock, TestClock};
pub use controller::{HaError, Role, Status, WriterController};
pub use lease::{Lease, LeaseProvider, LocalLeaseProvider};
#[cfg(feature = "postgres")]
pub use postgres::{PostgresLeaseProvider, LEASE_TABLE_DDL};
pub use registry::{InMemoryNodeRegistry, NodeRegistry};
#[cfg(feature = "postgres")]
pub use registry_postgres::{PostgresNodeRegistry, NODES_TABLE_DDL};
#[cfg(feature = "kubernetes")]
pub use registry_k8s::{in_cluster, K8sNodeRegistry};
