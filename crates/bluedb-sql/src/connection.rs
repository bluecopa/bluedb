//! [`Database`] — a multi-connection handle over one SlateDB [`Db`].
//!
//! `bluedb-sql` gives each `Glue` its own [`SlateDbStorage`] (a "connection").
//! Several connections can share one physical `Db` — but to be isolated from one
//! another they must share one **write lease** (the async mutex that serializes
//! write transactions; see [`crate::storage`]). `Database` is the supported way
//! to get such coordinated connections: it holds the `Arc<Db>` and the shared
//! lease and vends connections that all serialize their `BEGIN..COMMIT` blocks
//! through it.
//!
//! ## Isolation model
//!
//! - **Reads** are lock-free and **snapshot-isolated**: each transaction reads
//!   a point-in-time [`DbSnapshot`](slatedb::DbSnapshot) captured at `BEGIN`, so
//!   it never observes another connection's writes committed after it began.
//! - **Explicit write transactions are serializable**: a connection's explicit
//!   transaction takes the exclusive write lease at `BEGIN` (and snapshots
//!   *under* it), so a second connection's `BEGIN` blocks until the first
//!   commits — then snapshots the first's result. Concurrent read-modify-write
//!   transactions therefore cannot lose an update.
//! - **Autocommit statements** (no explicit `BEGIN`) do NOT take the lease, so
//!   they run concurrently and their durable commits batch at SlateDB's WAL
//!   (group commit — the throughput path). Auto-increment `INSERT`s are still
//!   collision-free: row keys come from a shared atomic counter, not a racy
//!   max-scan. An autocommit read-modify-write across *existing* keys (e.g.
//!   `UPDATE x = x + 1`) is not serialized against a concurrent writer, though —
//!   wrap those in a `BEGIN ... COMMIT` block.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use slatedb::Db;
//! # use slatedb::object_store::memory::InMemory;
//! use gluesql_core::prelude::Glue;
//! use bluedb_sql::Database;
//!
//! # async fn run() -> anyhow::Result<()> {
//! let db = Arc::new(Db::open("app", Arc::new(InMemory::new())).await?);
//! let database = Database::new(db);
//!
//! // Two isolated connections over the same Db, serialized on writes.
//! let mut conn_a = Glue::new(database.connection());
//! let mut conn_b = Glue::new(database.connection_for_tenant("tenant-b"));
//! # let _ = (&mut conn_a, &mut conn_b);
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use bluedb_storage::Substrate;
use slatedb::{Db, DbReader};
use tokio::sync::Mutex;

use crate::keyspace::DEFAULT_TENANT;
use crate::storage::{SeqAllocator, SlateDbStorage, WriteLease};

/// A handle to one SlateDB database that vends isolated [`SlateDbStorage`]
/// connections — either a **writer** handle (connections can read + write,
/// write transactions serialized by a shared lease) or a **read-replica**
/// handle (connections are read-only; writes/`BEGIN` error).
#[derive(Clone)]
pub struct Database {
    substrate: Substrate,
    write_lease: WriteLease,
    seq: SeqAllocator,
}

impl Database {
    /// Wrap an already-open writer `Db`. All connections vended from this handle
    /// share one write lease, so their write transactions serialize against each
    /// other.
    pub fn new(db: Arc<Db>) -> Self {
        Self::over(Substrate::writer(db))
    }

    /// Wrap a read-only [`DbReader`] replica. Connections vended from this handle
    /// serve reads (following the writer's manifest) and refuse writes — the
    /// standby role in the HA model.
    pub fn reader(reader: Arc<DbReader>) -> Self {
        Self::over(Substrate::reader(reader))
    }

    fn over(substrate: Substrate) -> Self {
        Self {
            substrate,
            write_lease: Arc::new(Mutex::new(())),
            seq: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Is this a writer handle (vs. a read replica)?
    pub fn is_writer(&self) -> bool {
        self.substrate.is_writer()
    }

    /// Flush outstanding writes to object storage (writer only; a no-op on a
    /// read replica). Use before a graceful step-down so a successor that opens
    /// the database observes every acked write.
    pub async fn flush(&self) -> anyhow::Result<()> {
        if let Ok(db) = self.substrate.require_writer() {
            db.flush()
                .await
                .map_err(|err| anyhow::anyhow!("flush: {err}"))?;
        }
        Ok(())
    }

    /// A new connection under the default tenant.
    pub fn connection(&self) -> SlateDbStorage {
        self.connection_for_tenant(DEFAULT_TENANT)
    }

    /// A new connection scoped to `tenant` (its keyspace is namespaced; see
    /// [`SlateDbStorage::new_for_tenant`]). It still shares this `Database`'s
    /// write lease, so write transactions across tenants serialize on the one
    /// underlying single-writer database.
    pub fn connection_for_tenant(&self, tenant: &str) -> SlateDbStorage {
        SlateDbStorage::with_substrate(
            self.substrate.clone(),
            tenant,
            self.write_lease.clone(),
            self.seq.clone(),
        )
    }
}
