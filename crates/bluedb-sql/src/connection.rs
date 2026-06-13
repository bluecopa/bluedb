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
//! - **Write transactions are serializable**: a connection's explicit
//!   transaction takes the exclusive write lease at `BEGIN` (and snapshots
//!   *under* it), so a second connection's `BEGIN` blocks until the first
//!   commits — then snapshots the first's result. Concurrent read-modify-write
//!   transactions therefore cannot lose an update. (SlateDB is single-writer;
//!   this lease is exactly that single writer, surfaced as a queue.)
//! - **Autocommit statements** (no explicit `BEGIN`) are individually atomic and
//!   physically safe on the shared `Db`, but are NOT serialized against other
//!   statements. For an atomic read-modify-write under concurrency, wrap it in a
//!   `BEGIN ... COMMIT` block.
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

use std::sync::Arc;

use slatedb::Db;
use tokio::sync::Mutex;

use crate::keyspace::DEFAULT_TENANT;
use crate::storage::{SlateDbStorage, WriteLease};

/// A handle to one SlateDB [`Db`] that vends isolated, write-serialized
/// [`SlateDbStorage`] connections.
#[derive(Clone)]
pub struct Database {
    db: Arc<Db>,
    write_lease: WriteLease,
}

impl Database {
    /// Wrap an already-open `Db`. All connections vended from this handle share
    /// one write lease, so their write transactions serialize against each
    /// other.
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            db,
            write_lease: Arc::new(Mutex::new(())),
        }
    }

    /// A new connection under the default tenant.
    pub fn connection(&self) -> SlateDbStorage {
        self.connection_for_tenant(DEFAULT_TENANT)
    }

    /// A new connection scoped to `tenant` (its keyspace is namespaced; see
    /// [`SlateDbStorage::new_for_tenant`]). It still shares this `Database`'s
    /// write lease, so write transactions across tenants serialize on the one
    /// underlying single-writer `Db`.
    pub fn connection_for_tenant(&self, tenant: &str) -> SlateDbStorage {
        SlateDbStorage::with_lease(self.db.clone(), tenant, self.write_lease.clone())
    }

    /// Borrow the underlying `Db`.
    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }
}
