//! A Postgres-backed [`LeaseProvider`] — the HA shared lease arbiter.
//!
//! This is the concrete, multi-node backing the in-memory [`LocalLeaseProvider`]
//! stands in for. The lease lives in one row of a `bluedb_lease` table, keyed by
//! `resource` (the database/index id being leased). All three operations are a
//! **single atomic SQL statement**, so Postgres's row-level locking gives the
//! single-holder invariant across processes/regions:
//!
//! - acquire — an upsert whose `ON CONFLICT ... DO UPDATE ... WHERE` only writes
//!   when the row is free, expired, or already ours; the `epoch` (fencing token)
//!   advances on a genuine change of holder and is preserved on self-renew.
//! - renew — a conditional `UPDATE` that succeeds only while the row is still
//!   ours at `epoch` and unexpired (else the caller has lost it → self-fence).
//! - release — a conditional `DELETE` of our own row.
//!
//! Run a highly-available Postgres (multi-AZ, synchronous standby) and this is
//! the cross-region arbiter from the HA design. Enable with the `postgres`
//! crate feature.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::RwLock;
use tokio_postgres::{Client, NoTls, Row};

use crate::lease::{Lease, LeaseProvider};

/// DDL for the lease table. Idempotent.
pub const LEASE_TABLE_DDL: &str = "\
CREATE TABLE IF NOT EXISTS bluedb_lease (\
    resource          TEXT   PRIMARY KEY,\
    holder            TEXT   NOT NULL,\
    epoch             BIGINT NOT NULL,\
    expires_at_millis BIGINT NOT NULL\
)";

/// Fixed advisory-lock key (shared by all nodes) that serializes table
/// creation. `CREATE TABLE IF NOT EXISTS` is NOT concurrency-safe across
/// sessions — two can both pass the existence check and one then fails with a
/// duplicate `pg_type` error — so creation is wrapped in a transaction-scoped
/// advisory lock. Matters when many nodes boot simultaneously.
const LEASE_TABLE_LOCK_KEY: i64 = 0x626C_7565; // 'blue'

/// A [`LeaseProvider`] backed by a Postgres `bluedb_lease` row.
///
/// The client is held behind an [`RwLock`] and **re-established on demand** when
/// the connection has dropped (e.g. Postgres restarted) — a `tokio_postgres`
/// client does not reconnect itself, and without this a Postgres bounce would
/// leave every node permanently unable to renew/acquire (writer-less). See
/// [`PostgresLeaseProvider::live`].
pub struct PostgresLeaseProvider {
    /// Connection string, kept so the client can be re-established after a drop.
    /// `None` when built from a borrowed pool client ([`Self::with_client`]),
    /// where reconnection is the pool's responsibility.
    conn_str: Option<String>,
    client: RwLock<Arc<Client>>,
    resource: String,
}

impl PostgresLeaseProvider {
    /// Connect to Postgres at `conn_str` and lease `resource`, creating the
    /// `bluedb_lease` table if needed. Spawns the connection's driver task.
    pub async fn connect(conn_str: &str, resource: impl Into<String>) -> Result<Self> {
        let client = Self::establish(conn_str).await?;
        let provider = Self {
            conn_str: Some(conn_str.to_string()),
            client: RwLock::new(client),
            resource: resource.into(),
        };
        provider.ensure_table().await?;
        Ok(provider)
    }

    /// Open a fresh connection and spawn its driver task.
    async fn establish(conn_str: &str) -> Result<Arc<Client>> {
        let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .context("connect to postgres lease store")?;
        // The connection must be driven on its own task for the client to work.
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("bluedb-ha: postgres lease connection error: {err}");
            }
        });
        Ok(Arc::new(client))
    }

    /// A live client: returns the current one, or transparently reconnects if it
    /// has closed (Postgres restart / network drop). The next lease op then
    /// succeeds, so a writer recovers once Postgres is back.
    async fn live(&self) -> Result<Arc<Client>> {
        {
            let client = self.client.read().await;
            if !client.is_closed() {
                return Ok(client.clone());
            }
        }
        // Closed — reconnect (double-checked under the write lock so concurrent
        // callers reconnect once). Needs a conn_str; a borrowed pool client can't.
        let conn_str = self
            .conn_str
            .as_ref()
            .context("postgres lease connection closed and no conn_str to reconnect")?;
        let mut slot = self.client.write().await;
        if slot.is_closed() {
            *slot = Self::establish(conn_str).await?;
        }
        Ok(slot.clone())
    }

    /// Build over an already-connected client (e.g. a shared pool client). The
    /// pool owns reconnection; this provider won't re-establish on its own.
    pub fn with_client(client: Arc<Client>, resource: impl Into<String>) -> Self {
        Self {
            conn_str: None,
            client: RwLock::new(client),
            resource: resource.into(),
        }
    }

    /// Create the lease table if it doesn't exist, safely under concurrent
    /// first-time connections (see [`LEASE_TABLE_LOCK_KEY`]). The advisory lock
    /// is transaction-scoped, so it is released on COMMIT (or on rollback if the
    /// DDL errors) — no risk of a stuck lock.
    pub async fn ensure_table(&self) -> Result<()> {
        self.live()
            .await?
            .batch_execute(&format!(
                "BEGIN; SELECT pg_advisory_xact_lock({LEASE_TABLE_LOCK_KEY}); {LEASE_TABLE_DDL}; COMMIT;"
            ))
            .await
            .context("create bluedb_lease table")?;
        Ok(())
    }

    fn row_to_lease(row: &Row) -> Lease {
        Lease {
            holder: row.get::<_, String>(0),
            epoch: row.get::<_, i64>(1) as u64,
            expires_at_millis: row.get::<_, i64>(2),
        }
    }
}

fn ttl_millis(ttl: Duration) -> i64 {
    ttl.as_millis() as i64
}

#[async_trait]
impl LeaseProvider for PostgresLeaseProvider {
    async fn try_acquire(&self, holder: &str, ttl: Duration, now_millis: i64) -> Result<Option<Lease>> {
        let expires = now_millis + ttl_millis(ttl);
        // Atomic upsert. The DO UPDATE only fires when the row is ours, free, or
        // expired (the WHERE); otherwise nothing is written and no row is
        // RETURNED → denied. Epoch advances except on a self-renew of a live lease.
        let sql = "\
            INSERT INTO bluedb_lease (resource, holder, epoch, expires_at_millis) \
            VALUES ($1, $2, 1, $3) \
            ON CONFLICT (resource) DO UPDATE SET \
                holder = EXCLUDED.holder, \
                epoch = CASE \
                    WHEN bluedb_lease.holder = EXCLUDED.holder AND bluedb_lease.expires_at_millis > $4 \
                    THEN bluedb_lease.epoch ELSE bluedb_lease.epoch + 1 END, \
                expires_at_millis = EXCLUDED.expires_at_millis \
            WHERE bluedb_lease.holder = EXCLUDED.holder OR bluedb_lease.expires_at_millis <= $4 \
            RETURNING holder, epoch, expires_at_millis";
        let row = self
            .live()
            .await?
            .query_opt(sql, &[&self.resource, &holder, &expires, &now_millis])
            .await
            .context("lease try_acquire")?;
        Ok(row.as_ref().map(Self::row_to_lease))
    }

    async fn renew(&self, holder: &str, epoch: u64, ttl: Duration, now_millis: i64) -> Result<Option<Lease>> {
        let expires = now_millis + ttl_millis(ttl);
        let sql = "\
            UPDATE bluedb_lease SET expires_at_millis = $1 \
            WHERE resource = $2 AND holder = $3 AND epoch = $4 AND expires_at_millis > $5 \
            RETURNING holder, epoch, expires_at_millis";
        let row = self
            .live()
            .await?
            .query_opt(sql, &[&expires, &self.resource, &holder, &(epoch as i64), &now_millis])
            .await
            .context("lease renew")?;
        Ok(row.as_ref().map(Self::row_to_lease))
    }

    async fn release(&self, holder: &str, epoch: u64) -> Result<()> {
        // Expire the lease in place rather than DELETE-ing the row: the row
        // carries the monotonic `epoch`, and a fencing token must NEVER go
        // backwards (a reset-to-1 epoch could be fenced by SlateDB, which tracks
        // the highest epoch it has seen). Setting `expires_at_millis = 0` frees
        // the lease for the next acquirer while preserving the epoch, so the
        // next acquisition advances it. (One row per resource — it does not grow.)
        self.live()
            .await?
            .execute(
                "UPDATE bluedb_lease SET expires_at_millis = 0 \
                 WHERE resource = $1 AND holder = $2 AND epoch = $3",
                &[&self.resource, &holder, &(epoch as i64)],
            )
            .await
            .context("lease release")?;
        Ok(())
    }
}
