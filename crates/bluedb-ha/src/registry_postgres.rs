//! A Postgres-backed [`NodeRegistry`] — the shared node-discovery store.
//!
//! The multi-node backing the in-memory [`InMemoryNodeRegistry`] stands in for,
//! and the discovery counterpart to [`PostgresLeaseProvider`](crate::PostgresLeaseProvider):
//! it reuses the same connect/reconnect plumbing and the same
//! `CREATE TABLE IF NOT EXISTS`-under-advisory-lock idiom.
//!
//! Each node owns one row of a `bluedb_nodes` table, keyed by `node_id`:
//!
//! - heartbeat — an upsert (`INSERT ... ON CONFLICT (node_id) DO UPDATE`) that
//!   stores the node's advertised `url` and stamps `last_heartbeat`.
//! - live_nodes / url_for — read the rows whose `last_heartbeat` is within the
//!   liveness TTL (`last_heartbeat > now - ttl`); stale rows are simply filtered
//!   out (a crashed node ages out of discovery without any explicit delete).
//!
//! Liveness uses the application's injected [`Clock`](crate::Clock) (epoch
//! millis), NOT Postgres `now()`, so it shares the same time source as the lease
//! and is deterministically testable. The TTL is fixed at construction.
//!
//! Enable with the `postgres` crate feature.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::RwLock;
use tokio_postgres::{Client, NoTls, Row};

use crate::registry::NodeRegistry;
use crate::Clock;

/// DDL for the node-registry table. Idempotent.
pub const NODES_TABLE_DDL: &str = "\
CREATE TABLE IF NOT EXISTS bluedb_nodes (\
    node_id              TEXT   PRIMARY KEY,\
    url                  TEXT   NOT NULL,\
    last_heartbeat_millis BIGINT NOT NULL\
)";

/// Fixed advisory-lock key serializing table creation across simultaneously
/// booting nodes — `CREATE TABLE IF NOT EXISTS` is not concurrency-safe across
/// sessions (two can pass the existence check, one then errors). Distinct from
/// the lease table's key so the two creations don't contend. `'nreg'`.
const NODES_TABLE_LOCK_KEY: i64 = 0x6E72_6567; // 'nreg'

/// A [`NodeRegistry`] backed by a Postgres `bluedb_nodes` table.
///
/// The client is held behind an [`RwLock`] and re-established on demand when the
/// connection drops (Postgres restart / network blip) — identical to
/// [`PostgresLeaseProvider`](crate::PostgresLeaseProvider), so a node recovers
/// discovery once Postgres is back rather than going permanently dark.
pub struct PostgresNodeRegistry {
    /// Connection string, kept so the client can be re-established after a drop.
    /// `None` when built from a borrowed pool client ([`Self::with_client`]).
    conn_str: Option<String>,
    client: RwLock<Arc<Client>>,
    ttl: Duration,
    clock: Arc<dyn Clock>,
}

impl PostgresNodeRegistry {
    /// Connect to Postgres at `conn_str`, create the `bluedb_nodes` table if
    /// needed, and treat entries as live for `ttl` after each heartbeat. Uses the
    /// wall clock ([`SystemClock`](crate::SystemClock)). Spawns the connection's
    /// driver task.
    pub async fn connect(conn_str: &str, ttl: Duration) -> Result<Self> {
        Self::connect_with_clock(conn_str, ttl, Arc::new(crate::SystemClock)).await
    }

    /// Like [`Self::connect`] with an injected clock (tests).
    pub async fn connect_with_clock(
        conn_str: &str,
        ttl: Duration,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let client = Self::establish(conn_str).await?;
        let registry = Self {
            conn_str: Some(conn_str.to_string()),
            client: RwLock::new(client),
            ttl,
            clock,
        };
        registry.ensure_table().await?;
        Ok(registry)
    }

    /// Open a fresh connection and spawn its driver task.
    async fn establish(conn_str: &str) -> Result<Arc<Client>> {
        let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .context("connect to postgres node registry")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("bluedb-ha: postgres node-registry connection error: {err}");
            }
        });
        Ok(Arc::new(client))
    }

    /// A live client: returns the current one, or transparently reconnects if it
    /// has closed (mirrors [`PostgresLeaseProvider::live`](crate::PostgresLeaseProvider)).
    async fn live(&self) -> Result<Arc<Client>> {
        {
            let client = self.client.read().await;
            if !client.is_closed() {
                return Ok(client.clone());
            }
        }
        let conn_str = self
            .conn_str
            .as_ref()
            .context("postgres node-registry connection closed and no conn_str to reconnect")?;
        let mut slot = self.client.write().await;
        if slot.is_closed() {
            *slot = Self::establish(conn_str).await?;
        }
        Ok(slot.clone())
    }

    /// Build over an already-connected client (e.g. a shared pool client). The
    /// pool owns reconnection; this registry won't re-establish on its own.
    pub fn with_client(client: Arc<Client>, ttl: Duration, clock: Arc<dyn Clock>) -> Self {
        Self {
            conn_str: None,
            client: RwLock::new(client),
            ttl,
            clock,
        }
    }

    /// Create the registry table if absent, safely under concurrent first-time
    /// connections (transaction-scoped advisory lock; released on COMMIT/rollback).
    pub async fn ensure_table(&self) -> Result<()> {
        self.live()
            .await?
            .batch_execute(&format!(
                "BEGIN; SELECT pg_advisory_xact_lock({NODES_TABLE_LOCK_KEY}); {NODES_TABLE_DDL}; COMMIT;"
            ))
            .await
            .context("create bluedb_nodes table")?;
        Ok(())
    }

    fn ttl_millis(&self) -> i64 {
        self.ttl.as_millis() as i64
    }

    fn row_to_pair(row: &Row) -> (String, String) {
        (row.get::<_, String>(0), row.get::<_, String>(1))
    }
}

#[async_trait]
impl NodeRegistry for PostgresNodeRegistry {
    async fn live_nodes(&self) -> Result<Vec<(String, String)>> {
        let cutoff = self.clock.now_millis() - self.ttl_millis();
        let rows = self
            .live()
            .await?
            .query(
                "SELECT node_id, url FROM bluedb_nodes WHERE last_heartbeat_millis > $1",
                &[&cutoff],
            )
            .await
            .context("registry live_nodes")?;
        Ok(rows.iter().map(Self::row_to_pair).collect())
    }

    async fn url_for(&self, node_id: &str) -> Result<Option<String>> {
        let cutoff = self.clock.now_millis() - self.ttl_millis();
        let row = self
            .live()
            .await?
            .query_opt(
                "SELECT url FROM bluedb_nodes WHERE node_id = $1 AND last_heartbeat_millis > $2",
                &[&node_id, &cutoff],
            )
            .await
            .context("registry url_for")?;
        Ok(row.map(|r| r.get::<_, String>(0)))
    }

    async fn heartbeat(&self, node_id: &str, url: &str) -> Result<()> {
        let now = self.clock.now_millis();
        self.live()
            .await?
            .execute(
                "INSERT INTO bluedb_nodes (node_id, url, last_heartbeat_millis) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (node_id) DO UPDATE SET \
                     url = EXCLUDED.url, \
                     last_heartbeat_millis = EXCLUDED.last_heartbeat_millis",
                &[&node_id, &url, &now],
            )
            .await
            .context("registry heartbeat")?;
        Ok(())
    }
}
