//! `bluedb-server` binary — builds the object store + lease controller and
//! serves the HTTP API, running a background HA loop for bootstrap/failover.
//!
//! The server speaks HTTP/1.1 and HTTP/2 (h2c, prior-knowledge) on the plaintext
//! listener — an h2 client can multiplex many concurrent in-flight writes over one
//! connection. HTTP/2 over TLS (ALPN) is a deployment-layer concern.
//!
//! Config via env vars:
//! - `BLUEDB_ADDR`     — listen address (default `0.0.0.0:8080`).
//! - `BLUEDB_DB_PATH`  — SlateDB path/prefix inside the object store (default `bluedb`).
//!   Also used as the lease `resource` key.
//! - Object store (first match wins):
//!   - `BLUEDB_S3_BUCKET` (+ `BLUEDB_S3_ENDPOINT` for MinIO, `BLUEDB_S3_REGION`,
//!     `BLUEDB_S3_ACCESS_KEY_ID`, `BLUEDB_S3_SECRET_ACCESS_KEY`) — S3/MinIO. The
//!     shared store for a real multi-node cluster.
//!   - `BLUEDB_DATA_DIR` — local filesystem (single-node persistence).
//!   - else — in-memory (ephemeral; single node only).
//! - `BLUEDB_LEASE_PG_URL` — Postgres lease arbiter for multi-node election; if
//!   unset, an in-process lease (single writer) is used.
//! - `BLUEDB_NODE_ID` (default `node-0`), `BLUEDB_LEASE_TTL_SECS` (15),
//!   `BLUEDB_LEASE_MARGIN_SECS` (5).
//! - `BLUEDB_START_PASSIVE` — start as a read replica and wait for the HA loop
//!   (or `POST /admin/promote`) to take the lease; default bootstraps to writer.
//! - `BLUEDB_FLUSH_INTERVAL_MS` — WAL flush interval in ms (default 25). Set at
//!   writer open; lower = lower write latency + more object-store PUTs under load.
//! - `BLUEDB_ENABLE_ADMIN_SQL` — set to `1` or `true` to enable `POST /admin/sql`
//!   (arbitrary SQL including DDL, audited). Off by default. `/sql` is always
//!   available but restricted to a single parameterized SELECT/INSERT/UPDATE/DELETE.

use std::sync::Arc;
use std::time::Duration;

use bluedb_ha::{LeaseProvider, LocalLeaseProvider, PostgresLeaseProvider, SystemClock, WriterController};
use bluedb_server::{build_app, AppState};
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

fn env_secs(key: &str, default: u64) -> Duration {
    Duration::from_secs(std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default))
}

/// Build the object store from env (S3/MinIO, local FS, or in-memory).
fn build_object_store() -> anyhow::Result<Arc<dyn ObjectStore>> {
    if let Ok(bucket) = std::env::var("BLUEDB_S3_BUCKET") {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(std::env::var("BLUEDB_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()));
        if let Ok(endpoint) = std::env::var("BLUEDB_S3_ENDPOINT") {
            // MinIO / non-AWS: custom endpoint, allow plain HTTP.
            builder = builder.with_endpoint(endpoint).with_allow_http(true);
        }
        if let Ok(key) = std::env::var("BLUEDB_S3_ACCESS_KEY_ID") {
            builder = builder.with_access_key_id(key);
        }
        if let Ok(secret) = std::env::var("BLUEDB_S3_SECRET_ACCESS_KEY") {
            builder = builder.with_secret_access_key(secret);
        }
        eprintln!("bluedb-server: object store = S3/MinIO");
        Ok(Arc::new(builder.build()?))
    } else if let Ok(dir) = std::env::var("BLUEDB_DATA_DIR") {
        std::fs::create_dir_all(&dir)?;
        eprintln!("bluedb-server: object store = local fs at {dir}");
        Ok(Arc::new(LocalFileSystem::new_with_prefix(&dir)?))
    } else {
        eprintln!("bluedb-server: object store = in-memory (ephemeral, single-node)");
        Ok(Arc::new(InMemory::new()))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = std::env::var("BLUEDB_DB_PATH").unwrap_or_else(|_| "bluedb".to_string());
    let object_store = build_object_store()?;

    // Lease arbiter: shared Postgres for real multi-node HA, else in-process.
    let lease: Arc<dyn LeaseProvider> = match std::env::var("BLUEDB_LEASE_PG_URL") {
        Ok(url) => {
            eprintln!("bluedb-server: lease arbiter = postgres");
            Arc::new(PostgresLeaseProvider::connect(&url, db_path.clone()).await?)
        }
        Err(_) => {
            eprintln!("bluedb-server: lease arbiter = in-process (single writer)");
            Arc::new(LocalLeaseProvider::new())
        }
    };

    let node_id = std::env::var("BLUEDB_NODE_ID").unwrap_or_else(|_| "node-0".to_string());
    let ttl = env_secs("BLUEDB_LEASE_TTL_SECS", 15);
    let writer = Arc::new(WriterController::new(
        node_id.clone(),
        lease,
        Arc::new(SystemClock),
        ttl,
        env_secs("BLUEDB_LEASE_MARGIN_SECS", 5),
    ));
    let admin_sql_enabled = std::env::var("BLUEDB_ENABLE_ADMIN_SQL")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let state = AppState::new(object_store, db_path, writer)
        .with_admin_sql_enabled(admin_sql_enabled);

    // Bootstrap: become writer unless asked to start as a replica.
    if std::env::var("BLUEDB_START_PASSIVE").is_err() {
        match state.promote().await {
            Ok(()) => eprintln!("bluedb-server: node '{node_id}' promoted to writer"),
            Err(_) => {
                eprintln!("bluedb-server: node '{node_id}' could not promote (another writer holds the lease) — starting as replica");
                state.attach_reader().await;
            }
        }
    } else {
        eprintln!("bluedb-server: node '{node_id}' starting passive");
        state.attach_reader().await;
    }

    // Background HA loop: renew while writer, take over on failover, self-fence.
    let ha_state = state.clone();
    let tick = std::cmp::max(ttl / 3, Duration::from_secs(1));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tick);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            ha_state.ha_tick().await;
        }
    });

    let app = build_app(state);
    let addr = std::env::var("BLUEDB_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    eprintln!("bluedb-server: listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
