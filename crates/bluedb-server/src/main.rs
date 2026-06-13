//! `bluedb-server` binary — opens the database and serves the HTTP API.
//!
//! Config via env vars:
//! - `BLUEDB_ADDR`     — listen address (default `0.0.0.0:8080`).
//! - `BLUEDB_DB_PATH`  — SlateDB path/prefix inside the object store (default `bluedb`).
//! - `BLUEDB_DATA_DIR` — local directory for the object store; if unset, an
//!   **in-memory** (ephemeral) store is used. Set it to persist.
//! - `BLUEDB_NODE_ID`  — this node's id for the writer lease (default `node-0`).
//! - `BLUEDB_LEASE_TTL_SECS` / `BLUEDB_LEASE_MARGIN_SECS` — lease lifetime and
//!   self-fence margin (defaults 15 / 5).
//! - `BLUEDB_START_PASSIVE` — if set, start read-only and wait to be promoted via
//!   `POST /admin/promote` (default: a standalone node auto-promotes).
//!
//! NOTE: the binary uses an in-process [`LocalLeaseProvider`], i.e. it is a
//! single writer on its own. Real multi-node HA supplies a shared `LeaseProvider`
//! (Postgres/K8s/NATS) and starts nodes passive; see `bluedb-ha`.

use std::sync::Arc;
use std::time::Duration;

use bluedb_ha::{LocalLeaseProvider, SystemClock, WriterController};
use bluedb_server::{build_app, AppState};
use bluedb_sql::Database;
use slatedb::object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};
use slatedb::Db;

fn env_secs(key: &str, default: u64) -> Duration {
    let secs = std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default);
    Duration::from_secs(secs)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = std::env::var("BLUEDB_DB_PATH").unwrap_or_else(|_| "bluedb".to_string());
    let object_store: Arc<dyn ObjectStore> = match std::env::var("BLUEDB_DATA_DIR") {
        Ok(dir) => {
            std::fs::create_dir_all(&dir)?;
            Arc::new(LocalFileSystem::new_with_prefix(&dir)?)
        }
        Err(_) => {
            eprintln!(
                "bluedb-server: BLUEDB_DATA_DIR not set — using an in-memory store (data is ephemeral)"
            );
            Arc::new(InMemory::new())
        }
    };

    let db = Arc::new(Db::open(db_path, object_store).await?);

    // Single-writer controller (in-process lease for the standalone binary).
    let node_id = std::env::var("BLUEDB_NODE_ID").unwrap_or_else(|_| "node-0".to_string());
    let writer = Arc::new(WriterController::new(
        node_id.clone(),
        Arc::new(LocalLeaseProvider::new()),
        Arc::new(SystemClock),
        env_secs("BLUEDB_LEASE_TTL_SECS", 15),
        env_secs("BLUEDB_LEASE_MARGIN_SECS", 5),
    ));
    if std::env::var("BLUEDB_START_PASSIVE").is_err() {
        writer.promote().await?;
        eprintln!("bluedb-server: node '{node_id}' promoted to active writer");
    } else {
        eprintln!("bluedb-server: node '{node_id}' started passive (POST /admin/promote to activate)");
    }
    writer.clone().spawn_renewal();

    let state = AppState::new(Database::new(db), writer);
    let app = build_app(state);

    let addr = std::env::var("BLUEDB_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    eprintln!("bluedb-server: listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
