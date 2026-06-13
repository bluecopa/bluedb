//! `bluedb-server` binary — opens the database and serves the HTTP API.
//!
//! Config via env vars:
//! - `BLUEDB_ADDR`     — listen address (default `0.0.0.0:8080`).
//! - `BLUEDB_DB_PATH`  — SlateDB path/prefix inside the object store (default `bluedb`).
//! - `BLUEDB_DATA_DIR` — local directory for the object store; if unset, an
//!   **in-memory** (ephemeral) store is used. Set it to persist.

use std::sync::Arc;

use bluedb_server::{build_app, AppState};
use bluedb_sql::Database;
use slatedb::object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};
use slatedb::Db;

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
    let state = AppState::new(Database::new(db));
    let app = build_app(state);

    let addr = std::env::var("BLUEDB_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    eprintln!("bluedb-server: listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
