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
//! - Object store (first match wins) — the same image runs on any cloud; only
//!   these env vars differ (see [`bluedb_server::objstore`]):
//!   - `BLUEDB_S3_BUCKET` (+ `BLUEDB_S3_ENDPOINT` for MinIO/R2, `BLUEDB_S3_REGION`,
//!     `BLUEDB_S3_ACCESS_KEY_ID`, `BLUEDB_S3_SECRET_ACCESS_KEY`) — S3/MinIO.
//!   - `BLUEDB_AZURE_CONTAINER` (+ `BLUEDB_AZURE_ACCOUNT`, `BLUEDB_AZURE_ACCESS_KEY`) — Azure Blob.
//!   - `BLUEDB_GCS_BUCKET` (+ `BLUEDB_GCS_SERVICE_ACCOUNT` path) — Google Cloud Storage.
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
//! - `BLUEDB_FTS_SEAL_INTERVAL_MS` — interval in ms for the background FTS
//!   seal/compaction scheduler (default 30000). On promote the node binds a durable
//!   FTS engine on the active writer and runs this scheduler; demote stops it.
//! - `BLUEDB_ENABLE_ADMIN_SQL` — set to `1` or `true` to enable `POST /admin/sql`
//!   (arbitrary SQL including DDL, audited). Off by default. `/sql` is always
//!   available but restricted to a single parameterized SELECT/INSERT/UPDATE/DELETE.
//! - `BLUEDB_AUTHZ_TOKENS` — bearer-token → scope map. Format:
//!   `tok1=scope,scope;tok2=scope`. Recognized scopes: `data:read`, `data:write`,
//!   `data:query`, `schema:admin`, `superuser`. When unset the server runs in
//!   **open mode** (all requests allowed without a token); **production deployments
//!   should always set this**. `Superuser` satisfies any required scope.
//!
//!   Per-route scope table:
//!   - `GET /tables/{table}` → `data:read`
//!   - `POST/PATCH/DELETE /tables/{table}` → `data:write`
//!   - `POST /sql` → `data:query`
//!   - `POST /admin/sql` → `superuser`
//!   - `POST|DELETE /schema/*` → `schema:admin`
//!   - `POST /admin/promote`, `POST /admin/demote` → `superuser`
//!   - `GET /health`, `GET /admin/status` → public (no token required)
//!
//! ## Structured DDL endpoints (writer-gated, validated)
//! - `POST   /schema/tables`                          — create table from typed column spec.
//! - `DELETE /schema/tables/{table}`                  — drop table.
//! - `POST   /schema/tables/{table}/indexes`          — create index on a table.
//! - `DELETE /schema/tables/{table}/indexes/{name}`   — drop index.
//! - `POST   /schema/tables/{table}/fulltext-indexes` — declare a full-text index
//!   on a text column (the table's integer primary key is auto-resolved).
//! - `POST   /schema/tables/{table}/trigram-indexes`  — declare a trigram index on
//!   a text column (accelerates `col LIKE '%lit%'`; primary key auto-resolved).
//!
//! All `/schema/*` endpoints validate every identifier (allow-list `^[A-Za-z_][A-Za-z0-9_]*$`)
//! and every type keyword against an explicit allow-list before building DDL; no
//! raw SQL is ever accepted from the client.
//!
//! ## Full-text search over `/sql`
//! Once a full-text index is declared, `POST /sql` accepts the PostgreSQL FTS
//! surface — `to_tsvector(cfg, col) @@ plainto_tsquery(q)` (and `ts_rank`) — and
//! rewrites it against the live index, returning matching rows. The index is
//! maintained on every committed write, so a `@@` query reads its own writes with
//! no explicit flush. (`/admin/sql` is left as the raw escape hatch and does *not*
//! rewrite `@@`.)
//!
//! The full-text index is **durable on the active writer**: on promote the node
//! reopens a durable FTS engine over the writer's substrate (reconnecting persisted
//! index defs to their sealed splits), and a background scheduler
//! (`BLUEDB_FTS_SEAL_INTERVAL_MS`, default 30000) folds the in-memory live tier into
//! object-storage splits and bounds split growth — so sealed splits survive a
//! restart. FTS reads run only on the active node (`require_active`); a demoted node
//! reverts to an empty in-memory engine and serves no FTS.

use std::sync::Arc;
use std::time::Duration;

use bluedb_ha::{LeaseProvider, LocalLeaseProvider, PostgresLeaseProvider, SystemClock, WriterController};
use bluedb_server::{authz::Authz, build_app, objstore, AppState};

fn env_secs(key: &str, default: u64) -> Duration {
    Duration::from_secs(std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = std::env::var("BLUEDB_DB_PATH").unwrap_or_else(|_| "bluedb".to_string());
    let objstore_cfg = objstore::parse_object_store_config(|k| std::env::var(k).ok());
    // Fully-qualified base for the lakehouse mirror's Iceberg locations (so a
    // warehouse can resolve them); the FileIO strips it back to object-store keys.
    let lakehouse_base = objstore_cfg.base_uri();
    let object_store = objstore::build_object_store(&objstore_cfg)?;

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
    let mut state = AppState::new(object_store, db_path, writer)
        .with_admin_sql_enabled(admin_sql_enabled)
        .with_lakehouse_base(lakehouse_base);
    if let Ok(raw) = std::env::var("BLUEDB_AUTHZ_TOKENS") {
        let authz = Authz::parse_env(&raw).expect("invalid BLUEDB_AUTHZ_TOKENS");
        state = state.with_authz(authz);
    }

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
