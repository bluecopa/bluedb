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
//! - `BLUEDB_ADVERTISE_ADDR` — this node's externally-reachable base URL (e.g.
//!   `http://bluedb-0.bluedb.default.svc:8080`), published to the node registry so
//!   other nodes can resolve `node_id → URL`. Defaults to `http://<BLUEDB_ADDR>`
//!   when unset (fine for a single host / local dev; set explicitly behind a
//!   Service/LB). Foundation for the later cross-node redirect + affinity routing.
//! - Node registry backend (cross-node discovery). `BLUEDB_NODE_REGISTRY_TTL_SECS`
//!   (default 3× the lease TTL) bounds liveness for the in-memory + Postgres
//!   backends. The backend is selected with this precedence:
//!   1. **Kubernetes** — if running in-cluster (`KUBERNETES_SERVICE_HOST` set) AND
//!      the binary was built `--features kubernetes`. Liveness comes from the K8s
//!      API (Ready endpoints of the coordinator Service), so there is no heartbeat
//!      loop. `BLUEDB_K8S_NAMESPACE` (default `default`), `BLUEDB_K8S_SERVICE`
//!      (default `bluedb`), `BLUEDB_K8S_PORT` (default the `BLUEDB_ADDR` port).
//!   2. **Postgres** — else if `BLUEDB_LEASE_PG_URL` is set, a shared `bluedb_nodes`
//!      table (reuses the lease's Postgres). A heartbeat loop refreshes this node's
//!      row every `BLUEDB_LEASE_TTL_SECS/3`.
//!   3. **In-memory** — else a single-process registry (heartbeat loop runs but
//!      only this node is ever discoverable). Single-node / local default.
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

use bluedb_ha::{
    InMemoryNodeRegistry, LeaseProvider, LocalLeaseProvider, NodeRegistry, PostgresLeaseProvider,
    PostgresNodeRegistry, SystemClock, WriterController,
};
use bluedb_server::{authz::Authz, build_app, objstore, AppState};

/// Default DRAM cache capacity for the analytical (Iceberg/Parquet) read tier.
/// `BLUEDB_ANALYTICAL_CACHE_BYTES=0` disables the cache entirely.
const DEFAULT_ANALYTICAL_CACHE_BYTES: usize = 256 * 1024 * 1024; // 256 MiB

fn parse_analytical_cache_bytes() -> usize {
    std::env::var("BLUEDB_ANALYTICAL_CACHE_BYTES")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_ANALYTICAL_CACHE_BYTES)
}

fn env_secs(key: &str, default: u64) -> Duration {
    Duration::from_secs(std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default))
}

/// This node's externally-reachable base URL, published to the node registry.
///
/// `BLUEDB_ADVERTISE_ADDR` when set; otherwise `http://<BLUEDB_ADDR>` (the bind
/// address). The fallback is correct for a single host / local dev — behind a
/// Service or load balancer set `BLUEDB_ADVERTISE_ADDR` to the routable address.
fn advertise_url(bind_addr: &str) -> String {
    std::env::var("BLUEDB_ADVERTISE_ADDR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| format!("http://{bind_addr}"))
}

/// The port other nodes reach this coordinator on (for the K8s registry's URL
/// derivation): `BLUEDB_K8S_PORT`, else the port parsed from `bind_addr`, else 8080.
#[cfg(feature = "kubernetes")]
fn coordinator_port(bind_addr: &str) -> u16 {
    std::env::var("BLUEDB_K8S_PORT")
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
        .or_else(|| bind_addr.rsplit(':').next().and_then(|p| p.parse::<u16>().ok()))
        .unwrap_or(8080)
}

/// Select + build the node-registry backend with documented precedence:
/// Kubernetes (in-cluster + built with the feature) → Postgres (PG URL set) →
/// in-memory. Returns the registry plus whether the caller must run a heartbeat
/// loop (true for in-memory/Postgres; false for K8s, where readiness is the
/// heartbeat). `pg_url` is the already-read `BLUEDB_LEASE_PG_URL` (reused so the
/// registry shares the lease's Postgres).
async fn build_node_registry(
    pg_url: Option<&str>,
    registry_ttl: Duration,
    _bind_addr: &str,
) -> anyhow::Result<(Arc<dyn NodeRegistry>, bool)> {
    // 1. Kubernetes — only when in-cluster AND compiled with the feature.
    #[cfg(feature = "kubernetes")]
    {
        if bluedb_ha::in_cluster() {
            let namespace =
                std::env::var("BLUEDB_K8S_NAMESPACE").unwrap_or_else(|_| "default".to_string());
            let service = std::env::var("BLUEDB_K8S_SERVICE").unwrap_or_else(|_| "bluedb".to_string());
            let port = coordinator_port(_bind_addr);
            eprintln!(
                "bluedb-server: node registry = kubernetes (service '{service}' in '{namespace}', port {port})"
            );
            let reg = bluedb_ha::K8sNodeRegistry::connect(namespace, service, port).await?;
            // No heartbeat loop: K8s readiness is the liveness signal.
            return Ok((Arc::new(reg), false));
        }
    }

    // 2. Postgres — shared bluedb_nodes table, reusing the lease's PG URL.
    if let Some(url) = pg_url {
        eprintln!("bluedb-server: node registry = postgres (bluedb_nodes)");
        let reg = PostgresNodeRegistry::connect(url, registry_ttl).await?;
        return Ok((Arc::new(reg), true));
    }

    // 3. In-memory — single-process default.
    eprintln!("bluedb-server: node registry = in-memory (single node)");
    Ok((Arc::new(InMemoryNodeRegistry::new(registry_ttl)), true))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = std::env::var("BLUEDB_DB_PATH").unwrap_or_else(|_| "bluedb".to_string());
    let objstore_cfg = objstore::parse_object_store_config(|k| std::env::var(k).ok());
    // Fully-qualified base for the lakehouse mirror's Iceberg locations (so a
    // warehouse can resolve them); the FileIO strips it back to object-store keys.
    let lakehouse_base = objstore_cfg.base_uri();
    let object_store = objstore::build_object_store(&objstore_cfg)?;

    // Analytical read cache: wrap a clone of the raw store with CachingObjectStore
    // so DataFusion/Iceberg Parquet reads are served from DRAM on repeated queries.
    // SlateDB keeps the raw `object_store` (it manages its own foyer block cache
    // internally; double-caching it would waste DRAM without benefit).
    let analytical_cache_bytes = parse_analytical_cache_bytes();
    let lakehouse_store = if analytical_cache_bytes > 0 {
        eprintln!(
            "bluedb-server: analytical read cache = {} MiB DRAM",
            analytical_cache_bytes / (1024 * 1024)
        );
        let cached = bluedb_cache::CachingObjectStoreBuilder::new(object_store.clone())
            .dram_bytes(analytical_cache_bytes)
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("build analytical cache: {e}"))?;
        Arc::new(cached) as Arc<dyn slatedb::object_store::ObjectStore>
    } else {
        eprintln!("bluedb-server: analytical read cache = disabled");
        object_store.clone()
    };

    // Read the Postgres URL once — shared by the lease arbiter AND the node
    // registry (the registry's bluedb_nodes table lives in the same Postgres).
    let pg_url = std::env::var("BLUEDB_LEASE_PG_URL").ok().filter(|s| !s.trim().is_empty());

    // Lease arbiter: shared Postgres for real multi-node HA, else in-process.
    let lease: Arc<dyn LeaseProvider> = match &pg_url {
        Some(url) => {
            eprintln!("bluedb-server: lease arbiter = postgres");
            Arc::new(PostgresLeaseProvider::connect(url, db_path.clone()).await?)
        }
        None => {
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

    // This node's externally-reachable URL + the node-registry backend. The URL
    // is published to the registry so other nodes can later resolve `node_id → URL`
    // (cross-node redirect / affinity routing — neither built yet). The registry
    // ttl defaults to 3× the lease ttl (≈ a few missed heartbeats before a node is
    // dropped). Built before the state's `Arc` is shared so `with_node_registry`'s
    // `Arc::get_mut` succeeds.
    let bind_addr = std::env::var("BLUEDB_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let advertise = advertise_url(&bind_addr);
    let registry_ttl = env_secs("BLUEDB_NODE_REGISTRY_TTL_SECS", ttl.as_secs().saturating_mul(3).max(1));
    let (node_registry, needs_heartbeat) =
        build_node_registry(pg_url.as_deref(), registry_ttl, &bind_addr).await?;

    let mut state = AppState::new(object_store, db_path, writer)
        .with_analytical_cache(lakehouse_store)
        .with_lakehouse_base(lakehouse_base)
        .with_node_registry(node_registry.clone())
        .with_admin_sql_enabled(admin_sql_enabled);
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

    // Node-registry heartbeat loop: publish `(node_id, advertise_url)` on a timer
    // well under the registry TTL so this node stays in `live_nodes()`. Skipped
    // for the K8s backend (`needs_heartbeat == false`), where readiness is the
    // heartbeat. The interval matches the lease-renew cadence (ttl/3).
    if needs_heartbeat {
        let hb_registry = node_registry.clone();
        let hb_node_id = node_id.clone();
        let hb_url = advertise.clone();
        eprintln!("bluedb-server: registering as '{hb_node_id}' at {hb_url}");
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tick);
            loop {
                ticker.tick().await; // fires immediately on the first tick → register at once
                if let Err(err) = hb_registry.heartbeat(&hb_node_id, &hb_url).await {
                    eprintln!("bluedb-server: node-registry heartbeat failed: {err}");
                }
            }
        });
    }

    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    eprintln!("bluedb-server: listening on http://{bind_addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
