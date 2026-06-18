//! `bluedb-server` — an HTTP/REST data service over [`bluedb_engine`].
//!
//! A PostgREST-style API: tables are addressed at `/tables/{table}` with
//! `GET`/`POST`/`PATCH`/`DELETE`, filters/order/limit live in the query string
//! (parsed by [`bluedb_rest`]), and bodies are JSON. `/sql` runs a single
//! parameterized non-DDL statement; `/admin/sql` runs arbitrary SQL (off by
//! default, audited); `/health` is a liveness probe; and
//! `/admin/{status,promote,demote}` drive the single-writer role.
//!
//! ## Role-aware storage
//!
//! A node binds to the SlateDB database dynamically by **role** (see
//! [`bluedb_ha`]):
//! - **promote** → acquire the writer lease, then open a writer `Db` (which
//!   bumps SlateDB's `writer_epoch`, fencing any dead writer); reads + writes.
//! - **demote** → release the lease, then open a read-only `DbReader` that
//!   follows the (new) writer's manifest; reads only.
//!
//! So the live [`Database`] handle is swapped under an `RwLock` as the role
//! changes. Writes require the active writer (`503` otherwise); reads are served
//! by whatever handle is bound (writer or replica), `503` only if the node has
//! no database yet (fresh cluster, not promoted).
//!
//! The router is built by [`build_app`] from an [`AppState`]; `main` builds the
//! object store + lease controller and serves. Tests drive [`build_app`] via
//! `tower::ServiceExt::oneshot` — no socket required.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde_json::{json, Map, Value};
use tokio::sync::RwLock;

pub mod authz;
mod catalog;
pub mod objstore;
mod schema;

use bluedb_engine::{rest_sql, EngineError, FtsEngine};
use bluedb_ha::{HaError, NodeRegistry, Status, WriterController};
use bluedb_ledger::Ledger;
use arrow_array::RecordBatch;

mod ledger_api;
mod evidence_api;
mod graph_api;
mod signer;

/// ES256-DER verification helper, re-exported for Rust consumers (and the e2e
/// test) to verify Signed Tree Heads against a published SPKI-PEM public key.
pub use signer::verify_es256_der;

use bluedb_lakehouse::{object_store_file_io, LakehouseConfig, LakehouseManager};
use bluedb_rest::{parse_filters, DeleteRequest, InsertRequest, UpdateRequest};
use bluedb_sql::{parse_lakehouse_pragma, CdcConfig, Database, LhPragma, SlateDbStorage, DEFAULT_TENANT};
use gluesql_core::prelude::{Glue, Payload, Value as SqlValue};
use slatedb::object_store::ObjectStore;
use slatedb::{Db, DbReader, Settings};

/// bluedb's default WAL flush interval (overrides SlateDB's 100 ms) — chosen for
/// the latency-sensitive HTTP profile. Override with `BLUEDB_FLUSH_INTERVAL_MS`.
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 25;

/// The node-level default FTS seal/compaction interval. Deliberately long (30 s):
/// the background seal's drain→durable-write window is non-atomic, so a sub-second
/// interval could race a test mid-seal — at 30 s no sub-second test ever triggers
/// a seal. Override with `BLUEDB_FTS_SEAL_INTERVAL_MS`. (Per-DB PRAGMA tuning is
/// deferred; this is the node-level knob.)
const DEFAULT_FTS_SEAL_INTERVAL_MS: u64 = 30_000;

/// Parse a `BLUEDB_FLUSH_INTERVAL_MS` value into a `Duration`. `None`, empty, or
/// unparseable → the 25 ms default (total + non-panicking).
fn parse_flush_interval_ms(raw: Option<&str>) -> Duration {
    let ms = raw
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_FLUSH_INTERVAL_MS);
    Duration::from_millis(ms)
}

/// Parse a `BLUEDB_FTS_SEAL_INTERVAL_MS` value into a `Duration`. `None`, empty, or
/// unparseable → the 30 s default (total + non-panicking). Mirrors
/// [`parse_flush_interval_ms`].
fn parse_fts_seal_interval_ms(raw: Option<&str>) -> Duration {
    let ms = raw
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_FTS_SEAL_INTERVAL_MS);
    Duration::from_millis(ms)
}

/// The FTS background seal interval for this node, from `BLUEDB_FTS_SEAL_INTERVAL_MS`
/// (default 30 s).
fn fts_seal_interval() -> Duration {
    parse_fts_seal_interval_ms(std::env::var("BLUEDB_FTS_SEAL_INTERVAL_MS").ok().as_deref())
}

/// Object-storage key prefix under which the Iceberg mirror lives (alongside the
/// SlateDB data in the same bucket). `BLUEDB_LAKEHOUSE_ROOT`, default `lakehouse`.
fn lakehouse_root() -> String {
    std::env::var("BLUEDB_LAKEHOUSE_ROOT")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "lakehouse".to_string())
}

/// Parse a millisecond env value into a `Duration`, falling back to `default_ms`.
fn parse_ms(raw: Option<&str>, default_ms: u64) -> Duration {
    Duration::from_millis(
        raw.and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(default_ms),
    )
}

/// Lakehouse seal debounce: coalesce a burst of commits this long before sealing.
/// `BLUEDB_LAKEHOUSE_SEAL_DEBOUNCE_MS`, default 2 s (seconds-fresh mirror).
fn lakehouse_seal_debounce() -> Duration {
    parse_ms(
        std::env::var("BLUEDB_LAKEHOUSE_SEAL_DEBOUNCE_MS").ok().as_deref(),
        2_000,
    )
}

/// Cap on how long a steady write stream delays a seal.
/// `BLUEDB_LAKEHOUSE_SEAL_MAX_INTERVAL_MS`, default 10 s.
fn lakehouse_seal_max_interval() -> Duration {
    parse_ms(
        std::env::var("BLUEDB_LAKEHOUSE_SEAL_MAX_INTERVAL_MS").ok().as_deref(),
        10_000,
    )
}

/// How often the compaction worker runs.
/// `BLUEDB_LAKEHOUSE_COMPACTION_INTERVAL_MS`, default 60 s.
fn lakehouse_compaction_interval() -> Duration {
    parse_ms(
        std::env::var("BLUEDB_LAKEHOUSE_COMPACTION_INTERVAL_MS").ok().as_deref(),
        60_000,
    )
}

/// Minor-compact (bin-pack) a table once its live data-file count exceeds this.
/// `BLUEDB_LAKEHOUSE_MAX_DATA_FILES`, default 8.
fn lakehouse_max_data_files() -> usize {
    std::env::var("BLUEDB_LAKEHOUSE_MAX_DATA_FILES")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .unwrap_or(8)
}

/// Major-compact (whole-table rewrite, reclaiming delete files) a table once its
/// live delete-file count exceeds this. `BLUEDB_LAKEHOUSE_MAX_DELETE_FILES`,
/// default 16.
fn lakehouse_max_delete_files() -> usize {
    std::env::var("BLUEDB_LAKEHOUSE_MAX_DELETE_FILES")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .unwrap_or(16)
}

/// The lakehouse seal + compaction tunables for this node, from the
/// `BLUEDB_LAKEHOUSE_*` env vars (see the individual helpers for defaults).
fn lakehouse_config() -> LakehouseConfig {
    LakehouseConfig {
        seal_debounce: lakehouse_seal_debounce(),
        seal_max_interval: lakehouse_seal_max_interval(),
        compaction_interval: lakehouse_compaction_interval(),
        max_data_files: lakehouse_max_data_files(),
        max_delete_files: lakehouse_max_delete_files(),
    }
}

/// SlateDB `Settings` for the writer `Db`: bluedb's `flush_interval` default,
/// env-overridable, everything else at SlateDB defaults.
fn writer_settings() -> Settings {
    let mut settings = Settings::default();
    settings.flush_interval = Some(parse_flush_interval_ms(
        std::env::var("BLUEDB_FLUSH_INTERVAL_MS").ok().as_deref(),
    ));
    settings
}

/// Required-env helper for `BLUEDB_EVIDENCE_SIGNING=vault`: a missing var is a
/// clear misconfiguration error rather than a silent fallback.
fn req_env(var: &str) -> Result<String, AppError> {
    std::env::var(var)
        .map_err(|_| AppError::internal(format!("BLUEDB_EVIDENCE_SIGNING=vault requires {var}")))
}

/// Shared service state. Cheap to clone (an `Arc` to the inner state).
#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    /// Object store + SlateDB path the writer/reader open against.
    object_store: Arc<dyn ObjectStore>,
    /// Object store handed to the lakehouse/Iceberg mirror.  Normally the same
    /// `Arc` as `object_store`, but on nodes whose `BLUEDB_ANALYTICAL_CACHE_BYTES`
    /// is non-zero it is a `CachingObjectStore` wrapping a clone of the raw store
    /// (so SlateDB — which has its own internal foyer block cache — is never
    /// double-cached, while Iceberg files benefit from the DRAM read cache).
    lakehouse_object_store: Arc<dyn ObjectStore>,
    db_path: String,
    /// Single-writer lease controller (HA election + self-fencing).
    writer: Arc<WriterController>,
    /// The live SQL handle, swapped by role: a writer `Database` when active, a
    /// read-replica `Database` when passive, `None` before the cluster's first
    /// writer has created the database.
    db: RwLock<Option<Database>>,
    /// Whether `POST /admin/sql` is enabled. Off by default; set via
    /// [`AppState::with_admin_sql_enabled`] or `BLUEDB_ENABLE_ADMIN_SQL=1`.
    admin_sql_enabled: AtomicBool,
    /// Whether [`Inner::db`] currently holds the **writer** `Db` (vs a replica
    /// reader or nothing). Set `true` only after [`AppState::promote`] installs the
    /// writer `Db`, and back to `false` whenever a reader is bound
    /// ([`AppState::attach_reader`]). The lease can be `Active` (in-memory) a beat
    /// *before* the writer `Db` is opened and swapped in — during that window the
    /// node still holds the pre-failover reader, so "active" must mean *lease held
    /// AND writer bound* or a routing client would read stale replica data from a
    /// node advertising itself as the writer. Gates [`AppState::require_active`] and
    /// the role reported by `GET /admin/status`.
    writer_bound: AtomicBool,
    /// Bearer-token → scopes map. Unset = open mode (all requests allowed).
    /// Set once at startup via [`AppState::with_authz`].
    authz: OnceLock<Arc<authz::Authz>>,
    /// The active FTS engine, swapped by role. Before the first promote it is a
    /// pure in-memory [`FtsEngine::new`]; on promote it is replaced by a **durable**
    /// engine reopened over the writer's substrate ([`FtsEngine::reopen`]) — which
    /// reconnects persisted index defs to their existing splits — and on demote it
    /// reverts to an empty in-memory engine (a passive node serves no FTS reads).
    /// Installed as a commit observer on every write connection (maintains the live
    /// index) and consulted by `/sql` to rewrite `@@`/`ts_rank` (read-your-writes).
    fts: RwLock<Arc<FtsEngine>>,
    /// The background seal/compaction scheduler for the durable FTS engine, spawned
    /// on promote and aborted on demote / re-promote. `None` until the first promote.
    seal_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Shared CDC control for the lakehouse mirror — installed on every write
    /// connection so mutations on mirror-enabled tables land in the CDC log, and
    /// mutated by `PRAGMA lakehouse_mirror`. Lives for the node's lifetime so the
    /// opt-out state is stable across role flips (the registry is the source of
    /// truth, re-applied on each promote).
    cdc: CdcConfig,
    /// Fully-qualified storage base URI for the lakehouse (e.g. `s3://bucket`,
    /// `file:///abs`). Empty = bare keys (in-memory). Makes the Iceberg
    /// `metadata.json` a warehouse loads contain resolvable locations. Set at
    /// startup via [`AppState::with_lakehouse_base`].
    lakehouse_base: String,
    /// The active lakehouse mirror manager (one engine per tenant): `Some` only
    /// while this node is the active writer (opened on promote over the writer's
    /// `Database` + the object store, shut down on demote). A passive node
    /// mirrors nothing. The manager owns the per-tenant engines and the shared
    /// seal/compaction loops.
    lakehouse: RwLock<Option<Arc<LakehouseManager>>>,
    /// Evidence digest signer (Signed Tree Heads). `None` ⇒ signing is off (the
    /// default); the signed-digest / signing-key endpoints return `501`. Built
    /// once at startup from the environment via [`AppState::with_evidence_signing`]
    /// (or injected directly in tests via [`AppState::with_signer`]). The private
    /// key never lives here in production — the `Vault` backend holds only a token.
    signer: Option<Arc<signer::EvidenceSigner>>,
    /// Node registry: cross-node discovery of live coordinators and their
    /// externally-reachable URLs, keyed by `node_id`. `None` until set at startup
    /// via [`AppState::with_node_registry`] (`main` picks the backend). Foundation
    /// for the later cross-node redirect (resolving the writer's URL via
    /// `url_for(lease.holder)`) and affinity routing — neither built yet. Kept as
    /// the deployment-selected backend (in-memory / Postgres / Kubernetes).
    node_registry: Option<Arc<dyn NodeRegistry>>,
}

impl AppState {
    /// Build state over an object store + SlateDB path + lease controller. The
    /// node starts with **no** bound database; call [`AppState::promote`] (to
    /// open a writer) or [`AppState::attach_reader`] (to follow an existing
    /// writer) — `main` does this at startup.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        db_path: impl Into<String>,
        writer: Arc<WriterController>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                // lakehouse_object_store defaults to the raw store; callers that
                // want the analytical read cache call `with_analytical_cache`.
                lakehouse_object_store: object_store.clone(),
                object_store,
                db_path: db_path.into(),
                writer,
                db: RwLock::new(None),
                admin_sql_enabled: AtomicBool::new(false),
                writer_bound: AtomicBool::new(false),
                authz: OnceLock::new(),
                fts: RwLock::new(FtsEngine::new()),
                seal_handle: Mutex::new(None),
                cdc: CdcConfig::default(),
                lakehouse_base: String::new(),
                lakehouse: RwLock::new(None),
                signer: None,
                node_registry: None,
            }),
        }
    }

    /// Install the node registry (cross-node discovery). Must be called at
    /// startup, before the `Arc<Inner>` is shared; `main` picks the backend
    /// (in-memory / Postgres / Kubernetes). No-op once the state is shared. When
    /// unset, discovery is unavailable (single-node / tests).
    pub fn with_node_registry(mut self, registry: Arc<dyn NodeRegistry>) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.node_registry = Some(registry);
        }
        self
    }

    /// The node registry, if one was installed. Discovery of live coordinators
    /// and their URLs (`live_nodes` / `url_for`). The later cross-node redirect
    /// will resolve the writer's URL via `url_for(self.writer().node_id())` once
    /// the lease holder is known; that consumer is not built yet.
    pub fn node_registry(&self) -> Option<&Arc<dyn NodeRegistry>> {
        self.inner.node_registry.as_ref()
    }

    /// Override the object store handed to the lakehouse/Iceberg mirror with a
    /// pre-built (e.g. `CachingObjectStore`-wrapped) store.  Must be called at
    /// startup, before `Arc<Inner>` is shared (`main` does this).  Silently
    /// ignored after the state is shared.
    pub fn with_analytical_cache(mut self, store: Arc<dyn ObjectStore>) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.lakehouse_object_store = store;
        }
        self
    }

    /// Set the fully-qualified storage base URI the lakehouse mirror publishes
    /// under (e.g. `s3://bucket`, `file:///abs/dir`), so the Iceberg metadata a
    /// warehouse loads has resolvable locations. Must be called at startup,
    /// before the `Arc<Inner>` is shared; `main` derives it from the object-store
    /// config. No-op once the state is shared.
    pub fn with_lakehouse_base(mut self, base: impl Into<String>) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.lakehouse_base = base.into();
        }
        self
    }

    /// Install an evidence digest signer. Must be called at startup, before the
    /// `Arc<Inner>` is shared. `None` ⇒ signing stays off (the default). Used by
    /// tests to inject a `LocalSigner`; production uses
    /// [`AppState::with_evidence_signing`] to build it from the environment.
    pub(crate) fn with_signer(mut self, signer: Option<Arc<signer::EvidenceSigner>>) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.signer = signer;
        }
        self
    }

    /// Build the evidence digest signer from the environment. Production never
    /// holds key material: signing is `off` (default) or `vault` (HashiCorp
    /// Vault Transit — the private key stays in the KMS). The in-process
    /// `LocalSigner` is a TEST fixture only ([`AppState::with_local_signer_for_tests`]),
    /// never selectable from config.
    ///
    /// `BLUEDB_EVIDENCE_SIGNING` = `off` (default) | `vault`. For `vault`:
    /// `VAULT_ADDR`, `VAULT_TOKEN`, `BLUEDB_EVIDENCE_VAULT_MOUNT` (default
    /// `transit`), `BLUEDB_EVIDENCE_VAULT_KEY` (transit `ecdsa-p256` key).
    pub fn with_evidence_signing(self) -> Result<Self, AppError> {
        use signer::EvidenceSigner;
        let mode = std::env::var("BLUEDB_EVIDENCE_SIGNING").unwrap_or_else(|_| "off".to_string());
        let built: Option<EvidenceSigner> = match mode.as_str() {
            "off" => None,
            "vault" => {
                let addr = req_env("VAULT_ADDR")?;
                let token = req_env("VAULT_TOKEN")?;
                let mount = std::env::var("BLUEDB_EVIDENCE_VAULT_MOUNT")
                    .unwrap_or_else(|_| "transit".to_string());
                let key = req_env("BLUEDB_EVIDENCE_VAULT_KEY")?;
                Some(EvidenceSigner::Vault(signer::vault::VaultTransitSigner::new(
                    addr, token, mount, key,
                )?))
            }
            other => {
                return Err(AppError::internal(format!(
                    "BLUEDB_EVIDENCE_SIGNING: unknown mode '{other}' (want off|vault)"
                )));
            }
        };
        Ok(self.with_signer(built.map(Arc::new)))
    }

    /// TEST ONLY: attach an in-process `LocalSigner` (ephemeral key). Not a
    /// production path — production signing is configured via
    /// [`AppState::with_evidence_signing`] (`off`/`vault`). Hidden from docs.
    #[doc(hidden)]
    pub fn with_local_signer_for_tests(self) -> Self {
        let s = signer::EvidenceSigner::Local(signer::LocalSigner::ephemeral());
        self.with_signer(Some(Arc::new(s)))
    }

    /// The evidence digest signer, or `None` if signing is off on this node.
    pub(crate) fn signer(&self) -> Option<Arc<signer::EvidenceSigner>> {
        self.inner.signer.clone()
    }

    /// Enable (or disable) `POST /admin/sql` (arbitrary SQL, audited). Returns
    /// `self` for chaining; the existing `AppState::new` signature is unchanged.
    /// Safe to call before or after [`AppState::promote`].
    pub fn with_admin_sql_enabled(self, enabled: bool) -> Self {
        self.inner.admin_sql_enabled.store(enabled, Ordering::Relaxed);
        self
    }

    /// Whether `POST /admin/sql` is enabled on this node.
    pub fn admin_sql_enabled(&self) -> bool {
        self.inner.admin_sql_enabled.load(Ordering::Relaxed)
    }

    /// Configure bearer-token authorization. Must be called at startup (before
    /// the `Arc<Inner>` is shared across tasks). Returns `self` for chaining.
    /// No-op if called after the first `with_authz` (OnceLock semantics).
    pub fn with_authz(self, authz: authz::Authz) -> Self {
        let _ = self.inner.authz.set(Arc::new(authz));
        self
    }

    /// Authorize `required` scope from the request's `Authorization: Bearer` token.
    /// Open mode (no authz configured) always allows. Composes with `require_active`.
    pub(crate) fn authorize(&self, headers: &axum::http::HeaderMap, required: authz::Scope) -> Result<(), AppError> {
        let Some(authz) = self.inner.authz.get() else { return Ok(()); };
        let token = bearer_token(headers);
        if authz.allows(token, required) {
            Ok(())
        } else if token.is_none() {
            Err(AppError::plain(StatusCode::UNAUTHORIZED, "missing or malformed bearer token"))
        } else {
            Err(AppError::plain(StatusCode::FORBIDDEN, "insufficient scope"))
        }
    }

    /// Authorize the request's token to act on `tenant`. Open mode allows; with
    /// authz on, the token must be bound to the tenant (or be `superuser`). See
    /// [`authz::Authz::allows_tenant`].
    pub(crate) fn authorize_tenant(&self, headers: &axum::http::HeaderMap, tenant: &str) -> Result<(), AppError> {
        let Some(authz) = self.inner.authz.get() else { return Ok(()); };
        if authz.allows_tenant(bearer_token(headers), tenant) {
            Ok(())
        } else {
            Err(AppError::plain(
                StatusCode::FORBIDDEN,
                format!("token not authorized for tenant '{tenant}'"),
            ))
        }
    }

    /// Resolve and authorize the request's tenant from `X-Bluedb-Tenant`
    /// (default `"_"` when absent). Rejects names outside `[A-Za-z0-9_-]` (they
    /// flow into object-store paths) and tenants the token may not access.
    pub(crate) fn tenant(&self, headers: &axum::http::HeaderMap) -> Result<String, AppError> {
        let tenant = headers
            .get("x-bluedb-tenant")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_TENANT)
            .to_string();
        let valid = tenant == DEFAULT_TENANT
            || tenant
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
        if !valid {
            return Err(AppError::bad_request(format!(
                "invalid tenant '{tenant}' (allowed: letters, digits, '_', '-')"
            )));
        }
        self.authorize_tenant(headers, &tenant)?;
        Ok(tenant)
    }

    /// The lease controller (for the HA background loop in `main`).
    pub fn writer(&self) -> &Arc<WriterController> {
        &self.inner.writer
    }

    /// Acquire the writer lease and open a writer `Db` (creating it if absent).
    /// Opening the writer bumps SlateDB's `writer_epoch`, fencing a dead writer.
    pub async fn promote(&self) -> Result<(), AppError> {
        self.inner.writer.promote().await.map_err(AppError::from_ha)?;
        let db = Db::builder(self.inner.db_path.clone(), self.inner.object_store.clone())
            .with_settings(writer_settings())
            .build()
            .await
            .map_err(|err| AppError::internal(format!("open writer db: {err}")))?;
        // One `Database` handle backs BOTH the SQL slot AND the durable FTS engine
        // (it's `Clone` + carries the shared substrate), so FTS splits live in the
        // same object store as the SQL data.
        let database = Database::new(Arc::new(db));
        *self.inner.db.write().await = Some(database.clone());
        // The writer `Db` is now the bound handle — only here does this node
        // become a *truthful* active writer. Releasing the flag with `Release`
        // pairs with the `Acquire` loads in `require_active`/`admin_status`, so a
        // node never advertises "active" (and never serves writer-gated traffic)
        // while still bound to the pre-failover reader. Set before the FTS /
        // lakehouse reopen below: those are auxiliary to SQL read/write
        // correctness, so a failure there must not strand a usable writer as
        // passive.
        self.inner.writer_bound.store(true, Ordering::Release);

        // Best-effort: ensure the ledger's SQL projection tables exist so
        // `/ledger/*` reads (and `SELECT ... FROM ledger_accounts`) work. A
        // failure here doesn't block promotion — the native ledger API still
        // works (lookups read canonical records, not the projection).
        if let Err(err) = bluedb_ledger::ensure_schema(&database).await {
            eprintln!("bluedb-server: ensure ledger schema: {err}");
        }

        // Reopen a durable FTS engine over the writer's substrate: this reconnects
        // any persisted index defs to their existing splits (restart durability).
        let fts = FtsEngine::reopen(database.substrate())
            .await
            .map_err(|e| AppError::internal(format!("reopen fts: {e}")))?;
        // Stop a prior scheduler (re-promote) before starting a new one (no leak).
        if let Some(h) = self.inner.seal_handle.lock().unwrap().take() {
            h.abort();
        }
        let handle = fts.clone().spawn_seal_scheduler(fts_seal_interval());
        *self.inner.seal_handle.lock().unwrap() = Some(handle);
        *self.inner.fts.write().await = fts;

        // Reopen the lakehouse mirror over the SAME object store + writer database
        // (the Iceberg tables live alongside the SQL data, in the same bucket the
        // warehouse reads). Restores the durable opt-out registry, then spawns the
        // event-driven seal loop + compaction worker.
        // Publish under a fully-qualified base URI so the Iceberg metadata a
        // warehouse loads has resolvable locations; the FileIO strips the base to
        // recover object-store keys. Empty base (in-memory) → bare-key root.
        let base = self.inner.lakehouse_base.clone();
        let root = if base.is_empty() {
            lakehouse_root()
        } else {
            format!("{base}/{}", lakehouse_root())
        };
        // Use the lakehouse-specific store (may be a CachingObjectStore wrapper
        // when BLUEDB_ANALYTICAL_CACHE_BYTES is set); SlateDB keeps the raw store.
        let file_io = object_store_file_io(self.inner.lakehouse_object_store.clone(), base);
        // Stop a prior manager (re-promote) before opening a fresh one.
        if let Some(old) = self.inner.lakehouse.write().await.take() {
            old.shutdown();
        }
        let lakehouse = LakehouseManager::open(
            file_io,
            root,
            database.clone(),
            self.inner.cdc.clone(),
            lakehouse_config(),
        )
        .await
        .map_err(|e| AppError::internal(format!("open lakehouse: {e}")))?;
        *self.inner.lakehouse.write().await = Some(lakehouse);
        Ok(())
    }

    /// Release the writer lease and rebind as a read replica (or `None` if the
    /// database doesn't exist yet). Flushes the writer first so a successor sees
    /// every acked write (graceful step-down).
    pub async fn demote(&self) -> Result<(), AppError> {
        if let Some(db) = self.inner.db.read().await.as_ref() {
            let _ = db.flush().await; // best-effort durability before handoff
        }
        self.inner
            .writer
            .demote()
            .await
            .map_err(|err| AppError::internal(format!("demote: {err}")))?;
        // Stop the background seal scheduler and revert to an empty in-memory FTS
        // engine — a demoted (passive) node serves no FTS reads (they 503 on
        // `require_active`), so an empty in-memory engine is safe and drops the
        // durable handles.
        if let Some(h) = self.inner.seal_handle.lock().unwrap().take() {
            h.abort();
        }
        *self.inner.fts.write().await = FtsEngine::new();
        // Stop the lakehouse seal/compaction loops and drop the manager — a
        // passive node mirrors nothing (the next promote reopens from the tenant
        // index + registries).
        if let Some(m) = self.inner.lakehouse.write().await.take() {
            m.shutdown();
        }
        self.attach_reader().await;
        Ok(())
    }

    /// Bind as a read replica only if currently unbound (avoids reopening a
    /// reader every HA tick).
    async fn attach_reader_if_unbound(&self) {
        if self.inner.db.read().await.is_none() {
            self.attach_reader().await;
        }
    }

    /// One iteration of the background HA control loop, driving automatic
    /// bootstrap, failover, and self-fencing:
    /// - **active** → renew the lease; if it was lost, flip to a read replica;
    /// - **passive** → try to take the lease (first-node bootstrap, or failover
    ///   after the previous writer's lease expired); if denied, ensure we are at
    ///   least serving reads as a replica.
    pub async fn ha_tick(&self) {
        if self.inner.writer.is_active() {
            match self.inner.writer.renew_once().await {
                Ok(true) => {}
                _ => self.attach_reader().await, // lost the lease → become a replica
            }
        } else {
            match self.promote().await {
                Ok(()) => {} // took over (bootstrap or failover)
                Err(_) => self.attach_reader_if_unbound().await,
            }
        }
    }

    /// Bind (or rebind) this node as a read replica following the writer's
    /// manifest. If the database doesn't exist yet, leaves the node unbound.
    pub async fn attach_reader(&self) {
        // Binding a reader (or nothing) means this node is no longer the writer:
        // clear the flag FIRST so no concurrent request observes "active + bound"
        // against a handle that is about to become a replica reader.
        self.inner.writer_bound.store(false, Ordering::Release);
        let bound = DbReader::builder(self.inner.db_path.clone(), self.inner.object_store.clone())
            .build()
            .await
            .ok()
            .map(|reader| Database::reader(Arc::new(reader)));
        *self.inner.db.write().await = bound;
    }

    /// The currently-bound FTS engine (for the `/schema/.../fulltext-indexes` and
    /// `/sql` handlers). Clones the `Arc` out of the role-swap `RwLock`, so callers
    /// hold a snapshot of the engine that was active when they asked.
    pub(crate) async fn fts(&self) -> Arc<FtsEngine> {
        self.inner.fts.read().await.clone()
    }

    /// A connection to the currently-bound database, or `503` if unbound. Carries
    /// the FTS commit observer so the live index is maintained on every commit.
    ///
    /// **Guarded:** every external route runs on a strict connection (the
    /// scan/sort guardrail + the no-schemaless/PK-required schema regime). There
    /// is no client bypass — a bare `SELECT` is auto-bounded, a non-indexed
    /// filter/sort is rejected. Engine-internal work (the ledger projection, FTS
    /// maintenance) uses the `Database` directly and is unaffected.
    async fn connection(&self, tenant: &str) -> Result<SlateDbStorage, AppError> {
        let fts = self.inner.fts.read().await.clone();
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(db
                .connection_for_tenant(tenant)
                .strict()
                .with_cdc(self.inner.cdc.clone())
                .with_commit_observer(fts)),
            None => Err(AppError::service_unavailable(
                "node has no database yet (no writer has been promoted)",
            )),
        }
    }

    /// The active lakehouse manager, or `None` if this node isn't the writer.
    pub(crate) async fn lakehouse(&self) -> Option<Arc<LakehouseManager>> {
        self.inner.lakehouse.read().await.clone()
    }

    /// Force the lakehouse mirror to seal every tenant's pending changes into
    /// Iceberg now (a no-op on a passive node). Useful before a graceful
    /// step-down and for deterministic tests; the background loop seals on its
    /// own otherwise.
    pub async fn seal_now(&self) -> Result<(), AppError> {
        if let Some(manager) = self.inner.lakehouse.read().await.clone() {
            manager
                .seal_all()
                .await
                .map_err(|e| AppError::internal(format!("seal: {e}")))?;
        }
        Ok(())
    }

    /// Return the last CDC sequence assigned to `tenant` on this writer (the
    /// in-memory counter after the most recent CDC commit). Returns 0 when no
    /// CDC write has yet been stamped for this tenant. Used to populate
    /// `X-Bluedb-Watermark` on write responses.
    pub(crate) async fn write_watermark(&self, tenant: &str) -> i64 {
        match self.inner.db.read().await.as_ref() {
            Some(db) => db.last_cdc_seq(tenant).await,
            None => 0,
        }
    }

    /// The sealed Iceberg watermark for `tenant`: the max CDC sequence durably
    /// committed to Iceberg for this tenant. Returns 0 when nothing has been
    /// sealed yet. Used to populate `X-Bluedb-Watermark` on read responses and
    /// to check `X-Bluedb-Min-Watermark` freshness constraints.
    pub(crate) async fn sealed_watermark(&self, tenant: &str) -> i64 {
        match self.inner.lakehouse.read().await.as_ref() {
            Some(manager) => manager.sealed_watermark(tenant).await,
            None => 0,
        }
    }

    /// Like [`Self::connection`] but the connection also serializes autocommit
    /// writes (see [`bluedb_sql::SlateDbStorage::serialize_writes`]). Used by the
    /// routes that can run a single-statement read-modify-write (`/sql`,
    /// `/admin/sql`, `PATCH`, `DELETE`) so concurrent RMWs can't lose an update.
    /// Guarded for the same reason as [`Self::connection`].
    pub(crate) async fn connection_serialized(&self, tenant: &str) -> Result<SlateDbStorage, AppError> {
        let fts = self.inner.fts.read().await.clone();
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(db
                .connection_for_tenant(tenant)
                .serialize_writes()
                .strict()
                .with_cdc(self.inner.cdc.clone())
                .with_commit_observer(fts)),
            None => Err(AppError::service_unavailable(
                "node has no database yet (no writer has been promoted)",
            )),
        }
    }

    /// An **unguarded** connection for internal read-backs — specifically the
    /// `Prefer: return=representation` path, which re-reads the exact rows a
    /// mutation just affected. The scan/sort guardrail deliberately does not apply:
    /// the read is bounded by the mutation's own filter (or the inserted keys), not
    /// a client-supplied scan, and reads the fresh SlateDB state (not the Iceberg
    /// mirror). Reads committed state on the active node's database.
    async fn connection_unguarded(&self, tenant: &str) -> Result<SlateDbStorage, AppError> {
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(db.connection_for_tenant(tenant)),
            None => Err(AppError::service_unavailable(
                "node has no database yet (no writer has been promoted)",
            )),
        }
    }

    /// Build a [`Ledger`] over the currently-bound database, or `503` if the
    /// node has no database yet. The `Database` is cheap to clone (`Arc`-based)
    /// and [`Ledger::new`] captures owned handles, so the returned ledger
    /// outlives the role lock — like the SQL connection path.
    async fn ledger(&self) -> Result<Ledger, AppError> {
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(Ledger::new(db)),
            None => Err(AppError::service_unavailable(
                "node has no database yet (no writer has been promoted)",
            )),
        }
    }

    /// Build an [`Evidence`] handle over the currently-bound database for `tenant`,
    /// or `503` if the node has no database yet. Mirrors [`Self::ledger`].
    pub(crate) async fn evidence(&self, tenant: &str) -> Result<bluedb_evidence::Evidence, AppError> {
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(bluedb_evidence::Evidence::new(db, tenant)),
            None => Err(AppError::service_unavailable(
                "node has no database yet (no writer has been promoted)",
            )),
        }
    }

    /// Build a [`Graph`] handle over the currently-bound database for `tenant`.
    pub(crate) async fn graph(&self, tenant: &str) -> Result<bluedb_evidence::Graph, AppError> {
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(bluedb_evidence::Graph::new(db, tenant)),
            None => Err(AppError::service_unavailable(
                "node has no database yet (no writer has been promoted)",
            )),
        }
    }

    /// Reject a mutating request unless this node is the active writer **and** has
    /// its writer `Db` bound (not still the pre-failover reader — see
    /// [`Inner::writer_bound`]). The `Acquire` load pairs with the `Release` store
    /// in [`AppState::promote`]/[`AppState::attach_reader`].
    pub(crate) fn require_active(&self) -> Result<(), AppError> {
        if self.is_writer() {
            Ok(())
        } else {
            Err(AppError::plain(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("node '{}' is passive (not the active writer)", self.inner.writer.node_id()),
            ))
        }
    }

    /// Whether this node is the active writer (lease held **and** writer `Db`
    /// bound — same condition as [`Self::require_active`], as a bool). Used by the
    /// HTAP analytical freshness gate to decide writer-local-serve vs. redirect.
    pub(crate) fn is_writer(&self) -> bool {
        self.inner.writer.is_active() && self.inner.writer_bound.load(Ordering::Acquire)
    }
}

/// Build the HTTP router over `state`.
pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/sql", post(exec_sql))
        .route(
            "/tables/{table}",
            get(select).post(insert).patch(update).delete(delete_rows),
        )
        .route("/admin/status", get(admin_status))
        .route("/admin/promote", post(admin_promote))
        .route("/admin/demote", post(admin_demote))
        .route("/admin/sql", post(admin_sql))
        .route("/schema/tables", post(schema::create_table))
        .route("/schema/tables/{table}", delete(schema::drop_table))
        .route("/schema/tables/{table}/indexes", post(schema::create_index))
        .route("/schema/tables/{table}/indexes/{name}", delete(schema::drop_index))
        .route(
            "/schema/tables/{table}/fulltext-indexes",
            post(schema::create_fulltext_index),
        )
        .route(
            "/schema/tables/{table}/trigram-indexes",
            post(schema::create_trigram_index),
        )
        .route("/ledger/accounts", post(ledger_api::create_accounts))
        .route("/ledger/transfers", post(ledger_api::create_transfers))
        .route("/ledger/accounts/{id}", get(ledger_api::get_account))
        .route("/ledger/transfers/{id}", get(ledger_api::get_transfer))
        // Evidence substrate.
        .route("/evidence/{chain}", put(evidence_api::create_chain))
        .route(
            "/evidence/{chain}/entries",
            post(evidence_api::append).get(evidence_api::read_entries),
        )
        .route("/evidence/{chain}/head", get(evidence_api::head))
        .route("/evidence/{chain}/entries/{seq}/redact", post(evidence_api::redact))
        .route("/evidence/{chain}/entries/{seq}", delete(evidence_api::hard_delete))
        .route("/evidence/{chain}/digest", get(evidence_api::digest))
        .route("/evidence/{chain}/digest/signed", get(evidence_api::digest_signed))
        .route("/evidence/signing-key", get(evidence_api::signing_key))
        .route("/evidence/{chain}/proof", get(evidence_api::inclusion))
        .route("/evidence/{chain}/consistency", get(evidence_api::consistency))
        // Native graph store (edge maintenance + read-only traversal).
        .route(
            "/graph/{graph}/edges",
            put(graph_api::upsert_edges).delete(graph_api::delete_edges),
        )
        .route("/graph/{graph}/mutate", post(graph_api::mutate))
        .route("/graph/{graph}", delete(graph_api::drop_graph))
        .route("/graph/{graph}/reachable", post(graph_api::reachable))
        .route("/graph/{graph}/widest-path", post(graph_api::widest_path))
        // Read-only Iceberg REST Catalog for warehouse discovery (Phase 5).
        .route("/catalog/v1/config", get(catalog::config))
        .route("/catalog/v1/namespaces", get(catalog::list_namespaces))
        .route("/catalog/v1/namespaces/{ns}", get(catalog::get_namespace))
        .route("/catalog/v1/namespaces/{ns}/tables", get(catalog::list_tables))
        .route(
            "/catalog/v1/namespaces/{ns}/tables/{table}",
            get(catalog::load_table),
        )
        .with_state(state)
}

// --- SQL request type -------------------------------------------------------

/// JSON body for `/sql` and `/admin/sql`.
#[derive(serde::Deserialize)]
struct SqlRequest {
    sql: String,
    #[serde(default)]
    params: Vec<Value>,
}

/// Map a JSON scalar to a [`bluedb_rest::Param`]. Arrays/objects are rejected.
fn json_to_param(v: &Value) -> Result<bluedb_rest::Param, AppError> {
    use bluedb_rest::Param;
    Ok(match v {
        Value::Null => Param::Null,
        Value::Bool(b) => Param::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Param::Int(i)
            } else if let Some(f) = n.as_f64() {
                Param::Float(f)
            } else {
                return Err(AppError::bad_request("unrepresentable number param"));
            }
        }
        Value::String(s) => Param::Str(s.clone()),
        // Object/array params bind as canonical JSON text — for a JSON column or a
        // JSON function argument on the query path.
        Value::Object(_) | Value::Array(_) => Param::Str(v.to_string()),
    })
}

/// Inverse of [`json_to_param`]: a bound REST [`bluedb_rest::Param`] back to a JSON
/// value, so a `/tables` query's params can be handed to the analytical front door
/// ([`bluedb_query::query_via_catalog`], which binds JSON values positionally).
fn param_to_json(p: &bluedb_rest::Param) -> Value {
    use bluedb_rest::Param;
    match p {
        Param::Null => Value::Null,
        Param::Bool(b) => Value::Bool(*b),
        Param::Int(i) => json!(*i),
        Param::Float(f) => json!(*f),
        Param::Str(s) => Value::String(s.clone()),
    }
}

/// True if `err` is the plan-time scan/sort guardrail rejection (a `/tables` read
/// whose filter / ORDER BY touches a non-indexed column on the GlueSQL fast path).
/// Such a read is re-routed to the analytical engine, which can scan/sort the
/// Iceberg mirror. Detected by the stable sentinel prefix the guardrail emits.
fn is_guardrail_reject(err: &EngineError) -> bool {
    matches!(
        err,
        EngineError::Sql(gluesql_core::error::Error::StorageMsg(msg))
            if msg.starts_with(bluedb_sql::GUARDRAIL_REJECT_PREFIX)
    )
}

// --- handlers ---------------------------------------------------------------

/// Extract the `Authorization: Bearer <token>` value, if present and well-formed.
fn bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

/// Build an `X-Bluedb-Watermark: <tenant>:<seq>` header map.
///
/// Skips the header when `seq == 0` on normal (success) responses — a zero
/// watermark means no CDC writes happened yet (CDC is off, or this was a
/// DDL-only request) and omitting it avoids a misleading `<tenant>:0`.
///
/// For error responses (e.g. the freshness 503) use [`watermark_headers_always`]
/// which emits the header even for 0 so the client learns the current sealed seq.
fn watermark_headers(tenant: &str, seq: i64) -> axum::http::HeaderMap {
    if seq > 0 {
        watermark_headers_always(tenant, seq)
    } else {
        axum::http::HeaderMap::new()
    }
}

/// Like [`watermark_headers`] but always emits the header, including `seq == 0`.
/// Used on error responses where 0 is a meaningful "nothing sealed yet" signal.
fn watermark_headers_always(tenant: &str, seq: i64) -> axum::http::HeaderMap {
    let mut map = axum::http::HeaderMap::new();
    let value = format!("{tenant}:{seq}");
    if let Ok(v) = axum::http::HeaderValue::from_str(&value) {
        map.insert("x-bluedb-watermark", v);
    }
    map
}

/// Parse the `X-Bluedb-Min-Watermark` request header.
///
/// Accepts both `<seq>` (bare integer, applies to the request's tenant) and
/// `<tenant>:<seq>` (ignores the tenant portion — the server always uses the
/// request's `X-Bluedb-Tenant` for scope). Returns `None` when the header is
/// absent or unparseable (no freshness gate applied).
fn parse_min_watermark(headers: &axum::http::HeaderMap) -> Option<i64> {
    let raw = headers.get("x-bluedb-min-watermark")?.to_str().ok()?;
    // Accept "<seq>" or "<tenant>:<seq>".
    let seq_str = raw.find(':').map_or(raw, |i| &raw[i + 1..]);
    seq_str.trim().parse::<i64>().ok()
}

// --- analytical read path helpers ------------------------------------------

/// Convert a slice of Arrow [`RecordBatch`]es into a JSON array of row-objects,
/// matching the shape of the existing read endpoints (`[{"col": val, ...}, ...]`).
///
/// Canonical JSON rendering per logical type (must match `sql_value_to_json`):
/// - Null → `null`
/// - Boolean → JSON bool
/// - Integer (i8/i16/i32/i64/u8/u16/u32/u64) → JSON number
/// - Float (f32/f64) → JSON number (NaN/Inf → `null`)
/// - Utf8 / LargeUtf8 → JSON string
/// - Decimal128 → JSON string preserving scale (e.g. `"10.50"`)
/// - Date32 → ISO-8601 date string `"YYYY-MM-DD"`
/// - Timestamp(Microsecond, _) → `"YYYY-MM-DDTHH:MM:SS[.ffffff]"`
/// - Time64(Microsecond) → `"HH:MM:SS[.ffffff]"`
/// - Everything else → JSON string via `format!("{:?}", ...)`
fn record_batches_to_json(batches: &[RecordBatch]) -> Value {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{
        Int8Type, Int16Type, Int32Type, Int64Type,
        UInt8Type, UInt16Type, UInt32Type, UInt64Type,
        Float32Type, Float64Type, Date32Type, Decimal128Type,
        TimestampMicrosecondType, Time64MicrosecondType,
    };
    use arrow_schema::{DataType, TimeUnit};

    let mut rows: Vec<Value> = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let n = batch.num_rows();
        for row_idx in 0..n {
            let mut obj = Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let val: Value = if col.is_null(row_idx) {
                    Value::Null
                } else {
                    match field.data_type() {
                        DataType::Boolean => {
                            if let Some(a) = col.as_any().downcast_ref::<arrow_array::BooleanArray>() {
                                Value::Bool(a.value(row_idx))
                            } else { Value::Null }
                        }
                        DataType::Int8 => json!(col.as_primitive::<Int8Type>().value(row_idx)),
                        DataType::Int16 => json!(col.as_primitive::<Int16Type>().value(row_idx)),
                        DataType::Int32 => json!(col.as_primitive::<Int32Type>().value(row_idx)),
                        DataType::Int64 => json!(col.as_primitive::<Int64Type>().value(row_idx)),
                        DataType::UInt8 => json!(col.as_primitive::<UInt8Type>().value(row_idx)),
                        DataType::UInt16 => json!(col.as_primitive::<UInt16Type>().value(row_idx)),
                        DataType::UInt32 => json!(col.as_primitive::<UInt32Type>().value(row_idx)),
                        DataType::UInt64 => json!(col.as_primitive::<UInt64Type>().value(row_idx)),
                        DataType::Float32 => {
                            let v = col.as_primitive::<Float32Type>().value(row_idx);
                            serde_json::Number::from_f64(v as f64).map(Value::Number).unwrap_or(Value::Null)
                        }
                        DataType::Float64 => {
                            let v = col.as_primitive::<Float64Type>().value(row_idx);
                            serde_json::Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null)
                        }
                        DataType::Utf8 => {
                            if let Some(a) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
                                Value::String(a.value(row_idx).to_string())
                            } else { Value::Null }
                        }
                        DataType::LargeUtf8 => {
                            if let Some(a) = col.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
                                Value::String(a.value(row_idx).to_string())
                            } else { Value::Null }
                        }
                        DataType::Decimal128(_precision, scale) => {
                            let scale = *scale as u32;
                            let raw = col.as_primitive::<Decimal128Type>().value(row_idx);
                            Value::String(decimal128_to_string(raw, scale))
                        }
                        DataType::Date32 => {
                            let days = col.as_primitive::<Date32Type>().value(row_idx);
                            match chrono::NaiveDate::from_epoch_days(days) {
                                Some(d) => Value::String(d.format("%Y-%m-%d").to_string()),
                                None => Value::Null,
                            }
                        }
                        DataType::Timestamp(TimeUnit::Microsecond, _) => {
                            let micros = col.as_primitive::<TimestampMicrosecondType>().value(row_idx);
                            match chrono::DateTime::from_timestamp_micros(micros) {
                                Some(dt) => Value::String(format_naive_datetime(&dt.naive_utc())),
                                None => Value::Null,
                            }
                        }
                        DataType::Time64(TimeUnit::Microsecond) => {
                            let micros = col.as_primitive::<Time64MicrosecondType>().value(row_idx);
                            Value::String(format_naive_time_micros(micros))
                        }
                        _dt => {
                            // Fallback: display the array element as debug string.
                            Value::String(format!("{:?}", col.slice(row_idx, 1)))
                        }
                    }
                };
                obj.insert(field.name().clone(), val);
            }
            rows.push(Value::Object(obj));
        }
    }
    Value::Array(rows)
}

/// Format a `Decimal128` raw mantissa + scale as a normalized decimal string.
///
/// Trailing fractional zeros are stripped so that the analytical path (always
/// scale 18 from Iceberg) and the SQL/OLTP path (scale from gluesql) produce
/// the same string for the same logical value, e.g. both give `"12.34"` not
/// `"12.340000000000000000"`.  An integer result has no decimal point.
fn decimal128_to_string(raw: i128, scale: u32) -> String {
    if scale == 0 {
        return format!("{raw}");
    }
    let neg = raw < 0;
    let abs_raw = raw.unsigned_abs();
    let divisor = 10u128.pow(scale);
    let whole = abs_raw / divisor;
    let frac = abs_raw % divisor;
    let sign = if neg { "-" } else { "" };
    // Pad fractional part to `scale` digits, then strip trailing zeros.
    let frac_str = format!("{frac:0>width$}", width = scale as usize);
    let frac_trimmed = frac_str.trim_end_matches('0');
    if frac_trimmed.is_empty() {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{frac_trimmed}")
    }
}

/// Format a `NaiveDateTime` as `"YYYY-MM-DDTHH:MM:SS"` or
/// `"YYYY-MM-DDTHH:MM:SS.ffffff"` when there are sub-second microseconds.
fn format_naive_datetime(dt: &chrono::NaiveDateTime) -> String {
    let micros = dt.and_utc().timestamp_subsec_micros();
    if micros == 0 {
        dt.format("%Y-%m-%dT%H:%M:%S").to_string()
    } else {
        dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
    }
}

/// Format microseconds-since-midnight as `"HH:MM:SS"` or `"HH:MM:SS.ffffff"`.
fn format_naive_time_micros(total_micros: i64) -> String {
    let total_micros = total_micros.unsigned_abs();
    let h = total_micros / 3_600_000_000;
    let rem = total_micros % 3_600_000_000;
    let m = rem / 60_000_000;
    let rem = rem % 60_000_000;
    let s = rem / 1_000_000;
    let micros = rem % 1_000_000;
    if micros == 0 {
        format!("{h:02}:{m:02}:{s:02}")
    } else {
        format!("{h:02}:{m:02}:{s:02}.{micros:06}")
    }
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// `POST /sql` — one parameterized non-DDL statement (SELECT/INSERT/UPDATE/DELETE).
///
/// Mutating statements (INSERT/UPDATE/DELETE) return `X-Bluedb-Watermark: <tenant>:<seq>`
/// so the client knows which CDC sequence this write reached.
///
/// **Analytical fallback (P2.2):** when a SELECT is rejected by the scan/sort
/// guardrail (non-indexed filter or in-memory sort), the handler automatically
/// routes it to the analytical path — `bluedb_query::query_sql` over the
/// tenant's sealed Iceberg snapshot. Only single-table SELECTs are routed;
/// multi-table SELECTs return the original guardrail-reject (noted in the error).
///
/// **Freshness gate on the analytical path (HTAP P4):** `X-Bluedb-Min-Watermark`
/// is checked against the sealed Iceberg watermark. If `min > sealed`, the
/// sealed snapshot can't satisfy the read, so [`serve_fresh_analytical`] decides:
/// the active writer serves the **fresh, unsealed** rows via the writer-local
/// read; a non-writer node would 302-redirect to the writer (deferred — no
/// resolvable writer URL today) and meanwhile fails fast with `503` (never
/// hangs). The `bluedb_read_wait_seal_n` PRAGMA tunes the tolerance.
async fn exec_sql(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<SqlRequest>,
) -> Result<impl IntoResponse, AppError> {
    state.authorize(&headers, authz::Scope::DataQuery)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;
    // Intercept bluedb PRAGMAs before gluesql (which would reject them): apply to
    // this tenant's mirror engine and ack. Covers `lakehouse_mirror[...]`,
    // `lakehouse_target_file_bytes`, and the HTAP `bluedb_read_wait_seal_n`
    // freshness tolerance — all parsed by `parse_lakehouse_pragma`.
    if let Some(pragma) = parse_lakehouse_pragma(&req.sql) {
        let name = match &pragma {
            LhPragma::GlobalDefault(_) | LhPragma::Table(_, _) => "lakehouse_mirror",
            LhPragma::TargetFileBytes(_) => "lakehouse_target_file_bytes",
            LhPragma::ReadWaitSealN(_) => "bluedb_read_wait_seal_n",
        };
        match state.lakehouse().await {
            Some(manager) => {
                manager
                    .apply_pragma(&tenant, pragma)
                    .await
                    .map_err(|e| AppError::internal(format!("lakehouse pragma: {e}")))?;
                return Ok((axum::http::HeaderMap::new(), Json(json!({ "ok": true, "pragma": name }))));
            }
            None => return Err(AppError::internal("lakehouse manager not bound")),
        }
    }
    // Reads are served by the analytical engine (the DataFusion front door);
    // writes and DDL stay on the transactional engine.
    if is_read_query(&req.sql) {
        return exec_sql_read(&state, &headers, &tenant, &req).await;
    }

    let params = req.params.iter().map(json_to_param).collect::<Result<Vec<_>, _>>()?;
    let mut glue = Glue::new(state.connection_serialized(&tenant).await?);
    // Writes flow through the FTS engine so its commit observer indexes them;
    // a non-`@@` statement runs unchanged.
    let payloads = state.fts().await.execute_fts(&mut glue, &req.sql, &params).await?;
    let wm = state.write_watermark(&tenant).await;
    Ok((watermark_headers(&tenant, wm), Json(payloads_to_json(payloads))))
}

/// True if `sql` is a single read query (`SELECT` / `VALUES` / CTE) — routed to
/// the analytical engine. Writes and DDL return false (the transactional engine
/// serves them). A parse failure is treated as not-a-read, so the transactional
/// engine surfaces the error.
fn is_read_query(sql: &str) -> bool {
    use gluesql_core::sqlparser::ast::Statement;
    match gluesql_core::parse_sql::parse(sql) {
        Ok(stmts) if stmts.len() == 1 => matches!(stmts[0], Statement::Query(_)),
        _ => false,
    }
}

/// Serve a `POST /sql` read through the DataFusion front door: the per-tenant
/// analytical engine, with every referenced table resolved via the bluedb schema
/// provider (joins / windows / aggregates / multi-table / CTEs). FTS predicates
/// are rewritten to plain SQL first; the freshness gate 503s on a non-writer node
/// that cannot satisfy a fresher-than-sealed read.
async fn exec_sql_read(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    tenant: &str,
    req: &SqlRequest,
) -> Result<(axum::http::HeaderMap, Json<Value>), AppError> {
    let manager = state.lakehouse().await.ok_or_else(|| {
        AppError::internal("analytical path unavailable: lakehouse manager not bound")
    })?;
    let engine = manager
        .engine_for(tenant)
        .await
        .map_err(|e| AppError::internal(format!("get lakehouse engine: {e}")))?;

    // Rewrite FTS (`@@` / `ts_rank` / trigram-`LIKE`) to plain SQL the analytical
    // engine runs; non-FTS SQL passes through unchanged.
    let rewritten = state.fts().await.rewrite_for(&req.sql).await?;
    let sql = rewritten.unwrap_or_else(|| req.sql.clone());

    // Freshness gate: a non-writer node holds no fresh unsealed tail, so it cannot
    // satisfy a read demanding a watermark beyond the sealed snapshot.
    let sealed = state.sealed_watermark(tenant).await;
    if let Some(min) = parse_min_watermark(headers) {
        if min > sealed && !state.is_writer() {
            return Err(AppError::plain(
                StatusCode::SERVICE_UNAVAILABLE,
                format!(
                    "read-your-writes freshness not satisfiable on this node: requested \
                     min-watermark {min} > sealed watermark {sealed}, and this node is not \
                     the active writer; retry against the writer or after the next seal"
                ),
            )
            .with_headers(watermark_headers_always(tenant, sealed)));
        }
    }

    let batches = bluedb_query::query_via_catalog(engine, &sql, &req.params)
        .await
        .map_err(|e| AppError::bad_request(format!("query: {e}")))?;

    // On the writer the union reflects unsealed writes (read-your-writes);
    // elsewhere it is at least the sealed snapshot.
    let wm = if state.is_writer() {
        state.write_watermark(tenant).await
    } else {
        sealed
    };
    Ok((watermark_headers(tenant, wm), Json(record_batches_to_json(&batches))))
}

/// `POST /admin/sql` — arbitrary SQL (DDL/txns/multi). Off by default; audited.
async fn admin_sql(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<SqlRequest>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::Superuser)?;
    if !state.inner.admin_sql_enabled.load(Ordering::Relaxed) {
        return Err(AppError::not_found("admin SQL endpoint is disabled"));
    }
    let tenant = state.tenant(&headers)?;
    state.require_active()?;
    eprintln!("bluedb-audit: /admin/sql executed: {}", req.sql);
    let params = req.params.iter().map(json_to_param).collect::<Result<Vec<_>, _>>()?;
    let mut glue = Glue::new(state.connection_serialized(&tenant).await?);
    let payloads = rest_sql::execute_sql(&mut glue, &req.sql, &params, true).await?;
    Ok(Json(payloads_to_json(payloads)))
}

/// `GET /tables/{table}?<filters>` — PostgREST SELECT (served by writer or replica).
///
/// Echoes the current Iceberg-sealed watermark in `X-Bluedb-Watermark` on every
/// response so clients can track mirror freshness. The freshness gate
/// (`X-Bluedb-Min-Watermark`) is intentionally **not** applied here: this path
/// reads from SlateDB, which is always at least as fresh as the Iceberg seal,
/// so a min-watermark constraint on the seal would incorrectly 503 queries that
/// the OLTP store can actually satisfy. The freshness gate belongs on the
/// analytical path (`POST /sql` guardrail-routed queries that read from Iceberg).
async fn select(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<impl IntoResponse, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;

    let sealed = state.sealed_watermark(&tenant).await;
    // Percent-decode the query string: a JSON-path key (`data->>status`) arrives
    // `%3E%3E`-encoded from any conformant client, and `parse_query` expects
    // already-decoded text. Separators (`&`/`=`) are literal in the URL, so they
    // survive; only `%XX` escapes within keys/values are decoded.
    let decoded_qs = percent_encoding::percent_decode_str(query.as_deref().unwrap_or(""))
        .decode_utf8_lossy();
    let rq = bluedb_rest::parse_query(&table, &decoded_qs).map_err(EngineError::from)?;

    let mut glue = Glue::new(state.connection(&tenant).await?);
    // JSON/JSONB columns are stored as TEXT; the catalog tells us which to
    // re-inflate to real JSON on the way out.
    let json_cols = glue
        .storage
        .json_columns(&table)
        .await
        .map_err(|e| AppError::internal(format!("read json catalog: {e}")))?
        .unwrap_or_default();

    // A JSON-path query (`data->>k`) can only be served by the analytical engine
    // (GlueSQL has no JSON functions) — route it there directly.
    if rq.has_json_path() {
        return route_select_to_analytical(&state, &headers, &tenant, &rq, &json_cols, sealed).await;
    }
    // Otherwise take the GlueSQL fast path. A guardrail reject (a filter / ORDER BY
    // on a non-indexed column) means GlueSQL can't bound the read; re-route it to
    // the analytical engine, which scans/sorts the Iceberg mirror. This is doc
    // ask #4: the grid filters/sorts arbitrary columns without a standing index.
    match rest_sql::execute_query(&mut glue, &rq).await {
        Ok(payloads) => Ok((
            watermark_headers(&tenant, sealed),
            Json(select_to_json(payloads, &json_cols)),
        )),
        Err(e) if is_guardrail_reject(&e) => {
            route_select_to_analytical(&state, &headers, &tenant, &rq, &json_cols, sealed).await
        }
        Err(e) => Err(e.into()),
    }
}

/// Serve a `/tables` read the GlueSQL fast path can't (an arbitrary-column filter
/// / sort, or a JSON-path predicate) through the analytical front door, exactly
/// like `POST /sql`: render the PostgREST request to SQL, run it on DataFusion
/// over the tenant's Iceberg mirror (+ writer-local unsealed tail), and re-inflate
/// JSON columns so the response matches the fast path. Reads only — writes stay on
/// GlueSQL. The freshness gate applies (routed reads hit the sealed snapshot).
async fn route_select_to_analytical(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    tenant: &str,
    rq: &bluedb_rest::RestQuery,
    json_cols: &[String],
    sealed: i64,
) -> Result<(axum::http::HeaderMap, Json<Value>), AppError> {
    let (sql, params) = rq.to_sql_with_params().map_err(EngineError::from)?;
    let json_params: Vec<Value> = params.iter().map(param_to_json).collect();

    // Freshness gate: a non-writer node can't satisfy a read demanding a watermark
    // beyond its sealed snapshot (same rule as `exec_sql_read`).
    if let Some(min) = parse_min_watermark(headers) {
        if min > sealed && !state.is_writer() {
            return Err(AppError::plain(
                StatusCode::SERVICE_UNAVAILABLE,
                format!(
                    "read-your-writes freshness not satisfiable on this node: requested \
                     min-watermark {min} > sealed watermark {sealed}, and this node is not \
                     the active writer; retry against the writer or after the next seal"
                ),
            )
            .with_headers(watermark_headers_always(tenant, sealed)));
        }
    }

    let manager = state.lakehouse().await.ok_or_else(|| {
        AppError::internal("analytical path unavailable: lakehouse manager not bound")
    })?;
    let engine = manager
        .engine_for(tenant)
        .await
        .map_err(|e| AppError::internal(format!("get lakehouse engine: {e}")))?;
    let batches = bluedb_query::query_via_catalog(engine, &sql, &json_params)
        .await
        .map_err(|e| AppError::bad_request(format!("query: {e}")))?;

    let wm = if state.is_writer() {
        state.write_watermark(tenant).await
    } else {
        sealed
    };
    Ok((
        watermark_headers(tenant, wm),
        Json(reinflate_rows(record_batches_to_json(&batches), json_cols)),
    ))
}

/// `POST /tables/{table}` — INSERT (JSON object → autocommit; array → one txn batch).
///
/// Returns `X-Bluedb-Watermark: <tenant>:<seq>` so the caller can pass it back
/// as `X-Bluedb-Min-Watermark` on a subsequent read to enforce read-your-writes.
async fn insert(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    state.authorize(&headers, authz::Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;
    let want_repr = wants_representation(&headers);
    let (req, is_batch) = build_insert(table, body)?;

    // Execute the insert on its usual connection (batch = atomic BEGIN..COMMIT on
    // the serialized lease-holder; single = group-commit fast path).
    let inserted: usize = if is_batch {
        let mut glue = Glue::new(state.connection_serialized(&tenant).await?);
        let payloads = rest_sql::execute_insert_batch(&mut glue, &req).await?;
        payloads
            .iter()
            .filter_map(|p| if let Payload::Insert(n) = p { Some(*n) } else { None })
            .sum()
    } else {
        let mut glue = Glue::new(state.connection(&tenant).await?);
        let payloads = rest_sql::execute_insert(&mut glue, &req).await?;
        payloads
            .iter()
            .filter_map(|p| if let Payload::Insert(n) = p { Some(*n) } else { None })
            .sum()
    };
    let wm = state.write_watermark(&tenant).await;

    // `Prefer: return=representation` → read the inserted rows back by primary key.
    if want_repr {
        let pk_cols = state
            .connection_unguarded(&tenant)
            .await?
            .primary_key_columns(&req.table)
            .await
            .map_err(|e| AppError::internal(format!("pk columns: {e}")))?;
        if let Some(filters) = insert_pk_filters(&pk_cols, &req) {
            let rows = read_affected_json(&state, &tenant, &req.table, filters).await?;
            return Ok((watermark_headers(&tenant, wm), Json(rows)));
        }
        // Can't identify the rows by PK (e.g. multi-row composite) → fall back to count.
    }
    Ok((watermark_headers(&tenant, wm), Json(json!({ "inserted": inserted }))))
}

/// `PATCH /tables/{table}?<filters>` — UPDATE (JSON assignments body).
///
/// Returns `X-Bluedb-Watermark` for read-your-writes freshness tracking.
async fn update(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
    RawQuery(query): RawQuery,
    Json(assignments): Json<Map<String, Value>>,
) -> Result<impl IntoResponse, AppError> {
    state.authorize(&headers, authz::Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;
    let want_repr = wants_representation(&headers);
    let filters = parse_filters(query.as_deref().unwrap_or("")).map_err(EngineError::from)?;
    let assignments = assignments
        .into_iter()
        .map(|(col, value)| Ok((col, json_scalar_to_dsl(&value)?)))
        .collect::<Result<Vec<_>, AppError>>()?;
    let req = UpdateRequest { table, assignments, filters };
    // UPDATE is a read-modify-write; serialize so concurrent ones can't lose.
    let mut glue = Glue::new(state.connection_serialized(&tenant).await?);
    let payloads = rest_sql::execute_update(&mut glue, &req).await?;
    let wm = state.write_watermark(&tenant).await;
    // `Prefer: return=representation` → read the updated rows back by the same
    // filter. (Caveat: if the filter targets a column the UPDATE changed, the
    // re-select reflects post-update matches — full RETURNING fidelity needs
    // engine support GlueSQL lacks.)
    if want_repr {
        let rows = read_affected_json(&state, &tenant, &req.table, req.filters.clone()).await?;
        return Ok((watermark_headers(&tenant, wm), Json(rows)));
    }
    Ok((watermark_headers(&tenant, wm), Json(payloads_to_json(payloads))))
}

/// `DELETE /tables/{table}?<filters>` — DELETE.
///
/// Returns `X-Bluedb-Watermark` for read-your-writes freshness tracking.
async fn delete_rows(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<impl IntoResponse, AppError> {
    state.authorize(&headers, authz::Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;
    let want_repr = wants_representation(&headers);
    let filters = parse_filters(query.as_deref().unwrap_or("")).map_err(EngineError::from)?;
    // `Prefer: return=representation` → capture the matching rows BEFORE the
    // delete (they're gone afterwards). This is what feeds the DELETE→Pusher
    // notification with the removed rows in one round trip.
    let repr = if want_repr {
        Some(read_affected_json(&state, &tenant, &table, filters.clone()).await?)
    } else {
        None
    };
    let req = DeleteRequest { table, filters };
    // DELETE reads the rows it removes; serialize for the same reason as UPDATE.
    let mut glue = Glue::new(state.connection_serialized(&tenant).await?);
    let payloads = rest_sql::execute_delete(&mut glue, &req).await?;
    let wm = state.write_watermark(&tenant).await;
    match repr {
        Some(rows) => Ok((watermark_headers(&tenant, wm), Json(rows))),
        None => Ok((watermark_headers(&tenant, wm), Json(payloads_to_json(payloads)))),
    }
}

// --- admin / high-availability control --------------------------------------

/// `GET /admin/status` — this node's writer role, fencing epoch, lease expiry.
/// Reports `active` only when the lease is held **and** the writer `Db` is bound,
/// so a routing client never targets a node that holds the lease but is still
/// serving from the pre-failover reader (see [`Inner::writer_bound`]).
async fn admin_status(State(state): State<AppState>) -> Json<Value> {
    let writer_bound = state.inner.writer_bound.load(Ordering::Acquire);
    Json(status_json_effective(&state.inner.writer.status(), writer_bound))
}

/// `POST /admin/promote` — acquire the lease + open the writer database.
async fn admin_promote(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::Superuser)?;
    state.promote().await?;
    Ok(Json(status_json(&state.inner.writer.status())))
}

/// `POST /admin/demote` — release the lease + rebind as a read replica.
async fn admin_demote(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::Superuser)?;
    state.demote().await?;
    Ok(Json(status_json(&state.inner.writer.status())))
}

fn status_json(status: &Status) -> Value {
    json!({
        "node_id": status.node_id,
        "role": status.role.as_str(),
        "epoch": status.epoch,
        "lease_expires_at_millis": status.lease_expires_at_millis,
    })
}

/// Like [`status_json`] but downgrades a node that holds the lease yet has not
/// installed its writer `Db` (`writer_bound == false`) from `active` to `passive`.
/// During the failover window between acquiring the lease and swapping in the
/// writer `Db`, the node is still bound to the pre-failover reader; advertising it
/// as `active` would let a routing client serve stale reads from it (the root
/// cause of the counter lost-acked-increment finding under `kill`).
fn status_json_effective(status: &Status, writer_bound: bool) -> Value {
    let role = if status.role.as_str() == "active" && !writer_bound {
        "passive"
    } else {
        status.role.as_str()
    };
    json!({
        "node_id": status.node_id,
        "role": role,
        "epoch": status.epoch,
        "lease_expires_at_millis": status.lease_expires_at_millis,
    })
}

// --- request/response mapping ----------------------------------------------

/// Turn a JSON insert body (object, or array of objects) into an
/// [`InsertRequest`]. Columns are taken from the (sorted) keys of the first
/// object; every row must carry exactly those keys.
fn build_insert(table: String, body: Value) -> Result<(InsertRequest, bool), AppError> {
    let (objects, is_batch): (Vec<Map<String, Value>>, bool) = match body {
        Value::Object(map) => (vec![map], false),
        Value::Array(items) => (
            items
                .into_iter()
                .map(|item| match item {
                    Value::Object(map) => Ok(map),
                    other => Err(AppError::bad_request(format!(
                        "insert rows must be JSON objects, got {other}"
                    ))),
                })
                .collect::<Result<_, _>>()?,
            true,
        ),
        other => {
            return Err(AppError::bad_request(format!(
                "insert body must be an object or array of objects, got {other}"
            )))
        }
    };

    let first = objects
        .first()
        .ok_or_else(|| AppError::bad_request("insert body has no rows"))?;
    let columns: Vec<String> = first.keys().cloned().collect();
    if columns.is_empty() {
        return Err(AppError::bad_request("insert row has no columns"));
    }

    let mut rows = Vec::with_capacity(objects.len());
    for object in &objects {
        let mut row = Vec::with_capacity(columns.len());
        for column in &columns {
            let value = object
                .get(column)
                .ok_or_else(|| AppError::bad_request(format!("row is missing column '{column}'")))?;
            row.push(json_scalar_to_dsl(value)?);
        }
        rows.push(row);
    }

    Ok((InsertRequest { table, columns, rows }, is_batch))
}

/// Render a JSON value into the DSL string form `bluedb-rest` expects. (Like
/// PostgREST, values are stringly-typed on the wire: the engine later types them
/// into typed `$N` parameters — numeric text → Int/Float, `true`/`false` → Bool,
/// `null` → Null, everything else → Str.)
///
/// A JSON object/array serializes to its **canonical compact JSON text** so it
/// can be stored in a `JSON`/`JSONB` (→ `TEXT`) column; validation is implicit
/// (the body already parsed as JSON). A bare JSON scalar keeps its natural type.
fn json_scalar_to_dsl(value: &Value) -> Result<String, AppError> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Null => Ok("null".to_string()),
        Value::Object(_) | Value::Array(_) => Ok(value.to_string()),
    }
}

/// Serialize `/tables` SELECT payloads, re-inflating JSON columns: a column
/// declared `JSON`/`JSONB` is stored as `TEXT`, and its text is parsed back to a
/// real JSON value on read so the grid sees an object/array, not an escaped
/// string. Non-JSON columns are unchanged; a JSON column holding non-JSON text
/// (e.g. written via raw SQL) is emitted as the string rather than failing.
fn select_to_json(payloads: Vec<Payload>, json_cols: &[String]) -> Value {
    let one = |payload: Payload| match payload {
        Payload::Select { labels, rows } => Value::Array(
            rows.into_iter()
                .map(|row| {
                    let obj: Map<String, Value> = labels
                        .iter()
                        .cloned()
                        .zip(row.iter().map(sql_value_to_json))
                        .map(|(label, v)| {
                            let v = reinflate_json(&label, v, json_cols);
                            (label, v)
                        })
                        .collect();
                    Value::Object(obj)
                })
                .collect(),
        ),
        Payload::SelectMap(maps) => Value::Array(
            maps.into_iter()
                .map(|m| {
                    let obj: Map<String, Value> = m
                        .iter()
                        .map(|(k, v)| (k.clone(), reinflate_json(k, sql_value_to_json(v), json_cols)))
                        .collect();
                    Value::Object(obj)
                })
                .collect(),
        ),
        other => payload_to_json(other),
    };
    if payloads.len() == 1 {
        one(payloads.into_iter().next().unwrap())
    } else {
        Value::Array(payloads.into_iter().map(one).collect())
    }
}

/// Parse a JSON column's stored text back to a real JSON value. Leaves non-JSON
/// columns, non-string cells, and unparseable text untouched.
fn reinflate_json(label: &str, v: Value, json_cols: &[String]) -> Value {
    if json_cols.iter().any(|c| c == label) {
        if let Value::String(s) = &v {
            if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                return parsed;
            }
        }
    }
    v
}

/// Re-inflate JSON columns in an array of row objects — the analytical path's
/// [`record_batches_to_json`] output, where a JSON column arrives as `Utf8` text.
/// The `/tables` GlueSQL fast path does this via [`select_to_json`]; this is the
/// equivalent for a read routed to DataFusion, so both surfaces return identical
/// JSON for a JSON column.
fn reinflate_rows(value: Value, json_cols: &[String]) -> Value {
    if json_cols.is_empty() {
        return value;
    }
    let Value::Array(rows) = value else {
        return value;
    };
    Value::Array(
        rows.into_iter()
            .map(|row| {
                let Value::Object(mut map) = row else {
                    return row;
                };
                for col in json_cols {
                    let parsed = match map.get(col) {
                        Some(Value::String(s)) => serde_json::from_str::<Value>(s).ok(),
                        _ => None,
                    };
                    if let Some(p) = parsed {
                        map.insert(col.clone(), p);
                    }
                }
                Value::Object(map)
            })
            .collect(),
    )
}

/// True if the request asked for the affected rows back (`Prefer:
/// return=representation`, PostgREST). `Prefer` may carry several
/// comma-separated preferences.
fn wants_representation(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get_all("prefer")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|p| p.trim().eq_ignore_ascii_case("return=representation"))
}

/// Read the rows matching `filters` from `table` on an **unguarded** connection
/// (the `Prefer: return=representation` read-back), serialized as `/tables` JSON
/// with JSON columns re-inflated. The read is bounded by the mutation's own
/// filter, so the scan guardrail is intentionally bypassed, and it reads fresh
/// SlateDB state (not the Iceberg mirror).
async fn read_affected_json(
    state: &AppState,
    tenant: &str,
    table: &str,
    filters: Vec<bluedb_rest::Filter>,
) -> Result<Value, AppError> {
    let storage = state.connection_unguarded(tenant).await?;
    let json_cols = storage
        .json_columns(table)
        .await
        .map_err(|e| AppError::internal(format!("read json catalog: {e}")))?
        .unwrap_or_default();
    let mut glue = Glue::new(storage);
    let q = bluedb_rest::RestQuery {
        table: table.to_string(),
        select: Vec::new(),
        filters,
        order: Vec::new(),
        limit: None,
        offset: None,
    };
    let payloads = rest_sql::execute_query(&mut glue, &q).await?;
    Ok(select_to_json(payloads, &json_cols))
}

/// The PostgREST filters identifying the rows an INSERT just added, from the
/// table's PK columns + the inserted values. `None` when the rows can't be
/// identified by PK — a PK column absent from the insert, or a multi-row insert
/// into a composite-PK table (the per-row AND-of-components OR'd across rows isn't
/// expressible as AND-only PostgREST filters). The caller falls back to the count.
fn insert_pk_filters(pk_cols: &[String], req: &InsertRequest) -> Option<Vec<bluedb_rest::Filter>> {
    use bluedb_rest::{Filter, Operator};
    if pk_cols.is_empty() {
        return None;
    }
    // Each PK column's position in the insert's column list (all must be present).
    let idx: Vec<usize> = pk_cols
        .iter()
        .map(|pk| req.columns.iter().position(|c| c == pk))
        .collect::<Option<_>>()?;
    match req.rows.len() {
        0 => None,
        // Single row: AND each PK component (works for single- and composite-PK).
        1 => Some(
            pk_cols
                .iter()
                .zip(&idx)
                .map(|(pk, &i)| Filter::new(pk.clone(), Operator::Eq, req.rows[0][i].clone()))
                .collect(),
        ),
        // Multi-row, single-column PK: one IN filter over all the key values.
        _ if pk_cols.len() == 1 => {
            let values: Vec<String> = req.rows.iter().map(|r| r[idx[0]].clone()).collect();
            Some(vec![Filter::new(
                pk_cols[0].clone(),
                Operator::In,
                format!("({})", values.join(",")),
            )])
        }
        _ => None,
    }
}

/// One payload → JSON; many (multi-statement `/sql`) → a JSON array.
fn payloads_to_json(payloads: Vec<Payload>) -> Value {
    if payloads.len() == 1 {
        payload_to_json(payloads.into_iter().next().unwrap())
    } else {
        Value::Array(payloads.into_iter().map(payload_to_json).collect())
    }
}

fn payload_to_json(payload: Payload) -> Value {
    match payload {
        Payload::Select { labels, rows } => Value::Array(
            rows.into_iter()
                .map(|row| {
                    let obj: Map<String, Value> = labels
                        .iter()
                        .cloned()
                        .zip(row.iter().map(sql_value_to_json))
                        .collect();
                    Value::Object(obj)
                })
                .collect(),
        ),
        Payload::SelectMap(maps) => Value::Array(
            maps.into_iter()
                .map(|m| {
                    let obj: Map<String, Value> =
                        m.iter().map(|(k, v)| (k.clone(), sql_value_to_json(v))).collect();
                    Value::Object(obj)
                })
                .collect(),
        ),
        Payload::Insert(n) => json!({ "inserted": n }),
        Payload::Update(n) => json!({ "updated": n }),
        Payload::Delete(n) => json!({ "deleted": n }),
        Payload::DropTable(n) => json!({ "dropped_tables": n }),
        Payload::Create => json!({ "created": true }),
        Payload::CreateIndex => json!({ "created_index": true }),
        Payload::DropIndex => json!({ "dropped_index": true }),
        Payload::AlterTable => json!({ "altered": true }),
        Payload::StartTransaction => json!({ "transaction": "begin" }),
        Payload::Commit => json!({ "transaction": "commit" }),
        Payload::Rollback => json!({ "transaction": "rollback" }),
        other => json!({ "status": format!("{other:?}") }),
    }
}

/// Map a GlueSQL scalar to clean JSON. Common scalars map directly; exotic types
/// fall back to a debug string rather than GlueSQL's tagged serde form.
fn sql_value_to_json(value: &SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Bool(b) => Value::Bool(*b),
        SqlValue::I8(n) => json!(*n),
        SqlValue::I16(n) => json!(*n),
        SqlValue::I32(n) => json!(*n),
        SqlValue::I64(n) => json!(*n),
        SqlValue::U8(n) => json!(*n),
        SqlValue::U16(n) => json!(*n),
        SqlValue::U32(n) => json!(*n),
        SqlValue::U64(n) => json!(*n),
        SqlValue::F32(x) => serde_json::Number::from_f64(*x as f64).map(Value::Number).unwrap_or(Value::Null),
        SqlValue::F64(x) => serde_json::Number::from_f64(*x).map(Value::Number).unwrap_or(Value::Null),
        // 128-bit ints exceed JSON's safe integer range, so emit them as
        // decimal strings (precision-preserving, like the `/ledger` API). This
        // is what makes the ledger's U128 projection columns usable over HTTP.
        SqlValue::U128(n) => Value::String(n.to_string()),
        SqlValue::I128(n) => Value::String(n.to_string()),
        SqlValue::Str(s) => Value::String(s.clone()),
        // Temporal/Decimal types — canonical rendering matches record_batches_to_json.
        // Decimal → normalized string (trailing fractional zeros stripped) so the
        // OLTP path matches the analytical path which always has Iceberg scale 18.
        SqlValue::Decimal(d) => Value::String(d.normalize().to_string()),
        // Date → ISO-8601 "YYYY-MM-DD".
        SqlValue::Date(d) => Value::String(d.format("%Y-%m-%d").to_string()),
        // Timestamp → "YYYY-MM-DDTHH:MM:SS[.ffffff]".
        SqlValue::Timestamp(dt) => Value::String(format_naive_datetime(dt)),
        // Time → "HH:MM:SS[.ffffff]".
        SqlValue::Time(t) => {
            use chrono::Timelike as _;
            let micros_frac = t.nanosecond() / 1_000;
            if micros_frac == 0 {
                Value::String(t.format("%H:%M:%S").to_string())
            } else {
                Value::String(t.format("%H:%M:%S%.6f").to_string())
            }
        }
        // Uuid (stored as u128) → canonical hyphenated 8-4-4-4-12 lowercase hex,
        // matching gluesql's own `Uuid::from_u128(..).hyphenated()` rendering so the
        // OLTP wire form agrees with the analytical path.
        SqlValue::Uuid(n) => {
            let hex = format!("{n:032x}");
            Value::String(format!(
                "{}-{}-{}-{}-{}",
                &hex[0..8],
                &hex[8..12],
                &hex[12..16],
                &hex[16..20],
                &hex[20..32]
            ))
        }
        // Bytea → standard base64 (the same encoding the evidence API uses on the wire).
        SqlValue::Bytea(b) => {
            use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
            Value::String(B64.encode(b))
        }
        other => Value::String(format!("{other:?}")),
    }
}

// --- tests ------------------------------------------------------------------

#[cfg(test)]
mod value_serialization {
    use super::sql_value_to_json;
    use gluesql_core::prelude::Value as SqlValue;
    use serde_json::Value;

    #[test]
    fn renders_uuid_as_canonical_hyphenated_string() {
        let u = SqlValue::Uuid(0x550e8400_e29b_41d4_a716_446655440000u128);
        assert_eq!(
            sql_value_to_json(&u),
            Value::String("550e8400-e29b-41d4-a716-446655440000".into())
        );
    }

    #[test]
    fn renders_bytea_as_base64() {
        let b = SqlValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(sql_value_to_json(&b), Value::String("3q2+7w==".into()));
    }
}

#[cfg(test)]
mod flush_interval_cfg {
    use super::parse_flush_interval_ms;
    use std::time::Duration;

    #[test]
    fn defaults_to_25ms_and_parses_override() {
        assert_eq!(parse_flush_interval_ms(None), Duration::from_millis(25));
        assert_eq!(parse_flush_interval_ms(Some("50")), Duration::from_millis(50));
        assert_eq!(parse_flush_interval_ms(Some("100")), Duration::from_millis(100));
        assert_eq!(parse_flush_interval_ms(Some("abc")), Duration::from_millis(25));
        assert_eq!(parse_flush_interval_ms(Some("")), Duration::from_millis(25));
        assert_eq!(parse_flush_interval_ms(Some("0")), Duration::from_millis(0));
    }
}

#[cfg(test)]
mod fts_seal_interval_cfg {
    use super::parse_fts_seal_interval_ms;
    use std::time::Duration;

    #[test]
    fn defaults_to_30s_and_parses_override() {
        assert_eq!(parse_fts_seal_interval_ms(None), Duration::from_millis(30_000));
        assert_eq!(parse_fts_seal_interval_ms(Some("5000")), Duration::from_millis(5_000));
        assert_eq!(parse_fts_seal_interval_ms(Some("100")), Duration::from_millis(100));
        assert_eq!(parse_fts_seal_interval_ms(Some("abc")), Duration::from_millis(30_000));
        assert_eq!(parse_fts_seal_interval_ms(Some("")), Duration::from_millis(30_000));
        assert_eq!(parse_fts_seal_interval_ms(Some("0")), Duration::from_millis(0));
    }
}

#[cfg(test)]
mod insert_routing {
    use super::build_insert;
    use serde_json::json;

    #[test]
    fn object_body_is_not_batch_single_row() {
        let (req, is_batch) = build_insert("docs".into(), json!({"id": "1"})).unwrap();
        assert!(!is_batch);
        assert_eq!(req.table, "docs");
        assert_eq!(req.columns, vec!["id".to_string()]);
        assert_eq!(req.rows.len(), 1);
        assert_eq!(req.rows[0], vec!["1".to_string()]);
    }

    #[test]
    fn array_body_is_batch_multi_row() {
        let (req, is_batch) =
            build_insert("docs".into(), json!([{"id": "1"}, {"id": "2"}])).unwrap();
        assert!(is_batch);
        assert_eq!(req.rows.len(), 2);
    }

    #[test]
    fn scalar_body_is_rejected() {
        assert!(build_insert("docs".into(), json!(42)).is_err());
    }
}

// --- errors -----------------------------------------------------------------

/// An HTTP error: a status plus a message rendered as `{"error": ...}`.
/// Extra headers (e.g. `X-Bluedb-Watermark` on a freshness 503) can be
/// attached via [`AppError::with_headers`].
#[derive(Debug)]
pub struct AppError {
    status: StatusCode,
    message: String,
    extra_headers: Option<axum::http::HeaderMap>,
}

impl AppError {
    /// Build a plain error without extra headers.
    fn plain(status: StatusCode, message: impl Into<String>) -> Self {
        Self { status, message: message.into(), extra_headers: None }
    }

    /// Attach extra response headers to this error (e.g. `X-Bluedb-Watermark`
    /// on a freshness `503`). Consumes and returns `Self` for chaining.
    pub(crate) fn with_headers(mut self, headers: axum::http::HeaderMap) -> Self {
        self.extra_headers = Some(headers);
        self
    }

    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::plain(StatusCode::BAD_REQUEST, message)
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::plain(StatusCode::INTERNAL_SERVER_ERROR, message)
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::plain(StatusCode::NOT_FOUND, message)
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self::plain(StatusCode::CONFLICT, message)
    }

    pub(crate) fn service_unavailable(message: impl Into<String>) -> Self {
        Self::plain(StatusCode::SERVICE_UNAVAILABLE, message)
    }

    /// `501 Not Implemented` — an optional capability (e.g. digest signing) is
    /// not enabled on this node.
    pub(crate) fn not_implemented(message: impl Into<String>) -> Self {
        Self::plain(StatusCode::NOT_IMPLEMENTED, message)
    }

    /// Map a lease error: another node holds it → `409 Conflict`; otherwise `500`.
    fn from_ha(err: HaError) -> Self {
        match err {
            HaError::LeaseHeldByAnother => Self::plain(
                StatusCode::CONFLICT,
                "cannot promote: the lease is held by another node",
            ),
            HaError::Provider(err) => Self::internal(err.to_string()),
        }
    }
}

impl From<EngineError> for AppError {
    fn from(err: EngineError) -> Self {
        let status = match &err {
            EngineError::Rest(_) | EngineError::Sql(_) | EngineError::Rejected(_) => StatusCode::BAD_REQUEST,
            EngineError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self::plain(status, err.to_string())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let body: BTreeMap<&str, String> = [("error", self.message)].into_iter().collect();
        match self.extra_headers {
            None => (self.status, Json(body)).into_response(),
            Some(extra) => (self.status, extra, Json(body)).into_response(),
        }
    }
}

// Read SELECTs route to the analytical engine (DataFusion); the scan/sort
// guardrail no longer gates the `/sql` read path (it still applies on the
// `GET /tables` REST fast path).
