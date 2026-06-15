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
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::{json, Map, Value};
use tokio::sync::RwLock;

pub mod authz;
pub mod objstore;
mod schema;

use bluedb_engine::{rest_sql, EngineError, FtsEngine};
use bluedb_ha::{HaError, Status, WriterController};
use bluedb_ledger::Ledger;

mod ledger_api;
use bluedb_rest::{parse_filters, DeleteRequest, InsertRequest, UpdateRequest};
use bluedb_sql::{Database, SlateDbStorage};
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

/// SlateDB `Settings` for the writer `Db`: bluedb's `flush_interval` default,
/// env-overridable, everything else at SlateDB defaults.
fn writer_settings() -> Settings {
    let mut settings = Settings::default();
    settings.flush_interval = Some(parse_flush_interval_ms(
        std::env::var("BLUEDB_FLUSH_INTERVAL_MS").ok().as_deref(),
    ));
    settings
}

/// Shared service state. Cheap to clone (an `Arc` to the inner state).
#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    /// Object store + SlateDB path the writer/reader open against.
    object_store: Arc<dyn ObjectStore>,
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
                object_store,
                db_path: db_path.into(),
                writer,
                db: RwLock::new(None),
                admin_sql_enabled: AtomicBool::new(false),
                authz: OnceLock::new(),
                fts: RwLock::new(FtsEngine::new()),
                seal_handle: Mutex::new(None),
            }),
        }
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
        let token = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if authz.allows(token, required) {
            Ok(())
        } else if token.is_none() {
            Err(AppError { status: StatusCode::UNAUTHORIZED, message: "missing or malformed bearer token".into() })
        } else {
            Err(AppError { status: StatusCode::FORBIDDEN, message: "insufficient scope".into() })
        }
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
    async fn connection(&self) -> Result<SlateDbStorage, AppError> {
        let fts = self.inner.fts.read().await.clone();
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(db.connection_guarded().with_commit_observer(fts)),
            None => Err(AppError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "node has no database yet (no writer has been promoted)".to_string(),
            }),
        }
    }

    /// Like [`Self::connection`] but the connection also serializes autocommit
    /// writes (see [`bluedb_sql::Database::connection_serialized_guarded`]). Used
    /// by the routes that can run a single-statement read-modify-write (`/sql`,
    /// `/admin/sql`, `PATCH`, `DELETE`) so concurrent RMWs can't lose an update.
    /// Guarded for the same reason as [`Self::connection`].
    pub(crate) async fn connection_serialized(&self) -> Result<SlateDbStorage, AppError> {
        let fts = self.inner.fts.read().await.clone();
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(db
                .connection_serialized_guarded()
                .with_commit_observer(fts)),
            None => Err(AppError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "node has no database yet (no writer has been promoted)".to_string(),
            }),
        }
    }

    /// Build a [`Ledger`] over the currently-bound database, or `503` if the
    /// node has no database yet. The `Database` is cheap to clone (`Arc`-based)
    /// and [`Ledger::new`] captures owned handles, so the returned ledger
    /// outlives the role lock — like the SQL connection path.
    async fn ledger(&self) -> Result<Ledger, AppError> {
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(Ledger::new(db)),
            None => Err(AppError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "node has no database yet (no writer has been promoted)".to_string(),
            }),
        }
    }

    /// Reject a mutating request unless this node is the active writer.
    pub(crate) fn require_active(&self) -> Result<(), AppError> {
        if self.inner.writer.is_active() {
            Ok(())
        } else {
            Err(AppError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: format!(
                    "node '{}' is passive (not the active writer)",
                    self.inner.writer.node_id()
                ),
            })
        }
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
        other => return Err(AppError::bad_request(format!("param must be a JSON scalar, got {other}"))),
    })
}

// --- handlers ---------------------------------------------------------------

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// `POST /sql` — one parameterized non-DDL statement (SELECT/INSERT/UPDATE/DELETE).
async fn exec_sql(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<SqlRequest>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataQuery)?;
    state.require_active()?;
    let params = req.params.iter().map(json_to_param).collect::<Result<Vec<_>, _>>()?;
    let mut glue = Glue::new(state.connection_serialized().await?);
    // Route through the FTS engine: it rewrites `@@`/`ts_rank` against the live
    // segment when a fulltext index is declared, else runs the SQL unchanged.
    let payloads = state.fts().await.execute_fts(&mut glue, &req.sql, &params).await?;
    Ok(Json(payloads_to_json(payloads)))
}

/// `POST /admin/sql` — arbitrary SQL (DDL/txns/multi). Off by default; audited.
async fn admin_sql(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<SqlRequest>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::Superuser)?;
    if !state.inner.admin_sql_enabled.load(Ordering::Relaxed) {
        return Err(AppError {
            status: StatusCode::NOT_FOUND,
            message: "admin SQL endpoint is disabled".to_string(),
        });
    }
    state.require_active()?;
    eprintln!("bluedb-audit: /admin/sql executed: {}", req.sql);
    let params = req.params.iter().map(json_to_param).collect::<Result<Vec<_>, _>>()?;
    let mut glue = Glue::new(state.connection_serialized().await?);
    let payloads = rest_sql::execute_sql(&mut glue, &req.sql, &params, true).await?;
    Ok(Json(payloads_to_json(payloads)))
}

/// `GET /tables/{table}?<filters>` — PostgREST SELECT (served by writer or replica).
async fn select(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    let mut glue = Glue::new(state.connection().await?);
    let payloads = rest_sql::execute_query_str(&mut glue, &table, query.as_deref().unwrap_or("")).await?;
    Ok(Json(payloads_to_json(payloads)))
}

/// `POST /tables/{table}` — INSERT (JSON object → autocommit; array → one txn batch).
async fn insert(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataWrite)?;
    state.require_active()?;
    let (req, is_batch) = build_insert(table, body)?;
    if is_batch {
        // Multi-row: atomic BEGIN..COMMIT on the serialized (lease-holding) connection.
        let mut glue = Glue::new(state.connection_serialized().await?);
        let payloads = rest_sql::execute_insert_batch(&mut glue, &req).await?;
        // Fold all per-row Insert(n) payloads from the transaction into one count.
        let total: usize = payloads
            .iter()
            .filter_map(|p| if let Payload::Insert(n) = p { Some(*n) } else { None })
            .sum();
        Ok(Json(json!({ "inserted": total })))
    } else {
        // Single row: autocommit on the group-commit connection (concurrent fast path).
        let mut glue = Glue::new(state.connection().await?);
        let payloads = rest_sql::execute_insert(&mut glue, &req).await?;
        Ok(Json(payloads_to_json(payloads)))
    }
}

/// `PATCH /tables/{table}?<filters>` — UPDATE (JSON assignments body).
async fn update(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
    RawQuery(query): RawQuery,
    Json(assignments): Json<Map<String, Value>>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataWrite)?;
    state.require_active()?;
    let filters = parse_filters(query.as_deref().unwrap_or("")).map_err(EngineError::from)?;
    let assignments = assignments
        .into_iter()
        .map(|(col, value)| Ok((col, json_scalar_to_dsl(&value)?)))
        .collect::<Result<Vec<_>, AppError>>()?;
    let req = UpdateRequest { table, assignments, filters };
    // UPDATE is a read-modify-write; serialize so concurrent ones can't lose.
    let mut glue = Glue::new(state.connection_serialized().await?);
    let payloads = rest_sql::execute_update(&mut glue, &req).await?;
    Ok(Json(payloads_to_json(payloads)))
}

/// `DELETE /tables/{table}?<filters>` — DELETE.
async fn delete_rows(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataWrite)?;
    state.require_active()?;
    let filters = parse_filters(query.as_deref().unwrap_or("")).map_err(EngineError::from)?;
    let req = DeleteRequest { table, filters };
    // DELETE reads the rows it removes; serialize for the same reason as UPDATE.
    let mut glue = Glue::new(state.connection_serialized().await?);
    let payloads = rest_sql::execute_delete(&mut glue, &req).await?;
    Ok(Json(payloads_to_json(payloads)))
}

// --- admin / high-availability control --------------------------------------

/// `GET /admin/status` — this node's writer role, fencing epoch, lease expiry.
async fn admin_status(State(state): State<AppState>) -> Json<Value> {
    Json(status_json(&state.inner.writer.status()))
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

/// Render a JSON scalar into the DSL string form `bluedb-rest` expects. (Like
/// PostgREST, values are stringly-typed on the wire: the engine later types them
/// into typed `$N` parameters — numeric text → Int/Float, `true`/`false` → Bool,
/// `null` → Null, everything else → Str.)
fn json_scalar_to_dsl(value: &Value) -> Result<String, AppError> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Null => Ok("null".to_string()),
        other => Err(AppError::bad_request(format!("expected a scalar value, got {other}"))),
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
        other => Value::String(format!("{other:?}")),
    }
}

// --- tests ------------------------------------------------------------------

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
#[derive(Debug)]
pub struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    /// Map a lease error: another node holds it → `409 Conflict`; otherwise `500`.
    fn from_ha(err: HaError) -> Self {
        match err {
            HaError::LeaseHeldByAnother => Self {
                status: StatusCode::CONFLICT,
                message: "cannot promote: the lease is held by another node".to_string(),
            },
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
        Self {
            status,
            message: err.to_string(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let body: BTreeMap<&str, String> = [("error", self.message)].into_iter().collect();
        (self.status, Json(body)).into_response()
    }
}
