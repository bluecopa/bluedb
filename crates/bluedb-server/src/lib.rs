//! `bluedb-server` — an HTTP/REST data service over [`bluedb_engine`].
//!
//! A PostgREST-style API: tables are addressed at `/tables/{table}` with
//! `GET`/`POST`/`PATCH`/`DELETE`, filters/order/limit live in the query string
//! (parsed by [`bluedb_rest`]), and bodies are JSON. A raw `/sql` endpoint runs
//! arbitrary SQL (DDL + queries) for setup/admin, `/health` is a liveness probe,
//! and `/admin/{status,promote,demote}` drive the single-writer role.
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
use std::sync::Arc;

use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Map, Value};
use tokio::sync::RwLock;

use bluedb_engine::{rest_sql, EngineError};
use bluedb_ha::{HaError, Status, WriterController};
use bluedb_ledger::Ledger;

mod ledger_api;
use bluedb_rest::{parse_filters, DeleteRequest, InsertRequest, UpdateRequest};
use bluedb_sql::{Database, SlateDbStorage};
use gluesql_core::prelude::{Glue, Payload, Value as SqlValue};
use slatedb::object_store::ObjectStore;
use slatedb::{Db, DbReader};

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
            }),
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
        let db = Db::open(self.inner.db_path.clone(), self.inner.object_store.clone())
            .await
            .map_err(|err| AppError::internal(format!("open writer db: {err}")))?;
        *self.inner.db.write().await = Some(Database::new(Arc::new(db)));
        // Best-effort: ensure the ledger's SQL projection tables exist so
        // `/ledger/*` reads (and `SELECT ... FROM ledger_accounts`) work. A
        // failure here doesn't block promotion — the native ledger API still
        // works (lookups read canonical records, not the projection).
        if let Some(db) = self.inner.db.read().await.as_ref() {
            if let Err(err) = bluedb_ledger::ensure_schema(db).await {
                eprintln!("bluedb-server: ensure ledger schema: {err}");
            }
        }
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

    /// A connection to the currently-bound database, or `503` if unbound.
    async fn connection(&self) -> Result<SlateDbStorage, AppError> {
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(db.connection()),
            None => Err(AppError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "node has no database yet (no writer has been promoted)".to_string(),
            }),
        }
    }

    /// Like [`Self::connection`] but the connection serializes autocommit writes
    /// (see [`bluedb_sql::Database::connection_serialized`]). Used by the routes
    /// that can run a single-statement read-modify-write (`/sql`, `PATCH`,
    /// `DELETE`) so concurrent RMWs can't lose an update.
    async fn connection_serialized(&self) -> Result<SlateDbStorage, AppError> {
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(db.connection_serialized()),
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
    fn require_active(&self) -> Result<(), AppError> {
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
        .route("/ledger/accounts", post(ledger_api::create_accounts))
        .route("/ledger/transfers", post(ledger_api::create_transfers))
        .route("/ledger/accounts/{id}", get(ledger_api::get_account))
        .route("/ledger/transfers/{id}", get(ledger_api::get_transfer))
        .with_state(state)
}

// --- handlers ---------------------------------------------------------------

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// `POST /sql` — run raw SQL (DDL + queries). Gated to the active writer.
async fn exec_sql(State(state): State<AppState>, body: String) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    // Raw SQL may be a read-modify-write (or an explicit transaction); serialize.
    let mut glue = Glue::new(state.connection_serialized().await?);
    let payloads = glue.execute(&body).await.map_err(EngineError::from)?;
    Ok(Json(payloads_to_json(payloads)))
}

/// `GET /tables/{table}?<filters>` — PostgREST SELECT (served by writer or replica).
async fn select(
    State(state): State<AppState>,
    Path(table): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<Json<Value>, AppError> {
    let mut glue = Glue::new(state.connection().await?);
    let payloads = rest_sql::execute_query_str(&mut glue, &table, query.as_deref().unwrap_or("")).await?;
    Ok(Json(payloads_to_json(payloads)))
}

/// `POST /tables/{table}` — INSERT (JSON object or array of objects).
async fn insert(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    let req = build_insert(table, body)?;
    let mut glue = Glue::new(state.connection().await?);
    let payloads = rest_sql::execute_insert(&mut glue, &req).await?;
    Ok(Json(payloads_to_json(payloads)))
}

/// `PATCH /tables/{table}?<filters>` — UPDATE (JSON assignments body).
async fn update(
    State(state): State<AppState>,
    Path(table): Path<String>,
    RawQuery(query): RawQuery,
    Json(assignments): Json<Map<String, Value>>,
) -> Result<Json<Value>, AppError> {
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
    Path(table): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<Json<Value>, AppError> {
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
async fn admin_promote(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    state.promote().await?;
    Ok(Json(status_json(&state.inner.writer.status())))
}

/// `POST /admin/demote` — release the lease + rebind as a read replica.
async fn admin_demote(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
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
fn build_insert(table: String, body: Value) -> Result<InsertRequest, AppError> {
    let objects: Vec<Map<String, Value>> = match body {
        Value::Object(map) => vec![map],
        Value::Array(items) => items
            .into_iter()
            .map(|item| match item {
                Value::Object(map) => Ok(map),
                other => Err(AppError::bad_request(format!(
                    "insert rows must be JSON objects, got {other}"
                ))),
            })
            .collect::<Result<_, _>>()?,
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

    Ok(InsertRequest { table, columns, rows })
}

/// Render a JSON scalar into the DSL string form `bluedb-rest` expects. (Like
/// PostgREST, values are stringly-typed: `render_value` re-types them — numeric
/// text → numeric literal, `true`/`false` → bool, `null` → NULL, else a quoted
/// string.)
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
        let status = match err {
            EngineError::Rest(_) | EngineError::Sql(_) => StatusCode::BAD_REQUEST,
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
