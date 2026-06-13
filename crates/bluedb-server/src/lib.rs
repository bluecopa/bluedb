//! `bluedb-server` — an HTTP/REST data service over [`bluedb_engine`].
//!
//! A PostgREST-style API: tables are addressed at `/tables/{table}` with
//! `GET`/`POST`/`PATCH`/`DELETE`, filters/order/limit live in the query string
//! (parsed by [`bluedb_rest`]), and bodies are JSON. A raw `/sql` endpoint runs
//! arbitrary SQL (DDL + queries) for setup/admin, and `/health` is a liveness
//! probe.
//!
//! Each request borrows a fresh connection from a shared [`Database`], so
//! requests are isolated and their write transactions serialize on the engine's
//! write lease (see `bluedb_sql`'s isolation model).
//!
//! The router is built by [`build_app`] from an [`AppState`]; `main` only opens
//! the database and serves it. Tests drive [`build_app`] directly via
//! `tower::ServiceExt::oneshot` — no socket required.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Map, Value};

use bluedb_engine::{rest_sql, EngineError};
use bluedb_ha::{HaError, Status, WriterController};
use bluedb_rest::{parse_filters, DeleteRequest, InsertRequest, UpdateRequest};
use bluedb_sql::{Database, SlateDbStorage};
use gluesql_core::prelude::{Glue, Payload, Value as SqlValue};

/// Shared service state: the database connections are drawn from, plus the
/// single-writer [`WriterController`] that gates mutations.
#[derive(Clone)]
pub struct AppState {
    db: Database,
    writer: Arc<WriterController>,
}

impl AppState {
    /// Wrap a [`Database`] and the node's [`WriterController`] for serving.
    pub fn new(db: Database, writer: Arc<WriterController>) -> Self {
        Self { db, writer }
    }

    fn glue(&self) -> Glue<SlateDbStorage> {
        Glue::new(self.db.connection())
    }

    /// Reject a mutating request unless this node is the active writer. Reads
    /// never call this — standby (Passive) nodes serve reads, only the elected
    /// writer mutates (single-writer safety; see [`bluedb_ha`]).
    fn require_active(&self) -> Result<(), AppError> {
        if self.writer.is_active() {
            Ok(())
        } else {
            Err(AppError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: format!("node '{}' is passive (not the active writer)", self.writer.node_id()),
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
        .with_state(state)
}

// --- handlers ---------------------------------------------------------------

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// `POST /sql` — run raw SQL (DDL + queries). Body is the SQL text. Gated to the
/// active writer (it can mutate; SELECT-only callers should use `GET /tables`).
async fn exec_sql(State(state): State<AppState>, body: String) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    let payloads = state.glue().execute(&body).await.map_err(EngineError::from)?;
    Ok(Json(payloads_to_json(payloads)))
}

/// `GET /tables/{table}?<filters>` — PostgREST SELECT.
async fn select(
    State(state): State<AppState>,
    Path(table): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<Json<Value>, AppError> {
    let mut glue = state.glue();
    let payloads = rest_sql::execute_query_str(&mut glue, &table, query.as_deref().unwrap_or("")).await?;
    Ok(Json(payloads_to_json(payloads)))
}

/// `POST /tables/{table}` — INSERT. Body is a JSON object or an array of
/// objects; each object's keys are the columns.
async fn insert(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    let req = build_insert(table, body)?;
    let mut glue = state.glue();
    let payloads = rest_sql::execute_insert(&mut glue, &req).await?;
    Ok(Json(payloads_to_json(payloads)))
}

/// `PATCH /tables/{table}?<filters>` — UPDATE. Body is a JSON object of
/// `column: value` assignments.
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
    let mut glue = state.glue();
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
    let mut glue = state.glue();
    let payloads = rest_sql::execute_delete(&mut glue, &req).await?;
    Ok(Json(payloads_to_json(payloads)))
}

// --- admin / high-availability control --------------------------------------

/// `GET /admin/status` — this node's writer role (`active`/`passive`), fencing
/// epoch, and lease expiry.
async fn admin_status(State(state): State<AppState>) -> Json<Value> {
    Json(status_json(&state.writer.status()))
}

/// `POST /admin/promote` — try to become the active writer (acquire the lease).
/// `409 Conflict` if another node holds it.
async fn admin_promote(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    match state.writer.promote().await {
        Ok(_) => Ok(Json(status_json(&state.writer.status()))),
        Err(HaError::LeaseHeldByAnother) => Err(AppError {
            status: StatusCode::CONFLICT,
            message: "cannot promote: the lease is held by another node".to_string(),
        }),
        Err(HaError::Provider(err)) => Err(AppError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: err.to_string(),
        }),
    }
}

/// `POST /admin/demote` — step down to passive (release the lease).
async fn admin_demote(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    state.writer.demote().await.map_err(|err| AppError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: err.to_string(),
    })?;
    Ok(Json(status_json(&state.writer.status())))
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
/// string. So a JSON string that *looks* numeric is treated as numeric.)
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
/// (decimal/date/uuid/list/map/...) fall back to their debug rendering as a
/// string rather than GlueSQL's tagged serde form.
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
        SqlValue::Str(s) => Value::String(s.clone()),
        other => Value::String(format!("{other:?}")),
    }
}

// --- errors -----------------------------------------------------------------

/// An HTTP error: a status plus a message rendered as `{"error": ...}`.
pub struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
}

impl From<EngineError> for AppError {
    fn from(err: EngineError) -> Self {
        let status = match err {
            // Client errors: a malformed DSL request or a rejected SQL statement.
            EngineError::Rest(_) | EngineError::Sql(_) => StatusCode::BAD_REQUEST,
            // Infrastructure (blob I/O, etc.).
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
