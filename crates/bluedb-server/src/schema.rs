//! Structured `/schema` DDL API — injection-proof typed surface for DDL.
//!
//! Clients describe tables and indexes with JSON; this module validates every
//! identifier and type keyword, builds a DDL string from those safe pieces, then
//! runs it via [`bluedb_engine::rest_sql::execute_sql`] with `allow_arbitrary =
//! true`. No raw SQL is ever accepted from the client.
//!
//! ## Endpoints
//! - `POST   /schema/tables`                          — create table
//! - `DELETE /schema/tables/{table}`                  — drop table
//! - `POST   /schema/tables/{table}/indexes`          — create index
//! - `DELETE /schema/tables/{table}/indexes/{name}`   — drop index
//! - `POST   /schema/tables/{table}/fulltext-indexes` — declare a full-text index
//! - `POST   /schema/tables/{table}/trigram-indexes`  — declare a trigram index
//!
//! All endpoints require the node to be the active writer (503 otherwise).

use axum::extract::{Path, State};
use axum::Json;
use gluesql_core::prelude::Glue;
use serde::Deserialize;
use serde_json::{json, Value};

use bluedb_engine::rest_sql;
use bluedb_rest::validate_ident;

use crate::{authz::Scope, AppError, AppState};

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// A single column definition in a `CreateTableRequest`.
#[derive(Deserialize)]
pub(crate) struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    // Accept the REST-natural camelCase `primaryKey` (what clients and our own
    // tests send) plus the snake_case form. Before this, `primaryKey` was
    // silently dropped → no PK → a follow-up CREATE FULLTEXT INDEX (which needs
    // a PK) would 400 confusingly.
    #[serde(default, rename = "primaryKey", alias = "primary_key")]
    pub primary_key: bool,
    /// Defaults to `true` (nullable) unless the caller says `false`.
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    #[serde(default)]
    pub unique: bool,
}

fn default_nullable() -> bool {
    true
}

/// Body for `POST /schema/tables`.
#[derive(Deserialize)]
pub(crate) struct CreateTableRequest {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

/// Body for `POST /schema/tables/{table}/indexes`.
#[derive(Deserialize)]
pub(crate) struct CreateIndexRequest {
    pub name: String,
    pub columns: Vec<String>,
}

/// Body for `POST /schema/tables/{table}/fulltext-indexes`. The table's primary
/// key is auto-resolved from its schema; the caller names only the text column
/// and (optionally) the analyzer.
#[derive(Deserialize)]
pub(crate) struct CreateFulltextIndexRequest {
    pub column: String,
    #[serde(default = "default_analyzer")]
    pub analyzer: String,
}

fn default_analyzer() -> String {
    "english".into()
}

/// Body for `POST /schema/tables/{table}/trigram-indexes`. Like the fulltext
/// variant, the table's primary key is auto-resolved; the caller names only the
/// text column. A trigram index always tokenizes through the `whitespace`
/// analyzer (it stores pre-trigramized text), so there is no analyzer field.
#[derive(Deserialize)]
pub(crate) struct CreateTrigramIndexRequest {
    pub column: String,
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/// Validate a SQL type keyword. Accepts a case-insensitive allow-list; returns
/// the canonical uppercase keyword or a 400 `AppError`. No user string ever
/// reaches the DDL unchanged — only one of the literals below can.
fn validate_type(ty: &str) -> Result<&'static str, AppError> {
    match ty.to_ascii_uppercase().as_str() {
        "TEXT" => Ok("TEXT"),
        "INTEGER" | "INT" => Ok("INTEGER"),
        "BOOLEAN" | "BOOL" => Ok("BOOLEAN"),
        "FLOAT" => Ok("FLOAT"),
        "DECIMAL" => Ok("DECIMAL"),
        "DATE" => Ok("DATE"),
        "TIME" => Ok("TIME"),
        "TIMESTAMP" => Ok("TIMESTAMP"),
        "UUID" => Ok("UUID"),
        other => Err(AppError::bad_request(format!("unsupported column type '{other}'"))),
    }
}

/// Wrap [`validate_ident`] to return an [`AppError`] (400) on rejection.
fn ident(name: &str) -> Result<&str, AppError> {
    validate_ident(name).map_err(|_| AppError::bad_request(format!("invalid identifier '{name}'")))
}

// ---------------------------------------------------------------------------
// DDL execution helper
// ---------------------------------------------------------------------------

async fn run_ddl(state: &AppState, tenant: &str, sql: String) -> Result<(), AppError> {
    state.require_active()?;
    let mut glue = Glue::new(state.connection_serialized(tenant).await?);
    rest_sql::execute_sql(&mut glue, &sql, &[], true).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `POST /schema/tables` — create a table from a typed column specification.
pub(crate) async fn create_table(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<CreateTableRequest>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    let table = ident(&req.name)?.to_string();

    if req.columns.is_empty() {
        return Err(AppError::bad_request("columns must not be empty"));
    }

    let mut col_defs = Vec::with_capacity(req.columns.len());
    for col in &req.columns {
        let col_name = ident(&col.name)?.to_string();
        let col_type = validate_type(&col.ty)?;
        let mut def = format!("{col_name} {col_type}");
        if col.primary_key {
            def.push_str(" PRIMARY KEY");
        } else {
            if col.unique {
                def.push_str(" UNIQUE");
            }
            if !col.nullable {
                def.push_str(" NOT NULL");
            }
        }
        col_defs.push(def);
    }

    let sql = format!("CREATE TABLE {table} ({});", col_defs.join(", "));
    run_ddl(&state, &tenant, sql).await?;
    Ok(Json(json!({ "created": true, "table": table })))
}

/// `DELETE /schema/tables/{table}` — drop a table.
pub(crate) async fn drop_table(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    let table = ident(&table)?.to_string();
    let sql = format!("DROP TABLE {table};");
    run_ddl(&state, &tenant, sql).await?;
    Ok(Json(json!({ "dropped": true, "table": table })))
}

/// `POST /schema/tables/{table}/indexes` — create an index on an existing table.
pub(crate) async fn create_index(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
    Json(req): Json<CreateIndexRequest>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    let table = ident(&table)?.to_string();
    let index_name = ident(&req.name)?.to_string();

    if req.columns.is_empty() {
        return Err(AppError::bad_request("index columns must not be empty"));
    }
    let cols: Vec<&str> = req
        .columns
        .iter()
        .map(|c| ident(c))
        .collect::<Result<Vec<_>, _>>()?;

    let sql = format!("CREATE INDEX {index_name} ON {table} ({});", cols.join(", "));
    run_ddl(&state, &tenant, sql).await?;
    Ok(Json(json!({ "created_index": true, "name": index_name, "table": table })))
}

/// `POST /schema/tables/{table}/fulltext-indexes` — declare a fulltext index on
/// a text column (Spec B §4.1). The table's integer primary key is resolved from
/// its schema; once declared, `/sql` rewrites `@@`/`ts_rank` over the live index.
pub(crate) async fn create_fulltext_index(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
    Json(req): Json<CreateFulltextIndexRequest>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;
    let table = ident(&table)?.to_string();
    let column = ident(&req.column)?.to_string();
    let conn = state.connection(&tenant).await?;
    state
        .fts()
        .await
        .create_fulltext_index_auto(&conn, &table, &column, &req.analyzer)
        .await?;
    Ok(Json(json!({
        "created_fulltext_index": column,
        "on": table,
        "analyzer": req.analyzer,
    })))
}

/// `POST /schema/tables/{table}/trigram-indexes` — declare a trigram index on a
/// text column (Spec B §4.6). The table's integer primary key is resolved from
/// its schema; once declared, `/sql` accelerates `col LIKE '%lit%'` over it (a
/// `pk IN (...)` prefilter with gluesql's `LIKE` kept as the exact verify).
pub(crate) async fn create_trigram_index(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
    Json(req): Json<CreateTrigramIndexRequest>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;
    let table = ident(&table)?.to_string();
    let column = ident(&req.column)?.to_string();
    let conn = state.connection(&tenant).await?;
    state
        .fts()
        .await
        .create_trigram_index_auto(&conn, &table, &column)
        .await?;
    Ok(Json(json!({
        "created_trigram_index": column,
        "on": table,
    })))
}

/// `DELETE /schema/tables/{table}/indexes/{name}` — drop an index.
pub(crate) async fn drop_index(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path((table, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    let table = ident(&table)?.to_string();
    let index_name = ident(&name)?.to_string();
    // GlueSQL DROP INDEX uses the table-qualified form: DROP INDEX table.index_name
    let sql = format!("DROP INDEX {table}.{index_name};");
    run_ddl(&state, &tenant, sql).await?;
    Ok(Json(json!({ "dropped_index": true, "name": index_name, "table": table })))
}
