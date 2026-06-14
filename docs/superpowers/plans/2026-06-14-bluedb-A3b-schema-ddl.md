# bluedb A3b — structured `/schema` DDL API Implementation Plan

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** A typed JSON DDL surface so schema changes don't need raw SQL: create/drop tables and indexes via validated JSON that compiles to allow-listed DDL (no SQL authorship by the client → injection-proof). This is the DDL tier of Spec A's four-surface model.

**Architecture:** A new `crates/bluedb-server/src/schema.rs` module: request types + a DDL compiler that validates identifiers (`bluedb_rest::validate_ident`) and column types (a fixed allow-list), builds the DDL string, and runs it via the engine's `rest_sql::execute_sql(.., allow_arbitrary=true)` (DDL has no bound values — safety is from ident + type-keyword allow-listing). Gated by `require_active()` (writer). Fine-grained `schema:admin` scope arrives in A3c.

**Tech Stack:** Rust, axum, serde, gluesql (CREATE/DROP TABLE/INDEX). **Introspection (GET /schema/tables) is deferred** — the `Metadata`/`GLUE_OBJECTS` backing is the empty default on this branch.

**Scope:** create table, drop table, create index, drop index. NOT: alter table, introspection, multi-column index correctness guarantees (gluesql index is effectively single-column; accept a column list but document single-column is the supported case).

---

## Task 1: `/schema` DDL endpoints

**Files:** create `crates/bluedb-server/src/schema.rs`; wire routes + module in `crates/bluedb-server/src/lib.rs`; tests in `crates/bluedb-server/tests/schema.rs`.

- [ ] **Step 1: failing integration test.** Create `crates/bluedb-server/tests/schema.rs` modeled on `tests/api.rs`'s harness (reuse its `make_app`/request helpers — read api.rs first). Cover, over HTTP:
  - `POST /schema/tables` `{ "name":"docs", "columns":[{"name":"id","type":"INTEGER","primaryKey":true},{"name":"body","type":"TEXT"}] }` → 2xx; then a row can be inserted (`POST /tables/docs {"id":1,"body":"x"}`) and read back.
  - `POST /schema/tables/docs/indexes` `{ "name":"idx_body", "columns":["body"] }` → 2xx.
  - `DELETE /schema/tables/docs/indexes/idx_body` → 2xx.
  - `DELETE /schema/tables/docs` → 2xx; subsequent `GET /tables/docs` errors (table gone).
  - **Injection/validation:** `POST /schema/tables` with `name` = `"x; DROP TABLE y"` or a column `type` = `"TEXT); DROP TABLE y; --"` → 400 (rejected by ident/type validation, nothing executed).
  Run `cargo test -p bluedb-server --test schema` → FAIL (routes 404 / module missing).

- [ ] **Step 2: implement `schema.rs`.** Request types + compiler + handlers:

```rust
//! Structured DDL surface (`/schema/...`): typed JSON → validated, allow-listed
//! DDL. The client authors no SQL; identifiers are `validate_ident`-checked and
//! column types are matched against a fixed allow-list, so there is no injection
//! vector. DDL runs via `rest_sql::execute_sql(.., allow_arbitrary=true)`.
use axum::{extract::{Path, State}, Json};
use bluedb_rest::validate_ident;
use gluesql_core::prelude::Glue;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{AppError, AppState};

#[derive(Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub unique: bool,
}

#[derive(Deserialize)]
pub struct CreateTableRequest {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Deserialize)]
pub struct CreateIndexRequest {
    pub name: String,
    pub columns: Vec<String>,
}

/// Allow-listed column types (case-insensitive). Anything else → 400.
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
        other => Err(AppError::bad_request(format!("unsupported column type: {other}"))),
    }
}

fn ident(s: &str) -> Result<&str, AppError> {
    validate_ident(s).map_err(|_| AppError::bad_request(format!("invalid identifier: {s}")))
}

/// Run a fully-validated DDL string through the (arbitrary-allowed) engine path.
/// The string is built ONLY from allow-listed idents + type keywords — no user
/// value is interpolated.
async fn run_ddl(state: &AppState, sql: String) -> Result<(), AppError> {
    state.require_active()?;
    let mut glue = Glue::new(state.connection_serialized().await?);
    crate::rest_sql_execute_arbitrary(&mut glue, &sql).await?; // thin wrapper, see lib.rs note
    Ok(())
}

pub async fn create_table(
    State(state): State<AppState>,
    Json(req): Json<CreateTableRequest>,
) -> Result<Json<Value>, AppError> {
    let table = ident(&req.name)?;
    if req.columns.is_empty() {
        return Err(AppError::bad_request("table needs at least one column"));
    }
    let mut cols = Vec::with_capacity(req.columns.len());
    for c in &req.columns {
        let name = ident(&c.name)?;
        let ty = validate_type(&c.ty)?;
        let mut def = format!("{name} {ty}");
        if c.primary_key { def.push_str(" PRIMARY KEY"); }
        if c.unique { def.push_str(" UNIQUE"); }
        if !c.nullable && !c.primary_key { def.push_str(" NOT NULL"); }
        cols.push(def);
    }
    run_ddl(&state, format!("CREATE TABLE {table} ({});", cols.join(", "))).await?;
    Ok(Json(json!({ "created": table })))
}

pub async fn drop_table(
    State(state): State<AppState>,
    Path(table): Path<String>,
) -> Result<Json<Value>, AppError> {
    let table = ident(&table)?;
    run_ddl(&state, format!("DROP TABLE {table};")).await?;
    Ok(Json(json!({ "dropped": table })))
}

pub async fn create_index(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Json(req): Json<CreateIndexRequest>,
) -> Result<Json<Value>, AppError> {
    let table = ident(&table)?;
    let name = ident(&req.name)?;
    if req.columns.is_empty() {
        return Err(AppError::bad_request("index needs at least one column"));
    }
    let cols = req.columns.iter().map(|c| ident(c)).collect::<Result<Vec<_>, _>>()?;
    run_ddl(&state, format!("CREATE INDEX {name} ON {table} ({});", cols.join(", "))).await?;
    Ok(Json(json!({ "created_index": name, "on": table })))
}

pub async fn drop_index(
    State(state): State<AppState>,
    Path((table, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let table = ident(&table)?;
    let name = ident(&name)?;
    // gluesql DROP INDEX is table-qualified: `DROP INDEX <table>.<index>;`
    run_ddl(&state, format!("DROP INDEX {table}.{name};")).await?;
    Ok(Json(json!({ "dropped_index": name, "on": table })))
}
```

- [ ] **Step 3: wire it in `lib.rs`.** Add `mod schema;` (or `pub mod schema;`). In `build_app`, add:
```rust
        .route("/schema/tables", post(schema::create_table))
        .route("/schema/tables/{table}", delete(schema::drop_table))
        .route("/schema/tables/{table}/indexes", post(schema::create_index))
        .route("/schema/tables/{table}/indexes/{name}", delete(schema::drop_index))
```
(Match axum 0.8 path-param syntax used elsewhere in this file — it uses `{table}` style per the existing `/tables/{table}` route.) Add a small helper that `schema.rs` calls for arbitrary DDL execution, OR have `schema.rs` call `bluedb_engine::rest_sql::execute_sql(&mut glue, &sql, &[], true)` directly (preferred — drop the `rest_sql_execute_arbitrary` indirection in the draft above and call `rest_sql::execute_sql(.., true)` inline in `run_ddl`). Ensure `AppError`/`AppState`/`AppError::bad_request`/`require_active`/`connection_serialized` are reachable from `schema.rs` (make them `pub(crate)` if needed).

- [ ] **Step 4: run.** `cargo test -p bluedb-server --test schema` (all pass, incl. the injection-rejection case) + `cargo test -p bluedb-server` (all pass) + `cargo build -p bluedb-server 2>&1 | grep -i warn` (clean).

- [ ] **Step 5: doc + commit.** Add a `//!` note in `main.rs` listing the `/schema/...` DDL endpoints. Commit:
```bash
git add crates/bluedb-server
git commit -m "feat(server): structured /schema DDL API (create/drop table + index, validated)"
```

## Self-Review
- Spec A DDL surface: typed JSON → validated DDL ✓; idents allow-listed ✓; types allow-listed ✓ (injection-proof — test proves rejection); create/drop table + index ✓. Introspection deferred (documented — no metadata backing on this branch). Fine-grained `schema:admin` scope is A3c (here gated by `require_active`).
- Placeholder scan: resolve the `rest_sql_execute_arbitrary` indirection to a direct `rest_sql::execute_sql(.., true)` call. Type consistency: handlers return `Json<Value>`; `validate_type`/`ident` return `Result<_, AppError>`.
