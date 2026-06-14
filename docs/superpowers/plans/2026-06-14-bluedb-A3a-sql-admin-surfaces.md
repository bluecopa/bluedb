# bluedb A3a — SQL + Admin surfaces Implementation Plan

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Split the raw-SQL endpoint by capability: `POST /sql` becomes a **parameterized, single non-DDL statement** surface (`{sql, params}`); `POST /admin/sql` is the **arbitrary** SQL escape hatch, **off by default** and audited. Closes today's unauthenticated-arbitrary-`/sql` exposure.

**Architecture:** A new engine fn `rest_sql::execute_sql(glue, sql, params, allow_arbitrary)` binds `$N` params via `execute_with_params`; when `!allow_arbitrary` it parses+translates and requires exactly one `Query/Insert/Update/Delete` statement (rejects DDL/transactions/multi-statement). The server adds a `{sql, params}` request type + JSON→`Param` mapping, replaces the old raw `/sql`, and adds `/admin/sql` gated by an `AppState.admin_sql_enabled` flag (from `BLUEDB_ENABLE_ADMIN_SQL`, default false).

**Tech Stack:** Rust, gluesql-core 0.19 (`parse`/`translate`/`ast::Statement`, `execute_with_params`), axum, serde_json.

---

## Task 1: engine `execute_sql` (param + single-DML guard)

**Files:** `crates/bluedb-engine/src/rest_sql.rs`, `crates/bluedb-engine/src/error.rs`, test `crates/bluedb-engine/tests/sql_surface.rs`.

- [ ] **Step 1: failing test.** Create `crates/bluedb-engine/tests/sql_surface.rs`:
```rust
use std::sync::Arc;
use bluedb_engine::rest_sql;
use bluedb_rest::Param;
use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn glue() -> Glue<SlateDbStorage> {
    let db = Db::open("t", Arc::new(InMemory::new())).await.unwrap();
    let mut g = Glue::new(SlateDbStorage::new(Arc::new(db)));
    g.execute("CREATE TABLE t (id INTEGER, body TEXT);").await.unwrap();
    g.execute("INSERT INTO t (id, body) VALUES (1, 'a'), (2, 'b');").await.unwrap();
    g
}

#[tokio::test]
async fn sql_surface_runs_single_param_select() {
    let mut g = glue().await;
    let out = rest_sql::execute_sql(&mut g, "SELECT body FROM t WHERE id = $1", &[Param::Int(2)], false).await.unwrap();
    match out.into_iter().next().unwrap() {
        Payload::Select { rows, .. } => assert_eq!(rows[0][0], Value::Str("b".into())),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn sql_surface_rejects_ddl() {
    let mut g = glue().await;
    let err = rest_sql::execute_sql(&mut g, "CREATE TABLE x (a INTEGER)", &[], false).await;
    assert!(err.is_err(), "DDL must be rejected on the /sql surface");
}

#[tokio::test]
async fn sql_surface_rejects_multi_statement() {
    let mut g = glue().await;
    let err = rest_sql::execute_sql(&mut g, "SELECT 1; SELECT 2;", &[], false).await;
    assert!(err.is_err(), "multi-statement must be rejected on the /sql surface");
}

#[tokio::test]
async fn admin_allows_ddl_when_arbitrary() {
    let mut g = glue().await;
    rest_sql::execute_sql(&mut g, "CREATE TABLE x (a INTEGER)", &[], true).await.unwrap();
}
```
Run `cargo test -p bluedb-engine --test sql_surface` → FAIL (no `execute_sql`).

- [ ] **Step 2: add an error variant.** In `crates/bluedb-engine/src/error.rs`, add a variant to `EngineError` for a rejected statement, e.g.:
```rust
    /// A statement was rejected by a restricted surface (e.g. DDL/multi-statement on `/sql`).
    #[error("statement not allowed on this surface: {0}")]
    Rejected(String),
```
(Match the existing `thiserror`/`#[error(...)]` style in that file. If `EngineError` isn't `thiserror`-based, add the variant in the file's existing idiom and ensure it maps to a 4xx at the server boundary — see Task 2.)

- [ ] **Step 3: implement `execute_sql`.** Add to `crates/bluedb-engine/src/rest_sql.rs`:
```rust
use crate::error::EngineError;
use gluesql_core::ast::Statement;

/// Execute a `{sql, params}` request. Values bind as `$N` (never interpolated).
///
/// When `allow_arbitrary` is false (the `/sql` surface) the SQL must be exactly
/// ONE `SELECT`/`INSERT`/`UPDATE`/`DELETE` statement — DDL, transactions, and
/// multi-statement are rejected (`EngineError::Rejected`). When true (the
/// `/admin/sql` surface) anything goes.
pub async fn execute_sql(
    glue: &mut Glue<SlateDbStorage>,
    sql: &str,
    params: &[Param],
    allow_arbitrary: bool,
) -> Result<Vec<Payload>> {
    if !allow_arbitrary {
        let parsed = gluesql_core::parse(sql).map_err(|e| EngineError::Rejected(e.to_string()))?;
        if parsed.len() != 1 {
            return Err(EngineError::Rejected(format!(
                "exactly one statement required, got {}",
                parsed.len()
            )));
        }
        let stmt = gluesql_core::translate(&parsed[0]).map_err(|e| EngineError::Rejected(e.to_string()))?;
        let is_dml = matches!(
            stmt,
            Statement::Query(_) | Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. }
        );
        if !is_dml {
            return Err(EngineError::Rejected(
                "only SELECT/INSERT/UPDATE/DELETE allowed on /sql; use /admin/sql for DDL".to_string(),
            ));
        }
    }
    Ok(glue.execute_with_params(sql, literals(params)).await?)
}
```
(`literals` is the existing private helper from the A1 work. `gluesql_core::{parse, translate}` and `gluesql_core::ast::Statement` are public.)

- [ ] **Step 4: run** `cargo test -p bluedb-engine --test sql_surface` (4 pass) + `cargo test -p bluedb-engine` (all pass) + `cargo build -p bluedb-engine 2>&1 | grep -i warn` (clean).

- [ ] **Step 5: commit**
```bash
git add crates/bluedb-engine/src/rest_sql.rs crates/bluedb-engine/src/error.rs crates/bluedb-engine/tests/sql_surface.rs
git commit -m "feat(engine): execute_sql — parameterized, single-DML guarded /sql + arbitrary admin"
```

---

## Task 2: server `/sql` (`{sql, params}`) + `/admin/sql` (flag-gated, audited)

**Files:** `crates/bluedb-server/src/lib.rs` (+ `main.rs` for the flag + doc), and migrate `crates/bluedb-server/tests/*.rs` that used the old raw `/sql`.

- [ ] **Step 1: study the existing tests.** Read `crates/bluedb-server/tests/api.rs` (and `tests/http2.rs`). Note every use of `POST /sql` — the ones doing DDL setup (`CREATE TABLE ...`) must move to `/admin/sql` (with the admin flag enabled on the test's `AppState`); data queries move to `/sql` with a `{sql, params}` JSON body. This is the migration you'll perform in Step 5.

- [ ] **Step 2: AppState admin flag.** Add a `bool` to `AppState` (e.g. `admin_sql_enabled`), defaulting `false`. Provide a way to set it: extend `AppState::new` with the flag, OR add a `pub fn with_admin_sql(self, enabled: bool) -> Self` builder. `main.rs` sets it from `std::env::var("BLUEDB_ENABLE_ADMIN_SQL").is_ok()` (presence = on; or `== "1"`/`"true"`). Tests set it `true`. Keep the change minimal and update all `AppState::new` call sites (main.rs + tests).

- [ ] **Step 3: request type + JSON→Param.** Add to `lib.rs`:
```rust
#[derive(serde::Deserialize)]
struct SqlRequest {
    sql: String,
    #[serde(default)]
    params: Vec<Value>,
}

/// Map a JSON scalar to a typed `bluedb_rest::Param` for `$N` binding.
fn json_to_param(v: &Value) -> Result<bluedb_rest::Param, AppError> {
    use bluedb_rest::Param;
    Ok(match v {
        Value::Null => Param::Null,
        Value::Bool(b) => Param::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() { Param::Int(i) }
            else if let Some(f) = n.as_f64() { Param::Float(f) }
            else { return Err(AppError::bad_request("unrepresentable number param")); }
        }
        Value::String(s) => Param::Str(s.clone()),
        other => return Err(AppError::bad_request(format!("param must be a JSON scalar, got {other}"))),
    })
}
```

- [ ] **Step 4: handlers + routes.** Replace the old `exec_sql` handler with:
```rust
/// `POST /sql` — one parameterized non-DDL statement: `{ "sql": "...$1...", "params": [...] }`.
async fn exec_sql(State(state): State<AppState>, Json(req): Json<SqlRequest>) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    let params = req.params.iter().map(json_to_param).collect::<Result<Vec<_>, _>>()?;
    let mut glue = Glue::new(state.connection_serialized().await?);
    let payloads = rest_sql::execute_sql(&mut glue, &req.sql, &params, false).await?;
    Ok(Json(payloads_to_json(payloads)))
}

/// `POST /admin/sql` — arbitrary SQL (DDL/txns/multi). Off by default; audited.
async fn admin_sql(State(state): State<AppState>, Json(req): Json<SqlRequest>) -> Result<Json<Value>, AppError> {
    if !state.inner.admin_sql_enabled {
        return Err(AppError { status: StatusCode::NOT_FOUND, message: "admin SQL endpoint is disabled".to_string() });
    }
    state.require_active()?;
    // Audit: record that arbitrary SQL ran (principal/authz arrives in A3c).
    eprintln!("bluedb-audit: /admin/sql executed: {}", req.sql);
    let params = req.params.iter().map(json_to_param).collect::<Result<Vec<_>, _>>()?;
    let mut glue = Glue::new(state.connection_serialized().await?);
    let payloads = rest_sql::execute_sql(&mut glue, &req.sql, &params, true).await?;
    Ok(Json(payloads_to_json(payloads)))
}
```
In `build_app`, keep `/sql` mapped to the new `exec_sql` (now `Json` body), and add `.route("/admin/sql", post(admin_sql))`. Ensure `EngineError::Rejected` maps to HTTP 400 (in the `From<EngineError> for AppError` / `AppError::from` path — find how `EngineError` currently converts to `AppError` and add a 400 arm for `Rejected`; other engine errors keep their current mapping).

- [ ] **Step 5: migrate tests.** Update `tests/api.rs` (+ any other test) per Step 1: build `AppState` with the admin flag on; DDL setup via `POST /admin/sql` with `{"sql": "CREATE TABLE ..."}`; data via `/sql` `{sql, params}` or `/tables`. Keep `tests/http2.rs`'s setup working (it hits `/health`, unaffected — but if it builds AppState via a shared helper you changed, keep it compiling).

- [ ] **Step 6: run + commit.** `cargo test -p bluedb-server` (all pass), `cargo build -p bluedb-server 2>&1 | grep -i warn` (clean). Add a `//!` doc note in `main.rs` for `BLUEDB_ENABLE_ADMIN_SQL` (default off) and the `/sql` vs `/admin/sql` split. Commit:
```bash
git add crates/bluedb-server
git commit -m "feat(server): /sql parameterized single-DML; /admin/sql arbitrary (off by default, audited)"
```

## Self-Review
- Spec A coverage: SQL surface = parameterized single-DML ✓; Admin = arbitrary, off-by-default, audited ✓; old unauth-arbitrary `/sql` removed ✓. (Per-scope authz is A3c; here Admin is gated by the enable-flag as the minimal gate.)
- Placeholder scan: none. Type consistency: `execute_sql(.., &[Param], bool)`; `SqlRequest{sql, params}`; `json_to_param -> Param`; `EngineError::Rejected` → 400.
- Note carried to A3c: replace the flag-only Admin gate + the `eprintln!` audit with real scope-based authz + structured audit.
