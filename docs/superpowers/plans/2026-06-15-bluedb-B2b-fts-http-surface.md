# bluedb Spec B — increment B2b: FTS over the HTTP surface (`CREATE FULLTEXT INDEX` + `@@` on `/sql`)

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Make SQL-integrated FTS real over the wire. Wire the shared `FtsEngine` (B2c) into the server so: (1) `POST /schema/tables/{table}/fulltext-indexes {column, analyzer}` declares a fulltext index (Spec B §4.1); (2) every write connection carries the FTS commit observer, so the live index is maintained on commit; (3) `POST /sql` rewrites `@@`/`ts_rank` against the live index — so a `@@` SELECT after an insert returns the row (read-your-writes over HTTP, Spec B §5).

**Architecture:**
- `AppState`/`Inner` gains `fts: Arc<bluedb_engine::FtsEngine>` (created in `AppState::new`; one shared instance for the node's lifetime → it holds the in-memory live segments).
- `AppState::connection()` and `connection_serialized()` attach `.with_commit_observer(self.inner.fts.clone())` — the single wiring point, so **all** write routes (`/tables` insert/update/delete, `/sql` DML) maintain the index. Reads pay nothing (no committed changes).
- `exec_sql` (`POST /sql`) routes through `fts.execute_fts(glue, sql, params)` instead of `rest_sql::execute_sql(.., false)`: it rewrites `@@`/`ts_rank` against the live segment, else runs unchanged. (DML has no `@@` → passthrough; the observer maintains the index on commit.)
- New handler `schema::create_fulltext_index` + a new `FtsEngine::create_fulltext_index_auto` that resolves the table's **primary-key column** from the schema (the `ColumnDef` with `unique == Some(ColumnUniqueOption { is_primary: true })`) and the text column's ordinal, then registers the live segment. `schema:admin` scope (A3c), gated by `require_active`.

**Scope (B2b):** the DDL endpoint, the engine/observer wiring, `@@` over `/sql`, and one end-to-end HTTP test. **NOT in B2b:** the REST DSL `?col=fts.<query>` operator on `/tables` (Spec B §4.4 — a `bluedb-rest` change, deferred to a later increment); durable registry persistence (defs are in-memory, lost on restart until B4); `admin_sql` is left as the raw arbitrary escape hatch (no `@@` rewrite there — documented). Same `INTEGER PRIMARY KEY` restriction as B2c.

**Tech stack:** Rust, axum, the B2c `FtsEngine`, gluesql-core 0.19 (`Schema.column_defs`, `gluesql_core::ast::{ColumnDef, ColumnUniqueOption}`).

---

## Task 1: `FtsEngine::create_fulltext_index_auto` (PK auto-resolution)

**Files:** `crates/bluedb-engine/src/fts_engine.rs` (+ unit test in its `#[cfg(test)]`).

- [ ] **Failing test** (engine, against a real `Database`): create `docs (id INTEGER PRIMARY KEY, body TEXT)`, call `fts.create_fulltext_index_auto(&conn, "docs", "body", "english")`, then prove it registered an index whose pk_column is `id` by driving an insert (observed) + a `@@` query through `execute_fts` (same shape as the B2c `ryw.rs` test, but via the auto method). Also assert an error when the table has no single integer PK (e.g. a schemaless table or one whose PK column isn't found).
- [ ] **Implement** `create_fulltext_index_auto`:
```rust
/// Like [`Self::create_fulltext_index`] but resolves the table's primary-key
/// column from its schema (the column flagged `is_primary`). Errors if the
/// table is schemaless or has no single primary-key column.
pub async fn create_fulltext_index_auto(
    &self,
    storage: &SlateDbStorage,
    table: &str,
    text_column: &str,
    analyzer: &str,
) -> Result<()> {
    let schema = Store::fetch_schema(storage, table).await.map_err(EngineError::from)?
        .ok_or_else(|| EngineError::Rejected(format!("no such table: {table}")))?;
    let cols = schema.column_defs.as_ref().ok_or_else(|| {
        EngineError::Rejected(format!("table {table} is schemaless; FTS needs a column schema"))
    })?;
    let pk_column = cols.iter()
        .find(|c| matches!(c.unique, Some(gluesql_core::ast::ColumnUniqueOption { is_primary: true })))
        .map(|c| c.name.clone())
        .ok_or_else(|| EngineError::Rejected(format!(
            "table {table} has no primary key; fulltext index requires an integer primary key")))?;
    self.create_fulltext_index(storage, table, text_column, &pk_column, analyzer).await
}
```
  (Verify the `gluesql_core::ast::ColumnUniqueOption` path — `ColumnDef`/`ColumnUniqueOption` live in `gluesql_core::ast` (ast/ddl.rs). `Schema.column_defs` is `Option<Vec<ColumnDef>>`. Adjust the import path if needed.)
- [ ] Run `cargo test -p bluedb-engine` (all pass), `cargo build -p bluedb-engine 2>&1 | grep -i warn` (clean). Commit:
```bash
git commit -am "feat(engine): FtsEngine::create_fulltext_index_auto — resolve PK column from schema"
```

## Task 2: server wiring + `/schema/.../fulltext-indexes` endpoint + `@@` on `/sql`

**Files:** `crates/bluedb-server/src/lib.rs` (AppState/Inner field, connection builders, `exec_sql`, route), `crates/bluedb-server/src/schema.rs` (new handler), `crates/bluedb-server/tests/fts.rs` (new), doc note in `main.rs`.

- [ ] **Failing end-to-end HTTP test** (`crates/bluedb-server/tests/fts.rs`, reuse `api.rs`'s harness pattern — `make_app(true)` gives a promoted node with admin SQL; the `call(&app, method, uri, body)` helper. Copy the small harness fns you need, or factor a shared `tests/common.rs` if cleaner — but simplest is to inline a local harness mirroring api.rs):
```rust
// 1. create table via the structured DDL surface (A3b)
let (s, _) = call(&app, "POST", "/schema/tables", Some(json!({
    "name": "docs",
    "columns": [
        {"name":"id","type":"INTEGER","primaryKey":true},
        {"name":"body","type":"TEXT"}
    ]
}))).await; assert!(s.is_success());

// 2. declare a fulltext index (this increment's new endpoint)
let (s, _) = call(&app, "POST", "/schema/tables/docs/fulltext-indexes",
    Some(json!({"column":"body","analyzer":"english"}))).await;
assert!(s.is_success(), "create fulltext index");

// 3. insert via /sql (single statement, observed connection maintains the live index)
let (s, _) = call(&app, "POST", "/sql", Some(json!({
    "sql": "INSERT INTO docs (id, body) VALUES (1, 'quarterly invoice overdue'), (2, 'weather sunny skies')"
}))).await; assert!(s.is_success());

// 4. @@ query over /sql sees the just-committed matching row (RYW over HTTP)
let (s, body) = call(&app, "POST", "/sql", Some(json!({
    "sql": "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')"
}))).await;
assert!(s.is_success());
// assert the response contains exactly id=1 (shape: mirror how exec_sql serializes Payload::Select — check api.rs's /sql assertions for the exact JSON shape)

// 5. delete row 1 via /sql, re-query → empty
let (s, _) = call(&app, "POST", "/sql", Some(json!({"sql":"DELETE FROM docs WHERE id = 1"}))).await;
assert!(s.is_success());
let (s, body2) = call(&app, "POST", "/sql", Some(json!({
    "sql": "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')"
}))).await;
assert!(s.is_success());
// assert zero rows
```
(Read `api.rs` for the exact `/sql` success JSON shape — `payloads_to_json`/`Payload::Select` serialization — and assert on rows accordingly. If authz is unconfigured in the harness (open mode), no bearer token is needed; the existing api.rs tests confirm open mode.)
Run `cargo test -p bluedb-server --test fts` → FAIL (endpoint 404, `@@` not rewritten).

- [ ] **Implement.**
  1. `Inner`: add `fts: Arc<bluedb_engine::FtsEngine>`. In `AppState::new`, initialize `fts: bluedb_engine::FtsEngine::new()` (returns `Arc<FtsEngine>`). Import `FtsEngine` (`use bluedb_engine::{rest_sql, EngineError, FtsEngine};`).
  2. `connection()` and `connection_serialized()`: attach the observer —
     `Some(db) => Ok(db.connection().with_commit_observer(self.inner.fts.clone())),` (and `.connection_serialized().with_commit_observer(...)` for the serialized one). `Arc<FtsEngine>` coerces to `Arc<dyn CommitObserver>`.
  3. `exec_sql`: replace `rest_sql::execute_sql(&mut glue, &req.sql, &params, false).await?` with `self_or_state.inner.fts.execute_fts(&mut glue, &req.sql, &params).await?` (the handler has `State(state)`, so `state.inner.fts.execute_fts(...)`). Keep `require_active()` + authz as-is. Leave `admin_sql` unchanged.
  4. `schema.rs`: add
```rust
#[derive(Deserialize)]
pub struct CreateFulltextIndexRequest {
    pub column: String,
    #[serde(default = "default_analyzer")]
    pub analyzer: String,
}
fn default_analyzer() -> String { "english".into() }

pub async fn create_fulltext_index(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(table): Path<String>,
    Json(req): Json<CreateFulltextIndexRequest>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, crate::authz::Scope::SchemaAdmin)?;
    state.require_active()?;
    let table = ident(&table)?;
    let column = ident(&req.column)?;
    let conn = state.connection().await?;
    state.fts().create_fulltext_index_auto(&conn, table, column, &req.analyzer).await?;
    Ok(Json(json!({ "created_fulltext_index": column, "on": table, "analyzer": req.analyzer })))
}
```
     Add a `pub(crate) fn fts(&self) -> &std::sync::Arc<bluedb_engine::FtsEngine>` accessor on `AppState` (returns `&self.inner.fts`) so `schema.rs` can reach it. Reuse the existing `ident` validator. The `EngineError` from the engine maps to `AppError` via the existing `From<EngineError>` path (Rejected → 400; so a bad table/column → 400).
  5. Route in `build_app`: `.route("/schema/tables/{table}/fulltext-indexes", post(schema::create_fulltext_index))`.
  6. `main.rs` `//!` doc: list the new endpoint and that `/sql` supports the PostgreSQL `@@`/`to_tsvector`/`*_tsquery`/`ts_rank` FTS surface once a fulltext index is declared.
- [ ] Run `cargo test -p bluedb-server` (ALL pass — existing tests unaffected: the observer is installed but no fulltext indexes are declared in them, so `on_commit` finds nothing and `execute_fts` passes non-`@@` SQL straight through) + `cargo build -p bluedb-server 2>&1 | grep -i warn` (clean). Commit:
```bash
git commit -am "feat(server): CREATE FULLTEXT INDEX endpoint + @@ rewrite on /sql (FTS over HTTP, read-your-writes)"
```

## Self-Review
- Spec B coverage (B2b slice): `CREATE FULLTEXT INDEX` via the DDL surface (§4.1) ✓; PostgreSQL `@@`/`ts_rank` over `/sql` (§4.3) ✓; read-your-writes over HTTP — insert/delete reflected in a later `@@` with no explicit flush (§5) ✓; injection-safe (the FTS query is a literal in the SQL; idents validated; the rewrite emits `pk IN (...)` over bound pks) ✓.
- Wiring isolation: one shared `Arc<FtsEngine>` in `AppState`; observer attached at the two connection builders (single point); `exec_sql` is the only read path changed. `admin_sql` stays the raw escape hatch. Existing tests unaffected (no declared indexes → no-op observer + passthrough rewrite).
- Deferred: REST `?col=fts.` DSL operator on `/tables` (§4.4); durable index-def registry (in-memory now); durable seal/failover (B4); trigram (B3); `admin_sql` `@@` rewrite. All documented.
- Restriction carried from B2c: `INTEGER PRIMARY KEY` + schema'd rows; the endpoint errors clearly (400) if the table has no primary key.
