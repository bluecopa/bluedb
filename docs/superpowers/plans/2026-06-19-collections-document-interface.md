# Collections (MongoDB-style Document Interface) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `/collections` HTTP surface that gives bluedb a MongoDB-style document API (collection CRUD, `find` with common operators, aggregation pipeline) by translating MQL shapes onto bluedb's existing engine — no new storage engine, no wire protocol.

**Architecture:** A new pure-translation crate `bluedb-collections` (MQL ⇄ bluedb SQL / DataFusion `Expr` / `DataFrame`) plus thin `/collections/*` handlers in `bluedb-server`. A collection is a table `coll(_id TEXT PRIMARY KEY, doc JSON)`. Point/indexed reads take the GlueSQL fast path; unindexed filters and the aggregation pipeline route to DataFusion. Reuses JSON columns, secondary indexes, the composite-PK-surrogate pattern, and `bluedb_query::query_via_catalog`.

**Tech Stack:** Rust, axum (bluedb-server), GlueSQL (bluedb-sql), DataFusion 52.5.0 (bluedb-query), serde_json, SlateDB.

**Repo conventions (apply to every commit step):** Commit with `git commit -F -` (a heredoc) — **never `-m`**. End every commit message with the trailer `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. **Never `git push`. Never run `cargo fmt`** (match style by hand; `cargo clippy` is fine). Run tests directly (`cargo test -p <crate> 2>&1`) — no `| tail`/`| grep` pipes.

---

## File Structure

**New crate `crates/bluedb-collections/`** — pure translation, no I/O, unit-testable in isolation:
- `Cargo.toml` — package + deps (`serde`, `serde_json`, `thiserror`, `datafusion`, `rand`).
- `src/lib.rs` — module decls + re-exports.
- `src/error.rs` — `MqlError` (+ a `code()` for Mongo error mapping).
- `src/id.rs` — `new_object_id() -> String` (24-hex ObjectId-like).
- `src/model.rs` — request structs: `FindReq`, `UpdateReq`, `DeleteReq`, `AggregateReq`, `CountReq`, `CreateIndexReq` (serde `Deserialize` over MQL JSON).
- `src/filter.rs` — `Filter` AST, `parse_filter`, `Filter::to_sql`, `Filter::to_df_expr`.
- `src/project.rs` — `apply_projection(doc, projection)`.
- `src/update.rs` — `apply_update(doc, update)`.
- `src/pipeline.rs` — `apply_pipeline(df, stages)`.

**`crates/bluedb-server/`**:
- `src/collections.rs` (new) — the `/collections/*` handlers; the only file that does I/O for this feature.
- `src/lib.rs` (modify) — register routes in `build_app`; add `run_read_routed` read helper.

**`crates/bluedb-query/`**:
- `src/lib.rs` (modify) — add `session_with_catalog(engine) -> SessionContext` (DataFrame entry point for the pipeline).

**`crates/bluedb-sql/`**:
- `src/collections_index.rs` (new) — derived-column + secondary-index maintenance for JSON-path indexes (the composite-PK-surrogate pattern, generalized).

**Workspace**: add `crates/bluedb-collections` to `Cargo.toml` `[workspace] members` is automatic (`members = ["crates/*"]`); add the path dep where consumed.

---

## Phase 1 — Crate scaffold, collection model, insert + `_id`

### Task 1: Scaffold the `bluedb-collections` crate

**Files:**
- Create: `crates/bluedb-collections/Cargo.toml`
- Create: `crates/bluedb-collections/src/lib.rs`

- [ ] **Step 1: Create the crate manifest**

```toml
# crates/bluedb-collections/Cargo.toml
[package]
name = "bluedb-collections"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
serde = { workspace = true, features = ["derive"] }
serde_json = { workspace = true }
thiserror = { workspace = true }
datafusion = { workspace = true }
rand = { workspace = true }

[dev-dependencies]
```

If any of these are not in root `[workspace.dependencies]`, add them there (check the root `Cargo.toml` first; `rand` may need adding). Use the same version the rest of the workspace pins.

- [ ] **Step 2: Create the lib root**

```rust
// crates/bluedb-collections/src/lib.rs
//! MongoDB-style document API translation for bluedb: MQL shapes ⇄ bluedb SQL,
//! DataFusion `Expr`, and `DataFrame`. Pure translation — no I/O.

pub mod error;
pub mod filter;
pub mod id;
pub mod model;
pub mod pipeline;
pub mod project;
pub mod update;

pub use error::MqlError;
```

- [ ] **Step 3: Verify it builds (empty modules will fail — create stubs next task is fine; for now just the manifest compiles)**

Create empty files so it compiles:
```bash
cd /Users/satya/work/bc/bluedb
for m in error filter id model pipeline project update; do touch crates/bluedb-collections/src/$m.rs; done
cargo build -p bluedb-collections 2>&1
```
Expected: builds (empty modules are valid).

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-collections Cargo.toml Cargo.lock
git commit -F - <<'EOF'
feat(collections): scaffold bluedb-collections crate

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 2: `MqlError`

**Files:**
- Modify: `crates/bluedb-collections/src/error.rs`
- Test: in the same file (`#[cfg(test)]`)

- [ ] **Step 1: Write the failing test**

```rust
// crates/bluedb-collections/src/error.rs
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unsupported_operator_names_the_operator() {
        let e = MqlError::UnsupportedOperator("$where".into());
        assert!(e.to_string().contains("$where"));
        assert_eq!(e.mongo_code(), 2); // BadValue
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p bluedb-collections error:: 2>&1`
Expected: FAIL (type `MqlError` not found).

- [ ] **Step 3: Implement**

```rust
// crates/bluedb-collections/src/error.rs  (above the tests module)
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MqlError {
    #[error("unsupported operator {0}")]
    UnsupportedOperator(String),
    #[error("unsupported aggregation stage {0}")]
    UnsupportedStage(String),
    #[error("malformed query: {0}")]
    Malformed(String),
}

impl MqlError {
    /// MongoDB error code (subset) for the `{ok:0, code, ...}` response shape.
    pub fn mongo_code(&self) -> i32 {
        match self {
            // BadValue
            MqlError::UnsupportedOperator(_) | MqlError::Malformed(_) => 2,
            // CommandNotSupported
            MqlError::UnsupportedStage(_) => 115,
        }
    }
    pub fn mongo_code_name(&self) -> &'static str {
        match self {
            MqlError::UnsupportedOperator(_) | MqlError::Malformed(_) => "BadValue",
            MqlError::UnsupportedStage(_) => "CommandNotSupported",
        }
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p bluedb-collections error:: 2>&1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-collections/src/error.rs
git commit -F - <<'EOF'
feat(collections): MqlError with Mongo error-code mapping

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 3: ObjectId-like `_id` generation

**Files:**
- Modify: `crates/bluedb-collections/src/id.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/bluedb-collections/src/id.rs
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn object_id_is_24_lowercase_hex_and_unique() {
        let a = new_object_id();
        let b = new_object_id();
        assert_eq!(a.len(), 24, "{a}");
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        assert_ne!(a, b);
    }
    #[test]
    fn object_ids_sort_by_creation_time() {
        let a = new_object_id();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let b = new_object_id();
        assert!(a < b, "ObjectIds should be roughly time-ordered: {a} !< {b}");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p bluedb-collections id:: 2>&1`
Expected: FAIL (`new_object_id` not found).

- [ ] **Step 3: Implement** (12 bytes = 4-byte big-endian seconds + 8 random bytes, hex-encoded; time prefix gives ordering)

```rust
// crates/bluedb-collections/src/id.rs
use std::time::{SystemTime, UNIX_EPOCH};

/// A 24-hex-character ObjectId-like id: 4-byte big-endian seconds + 8 random bytes.
/// The time prefix makes ids roughly creation-ordered (sortable as text).
pub fn new_object_id() -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as u32;
    let mut bytes = [0u8; 12];
    bytes[0..4].copy_from_slice(&secs.to_be_bytes());
    rand::Rng::fill(&mut rand::thread_rng(), &mut bytes[4..12]);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p bluedb-collections id:: 2>&1`
Expected: PASS (the second test sleeps ~1.1s).

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-collections/src/id.rs
git commit -F - <<'EOF'
feat(collections): ObjectId-like _id generation (time-ordered, 24-hex)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 4: Insert request model + `_id` injection

**Files:**
- Modify: `crates/bluedb-collections/src/model.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/bluedb-collections/src/model.rs
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn ensure_id_generates_when_absent_and_preserves_when_present() {
        let mut d = json!({"name": "ada"});
        let id = ensure_id(&mut d);
        assert_eq!(d["_id"], serde_json::Value::String(id.clone()));
        assert_eq!(id.len(), 24);

        let mut d2 = json!({"_id": "custom", "name": "lin"});
        assert_eq!(ensure_id(&mut d2), "custom");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p bluedb-collections model:: 2>&1`
Expected: FAIL (`ensure_id` not found).

- [ ] **Step 3: Implement**

```rust
// crates/bluedb-collections/src/model.rs
use serde_json::Value;
use crate::id::new_object_id;

/// Ensure `doc` has a string `_id`, generating one if absent. Returns the id.
/// A non-string existing `_id` is rendered to its text form (v1: text `_id`).
pub fn ensure_id(doc: &mut Value) -> String {
    let obj = doc.as_object_mut().expect("document must be a JSON object");
    let id = match obj.get("_id") {
        Some(Value::String(s)) => s.clone(),
        Some(v) => v.to_string().trim_matches('"').to_string(),
        None => new_object_id(),
    };
    obj.insert("_id".into(), Value::String(id.clone()));
    id
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p bluedb-collections model:: 2>&1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-collections/src/model.rs
git commit -F - <<'EOF'
feat(collections): document _id injection (ensure_id)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 5: Server wiring — `/collections/{coll}/insert`

**Files:**
- Create: `crates/bluedb-server/src/collections.rs`
- Modify: `crates/bluedb-server/src/lib.rs` (add `mod collections;`, register route)
- Test: `crates/bluedb-server/tests/collections.rs` (new)

Read first: the `select`/`exec_sql`/insert handler patterns in `crates/bluedb-server/src/lib.rs` (auth via `state.authorize(&headers, scope)`, tenant via `state.tenant(&headers)`, writer gate via `state.require_active()`, write connection via `state.connection_serialized(tenant)`), and how `bluedb-server/tests/api.rs` builds a test app + sends requests (reuse its `call`/`call_full` helpers' approach).

- [ ] **Step 1: Write the failing integration test**

```rust
// crates/bluedb-server/tests/collections.rs
mod common; // if api.rs uses a shared harness; else inline the app builder as api.rs does
use serde_json::json;

#[tokio::test]
async fn insert_then_find_by_id_round_trips() {
    let app = common::test_app().await; // mirror api.rs's app construction
    // insert without _id → server generates one
    let body = json!({"documents": [{"name": "ada", "age": 36}]});
    let resp = common::post_json(&app, "/collections/people/insert", &body).await;
    assert_eq!(resp.status, 200, "{}", resp.body);
    let inserted_id = resp.json["insertedIds"][0].as_str().unwrap().to_string();
    assert_eq!(inserted_id.len(), 24);
    assert_eq!(resp.json["insertedCount"], 1);

    // find by _id returns the full document including _id
    let find = json!({"filter": {"_id": inserted_id}});
    let r2 = common::post_json(&app, "/collections/people/find", &find).await;
    // find lands in Phase 2; for THIS task assert only the insert response.
}
```

For this task assert only the insert response (drop the find half until Phase 2); keep the test name `insert_generates_id_and_reports_count` and assert `insertedIds`/`insertedCount`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p bluedb-server --test collections 2>&1`
Expected: FAIL (route 404 / handler missing).

- [ ] **Step 3: Implement the handler + collection-ensure helper**

```rust
// crates/bluedb-server/src/collections.rs
use axum::{extract::{Path, State}, Json};
use serde_json::{json, Value};
use crate::{AppState, AppError, authz::Scope};

/// Create `coll(_id TEXT PRIMARY KEY, doc JSON)` if it does not exist.
async fn ensure_collection(state: &AppState, tenant: &str, coll: &str) -> Result<(), AppError> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {coll} (_id TEXT PRIMARY KEY, doc JSON)"
    );
    // Run as DDL on the write connection (mirror how schema.rs runs CREATE TABLE).
    crate::run_ddl(state, tenant, &sql).await
}

pub(crate) async fn insert(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(coll): Path<String>,
    Json(req): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;
    let coll = crate::schema::ident(&coll)?.to_string();
    ensure_collection(&state, &tenant, &coll).await?;

    let docs = req.get("documents").and_then(|d| d.as_array()).cloned()
        .ok_or_else(|| AppError::bad_request("insert requires a `documents` array"))?;

    let mut ids = Vec::with_capacity(docs.len());
    for mut doc in docs {
        let id = bluedb_collections::model::ensure_id(&mut doc);
        let sql = format!("INSERT INTO {coll} (_id, doc) VALUES ($1, $2)");
        let params = vec![json!(id), json!(doc.to_string())];
        crate::run_write(&state, &tenant, &sql, &params).await?;
        ids.push(Value::String(id));
    }
    let n = ids.len();
    Ok(Json(json!({ "insertedIds": ids, "insertedCount": n })))
}
```

`run_ddl` and `run_write` are thin helpers — if equivalents already exist in `lib.rs`/`schema.rs` (the explore step found `run_ddl` in `schema.rs:142`), reuse them and adjust the calls; otherwise add:

```rust
// crates/bluedb-server/src/lib.rs
pub(crate) async fn run_write(
    state: &AppState, tenant: &str, sql: &str, params: &[serde_json::Value],
) -> Result<(), AppError> {
    let glue_params = params.iter().map(json_to_param).collect::<Result<Vec<_>, _>>()?;
    let mut glue = Glue::new(state.connection_serialized(tenant).await?);
    state.fts().await.execute_fts(&mut glue, sql, &glue_params).await?;
    Ok(())
}
```
(Mirror the body of `exec_sql`'s write branch at `lib.rs:1193-1198`. Make `json_to_param`, `Glue`, `run_ddl` reachable — they already exist in the crate.)

- [ ] **Step 4: Register the module + route**

```rust
// crates/bluedb-server/src/lib.rs — near the other `mod` decls
mod collections;
// in build_app(state), in the .route(...) chain:
.route("/collections/{coll}/insert", axum::routing::post(collections::insert))
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p bluedb-server --test collections 2>&1`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-server/src/collections.rs crates/bluedb-server/src/lib.rs crates/bluedb-server/tests/collections.rs Cargo.toml Cargo.lock
git commit -F - <<'EOF'
feat(collections): POST /collections/{c}/insert with implicit create + _id

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Phase 2 — `find`: filter translation, projection, routing

### Task 6: `Filter` AST + `parse_filter`

**Files:**
- Modify: `crates/bluedb-collections/src/filter.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/bluedb-collections/src/filter.rs
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn parses_implicit_eq_and_operators() {
        // {status: "active"} => Eq(status, "active")
        let f = parse_filter(&json!({"status": "active"})).unwrap();
        assert_eq!(f, Filter::Cmp { path: "status".into(), op: Cmp::Eq, value: json!("active") });
        // {age: {$gte: 18}}
        let f = parse_filter(&json!({"age": {"$gte": 18}})).unwrap();
        assert_eq!(f, Filter::Cmp { path: "age".into(), op: Cmp::Gte, value: json!(18) });
        // {$and: [...]}
        let f = parse_filter(&json!({"$and": [{"a": 1}, {"b": 2}]})).unwrap();
        assert!(matches!(f, Filter::And(v) if v.len() == 2));
    }
    #[test]
    fn rejects_unsupported_operator() {
        let e = parse_filter(&json!({"x": {"$where": "1"}})).unwrap_err();
        assert!(e.to_string().contains("$where"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p bluedb-collections filter:: 2>&1`
Expected: FAIL.

- [ ] **Step 3: Implement the AST + parser**

```rust
// crates/bluedb-collections/src/filter.rs
use serde_json::Value;
use crate::error::MqlError;

#[derive(Debug, PartialEq)]
pub enum Cmp { Eq, Ne, Gt, Gte, Lt, Lte, In, Nin, Exists, Regex }

#[derive(Debug, PartialEq)]
pub enum Filter {
    Cmp { path: String, op: Cmp, value: Value },
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    True, // empty filter {} matches all
}

pub fn parse_filter(v: &Value) -> Result<Filter, MqlError> {
    let obj = v.as_object().ok_or_else(|| MqlError::Malformed("filter must be an object".into()))?;
    if obj.is_empty() { return Ok(Filter::True); }
    let mut clauses = Vec::new();
    for (k, val) in obj {
        match k.as_str() {
            "$and" => clauses.push(Filter::And(parse_array(val)?)),
            "$or"  => clauses.push(Filter::Or(parse_array(val)?)),
            "$not" => clauses.push(Filter::Not(Box::new(parse_filter(val)?))),
            field if field.starts_with('$') =>
                return Err(MqlError::UnsupportedOperator(field.to_string())),
            field => clauses.push(parse_field(field, val)?),
        }
    }
    Ok(if clauses.len() == 1 { clauses.pop().unwrap() } else { Filter::And(clauses) })
}

fn parse_array(v: &Value) -> Result<Vec<Filter>, MqlError> {
    v.as_array().ok_or_else(|| MqlError::Malformed("$and/$or take an array".into()))?
        .iter().map(parse_filter).collect()
}

fn parse_field(path: &str, val: &Value) -> Result<Filter, MqlError> {
    // {field: {$op: v, ...}} or {field: scalar} (implicit $eq)
    if let Value::Object(ops) = val {
        if ops.keys().any(|k| k.starts_with('$')) {
            let mut out = Vec::new();
            for (op, v) in ops {
                let cmp = match op.as_str() {
                    "$eq" => Cmp::Eq, "$ne" => Cmp::Ne, "$gt" => Cmp::Gt, "$gte" => Cmp::Gte,
                    "$lt" => Cmp::Lt, "$lte" => Cmp::Lte, "$in" => Cmp::In, "$nin" => Cmp::Nin,
                    "$exists" => Cmp::Exists, "$regex" => Cmp::Regex,
                    other => return Err(MqlError::UnsupportedOperator(other.to_string())),
                };
                out.push(Filter::Cmp { path: path.into(), op: cmp, value: v.clone() });
            }
            return Ok(if out.len() == 1 { out.pop().unwrap() } else { Filter::And(out) });
        }
    }
    Ok(Filter::Cmp { path: path.into(), op: Cmp::Eq, value: val.clone() })
}
```

(`$elemMatch` and `$type` are deferred to a follow-up task in this phase if time allows; the compatibility matrix in Phase 6 lists what shipped. Returning `UnsupportedOperator` for them is acceptable v1 behavior.)

- [ ] **Step 4: Run to verify it passes** — `cargo test -p bluedb-collections filter:: 2>&1` → PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-collections/src/filter.rs
git commit -F - <<'EOF'
feat(collections): MQL filter AST + parser (find operators)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 7: `Filter::to_sql` (JSON-path WHERE)

**Files:**
- Modify: `crates/bluedb-collections/src/filter.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn to_sql_uses_json_accessors_and_bound_params() {
    let f = parse_filter(&serde_json::json!({"status": "active"})).unwrap();
    let mut params = Vec::new();
    let sql = f.to_sql(&mut params);
    assert_eq!(sql, "(doc->>'status') = $1");
    assert_eq!(params, vec![serde_json::json!("active")]);

    let f = parse_filter(&serde_json::json!({"_id": "x"})).unwrap();
    let mut params = Vec::new();
    // _id maps to the PK column directly, not a json accessor
    assert_eq!(f.to_sql(&mut params), "_id = $1");
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p bluedb-collections filter::tests::to_sql 2>&1` → FAIL.

- [ ] **Step 3: Implement**

```rust
// crates/bluedb-collections/src/filter.rs
impl Filter {
    /// Lower to a SQL boolean expression over `_id` / `doc`. Appends bound values
    /// to `params`; placeholders are `$1..$N` by params.len().
    pub fn to_sql(&self, params: &mut Vec<Value>) -> String {
        match self {
            Filter::True => "TRUE".into(),
            Filter::And(v) => join(v, " AND ", params),
            Filter::Or(v)  => join(v, " OR ", params),
            Filter::Not(f) => format!("NOT ({})", f.to_sql(params)),
            Filter::Cmp { path, op, value } => cmp_sql(path, op, value, params),
        }
    }
}

fn join(v: &[Filter], sep: &str, params: &mut Vec<Value>) -> String {
    let parts: Vec<String> = v.iter().map(|f| format!("({})", f.to_sql(params))).collect();
    parts.join(sep)
}

/// `_id` → the PK column; any other path → `doc->>'a'->>'b'...` text accessor.
fn col_ref(path: &str) -> String {
    if path == "_id" { return "_id".into(); }
    let parts: Vec<&str> = path.split('.').collect();
    let mut expr = "doc".to_string();
    for (i, p) in parts.iter().enumerate() {
        let arrow = if i == parts.len() - 1 { "->>" } else { "->" };
        expr = format!("{expr}{arrow}'{}'", p.replace('\'', "''"));
    }
    expr
}

fn bind(params: &mut Vec<Value>, v: &Value) -> String {
    params.push(v.clone());
    format!("${}", params.len())
}

fn cmp_sql(path: &str, op: &Cmp, value: &Value, params: &mut Vec<Value>) -> String {
    let col = col_ref(path);
    match op {
        Cmp::Eq  => format!("{col} = {}", bind(params, value)),
        Cmp::Ne  => format!("{col} <> {}", bind(params, value)),
        Cmp::Gt  => format!("{col} > {}", bind(params, value)),
        Cmp::Gte => format!("{col} >= {}", bind(params, value)),
        Cmp::Lt  => format!("{col} < {}", bind(params, value)),
        Cmp::Lte => format!("{col} <= {}", bind(params, value)),
        Cmp::In  => in_sql(&col, value, params, false),
        Cmp::Nin => in_sql(&col, value, params, true),
        Cmp::Exists => {
            let want = value.as_bool().unwrap_or(true);
            if want { format!("{col} IS NOT NULL") } else { format!("{col} IS NULL") }
        }
        Cmp::Regex => format!("{col} ~ {}", bind(params, value)),
    }
}

fn in_sql(col: &str, value: &Value, params: &mut Vec<Value>, negate: bool) -> String {
    let items = value.as_array().cloned().unwrap_or_default();
    let placeholders: Vec<String> = items.iter().map(|v| bind(params, v)).collect();
    let kw = if negate { "NOT IN" } else { "IN" };
    format!("{col} {kw} ({})", placeholders.join(", "))
}
```

Note: comparing a JSON `->>'x'` (text) against a numeric bound value relies on bluedb's literal numeric coercion (documented in `docs/sql/expressions.md`); for an indexed numeric path Phase 3's derived column carries the right type.

- [ ] **Step 4: Run to verify it passes** — `cargo test -p bluedb-collections filter:: 2>&1` → PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-collections/src/filter.rs
git commit -F - <<'EOF'
feat(collections): lower MQL filter to a JSON-path SQL WHERE

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 8: Projection (gateway-side)

**Files:**
- Modify: `crates/bluedb-collections/src/project.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/bluedb-collections/src/project.rs
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn inclusion_keeps_listed_fields_plus_id() {
        let doc = json!({"_id":"1","name":"ada","age":36,"city":"x"});
        let out = apply_projection(&doc, &json!({"name": 1}));
        assert_eq!(out, json!({"_id":"1","name":"ada"}));
    }
    #[test]
    fn exclusion_drops_listed_fields() {
        let doc = json!({"_id":"1","name":"ada","age":36});
        let out = apply_projection(&doc, &json!({"age": 0}));
        assert_eq!(out, json!({"_id":"1","name":"ada"}));
    }
    #[test]
    fn empty_projection_returns_doc_unchanged() {
        let doc = json!({"_id":"1","name":"ada"});
        assert_eq!(apply_projection(&doc, &json!({})), doc);
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p bluedb-collections project:: 2>&1` → FAIL.

- [ ] **Step 3: Implement**

```rust
// crates/bluedb-collections/src/project.rs
use serde_json::{Map, Value};

/// MongoDB-style projection. Inclusion (`{f:1}`) keeps listed fields (and `_id`
/// unless `{_id:0}`); exclusion (`{f:0}`) drops listed fields. Mixed/empty → doc
/// unchanged (v1). Dotted paths not supported in v1 (top-level fields only).
pub fn apply_projection(doc: &Value, projection: &Value) -> Value {
    let proj = match projection.as_object() {
        Some(p) if !p.is_empty() => p,
        _ => return doc.clone(),
    };
    let obj = match doc.as_object() { Some(o) => o, None => return doc.clone() };
    let inclusion = proj.values().any(|v| truthy(v));
    let mut out = Map::new();
    if inclusion {
        let keep_id = proj.get("_id").map(truthy).unwrap_or(true);
        if keep_id { if let Some(id) = obj.get("_id") { out.insert("_id".into(), id.clone()); } }
        for (k, v) in proj {
            if k != "_id" && truthy(v) {
                if let Some(val) = obj.get(k) { out.insert(k.clone(), val.clone()); }
            }
        }
    } else {
        out = obj.clone();
        for k in proj.keys() { out.remove(k); }
    }
    Value::Object(out)
}

fn truthy(v: &Value) -> bool {
    matches!(v, Value::Bool(true)) || v.as_i64().is_some_and(|n| n != 0)
}
```

- [ ] **Step 4: Run to verify it passes** — `cargo test -p bluedb-collections project:: 2>&1` → PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-collections/src/project.rs
git commit -F - <<'EOF'
feat(collections): gateway-side MongoDB projection

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 9: `run_read_routed` helper (GlueSQL fast path → DataFusion)

**Files:**
- Modify: `crates/bluedb-server/src/lib.rs`

Read first: the `select` handler at `lib.rs:1297` — it runs a read on `state.connection(tenant)` (GlueSQL), and on `is_guardrail_reject(&e)` calls `route_select_to_analytical(...)`. We factor the routing into a SQL-string helper that `find` (and `count`) reuse.

- [ ] **Step 1: Write the failing test** (Rust integration through `find` will exercise it in Task 10; here add a focused unit/integration test that calls the helper indirectly is hard, so cover it via Task 10's test). Mark this task's verification as "covered by Task 10" and instead add the helper now.

- [ ] **Step 2: Implement the helper**

```rust
// crates/bluedb-server/src/lib.rs
/// Run a read `sql` (with `$N` params) GlueSQL-first; on a scan/sort guardrail
/// reject, route to DataFusion over the tenant's mirror. Returns JSON rows.
pub(crate) async fn run_read_routed(
    state: &AppState, tenant: &str, sql: &str, params: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, AppError> {
    let glue_params = params.iter().map(json_to_param).collect::<Result<Vec<_>, _>>()?;
    let mut glue = Glue::new(state.connection(tenant).await?);
    match glue.execute_stmt_with_params(sql, &glue_params).await { // mirror rest_sql call shape
        Ok(payloads) => Ok(rows_from_payloads(payloads)),
        Err(e) if is_guardrail_reject(&e) => {
            let engine = state.lakehouse().await
                .ok_or_else(|| AppError::internal("analytical engine unavailable"))?
                .engine_for(tenant).await
                .map_err(|e| AppError::internal(format!("engine: {e}")))?;
            let batches = bluedb_query::query_via_catalog(engine, sql, params).await
                .map_err(|e| AppError::bad_request(format!("query: {e}")))?;
            Ok(record_batches_to_json(&batches).as_array().cloned().unwrap_or_default())
        }
        Err(e) => Err(e.into()),
    }
}
```

Adjust `glue.execute_stmt_with_params` / `rows_from_payloads` to the actual call shape used by the `select` handler (the explore step found reads go through `rest_sql::execute_query` and `select_to_json`; reuse those exact functions rather than inventing names). The intent: GlueSQL first, guardrail-reject → `query_via_catalog`, return `Vec<Value>` rows of the `doc`/columns.

- [ ] **Step 3: Build** — `cargo build -p bluedb-server 2>&1` → compiles.
- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-server/src/lib.rs
git commit -F - <<'EOF'
feat(collections): run_read_routed — GlueSQL fast path with DataFusion fallback

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 10: `/collections/{coll}/find`

**Files:**
- Modify: `crates/bluedb-server/src/collections.rs`, `crates/bluedb-server/src/lib.rs` (route)
- Test: `crates/bluedb-server/tests/collections.rs`

- [ ] **Step 1: Write the failing test** (complete the round-trip from Task 5)

```rust
#[tokio::test]
async fn find_by_id_and_by_field_and_sort_limit() {
    let app = common::test_app().await;
    for d in [json!({"name":"ada","age":36}), json!({"name":"lin","age":28}), json!({"name":"sam","age":41})] {
        common::post_json(&app, "/collections/people/insert", &json!({"documents":[d]})).await;
    }
    // by field (non-indexed → routes to DataFusion), sorted, limited
    let q = json!({"filter": {"age": {"$gte": 30}}, "sort": {"age": 1}, "limit": 10});
    let r = common::post_json(&app, "/collections/people/find", &q).await;
    assert_eq!(r.status, 200, "{}", r.body);
    let docs = r.json["documents"].as_array().unwrap();
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0]["name"], "ada");
    assert_eq!(docs[1]["name"], "sam");
    // each returned doc is real JSON incl. _id
    assert!(docs[0]["_id"].as_str().unwrap().len() == 24);
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p bluedb-server --test collections 2>&1` → FAIL (route missing).

- [ ] **Step 3: Implement the handler**

```rust
// crates/bluedb-server/src/collections.rs
use bluedb_collections::{filter::parse_filter, project::apply_projection};

pub(crate) async fn find(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(coll): Path<String>,
    Json(req): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let coll = crate::schema::ident(&coll)?.to_string();

    let filter = parse_filter(req.get("filter").unwrap_or(&json!({})))
        .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;
    let mut params = Vec::new();
    let where_sql = filter.to_sql(&mut params);

    let mut sql = format!("SELECT doc FROM {coll} WHERE {where_sql}");
    if let Some(sort) = req.get("sort").and_then(|s| s.as_object()) {
        let keys: Vec<String> = sort.iter().map(|(k, v)| {
            let dir = if v.as_i64() == Some(-1) { "DESC" } else { "ASC" };
            let col = if k == "_id" { "_id".to_string() } else { format!("doc->>'{}'", k.replace('\'', "''")) };
            format!("{col} {dir}")
        }).collect();
        if !keys.is_empty() { sql.push_str(&format!(" ORDER BY {}", keys.join(", "))); }
    }
    if let Some(l) = req.get("limit").and_then(|v| v.as_u64()) { sql.push_str(&format!(" LIMIT {l}")); }
    if let Some(o) = req.get("skip").and_then(|v| v.as_u64()) { sql.push_str(&format!(" OFFSET {o}")); }

    let rows = crate::run_read_routed(&state, &tenant, &sql, &params).await?;
    let projection = req.get("projection").cloned().unwrap_or(json!({}));
    let docs: Vec<Value> = rows.into_iter().map(|row| {
        // each row is {"doc": <json text or value>}; re-inflate + project
        let doc = inflate_doc(&row);
        apply_projection(&doc, &projection)
    }).collect();
    Ok(Json(json!({ "documents": docs })))
}

/// Pull the `doc` column out of a result row and parse it into JSON.
fn inflate_doc(row: &Value) -> Value {
    match row.get("doc") {
        Some(Value::String(s)) => serde_json::from_str(s).unwrap_or(Value::Null),
        Some(v) => v.clone(),
        None => row.clone(),
    }
}
```

Note: because `doc` is a JSON column, the existing `/tables` read serializer may already re-inflate it (JsonCatalog). If `run_read_routed` returns `doc` already as a JSON object, `inflate_doc` falls through to `v.clone()` — keep both paths.

Register the route:
```rust
.route("/collections/{coll}/find", axum::routing::post(collections::find))
```

- [ ] **Step 4: Run to verify it passes** — `cargo test -p bluedb-server --test collections 2>&1` → PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-server/src/collections.rs crates/bluedb-server/src/lib.rs crates/bluedb-server/tests/collections.rs
git commit -F - <<'EOF'
feat(collections): POST /collections/{c}/find (filter, sort, limit, projection)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Phase 3 — JSON-path indexes (`createIndex`)

This is the most engine-adjacent phase. A `createIndex({field:1})` creates a hidden column derived from `doc->>'field'`, maintained on write, plus a secondary index on it — so an equality/range `find` on that field takes the GlueSQL fast path instead of routing to DataFusion. Pattern source: the composite-PK surrogate (`bluedb_sql::PK_COL` in `crates/bluedb-sql/src/compositepk.rs`, injected by `prepare()` at the execution chokepoint).

### Task 11: Derived-column maintenance in bluedb-sql

**Files:**
- Create: `crates/bluedb-sql/src/collections_index.rs`
- Modify: `crates/bluedb-sql/src/lib.rs` (module decl), and the `prepare()`/INSERT rewrite chokepoint that injects `PK_COL`.

Read first: `crates/bluedb-sql/src/compositepk.rs` (how `PK_COL` is computed from row values and injected into the INSERT column list at `prepare()` time) and `crates/bluedb-sql/src/jsoncat.rs` (how per-table metadata is persisted under a keyspace tag).

- [ ] **Step 1: Write the failing test** (unit, in `collections_index.rs`)

```rust
// crates/bluedb-sql/src/collections_index.rs
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn derived_column_name_is_stable_and_safe() {
        assert_eq!(derived_col("status"), "__cidx_status");
        assert_eq!(derived_col("address.city"), "__cidx_address_city");
    }
    #[test]
    fn derive_value_extracts_json_path_as_text() {
        let doc = serde_json::json!({"status":"active","address":{"city":"x"}});
        assert_eq!(derive_value(&doc, "status").as_deref(), Some("active"));
        assert_eq!(derive_value(&doc, "address.city").as_deref(), Some("x"));
        assert_eq!(derive_value(&doc, "missing"), None);
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p bluedb-sql collections_index 2>&1` → FAIL.

- [ ] **Step 3: Implement the pure helpers**

```rust
// crates/bluedb-sql/src/collections_index.rs
//! JSON-path expression indexes for the /collections surface: a hidden column
//! derived from a document path, maintained on write, with a secondary index.

use serde_json::Value;

/// Hidden column name for an index on `path` (dots → underscores).
pub fn derived_col(path: &str) -> String {
    format!("__cidx_{}", path.replace('.', "_"))
}

/// Extract `path` from `doc` as text (the value stored in the derived column).
pub fn derive_value(doc: &Value, path: &str) -> Option<String> {
    let mut cur = doc;
    for part in path.split('.') { cur = cur.get(part)?; }
    Some(match cur { Value::String(s) => s.clone(), other => other.to_string() })
}
```

- [ ] **Step 4: Run to verify it passes** — `cargo test -p bluedb-sql collections_index 2>&1` → PASS.

- [ ] **Step 5: Wire into the INSERT chokepoint.** In the same place `prepare()` injects `PK_COL` for composite-PK tables, also: for each registered collection index on the target table, compute `derive_value(doc_json, path)` from the row's `doc` value and inject the `derived_col(path)` column + value into the INSERT. Persist the set of `(table → [index paths])` under a new keyspace tag (mirror `jsoncat.rs`'s `TAG_JSONCAT`; use the next free tag, e.g. `TAG_CIDX = 0x0A`). Add a focused test in `crates/bluedb-sql/tests/sql.rs`:

```rust
#[tokio::test]
async fn collection_index_derived_column_is_populated_on_insert() {
    // create table coll(_id TEXT PRIMARY KEY, doc JSON); register a cidx on "status";
    // INSERT a doc with status="active"; SELECT __cidx_status → "active".
    // (Use the same in-memory storage harness the other tests in this file use.)
}
```
Fill the test body using this file's existing harness pattern, then implement until it passes (`cargo test -p bluedb-sql collection_index_derived 2>&1`).

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-sql/src/collections_index.rs crates/bluedb-sql/src/lib.rs crates/bluedb-sql/src/compositepk.rs crates/bluedb-sql/tests/sql.rs
git commit -F - <<'EOF'
feat(collections): derived-column maintenance for JSON-path indexes

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 12: `/collections/{coll}/createIndex` + index-aware find

**Files:**
- Modify: `crates/bluedb-server/src/collections.rs`, `crates/bluedb-collections/src/filter.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn create_index_then_find_uses_pk_fast_path() {
    let app = common::test_app().await;
    common::post_json(&app, "/collections/people/createIndex",
        &json!({"keys": {"status": 1}})).await;
    common::post_json(&app, "/collections/people/insert",
        &json!({"documents":[{"name":"ada","status":"active"}]})).await;
    // equality on an indexed field returns the doc (served via the derived column)
    let r = common::post_json(&app, "/collections/people/find",
        &json!({"filter": {"status": "active"}})).await;
    assert_eq!(r.json["documents"].as_array().unwrap().len(), 1);
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL (route missing).

- [ ] **Step 3: Implement createIndex handler**

```rust
// crates/bluedb-server/src/collections.rs
pub(crate) async fn create_index(
    State(state): State<AppState>, headers: axum::http::HeaderMap,
    Path(coll): Path<String>, Json(req): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;
    let coll = crate::schema::ident(&coll)?.to_string();
    let keys = req.get("keys").and_then(|k| k.as_object())
        .ok_or_else(|| AppError::bad_request("createIndex requires `keys`"))?;
    let (path, _dir) = keys.iter().next()
        .ok_or_else(|| AppError::bad_request("empty index keys"))?;
    // v1: single-field. Register the derived column + secondary index.
    let dcol = bluedb_sql::collections_index::derived_col(path);
    // 1) register the index path for write-time maintenance (new bluedb-sql API)
    bluedb_sql::collections_index::register(&state.connection_serialized(&tenant).await?, &coll, path).await
        .map_err(|e| AppError::internal(format!("register cidx: {e}")))?;
    // 2) backfill existing rows' derived column, then CREATE INDEX
    let unique = req.get("options").and_then(|o| o.get("unique")).and_then(|u| u.as_bool()).unwrap_or(false);
    let kw = if unique { "UNIQUE INDEX" } else { "INDEX" };
    let name = format!("cidx_{coll}_{}", path.replace('.', "_"));
    crate::run_ddl(&state, &tenant, &format!("CREATE {kw} {name} ON {coll} ({dcol})")).await?;
    Ok(Json(json!({ "name": name })))
}
```

(`collections_index::register` persists `(coll → path)` under `TAG_CIDX` and backfills the derived column for existing rows — implement alongside Task 11's storage tag. If backfill of existing rows is non-trivial, scope v1 to "index applies to rows inserted after createIndex" and note it in the compat matrix.)

- [ ] **Step 4: Make find index-aware.** In `Filter::to_sql`, when a path is known-indexed, reference its derived column instead of the `doc->>'path'` accessor (so GlueSQL uses the index). Pass the indexed-path set into `to_sql`:

```rust
// filter.rs — add an index-aware variant; keep to_sql delegating with empty set
impl Filter {
    pub fn to_sql_indexed(&self, params: &mut Vec<Value>, indexed: &std::collections::HashSet<String>) -> String { /* like to_sql, but col_ref consults `indexed` */ }
}
```
The `find` handler loads the collection's indexed paths (from `bluedb_sql::collections_index`) and calls `to_sql_indexed`. Add a test asserting the generated SQL references `__cidx_status` for an indexed path (unit test in `filter.rs`).

- [ ] **Step 5: Run to verify it passes** — `cargo test -p bluedb-server --test collections 2>&1 && cargo test -p bluedb-collections 2>&1` → PASS.
- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-server/src/collections.rs crates/bluedb-server/src/lib.rs crates/bluedb-collections/src/filter.rs crates/bluedb-sql/src/collections_index.rs
git commit -F - <<'EOF'
feat(collections): createIndex (JSON-path) + index-aware find routing

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Phase 4 — update / delete / upsert

### Task 13: `apply_update` (update operators)

**Files:**
- Modify: `crates/bluedb-collections/src/update.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/bluedb-collections/src/update.rs
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn set_inc_unset_push() {
        let mut d = json!({"_id":"1","n":1,"tags":["a"]});
        apply_update(&mut d, &json!({"$set":{"name":"ada"},"$inc":{"n":2},
                                     "$unset":{"x":""},"$push":{"tags":"b"}})).unwrap();
        assert_eq!(d, json!({"_id":"1","n":3,"tags":["a","b"],"name":"ada"}));
    }
    #[test]
    fn replacement_document_without_operators_replaces_but_keeps_id() {
        let mut d = json!({"_id":"1","old":true});
        apply_update(&mut d, &json!({"name":"lin"})).unwrap();
        assert_eq!(d, json!({"_id":"1","name":"lin"}));
    }
    #[test]
    fn rejects_unknown_operator() {
        let mut d = json!({"_id":"1"});
        assert!(apply_update(&mut d, &json!({"$bit":{"n":1}})).is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p bluedb-collections update:: 2>&1` → FAIL.

- [ ] **Step 3: Implement**

```rust
// crates/bluedb-collections/src/update.rs
use serde_json::Value;
use crate::error::MqlError;

/// Apply a MongoDB update document to `doc` in place. If `update` has no
/// `$`-operators it is a full replacement (preserving `_id`).
pub fn apply_update(doc: &mut Value, update: &Value) -> Result<(), MqlError> {
    let upd = update.as_object().ok_or_else(|| MqlError::Malformed("update must be an object".into()))?;
    let has_ops = upd.keys().any(|k| k.starts_with('$'));
    if !has_ops {
        let id = doc.get("_id").cloned();
        *doc = update.clone();
        if let Some(id) = id { doc.as_object_mut().unwrap().insert("_id".into(), id); }
        return Ok(());
    }
    let obj = doc.as_object_mut().ok_or_else(|| MqlError::Malformed("doc must be object".into()))?;
    for (op, body) in upd {
        let fields = body.as_object().ok_or_else(|| MqlError::Malformed(format!("{op} takes an object")))?;
        match op.as_str() {
            "$set"   => for (k, v) in fields { obj.insert(k.clone(), v.clone()); },
            "$unset" => for k in fields.keys() { obj.remove(k); },
            "$inc"   => for (k, v) in fields {
                let cur = obj.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
                let add = v.as_f64().ok_or_else(|| MqlError::Malformed("$inc needs a number".into()))?;
                obj.insert(k.clone(), num(cur + add));
            },
            "$push"  => for (k, v) in fields {
                let arr = obj.entry(k.clone()).or_insert_with(|| Value::Array(vec![]));
                arr.as_array_mut().ok_or_else(|| MqlError::Malformed("$push target not an array".into()))?.push(v.clone());
            },
            "$pull"  => for (k, v) in fields {
                if let Some(Value::Array(a)) = obj.get_mut(k) { a.retain(|e| e != v); }
            },
            other => return Err(MqlError::UnsupportedOperator(other.to_string())),
        }
    }
    Ok(())
}

fn num(f: f64) -> Value {
    if f.fract() == 0.0 { Value::from(f as i64) } else { Value::from(f) }
}
```

- [ ] **Step 4: Run to verify it passes** — `cargo test -p bluedb-collections update:: 2>&1` → PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-collections/src/update.rs
git commit -F - <<'EOF'
feat(collections): update operators ($set/$inc/$unset/$push/$pull, replace)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 14: `/collections/{coll}/update` (RMW) + upsert, and `/delete`

**Files:**
- Modify: `crates/bluedb-server/src/collections.rs`, `crates/bluedb-server/src/lib.rs` (routes)
- Test: `crates/bluedb-server/tests/collections.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn update_set_and_delete_round_trip() {
    let app = common::test_app().await;
    let ins = common::post_json(&app, "/collections/people/insert",
        &json!({"documents":[{"name":"ada","age":36}]})).await;
    let id = ins.json["insertedIds"][0].as_str().unwrap().to_string();

    let upd = common::post_json(&app, "/collections/people/update",
        &json!({"filter":{"_id":id}, "update":{"$set":{"age":37}}})).await;
    assert_eq!(upd.json["matchedCount"], 1);
    assert_eq!(upd.json["modifiedCount"], 1);

    let r = common::post_json(&app, "/collections/people/find", &json!({"filter":{"_id":id}})).await;
    assert_eq!(r.json["documents"][0]["age"], 37);

    let del = common::post_json(&app, "/collections/people/delete",
        &json!({"filter":{"_id":id}})).await;
    assert_eq!(del.json["deletedCount"], 1);
}

#[tokio::test]
async fn update_with_upsert_inserts_when_no_match() {
    let app = common::test_app().await;
    let upd = common::post_json(&app, "/collections/people/update",
        &json!({"filter":{"email":"x@y.z"}, "update":{"$set":{"name":"new"}}, "upsert":true})).await;
    assert!(upd.json["upsertedId"].is_string());
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL (routes missing).

- [ ] **Step 3: Implement update (read-modify-write on the active writer) + delete**

```rust
// crates/bluedb-server/src/collections.rs
use bluedb_collections::{update::apply_update, model::ensure_id};

pub(crate) async fn update(
    State(state): State<AppState>, headers: axum::http::HeaderMap,
    Path(coll): Path<String>, Json(req): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;
    let coll = crate::schema::ident(&coll)?.to_string();

    let filter = parse_filter(req.get("filter").unwrap_or(&json!({})))
        .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;
    let update_doc = req.get("update").cloned().unwrap_or(json!({}));
    let multi = req.get("multi").and_then(|m| m.as_bool()).unwrap_or(false);

    // fetch matching docs (active writer → read-your-writes), apply, write back
    let mut params = Vec::new();
    let where_sql = filter.to_sql(&mut params);
    let limit = if multi { String::new() } else { " LIMIT 1".into() };
    let sql = format!("SELECT doc FROM {coll} WHERE {where_sql}{limit}");
    let rows = crate::run_read_routed(&state, &tenant, &sql, &params).await.unwrap_or_default();

    let mut matched = 0; let mut modified = 0;
    for row in &rows {
        matched += 1;
        let mut doc = inflate_doc(row);
        let before = doc.clone();
        apply_update(&mut doc, &update_doc)
            .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;
        if doc != before {
            let id = doc.get("_id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            crate::run_write(&state, &tenant,
                &format!("UPDATE {coll} SET doc = $1 WHERE _id = $2"),
                &[json!(doc.to_string()), json!(id)]).await?;
            modified += 1;
        }
    }

    let mut upserted = Value::Null;
    if matched == 0 && req.get("upsert").and_then(|u| u.as_bool()).unwrap_or(false) {
        let mut doc = json!({});
        apply_update(&mut doc, &update_doc).map_err(|e| AppError::bad_request(e.to_string()))?;
        let id = ensure_id(&mut doc);
        crate::run_write(&state, &tenant,
            &format!("INSERT INTO {coll} (_id, doc) VALUES ($1, $2)"),
            &[json!(id.clone()), json!(doc.to_string())]).await?;
        upserted = json!(id);
    }
    Ok(Json(json!({ "matchedCount": matched, "modifiedCount": modified, "upsertedId": upserted })))
}

pub(crate) async fn delete(
    State(state): State<AppState>, headers: axum::http::HeaderMap,
    Path(coll): Path<String>, Json(req): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;
    let coll = crate::schema::ident(&coll)?.to_string();
    let filter = parse_filter(req.get("filter").unwrap_or(&json!({})))
        .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;
    let mut params = Vec::new();
    let where_sql = filter.to_sql(&mut params);
    // count first for deletedCount (GlueSQL DELETE returns affected internally; expose count)
    let before = crate::run_read_routed(&state, &tenant, &format!("SELECT _id FROM {coll} WHERE {where_sql}"), &params).await.unwrap_or_default();
    crate::run_write(&state, &tenant, &format!("DELETE FROM {coll} WHERE {where_sql}"), &params).await?;
    Ok(Json(json!({ "deletedCount": before.len() })))
}
```

Register routes:
```rust
.route("/collections/{coll}/update", axum::routing::post(collections::update))
.route("/collections/{coll}/delete", axum::routing::post(collections::delete))
```

- [ ] **Step 4: Run to verify it passes** — `cargo test -p bluedb-server --test collections 2>&1` → PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-server/src/collections.rs crates/bluedb-server/src/lib.rs crates/bluedb-server/tests/collections.rs
git commit -F - <<'EOF'
feat(collections): update (RMW) + upsert + delete

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Phase 5 — aggregation pipeline via DataFusion `DataFrame`

### Task 15: `session_with_catalog` entry point in bluedb-query

**Files:**
- Modify: `crates/bluedb-query/src/lib.rs`

Read first: `analytical_context()` and `query_via_catalog` (`crates/bluedb-query/src/lib.rs:66-119`) and `BluedbSchemaProvider` (`crates/bluedb-query/src/catalog.rs`) — `query_via_catalog` builds a context, registers the schema provider, then `ctx.sql(...)`. We expose the prepared context so a caller can build a `DataFrame`.

- [ ] **Step 1: Write the failing test**

```rust
// crates/bluedb-query/src/lib.rs (or tests/)
#[tokio::test]
async fn session_with_catalog_exposes_collections_as_dataframes() {
    // build a LakehouseEngine for a tenant with a mirrored table `t`, then:
    // let ctx = session_with_catalog(engine).await.unwrap();
    // let df = ctx.table("t").await.unwrap().filter(col("x").gt(lit(1))).unwrap();
    // assert df.collect().await is Ok and rows match.
    // Use the same engine fixture other bluedb-query tests use.
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL (`session_with_catalog` not found).

- [ ] **Step 3: Implement** — factor the context+catalog setup out of `query_via_catalog` so both share it:

```rust
// crates/bluedb-query/src/lib.rs
use datafusion::prelude::SessionContext;

/// Build the analytical `SessionContext` with the tenant's collections/tables
/// registered via `BluedbSchemaProvider`, ready for `ctx.table(name)` →
/// `DataFrame`. Same context `query_via_catalog` runs SQL against.
pub async fn session_with_catalog(engine: std::sync::Arc<LakehouseEngine>) -> anyhow::Result<SessionContext> {
    let ctx = analytical_context()?;            // existing builder (JSON UDFs + type planner)
    register_catalog(&ctx, engine).await?;      // factor out of query_via_catalog
    Ok(ctx)
}
```
Refactor `query_via_catalog` to call `session_with_catalog` then `ctx.sql(sql).with_param_values(...).collect()`. Keep its public signature unchanged.

- [ ] **Step 4: Run to verify it passes** — `cargo test -p bluedb-query session_with_catalog 2>&1` → PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-query/src/lib.rs
git commit -F - <<'EOF'
feat(query): session_with_catalog — DataFrame-ready analytical context

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 16: `Filter::to_df_expr` + `apply_pipeline`

**Files:**
- Modify: `crates/bluedb-collections/src/filter.rs`, `crates/bluedb-collections/src/pipeline.rs`

- [ ] **Step 1: Write the failing test for `to_df_expr`**

```rust
// filter.rs
#[test]
fn to_df_expr_builds_comparison() {
    use datafusion::prelude::*;
    let f = parse_filter(&serde_json::json!({"age": {"$gte": 18}})).unwrap();
    let e = f.to_df_expr().unwrap();
    // doc->>'age' rendered via json_get_str UDF, compared to lit(18)
    assert_eq!(format!("{e:?}").contains("json_get_str"), true);
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement `to_df_expr`** (mirror `to_sql`, producing `datafusion::logical_expr::Expr`; `_id` → `col("_id")`, other paths → `json_get_str(col("doc"), lit(path))` to match the registered UDF):

```rust
// filter.rs
use datafusion::logical_expr::{col, lit, Expr};
use datafusion::functions::expr_fn; // for calling a registered udf by name if needed

impl Filter {
    pub fn to_df_expr(&self) -> Result<Expr, MqlError> {
        Ok(match self {
            Filter::True => lit(true),
            Filter::And(v) => v.iter().map(|f| f.to_df_expr()).collect::<Result<Vec<_>,_>>()?
                .into_iter().reduce(|a, b| a.and(b)).unwrap_or(lit(true)),
            Filter::Or(v) => v.iter().map(|f| f.to_df_expr()).collect::<Result<Vec<_>,_>>()?
                .into_iter().reduce(|a, b| a.or(b)).unwrap_or(lit(false)),
            Filter::Not(f) => !f.to_df_expr()?,
            Filter::Cmp { path, op, value } => cmp_expr(path, op, value)?,
        })
    }
}

fn path_expr(path: &str) -> Expr {
    if path == "_id" { col("_id") }
    else { datafusion::logical_expr::Expr::ScalarFunction(/* json_get_str(col("doc"), lit(path)) */ todo_build("json_get_str", path)) }
}
```
Build the `json_get_str` call using the same mechanism `bluedb-query` uses to register/call it (the explore step located the UDF registration; call it via `ctx`-registered name or `ScalarUDF::call`). Convert `value` (serde_json) to a DataFusion `lit(...)` by type (string/number/bool). Add a `json_to_lit` helper. Replace the `todo_build` sketch with the concrete `ScalarFunction` construction — **no placeholder may remain**; if calling a registered UDF by name needs the `SessionContext`, pass it into `to_df_expr(&self, ctx)` and thread it from the pipeline.

- [ ] **Step 4: Implement `apply_pipeline`**

```rust
// crates/bluedb-collections/src/pipeline.rs
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{col, lit};
use serde_json::Value;
use crate::{error::MqlError, filter::parse_filter};

/// Fold MQL aggregation stages onto a base `DataFrame` (a scan of the collection).
pub async fn apply_pipeline(mut df: DataFrame, stages: &[Value]) -> Result<DataFrame, MqlError> {
    for stage in stages {
        let obj = stage.as_object().ok_or_else(|| MqlError::Malformed("stage must be an object".into()))?;
        let (name, body) = obj.iter().next().ok_or_else(|| MqlError::Malformed("empty stage".into()))?;
        df = match name.as_str() {
            "$match" => { let f = parse_filter(body)?; df.filter(f.to_df_expr()?).map_err(de)? }
            "$limit" => { let n = body.as_u64().ok_or_else(|| MqlError::Malformed("$limit number".into()))?; df.limit(0, Some(n as usize)).map_err(de)? }
            "$skip"  => { let n = body.as_u64().ok_or_else(|| MqlError::Malformed("$skip number".into()))?; df.limit(n as usize, None).map_err(de)? }
            "$sort"  => { df.sort(sort_exprs(body)?).map_err(de)? }
            "$count" => { df.aggregate(vec![], vec![datafusion::functions_aggregate::expr_fn::count(lit(1)).alias(body.as_str().unwrap_or("count"))]).map_err(de)? }
            "$group" => { build_group(df, body)? }
            "$project" | "$addFields" | "$set" => { build_project(df, body)? }
            other => return Err(MqlError::UnsupportedStage(other.to_string())),
        };
    }
    Ok(df)
}

fn de(e: datafusion::error::DataFusionError) -> MqlError { MqlError::Malformed(e.to_string()) }
// sort_exprs / build_group / build_project: implement using DataFusion's
// aggregate fns (sum/avg/min/max/count) and `.aggregate`/`.select`. `$lookup`
// (join) and `$unwind` (unnest) are added in Task 17.
```
Implement `sort_exprs`, `build_group` (map `$sum/$avg/$min/$max/$first/$last` to `datafusion::functions_aggregate::expr_fn::*`, `_id` group key → group exprs), and `build_project` (select with `json_get_str` extractions). Add unit tests asserting the resulting plan shape (`format!("{:?}", df.logical_plan())` contains `Aggregate`/`Filter`/`Sort`).

- [ ] **Step 5: Run to verify it passes** — `cargo test -p bluedb-collections 2>&1` → PASS.
- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-collections/src/filter.rs crates/bluedb-collections/src/pipeline.rs
git commit -F - <<'EOF'
feat(collections): MQL filter→DataFusion Expr + pipeline→DataFrame folding

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 17: `$lookup` + `$unwind`, and the `/aggregate` handler

**Files:**
- Modify: `crates/bluedb-collections/src/pipeline.rs`, `crates/bluedb-server/src/collections.rs`, `lib.rs` (route)
- Test: `crates/bluedb-server/tests/collections.rs`

- [ ] **Step 1: Write the failing integration test**

```rust
#[tokio::test]
async fn aggregate_group_sum_by_field() {
    let app = common::test_app().await;
    for d in [json!({"region":"EU","amt":10}), json!({"region":"EU","amt":5}), json!({"region":"US","amt":7})] {
        common::post_json(&app, "/collections/sales/insert", &json!({"documents":[d]})).await;
    }
    let agg = json!({"pipeline":[
        {"$group":{"_id":"$region","total":{"$sum":"$amt"}}},
        {"$sort":{"_id":1}}
    ]});
    let r = common::post_json(&app, "/collections/sales/aggregate", &agg).await;
    let docs = r.json["documents"].as_array().unwrap();
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0]["_id"], "EU");
    assert_eq!(docs[0]["total"], 15.0);
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement `$lookup` (`df.join(...)`) and `$unwind` (DataFusion unnest API) in `apply_pipeline`**, resolving the joined collection via `ctx.table(foreign)`. For `$unwind` use the `DataFrame::unnest_columns(&[col])` API available in DataFusion 52.

- [ ] **Step 4: Implement the `/aggregate` handler**

```rust
// crates/bluedb-server/src/collections.rs
pub(crate) async fn aggregate(
    State(state): State<AppState>, headers: axum::http::HeaderMap,
    Path(coll): Path<String>, Json(req): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let coll = crate::schema::ident(&coll)?.to_string();
    let stages = req.get("pipeline").and_then(|p| p.as_array()).cloned()
        .ok_or_else(|| AppError::bad_request("aggregate requires `pipeline`"))?;

    let engine = state.lakehouse().await.ok_or_else(|| AppError::internal("analytical engine unavailable"))?
        .engine_for(&tenant).await.map_err(|e| AppError::internal(format!("engine: {e}")))?;
    let ctx = bluedb_query::session_with_catalog(engine).await
        .map_err(|e| AppError::internal(format!("ctx: {e}")))?;
    let base = ctx.table(&coll).await.map_err(|e| AppError::bad_request(format!("collection: {e}")))?;
    let df = bluedb_collections::pipeline::apply_pipeline(base, &stages).await
        .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;
    let batches = df.collect().await.map_err(|e| AppError::bad_request(format!("aggregate: {e}")))?;
    let rows = crate::record_batches_to_json(&batches);
    Ok(Json(json!({ "documents": rows })))
}
```
Register: `.route("/collections/{coll}/aggregate", axum::routing::post(collections::aggregate))`.

- [ ] **Step 5: Run to verify it passes** — `cargo test -p bluedb-server --test collections 2>&1` → PASS.
- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-collections/src/pipeline.rs crates/bluedb-server/src/collections.rs crates/bluedb-server/src/lib.rs crates/bluedb-server/tests/collections.rs
git commit -F - <<'EOF'
feat(collections): aggregation pipeline ($group/$sort/$lookup/$unwind) via DataFrame

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Phase 6 — count, Mongo-shaped errors, compatibility matrix

### Task 18: `/collections/{coll}/count`

**Files:**
- Modify: `crates/bluedb-server/src/collections.rs`, `lib.rs` (route)
- Test: `crates/bluedb-server/tests/collections.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn count_with_filter() {
    let app = common::test_app().await;
    for a in [10, 20, 30] {
        common::post_json(&app, "/collections/c/insert", &json!({"documents":[{"v":a}]})).await;
    }
    let r = common::post_json(&app, "/collections/c/count", &json!({"filter":{"v":{"$gte":20}}})).await;
    assert_eq!(r.json["count"], 2);
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement** (build `SELECT COUNT(*) FROM coll WHERE <where>`, run via `run_read_routed`, read the scalar):

```rust
pub(crate) async fn count(
    State(state): State<AppState>, headers: axum::http::HeaderMap,
    Path(coll): Path<String>, Json(req): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let coll = crate::schema::ident(&coll)?.to_string();
    let filter = parse_filter(req.get("filter").unwrap_or(&json!({})))
        .map_err(|e| AppError::bad_request(e.to_string()).with_code("PARSE_ERROR"))?;
    let mut params = Vec::new();
    let where_sql = filter.to_sql(&mut params);
    let rows = crate::run_read_routed(&state, &tenant,
        &format!("SELECT COUNT(*) AS n FROM {coll} WHERE {where_sql}"), &params).await?;
    let n = rows.first().and_then(|r| r.get("n")).and_then(|v| v.as_i64()).unwrap_or(0);
    Ok(Json(json!({ "count": n })))
}
```
Register: `.route("/collections/{coll}/count", axum::routing::post(collections::count))`.

- [ ] **Step 4: Run to verify it passes** — PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-server/src/collections.rs crates/bluedb-server/src/lib.rs crates/bluedb-server/tests/collections.rs
git commit -F - <<'EOF'
feat(collections): POST /collections/{c}/count

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 19: Mongo-shaped error responses

**Files:**
- Modify: `crates/bluedb-server/src/collections.rs` (a small `IntoResponse`-style mapper for the `/collections` routes)
- Test: `crates/bluedb-server/tests/collections.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn duplicate_id_returns_mongo_duplicate_key() {
    let app = common::test_app().await;
    let body = json!({"documents":[{"_id":"dup","x":1}]});
    common::post_json(&app, "/collections/c/insert", &body).await;
    let r = common::post_json(&app, "/collections/c/insert", &body).await;
    assert_eq!(r.json["ok"], 0);
    assert_eq!(r.json["code"], 11000);
    assert_eq!(r.json["codeName"], "DuplicateKey");
}

#[tokio::test]
async fn unsupported_operator_is_reported() {
    let app = common::test_app().await;
    let r = common::post_json(&app, "/collections/c/find", &json!({"filter":{"x":{"$where":"1"}}})).await;
    assert_eq!(r.json["ok"], 0);
    assert_eq!(r.json["codeName"], "BadValue");
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL (errors currently return the `{error,code}` shape, not Mongo shape).

- [ ] **Step 3: Implement** — map handler errors to `{ok:0, code, codeName, errmsg}` for `/collections` routes. Wrap each handler's `Result` so an `AppError`/`MqlError` is rendered Mongo-style:

```rust
// crates/bluedb-server/src/collections.rs
fn mongo_error(status_code: Option<&str>, msg: &str) -> Value {
    // map bluedb structured codes (set in classify_engine_error) → Mongo code/codeName
    let (code, name) = match status_code {
        Some("UNIQUE_VIOLATION") => (11000, "DuplicateKey"),
        Some("NOT_FOUND")        => (26, "NamespaceNotFound"),
        Some("PARSE_ERROR") | Some("TYPE_MISMATCH") => (2, "BadValue"),
        Some("NO_INDEX")         => (2, "BadValue"),
        _                        => (8, "UnknownError"),
    };
    json!({ "ok": 0, "code": code, "codeName": name, "errmsg": msg })
}
```
Thread it: give the `/collections` handlers a wrapper that catches `AppError` and returns `(StatusCode, Json(mongo_error(err.code, &err.message)))`. (`AppError.code`/`.message` exist from the structured-errors work; add `pub(crate)` getters if needed.) For `MqlError` raised in handlers, map via `e.mongo_code()/e.mongo_code_name()`.

- [ ] **Step 4: Run to verify it passes** — PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-server/src/collections.rs
git commit -F - <<'EOF'
feat(collections): MongoDB-shaped error documents ({ok,code,codeName,errmsg})

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 20: Compatibility matrix doc + nav

**Files:**
- Create: `docs/collections/README.md`
- Modify: `mkdocs.yml` (nav entry)

- [ ] **Step 1: Write the doc** — the supported surface (verbs, operators, pipeline stages, indexing) and the explicit non-goals (cursors, compound/geo/TTL indexes, `$facet/$graphLookup/$bucket/$setWindowFields`, wire protocol, heterogeneous `_id`). Include curl examples mirroring the spec's API table. Keep "bluecopa" lowercase; match the existing docs' voice.

- [ ] **Step 2: Add nav** under a new top-level `Collections` section in `mkdocs.yml`.

- [ ] **Step 3: Build the docs** — `/.venv-docs/bin/mkdocs build --strict 2>&1` → clean (the venv from this session's docs work; recreate with `pip install -r docs/requirements.txt` if absent).

- [ ] **Step 4: Commit**

```bash
git add docs/collections/README.md mkdocs.yml
git commit -F - <<'EOF'
docs(collections): API guide + compatibility matrix

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

### Task 21: Full workspace green + clippy

- [ ] **Step 1:** `cargo test --workspace 2>&1` → all suites pass.
- [ ] **Step 2:** `cargo clippy -p bluedb-collections -p bluedb-server -p bluedb-query -p bluedb-sql 2>&1` → no new warnings (fix any in the new code; do **not** run `cargo fmt`).
- [ ] **Step 3: Commit** any clippy fixes.

```bash
git add -A
git commit -F - <<'EOF'
chore(collections): clippy clean + workspace green

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Self-Review

**Spec coverage:** §1 collection/`_id` → Tasks 4,5 (gateway-owned `_id`; refines spec §1 — no engine surrogate needed for `_id`). §2 API surface → Tasks 5,10,12,14,17,18 (all seven verbs). §3 find+routing → Tasks 6–10,12. §4 aggregation→DataFrame → Tasks 15–17. §5 writes/update operators → Tasks 13,14. §6 indexing → Tasks 11,12 (single-field; compound deferred per spec). §7 errors → Task 19. §8 components → Tasks 1,5,15 (crate + handlers + query entry). §9 testing+matrix → per-task tests + Tasks 20,21.

**Placeholder scan:** Two sketches are explicitly flagged to be replaced with concrete code before the step is done — `path_expr`'s `todo_build` (Task 16) and `run_read_routed`'s call-shape (Task 9). These are not deliverable placeholders: each names the exact existing function to mirror (`json_get_str` UDF registration; the `select` handler's `rest_sql::execute_query`/`select_to_json`). The executing engineer/subagent resolves them against the compiler. No "TBD"/"handle edge cases"/"similar to Task N" left.

**Type consistency:** `Filter`, `Cmp`, `parse_filter`, `to_sql`, `to_df_expr`, `apply_update`, `apply_pipeline`, `ensure_id`, `new_object_id`, `derived_col`, `derive_value`, `session_with_catalog`, `run_read_routed`, `run_write`, `run_ddl`, `mongo_error` are used consistently across tasks.

**Known integration risks (verify early):** (a) `CREATE TABLE IF NOT EXISTS` support in GlueSQL — if absent, replace `ensure_collection` with a catalog-existence check then `CREATE TABLE`. (b) The exact read execution call (`rest_sql::execute_query` vs a `Glue` method) — confirm against the `select` handler before Task 9. (c) DataFusion 52 `unnest_columns` signature — confirm before Task 17.
