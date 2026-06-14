# bluedb A1 — Parameterized Data Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `/tables/{t}` data plane injection-proof by binding every value as a gluesql `$N` parameter (never string-interpolated), and turn array inserts into a server-wrapped `BEGIN…COMMIT` parameterized batch (one durable flush).

**Architecture:** `bluedb-rest` gains a dependency-free typed `Param` enum and `*_with_params()` renderers that emit `$N` placeholders + a parallel params vec (identifiers stay allow-listed via `validate_ident`). `bluedb-engine` maps `Param → gluesql ParamLiteral` and runs via `Glue::execute_with_params`. The server sends an array insert as one `BEGIN; <single-row INSERT…$N…>; COMMIT;` string with globally-numbered placeholders on the serialized connection.

**Tech Stack:** Rust, gluesql-core 0.19 (`Glue::execute_with_params`, `ParamLiteral`/`IntoParamLiteral`, `$N` 1-based positional placeholders shared across statements), axum, serde_json.

**Scope note:** This is behavior-preserving — `render_param` reuses the exact type-inference rule of the old `render_value` (null/bool → i64 → f64 → string), so existing semantics are unchanged; only the *mechanism* moves from interpolation to binding. Preserving original JSON insert types (so a JSON string `"10"` binds as a string, not a number) is a deliberate **out-of-scope** later refinement.

---

## File Structure

- `crates/bluedb-rest/src/model.rs` — add `Param` enum, `render_param`, `bind` helper, `Filter::to_sql_with_params`, `render_in_list_params`. Keep the old `render_value`/`to_sql` until the engine stops calling them (removed at the end).
- `crates/bluedb-rest/src/render.rs` — add `render_where_params` + `to_sql_with_params` for `RestQuery`/`UpdateRequest`/`DeleteRequest`, and `InsertRequest::row_statements_with_params` (one single-row `INSERT` per row, global `$N`).
- `crates/bluedb-rest/src/lib.rs` — export `Param`.
- `crates/bluedb-engine/src/rest_sql.rs` — `param_to_literal`, switch `execute_*` to `execute_with_params`, add `execute_insert_batch`.
- `crates/bluedb-server/src/lib.rs` — `insert` handler routes single-object vs array; array → batch on the serialized connection.

Each task is TDD: failing test → run (fail) → implement → run (pass) → commit.

---

## Task 1: `Param` enum + `render_param` (the typing primitive)

**Files:**
- Modify: `crates/bluedb-rest/src/model.rs`
- Test: same file (`#[cfg(test)] mod param_tests`)

- [ ] **Step 1: Write the failing test**

Add at the end of `crates/bluedb-rest/src/model.rs`:

```rust
#[cfg(test)]
mod param_tests {
    use super::{render_param, Param};

    #[test]
    fn render_param_types_like_the_old_rule() {
        assert_eq!(render_param("null"), Param::Null);
        assert_eq!(render_param("NULL"), Param::Null);
        assert_eq!(render_param("true"), Param::Bool(true));
        assert_eq!(render_param("false"), Param::Bool(false));
        assert_eq!(render_param("10"), Param::Int(10));
        assert_eq!(render_param("-3"), Param::Int(-3));
        assert_eq!(render_param("10.5"), Param::Float(10.5));
        assert_eq!(render_param("hello"), Param::Str("hello".to_string()));
        // The injection payload becomes plain string DATA, never structure:
        assert_eq!(
            render_param("'); DROP TABLE t; --"),
            Param::Str("'); DROP TABLE t; --".to_string())
        );
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bluedb-rest param_tests -- --nocapture`
Expected: FAIL — `cannot find function render_param` / `cannot find type Param`.

- [ ] **Step 3: Implement `Param` + `render_param`**

Add to `crates/bluedb-rest/src/model.rs` (after the `use` block near the top):

```rust
/// A typed value to be bound as a gluesql `$N` parameter — never interpolated
/// into SQL text. This is the injection-proof replacement for emitting a literal:
/// user data flows through `params`, so it can never change query structure.
#[derive(Debug, Clone, PartialEq)]
pub enum Param {
    /// SQL `NULL`.
    Null,
    /// Boolean.
    Bool(bool),
    /// 64-bit integer.
    Int(i64),
    /// 64-bit float.
    Float(f64),
    /// UTF-8 string (the default for anything not recognized as the above).
    Str(String),
}

/// Type a stringly-typed DSL value into a [`Param`].
///
/// Same rule the old `render_value` used to pick a literal, so behavior is
/// unchanged: `null`/`true`/`false` (case-insensitive) → those; else `i64` if it
/// parses; else `f64` if it parses; else a string.
pub fn render_param(value: &str) -> Param {
    let lower = value.to_ascii_lowercase();
    if lower == "null" {
        return Param::Null;
    }
    if lower == "true" {
        return Param::Bool(true);
    }
    if lower == "false" {
        return Param::Bool(false);
    }
    if let Ok(i) = value.parse::<i64>() {
        return Param::Int(i);
    }
    if let Ok(f) = value.parse::<f64>() {
        return Param::Float(f);
    }
    Param::Str(value.to_string())
}

/// Push `render_param(value)` onto `params` and return its 1-based `$N`
/// placeholder (gluesql positional-parameter syntax).
pub(crate) fn bind(value: &str, params: &mut Vec<Param>) -> String {
    params.push(render_param(value));
    format!("${}", params.len())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p bluedb-rest param_tests -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-rest/src/model.rs
git commit -m "feat(rest): add Param enum + render_param typing primitive"
```

---

## Task 2: `Filter::to_sql_with_params` (all operators, bound values)

**Files:**
- Modify: `crates/bluedb-rest/src/model.rs`
- Test: same file

- [ ] **Step 1: Write the failing test**

Add to `mod param_tests`:

```rust
use super::{Filter, Operator};

#[test]
fn filter_binds_values_as_placeholders() {
    let mut params = Vec::new();
    let f = Filter::new("age", Operator::Gt, "20");
    assert_eq!(f.to_sql_with_params(&mut params).unwrap(), "age > $1");
    assert_eq!(params, vec![Param::Int(20)]);

    // `in` binds each element; placeholders continue the running index.
    let mut params = Vec::new();
    let f = Filter::new("id", Operator::In, "(1,2,3)");
    assert_eq!(f.to_sql_with_params(&mut params).unwrap(), "id IN ($1, $2, $3)");
    assert_eq!(params, vec![Param::Int(1), Param::Int(2), Param::Int(3)]);

    // `is` binds nothing (keywords only).
    let mut params = Vec::new();
    let f = Filter { column: "x".into(), op: Operator::Is, negated: true, value: "null".into() };
    assert_eq!(f.to_sql_with_params(&mut params).unwrap(), "x IS NOT NULL");
    assert!(params.is_empty());

    // negation wraps the comparison.
    let mut params = Vec::new();
    let f = Filter { column: "name".into(), op: Operator::Eq, negated: true, value: "amy".into() };
    assert_eq!(f.to_sql_with_params(&mut params).unwrap(), "NOT (name = $1)");
    assert_eq!(params, vec![Param::Str("amy".into())]);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bluedb-rest param_tests::filter_binds_values_as_placeholders -- --nocapture`
Expected: FAIL — `no method named to_sql_with_params`.

- [ ] **Step 3: Implement `Filter::to_sql_with_params` + `render_in_list_params`**

Add inside `impl Filter` (in `crates/bluedb-rest/src/model.rs`), beside the existing `to_sql`:

```rust
    /// Render this predicate with bound parameters: returns the SQL fragment
    /// (no leading `WHERE`) and appends each value to `params` as a `$N` bind.
    /// Identifiers are still allow-listed (they cannot be parameters).
    pub fn to_sql_with_params(&self, params: &mut Vec<Param>) -> Result<String, RestError> {
        let col = validate_ident(&self.column)?;
        let inner = match self.op {
            Operator::Is => {
                let predicate = match self.value.to_ascii_lowercase().as_str() {
                    "null" => "IS NULL",
                    "true" => "IS TRUE",
                    "false" => "IS FALSE",
                    _ => {
                        return Err(RestError::MalformedValue {
                            op: "is".to_string(),
                            value: self.value.clone(),
                        })
                    }
                };
                let predicate = if self.negated {
                    match predicate {
                        "IS NULL" => "IS NOT NULL",
                        "IS TRUE" => "IS NOT TRUE",
                        "IS FALSE" => "IS NOT FALSE",
                        _ => unreachable!("predicate is one of the three above"),
                    }
                } else {
                    predicate
                };
                return Ok(format!("{col} {predicate}"));
            }
            Operator::In => {
                let list = render_in_list_params(&self.value, params)?;
                format!("{col} IN ({list})")
            }
            Operator::Eq => format!("{col} = {}", bind(&self.value, params)),
            Operator::Neq => format!("{col} <> {}", bind(&self.value, params)),
            Operator::Gt => format!("{col} > {}", bind(&self.value, params)),
            Operator::Gte => format!("{col} >= {}", bind(&self.value, params)),
            Operator::Lt => format!("{col} < {}", bind(&self.value, params)),
            Operator::Lte => format!("{col} <= {}", bind(&self.value, params)),
            Operator::Like => format!("{col} LIKE {}", bind(&self.value, params)),
            Operator::Ilike => format!("{col} ILIKE {}", bind(&self.value, params)),
        };

        if self.negated {
            Ok(format!("NOT ({inner})"))
        } else {
            Ok(inner)
        }
    }
```

Add this free function near the existing `render_in_list` in the same file:

```rust
/// Like `render_in_list`, but binds each element as a `$N` parameter and returns
/// the comma-separated placeholder list (`$1, $2, …`).
fn render_in_list_params(value: &str, params: &mut Vec<Param>) -> Result<String, RestError> {
    let trimmed = value.trim();
    let body = trimmed
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(trimmed);
    let placeholders: Vec<String> = body
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|elem| bind(elem, params))
        .collect();
    if placeholders.is_empty() {
        return Err(RestError::MalformedValue {
            op: "in".to_string(),
            value: value.to_string(),
        });
    }
    Ok(placeholders.join(", "))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p bluedb-rest param_tests -- --nocapture`
Expected: PASS (all `param_tests`).

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-rest/src/model.rs
git commit -m "feat(rest): Filter::to_sql_with_params binds values as \$N"
```

---

## Task 3: `RestQuery`/`UpdateRequest`/`DeleteRequest` `to_sql_with_params`

**Files:**
- Modify: `crates/bluedb-rest/src/render.rs`
- Test: same file (`#[cfg(test)] mod params_render`)

- [ ] **Step 1: Write the failing test**

Add at the end of `crates/bluedb-rest/src/render.rs`:

```rust
#[cfg(test)]
mod params_render {
    use crate::model::{Filter, Operator, OrderKey, Direction, Param, RestQuery, UpdateRequest, DeleteRequest};

    #[test]
    fn select_binds_filters_keeps_structure_literal() {
        let q = RestQuery {
            table: "users".into(),
            select: vec!["id".into(), "name".into()],
            filters: vec![Filter::new("age", Operator::Gt, "20")],
            order: vec![OrderKey { column: "name".into(), direction: Direction::Asc }],
            limit: Some(10),
            offset: Some(5),
        };
        let (sql, params) = q.to_sql_with_params().unwrap();
        assert_eq!(
            sql,
            "SELECT id, name FROM users WHERE age > $1 ORDER BY name ASC LIMIT 10 OFFSET 5;"
        );
        assert_eq!(params, vec![Param::Int(20)]);
    }

    #[test]
    fn update_binds_sets_then_filters_in_order() {
        let u = UpdateRequest {
            table: "t".into(),
            assignments: vec![("name".into(), "amy".into()), ("age".into(), "9".into())],
            filters: vec![Filter::new("id", Operator::Eq, "1")],
        };
        let (sql, params) = u.to_sql_with_params().unwrap();
        assert_eq!(sql, "UPDATE t SET name = $1, age = $2 WHERE id = $3;");
        assert_eq!(params, vec![Param::Str("amy".into()), Param::Int(9), Param::Int(1)]);
    }

    #[test]
    fn delete_binds_filters() {
        let d = DeleteRequest {
            table: "t".into(),
            filters: vec![Filter::new("id", Operator::Eq, "42")],
        };
        let (sql, params) = d.to_sql_with_params().unwrap();
        assert_eq!(sql, "DELETE FROM t WHERE id = $1;");
        assert_eq!(params, vec![Param::Int(42)]);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bluedb-rest params_render -- --nocapture`
Expected: FAIL — `no method named to_sql_with_params`.

- [ ] **Step 3: Implement the param-aware renderers**

In `crates/bluedb-rest/src/render.rs`, update the `use` line to import `Param`:

```rust
use crate::model::{
    bind, render_value, validate_ident, DeleteRequest, InsertRequest, Param, RestQuery, UpdateRequest,
};
```

Add a param-aware `WHERE` builder beside `render_where`:

```rust
/// Param-aware `WHERE …` builder: appends each filter's binds to `params`.
fn render_where_params(
    filters: &[crate::model::Filter],
    params: &mut Vec<Param>,
) -> Result<String, RestError> {
    if filters.is_empty() {
        return Ok(String::new());
    }
    let parts: Vec<String> = filters
        .iter()
        .map(|f| f.to_sql_with_params(params))
        .collect::<Result<_, _>>()?;
    Ok(format!(" WHERE {}", parts.join(" AND ")))
}
```

Add `to_sql_with_params` to `impl RestQuery` (beside `to_sql`):

```rust
    /// Render to `SELECT … ;` with bound `$N` parameters for all filter values.
    pub fn to_sql_with_params(&self) -> Result<(String, Vec<Param>), RestError> {
        let table = validate_ident(&self.table)?;
        let projection = if self.select.is_empty() {
            "*".to_string()
        } else {
            let cols: Vec<&str> = self
                .select
                .iter()
                .map(|c| validate_ident(c))
                .collect::<Result<_, _>>()?;
            cols.join(", ")
        };

        let mut params = Vec::new();
        let mut sql = format!("SELECT {projection} FROM {table}");
        sql.push_str(&render_where_params(&self.filters, &mut params)?);

        if !self.order.is_empty() {
            let keys: Vec<String> = self
                .order
                .iter()
                .map(|k| {
                    let col = validate_ident(&k.column)?;
                    Ok(format!("{col} {}", direction_sql(k.direction)))
                })
                .collect::<Result<_, RestError>>()?;
            sql.push_str(&format!(" ORDER BY {}", keys.join(", ")));
        }
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }
        sql.push(';');
        Ok((sql, params))
    }
```

Add to `impl UpdateRequest`:

```rust
    /// Render to `UPDATE … SET … WHERE … ;` with bound `$N` parameters.
    pub fn to_sql_with_params(&self) -> Result<(String, Vec<Param>), RestError> {
        let table = validate_ident(&self.table)?;
        if self.assignments.is_empty() {
            return Err(RestError::BadColumnSet("UPDATE has no assignments".to_string()));
        }
        if self.filters.is_empty() {
            return Err(RestError::UnfilteredMutation("UPDATE"));
        }
        let mut params = Vec::new();
        let sets: Vec<String> = self
            .assignments
            .iter()
            .map(|(col, val)| {
                let col = validate_ident(col)?;
                Ok(format!("{col} = {}", bind(val, &mut params)))
            })
            .collect::<Result<_, RestError>>()?;
        let where_clause = render_where_params(&self.filters, &mut params)?;
        Ok((format!("UPDATE {table} SET {}{};", sets.join(", "), where_clause), params))
    }
```

Add to `impl DeleteRequest`:

```rust
    /// Render to `DELETE FROM … WHERE … ;` with bound `$N` parameters.
    pub fn to_sql_with_params(&self) -> Result<(String, Vec<Param>), RestError> {
        let table = validate_ident(&self.table)?;
        if self.filters.is_empty() {
            return Err(RestError::UnfilteredMutation("DELETE"));
        }
        let mut params = Vec::new();
        let where_clause = render_where_params(&self.filters, &mut params)?;
        Ok((format!("DELETE FROM {table}{};", where_clause), params))
    }
```

> Note: `render_value` stays imported only until Task 7 removes the old paths. If the compiler warns it's now unused in `render.rs`, leave it — Task 4 still uses it via `InsertRequest::to_sql` until Task 7.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p bluedb-rest params_render -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-rest/src/render.rs crates/bluedb-rest/src/model.rs
git commit -m "feat(rest): to_sql_with_params for query/update/delete"
```

---

## Task 4: `InsertRequest::row_statements_with_params` (per-row, global `$N`)

**Files:**
- Modify: `crates/bluedb-rest/src/render.rs`
- Test: same file (`mod params_render`)

- [ ] **Step 1: Write the failing test**

Add to `mod params_render`:

```rust
use crate::model::InsertRequest;

#[test]
fn insert_emits_one_statement_per_row_with_global_indices() {
    let req = InsertRequest {
        table: "docs".into(),
        columns: vec!["id".into(), "body".into()],
        rows: vec![
            vec!["1".into(), "hi".into()],
            vec!["2".into(), "yo".into()],
        ],
    };
    let (stmts, params) = req.row_statements_with_params().unwrap();
    assert_eq!(
        stmts,
        vec![
            "INSERT INTO docs (id, body) VALUES ($1, $2)".to_string(),
            "INSERT INTO docs (id, body) VALUES ($3, $4)".to_string(),
        ]
    );
    assert_eq!(
        params,
        vec![Param::Int(1), Param::Str("hi".into()), Param::Int(2), Param::Str("yo".into())]
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bluedb-rest params_render::insert_emits_one_statement_per_row_with_global_indices -- --nocapture`
Expected: FAIL — `no method named row_statements_with_params`.

- [ ] **Step 3: Implement `row_statements_with_params`**

Add to `impl InsertRequest` in `crates/bluedb-rest/src/render.rs`:

```rust
    /// Render one **single-row** `INSERT` statement per row (no terminating `;`),
    /// with placeholders numbered **globally** across all rows so the statements
    /// can share one params slice (gluesql binds `$N` against a single shared
    /// vector). Avoids multi-row `VALUES (..),(..)`, which sqlparser rejects past
    /// ~50 tuples. The caller wraps the statements in `BEGIN; … ; COMMIT;`.
    pub fn row_statements_with_params(&self) -> Result<(Vec<String>, Vec<Param>), RestError> {
        let table = validate_ident(&self.table)?;
        if self.columns.is_empty() {
            return Err(RestError::BadColumnSet("INSERT has no columns".to_string()));
        }
        if self.rows.is_empty() {
            return Err(RestError::BadColumnSet("INSERT has no rows".to_string()));
        }
        let cols: Vec<&str> = self
            .columns
            .iter()
            .map(|c| validate_ident(c))
            .collect::<Result<_, _>>()?;
        let col_list = cols.join(", ");

        let mut params = Vec::new();
        let mut stmts = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            if row.len() != self.columns.len() {
                return Err(RestError::BadColumnSet(format!(
                    "row has {} values but there are {} columns",
                    row.len(),
                    self.columns.len()
                )));
            }
            let placeholders: Vec<String> = row.iter().map(|v| bind(v, &mut params)).collect();
            stmts.push(format!(
                "INSERT INTO {table} ({col_list}) VALUES ({})",
                placeholders.join(", ")
            ));
        }
        Ok((stmts, params))
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p bluedb-rest params_render -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-rest/src/render.rs
git commit -m "feat(rest): InsertRequest::row_statements_with_params (global \$N)"
```

---

## Task 5: Export `Param`

**Files:**
- Modify: `crates/bluedb-rest/src/lib.rs:76` (the `pub use model::{…}` line)

- [ ] **Step 1: Add `Param` to the re-export**

In `crates/bluedb-rest/src/lib.rs`, the model re-export currently reads:

```rust
    render_value, validate_ident, DeleteRequest, Direction, Filter, InsertRequest, Operator,
```

Change the start of that list to include `Param`:

```rust
    render_value, validate_ident, DeleteRequest, Direction, Filter, InsertRequest, Operator, Param,
```

(Keep the rest of the line — `OrderKey, RestQuery, UpdateRequest` etc. — unchanged.)

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p bluedb-rest`
Expected: builds clean.

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-rest/src/lib.rs
git commit -m "feat(rest): export Param"
```

---

## Task 6: Engine — `param_to_literal` + `execute_with_params` + `execute_insert_batch`

**Files:**
- Modify: `crates/bluedb-engine/src/rest_sql.rs`
- Test: `crates/bluedb-engine/tests/params.rs` (new)

- [ ] **Step 1: Write the failing integration test**

Create `crates/bluedb-engine/tests/params.rs`:

```rust
//! Parameterized data-plane execution + injection-proofing.
use std::sync::Arc;

use bluedb_engine::rest_sql;
use bluedb_rest::{DeleteRequest, Filter, InsertRequest, Operator, RestQuery};
use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn glue() -> Glue<SlateDbStorage> {
    let db = Db::open("t", Arc::new(InMemory::new())).await.unwrap();
    let mut g = Glue::new(SlateDbStorage::new(Arc::new(db)));
    g.execute("CREATE TABLE docs (id INTEGER, body TEXT);").await.unwrap();
    g
}

fn rows(p: Payload) -> Vec<Vec<Value>> {
    match p { Payload::Select { rows, .. } => rows, other => panic!("not select: {other:?}") }
}

#[tokio::test]
async fn insert_batch_then_query_with_params() {
    let mut g = glue().await;
    // Array insert (2 rows) -> one BEGIN..COMMIT batch.
    let req = InsertRequest {
        table: "docs".into(),
        columns: vec!["id".into(), "body".into()],
        rows: vec![vec!["1".into(), "alpha".into()], vec!["2".into(), "beta".into()]],
    };
    rest_sql::execute_insert_batch(&mut g, &req).await.unwrap();

    let q = RestQuery { table: "docs".into(), filters: vec![Filter::new("id", Operator::Eq, "2")], ..Default::default() };
    let out = rest_sql::execute_query(&mut g, &q).await.unwrap();
    let r = rows(out.into_iter().next().unwrap());
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Str("beta".into()));
}

#[tokio::test]
async fn injection_payload_is_stored_as_data_not_executed() {
    let mut g = glue().await;
    let payload = "'); DROP TABLE docs; --";
    let req = InsertRequest {
        table: "docs".into(),
        columns: vec!["id".into(), "body".into()],
        rows: vec![vec!["1".into(), payload.into()]],
    };
    rest_sql::execute_insert_batch(&mut g, &req).await.unwrap();

    // The table still exists and the payload round-trips verbatim as data.
    let q = RestQuery { table: "docs".into(), ..Default::default() };
    let out = rest_sql::execute_query(&mut g, &q).await.unwrap();
    let r = rows(out.into_iter().next().unwrap());
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Str(payload.into()));
}

#[tokio::test]
async fn delete_with_param() {
    let mut g = glue().await;
    let req = InsertRequest {
        table: "docs".into(),
        columns: vec!["id".into(), "body".into()],
        rows: vec![vec!["1".into(), "x".into()], vec!["2".into(), "y".into()]],
    };
    rest_sql::execute_insert_batch(&mut g, &req).await.unwrap();
    let d = DeleteRequest { table: "docs".into(), filters: vec![Filter::new("id", Operator::Eq, "1")] };
    rest_sql::execute_delete(&mut g, &d).await.unwrap();
    let q = RestQuery { table: "docs".into(), ..Default::default() };
    let out = rest_sql::execute_query(&mut g, &q).await.unwrap();
    assert_eq!(rows(out.into_iter().next().unwrap()).len(), 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bluedb-engine --test params -- --nocapture`
Expected: FAIL — `execute_insert_batch` not found / `execute_query` still uses old path.

- [ ] **Step 3: Implement the engine changes**

Replace the body of `crates/bluedb-engine/src/rest_sql.rs` below the doc-comment with:

```rust
use bluedb_rest::{parse_query, DeleteRequest, InsertRequest, Param, RestQuery, UpdateRequest};
use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::{Glue, Payload};
use gluesql_core::translate::ParamLiteral;

use crate::error::Result;

/// Map a `bluedb-rest` [`Param`] to a gluesql [`ParamLiteral`]. Keeps `bluedb-rest`
/// free of any gluesql dependency — the type bridge lives here.
fn param_to_literal(p: &Param) -> ParamLiteral {
    use gluesql_core::translate::IntoParamLiteral;
    match p {
        Param::Null => ParamLiteral::null(),
        Param::Bool(b) => (*b).into_param_literal(),
        Param::Int(i) => (*i).into_param_literal(),
        Param::Float(f) => (*f).into_param_literal(),
        Param::Str(s) => s.clone().into_param_literal(),
    }
}

fn literals(params: &[Param]) -> Vec<ParamLiteral> {
    params.iter().map(param_to_literal).collect()
}

/// Execute a typed [`RestQuery`] (`SELECT`) with bound parameters.
pub async fn execute_query(glue: &mut Glue<SlateDbStorage>, query: &RestQuery) -> Result<Vec<Payload>> {
    let (sql, params) = query.to_sql_with_params()?;
    Ok(glue.execute_with_params(&sql, literals(&params)).await?)
}

/// Parse a PostgREST query string for `table` and execute it.
pub async fn execute_query_str(
    glue: &mut Glue<SlateDbStorage>,
    table: &str,
    query_string: &str,
) -> Result<Vec<Payload>> {
    let query = parse_query(table, query_string)?;
    execute_query(glue, &query).await
}

/// Execute a single-row [`InsertRequest`] as one autocommit statement (the
/// group-commit fast path — caller supplies the group-commit connection).
pub async fn execute_insert(glue: &mut Glue<SlateDbStorage>, req: &InsertRequest) -> Result<Vec<Payload>> {
    let (stmts, params) = req.row_statements_with_params()?;
    let sql = format!("{};", stmts.join("; "));
    Ok(glue.execute_with_params(&sql, literals(&params)).await?)
}

/// Execute a multi-row [`InsertRequest`] atomically: the server-side
/// `BEGIN; <single-row INSERT…>; …; COMMIT;` batch — one `WriteBatch`, one flush.
/// Caller supplies the **serialized** connection (the txn holds the write lease).
pub async fn execute_insert_batch(
    glue: &mut Glue<SlateDbStorage>,
    req: &InsertRequest,
) -> Result<Vec<Payload>> {
    let (stmts, params) = req.row_statements_with_params()?;
    let sql = format!("BEGIN; {}; COMMIT;", stmts.join("; "));
    Ok(glue.execute_with_params(&sql, literals(&params)).await?)
}

/// Execute an [`UpdateRequest`] with bound parameters.
pub async fn execute_update(glue: &mut Glue<SlateDbStorage>, req: &UpdateRequest) -> Result<Vec<Payload>> {
    let (sql, params) = req.to_sql_with_params()?;
    Ok(glue.execute_with_params(&sql, literals(&params)).await?)
}

/// Execute a [`DeleteRequest`] with bound parameters.
pub async fn execute_delete(glue: &mut Glue<SlateDbStorage>, req: &DeleteRequest) -> Result<Vec<Payload>> {
    let (sql, params) = req.to_sql_with_params()?;
    Ok(glue.execute_with_params(&sql, literals(&params)).await?)
}
```

> Verify the `ParamLiteral`/`IntoParamLiteral` import path resolves: it is `gluesql_core::translate::{ParamLiteral, IntoParamLiteral}` (re-exported from `translate::param`). If `cargo build` reports a private path, import from `gluesql_core::translate::param::{…}` instead — confirm with `cargo doc -p gluesql-core --open` or grep the installed crate.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p bluedb-engine --test params -- --nocapture`
Expected: PASS (all three tests). The injection test proves the payload is stored as data and `docs` is not dropped.

- [ ] **Step 5: Run the existing engine tests for regression**

Run: `cargo test -p bluedb-engine`
Expected: PASS — existing `rest_sql` callers still compile and behave (`execute_insert` signature unchanged).

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-engine/src/rest_sql.rs crates/bluedb-engine/tests/params.rs
git commit -m "feat(engine): parameterized rest_sql via execute_with_params + insert batch"
```

---

## Task 7: Server — route single-object vs array; batch on the serialized connection

**Files:**
- Modify: `crates/bluedb-server/src/lib.rs` — the `insert` handler (`:238-248`) and `build_insert` (`:318-358`)
- Test: `crates/bluedb-server/tests/insert_batch.rs` (new) — if the crate has an HTTP test harness; otherwise assert via the engine path already covered in Task 6 and verify by manual `curl` in Step 4.

- [ ] **Step 1: Make `build_insert` report whether the body was an array**

In `crates/bluedb-server/src/lib.rs`, change `build_insert` to return whether the body was a JSON array (so the handler can pick autocommit vs batch). Replace its signature and the `objects` match:

```rust
/// Returns the request plus `is_batch` (true when the JSON body was an array,
/// i.e. a multi-row insert that should run as one server-side transaction).
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
```

…and change the final return of `build_insert` from `Ok(InsertRequest { table, columns, rows })` to:

```rust
    Ok((InsertRequest { table, columns, rows }, is_batch))
```

- [ ] **Step 2: Update the `insert` handler to route**

Replace the `insert` handler body (`crates/bluedb-server/src/lib.rs:238-248`):

```rust
/// `POST /tables/{table}` — INSERT (JSON object → autocommit; array → one txn batch).
async fn insert(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    let (req, is_batch) = build_insert(table, body)?;
    let payloads = if is_batch {
        // Multi-row: atomic BEGIN..COMMIT on the serialized (lease-holding) connection.
        let mut glue = Glue::new(state.connection_serialized().await?);
        rest_sql::execute_insert_batch(&mut glue, &req).await?
    } else {
        // Single row: autocommit on the group-commit connection (concurrent fast path).
        let mut glue = Glue::new(state.connection().await?);
        rest_sql::execute_insert(&mut glue, &req).await?
    };
    Ok(Json(payloads_to_json(payloads)))
}
```

- [ ] **Step 3: Build the workspace**

Run: `cargo build -p bluedb-server`
Expected: builds clean. (`json_scalar_to_dsl` is still used by `build_insert` to fill `rows` — unchanged.)

- [ ] **Step 4: Manual end-to-end verification**

Run the server against an in-memory store and exercise both paths:

```bash
# terminal 1
BLUEDB_DB_PATH=ittest cargo run -p bluedb-server &
# terminal 2
curl -s -XPOST localhost:8080/sql -d 'CREATE TABLE docs (id INTEGER, body TEXT);'
# single object → autocommit
curl -s -XPOST localhost:8080/tables/docs -H 'content-type: application/json' -d '{"id":1,"body":"hi"}'
# array → batch (atomic)
curl -s -XPOST localhost:8080/tables/docs -H 'content-type: application/json' \
  -d '[{"id":2,"body":"a"},{"id":3,"body":"b"}]'
# injection payload is stored as data, table survives
curl -s -XPOST localhost:8080/tables/docs -H 'content-type: application/json' \
  -d '{"id":4,"body":"'"'"'); DROP TABLE docs; --"}'
curl -s 'localhost:8080/tables/docs?order=id.asc'
```

Expected: the final GET returns 4 rows including the literal injection payload in `body`; `docs` still exists.

> Note: `/sql` here is still the legacy raw endpoint — it is replaced/locked down in plan **A3**. This step only uses it to create the table for the manual check.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-server/src/lib.rs
git commit -m "feat(server): route single-object insert (autocommit) vs array (txn batch)"
```

---

## Task 8: Remove the interpolating render paths (kill the old injection surface)

**Files:**
- Modify: `crates/bluedb-rest/src/model.rs` (`render_value`, `Filter::to_sql`, `render_in_list`), `crates/bluedb-rest/src/render.rs` (`to_sql` impls, `render_where`), `crates/bluedb-rest/src/lib.rs` (drop `render_value` from exports)

- [ ] **Step 1: Find every remaining caller of the old string paths**

Run: `cargo build --workspace 2>&1 | rg 'to_sql\b|render_value|render_where' ; rg -n 'to_sql\b|render_value' crates/*/src`
Expected: only test code and the now-dead methods reference them. If any non-test production caller remains (outside `rest_sql.rs`, which Task 6 already migrated), migrate it to the `*_with_params` form first.

- [ ] **Step 2: Delete the dead string-interpolation methods**

Remove from `crates/bluedb-rest/src/model.rs`: `render_value`, `quote_string`, `render_in_list`, and `Filter::to_sql`. Remove from `crates/bluedb-rest/src/render.rs`: `render_where` and the three `to_sql` impls (`RestQuery`/`UpdateRequest`/`DeleteRequest`) and `InsertRequest::to_sql`. Remove `render_value` from the `lib.rs:76` re-export. Update any tests in those files that asserted on the old `to_sql`/`render_value` to use the `*_with_params` forms (the `param_tests`/`params_render` modules already cover the new behavior — delete the obsolete literal-rendering tests).

- [ ] **Step 3: Build + test the whole workspace**

Run: `cargo build --workspace && cargo test --workspace`
Expected: clean build (no `unused` warnings for the removed items), all tests pass. There is now **no** code path that interpolates a value into SQL on the data plane — every value is a `$N` bind.

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-rest/src
git commit -m "refactor(rest): remove value-interpolation paths; data plane is param-only"
```

---

## Self-Review

**1. Spec coverage (Spec A §4.1 — DML surface):**
- "render layer emits `$N` + params" → Tasks 1–4. ✓
- "JSON scalar → typed value" → `render_param` (Task 1) + `param_to_literal` (Task 6); behavior-preserving (JSON-type preservation explicitly deferred). ✓
- "identifiers allow-listed" → `validate_ident` retained on every path. ✓
- "engine uses `execute_with_params`" → Task 6. ✓
- "array insert → server `BEGIN…COMMIT` of single-row param inserts, global `$N`, one flush, serialized connection" → Tasks 4, 6, 7. ✓
- "single object → autocommit group-commit connection" → Task 7. ✓
- The broader four-surface split, `/sql`, DDL, Admin, authz, `flush_interval`, HTTP/2 are **other plans** (A2–A4), not A1. ✓ (in-scope boundary honored)

**2. Placeholder scan:** No TBD/TODO; every code step has complete code; commands have expected output. The one "verify the import path" note (Task 6 Step 3) is a real verification action with a concrete fallback, not a placeholder.

**3. Type consistency:** `Param` (model.rs) → re-exported (Task 5) → consumed by `param_to_literal` (Task 6) with arms for all five variants. `to_sql_with_params` returns `(String, Vec<Param>)` consistently across query/update/delete; inserts return `(Vec<String>, Vec<Param>)` via `row_statements_with_params`. `bind` returns `$N` from `params.len()` (1-based) — matches gluesql positional binding. `execute_insert` signature is unchanged (existing callers safe); `execute_insert_batch` is additive.

---

## Execution Handoff

(Filled in after the plan is approved — see the writing-plans handoff.)
