# Lakehouse Schema-Evolution Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reconcile `ALTER TABLE` ADD/DROP/RENAME column into a table's Iceberg mirror so the warehouse view stays correct, removing the "avoid ALTER on a mirrored table" limitation.

**Architecture:** Derive Iceberg field-ids from the bluedb `ColumnCatalog` physical slot (`field_id = slot + 1`; identity for never-altered tables → byte-identical to today's positional ids). At seal time the writer reconciles the persisted Iceberg schema toward the freshly-computed (slot-based) schema — reuse-by-id, rename, add, drop — and self-authors a schema-only metadata commit via iceberg-rust's `TableMetadataBuilder::add_current_schema`. Because the evolved schema's field order is the current logical column order, the existing positional record-batch construction stays correct.

**Tech Stack:** Rust, gluesql-core 0.19, iceberg-rust 0.9.1, tokio. Workspace at `/Users/satya/work/bc/bluedb`. Spec: `docs/superpowers/specs/2026-06-15-bluedb-lakehouse-schema-evolution-design.md`.

**Conventions:** commit-only (NEVER push). Commit trailer `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Use `git commit -F -` with a heredoc (avoid backticks in `-m`). NEVER run `cargo fmt` (rustfmt skew). "bluecopa" always lowercase. Run tests directly with `2>&1`, no tail/grep pipes.

---

### Task 1: `column_slots` accessor on the storage connection

**Files:**
- Modify: `crates/bluedb-sql/src/storage.rs` (add a public method next to `pk_columns` at ~line 645)
- Test: `crates/bluedb-sql/src/storage.rs` (the existing `#[cfg(test)] mod tests` in this crate, or a new integration test if no inline module — see Step 1)

- [ ] **Step 1: Write the failing test**

Add to `crates/bluedb-sql/tests/` a new file `column_slots.rs`:

```rust
//! `SlateDbStorage::column_slots` exposes the colcat slot mapping the lakehouse
//! turns into stable Iceberg field-ids.
use bluedb_sql::Database;

async fn db() -> Database {
    Database::open_in_memory().await.unwrap()
}

#[tokio::test]
async fn never_altered_table_has_no_slots() {
    let db = db().await;
    let conn = db.connection_for_tenant("_");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)")
        .await
        .unwrap();
    // Identity (no catalog persisted) ⇒ None.
    assert_eq!(conn.column_slots("t").await.unwrap(), None);
}

#[tokio::test]
async fn dropped_column_leaves_a_gap_in_slots() {
    let db = db().await;
    let conn = db.connection_for_tenant("_");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
        .await
        .unwrap();
    conn.execute("ALTER TABLE t DROP COLUMN a").await.unwrap();
    // slots were [0,1,2]; dropping logical idx 1 (`a`) leaves [0,2].
    assert_eq!(conn.column_slots("t").await.unwrap(), Some(vec![0, 2]));
}

#[tokio::test]
async fn added_column_appends_a_new_slot() {
    let db = db().await;
    let conn = db.connection_for_tenant("_");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)")
        .await
        .unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN c TEXT").await.unwrap();
    // [0,1] then append slot 2 ⇒ [0,1,2].
    assert_eq!(conn.column_slots("t").await.unwrap(), Some(vec![0, 1, 2]));
}
```

Note: confirm the test setup helpers (`Database::open_in_memory`, `connection_for_tenant`, `execute`) match this crate's existing test conventions — open an existing file in `crates/bluedb-sql/tests/` (e.g. `pk_range.rs`) and copy its harness exactly. Adjust the calls above to match (the assertions on `column_slots` are the point).

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p bluedb-sql --test column_slots 2>&1`
Expected: FAIL — `no method named column_slots`.

- [ ] **Step 3: Add the accessor**

In `crates/bluedb-sql/src/storage.rs`, immediately after the `pk_columns` method (ends ~line 647), add:

```rust
    /// The stable physical slot of each current logical column (in schema
    /// order), or `None` for a never-altered table (identity: slot == position).
    /// The lakehouse mirror derives Iceberg field-ids as `slot + 1`, so a column
    /// keeps its field-id across ADD/DROP/RENAME. Mirrors [`Self::pk_columns`].
    pub async fn column_slots(&self, table_name: &str) -> Result<Option<Vec<u32>>, SqlError> {
        Ok(self.read_catalog(table_name).await?.map(|c| c.slots))
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p bluedb-sql --test column_slots 2>&1`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-sql/src/storage.rs crates/bluedb-sql/tests/column_slots.rs
git commit -F - <<'EOF'
feat(schema-evo): expose ColumnCatalog slots via column_slots accessor

The lakehouse derives stable Iceberg field-ids as slot+1; None means a
never-altered (identity) table.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task 2: Reject DROP of a primary-key / composite-key component column

**Files:**
- Modify: `crates/bluedb-sql/src/storage.rs` — `drop_column` (lines 1431-1464)
- Test: `crates/bluedb-sql/tests/column_slots.rs` (extend) or a new `crates/bluedb-sql/tests/alter_key_guard.rs`

- [ ] **Step 1: Write the failing test**

Add to `crates/bluedb-sql/tests/alter_key_guard.rs` (use the same harness as Task 1):

```rust
use bluedb_sql::Database;

#[tokio::test]
async fn cannot_drop_single_pk_column() {
    let db = Database::open_in_memory().await.unwrap();
    let conn = db.connection_for_tenant("_");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)")
        .await
        .unwrap();
    let err = conn.execute("ALTER TABLE t DROP COLUMN id").await.unwrap_err();
    assert!(
        format!("{err}").to_lowercase().contains("primary key"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn cannot_drop_composite_component_column() {
    let db = Database::open_in_memory().await.unwrap();
    let conn = db.connection_for_tenant("_");
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER, c TEXT, PRIMARY KEY (a, b))")
        .await
        .unwrap();
    let err = conn.execute("ALTER TABLE t DROP COLUMN a").await.unwrap_err();
    assert!(
        format!("{err}").to_lowercase().contains("primary key"),
        "unexpected error: {err}"
    );
    // A non-key column still drops fine.
    conn.execute("ALTER TABLE t DROP COLUMN c").await.unwrap();
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p bluedb-sql --test alter_key_guard 2>&1`
Expected: FAIL — `id`/`a` drop currently succeeds (no error), so `unwrap_err` panics.

- [ ] **Step 3: Implement the guardrail**

In `crates/bluedb-sql/src/storage.rs`, inside `drop_column`, after the column index `i` is resolved (after line 1452, before `column_defs.remove(i)`), insert:

```rust
        // A key column is the merge-on-read identity and the clustering key —
        // dropping it would orphan the lakehouse equality-delete identifier and
        // the sort order, and is meaningless for an index-organized table. Reject
        // it (mirrors the UPDATE-of-key-column non-goal).
        let dropping_single_pk = column_defs[i]
            .unique
            .as_ref()
            .is_some_and(|u| u.is_primary);
        let dropping_component = self
            .read_pk_catalog(table_name)
            .await?
            .is_some_and(|cat| cat.columns.iter().any(|c| c == column_name));
        if dropping_single_pk || dropping_component || column_name == crate::compositepk::PK_COL {
            return Err(SqlError::CompositePk(format!(
                "cannot DROP COLUMN `{column_name}`: it is part of the PRIMARY KEY \
                 (drop is rejected — change the key by recreating the table)"
            ))
            .into());
        }
```

Check two facts before writing: (a) `read_pk_catalog` is reachable from `drop_column` (it is — both are methods on `SlateDbStorage`, `read_pk_catalog` at line 631); (b) `SqlError` converts into gluesql's error via `From`/`.into()` here — confirm by how other bluedb-specific errors are returned in this file (search `SqlError::` returns inside `impl ... for SlateDbStorage`). If `SqlError` does not `Into<gluesql_core::error::Error>` in this context, return the same way neighboring bluedb errors do (e.g. wrap via the crate's existing error bridge). The composite-PK rewrite already raises `SqlError::CompositePk` for UPDATE-of-key — match that exact return pattern.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p bluedb-sql --test alter_key_guard 2>&1`
Expected: PASS (2 tests).

- [ ] **Step 5: Run the full crate suite (no regression)**

Run: `cargo test -p bluedb-sql 2>&1`
Expected: all pass (lib + gluesql suite + integration).

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-sql/src/storage.rs crates/bluedb-sql/tests/alter_key_guard.rs
git commit -F - <<'EOF'
feat(schema-evo): reject DROP COLUMN of a primary-key / composite component

A key column is the merge-on-read identity and clustering key; dropping it
is rejected (mirrors the UPDATE-of-key-column non-goal).

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task 3: Slot-based field-ids in `table_to_iceberg`

**Files:**
- Modify: `crates/bluedb-lakehouse/src/schema.rs` — `table_to_iceberg` signature + field-id loop
- Modify: `crates/bluedb-lakehouse/src/engine.rs` — `writer_for` (pass slots), `sort_field_ids` (slot-based)
- Test: `crates/bluedb-lakehouse/src/schema.rs` `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing tests**

Add to the `mod tests` in `crates/bluedb-lakehouse/src/schema.rs`. First a helper to build a minimal gluesql `Schema` with a PK on column 0:

```rust
    use gluesql_core::ast::{ColumnDef, ColumnUniqueOption};

    fn schema_with(cols: &[(&str, DataType)]) -> GlueSchema {
        let column_defs = cols
            .iter()
            .enumerate()
            .map(|(i, (name, dt))| ColumnDef {
                name: name.to_string(),
                data_type: dt.clone(),
                nullable: i != 0,
                default: None,
                unique: if i == 0 {
                    Some(ColumnUniqueOption { is_primary: true })
                } else {
                    None
                },
                comment: None,
            })
            .collect();
        GlueSchema {
            table_name: "t".into(),
            column_defs: Some(column_defs),
            indexes: vec![],
            engine: None,
            foreign_keys: vec![],
            comment: None,
        }
    }

    fn field_ids(s: &IcebergSchema) -> Vec<i32> {
        s.as_struct().fields().iter().map(|f| f.id).collect()
    }

    #[test]
    fn identity_slots_give_positional_field_ids() {
        let s = schema_with(&[("id", DataType::Int), ("a", DataType::Text)]);
        let (ice, pk) = table_to_iceberg(&s, &[], None).unwrap();
        assert_eq!(field_ids(&ice), vec![1, 2]); // back-compat with today
        assert_eq!(pk, 1);
    }

    #[test]
    fn slots_drive_field_ids_after_drop() {
        // logical [id, b] after dropping middle `a`; slots [0, 2].
        let s = schema_with(&[("id", DataType::Int), ("b", DataType::Text)]);
        let (ice, pk) = table_to_iceberg(&s, &[], Some(&[0, 2])).unwrap();
        assert_eq!(field_ids(&ice), vec![1, 3]);
        assert_eq!(pk, 1);
    }

    #[test]
    fn added_slot_gets_its_own_field_id() {
        // logical [id, a, c]; `c` added at slot 2 ⇒ ids [1,2,3].
        let s = schema_with(&[
            ("id", DataType::Int),
            ("a", DataType::Text),
            ("c", DataType::Text),
        ]);
        let (ice, _) = table_to_iceberg(&s, &[], Some(&[0, 1, 2])).unwrap();
        assert_eq!(field_ids(&ice), vec![1, 2, 3]);
    }
```

Check the `ColumnDef` field list against this gluesql version before running (open `crates/bluedb-sql` usages or the gluesql source); fix the struct literal to match exactly (field names/order may differ — `comment`, `foreign_keys`, etc.). The assertions are the contract.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p bluedb-lakehouse --lib schema 2>&1`
Expected: FAIL — `table_to_iceberg` takes 2 args, not 3.

- [ ] **Step 3: Change `table_to_iceberg` to slot-based ids**

In `crates/bluedb-lakehouse/src/schema.rs`, change the signature and the field-id assignment:

```rust
pub fn table_to_iceberg(
    schema: &GlueSchema,
    sample_rows: &[&[Value]],
    slots: Option<&[u32]>,
) -> Result<(IcebergSchema, i32)> {
```

Replace the field-id derivation. After `let columns = ...` and the `pk_idx` computation, compute the per-column field-id from slots:

```rust
    let n = columns.len();
    // field_id(column i) = slot(i) + 1. Absent catalog ⇒ identity (slot == i),
    // which reproduces the historical positional ids 1..N exactly.
    let field_id_of = |i: usize| -> i32 {
        match slots {
            Some(s) => s[i] as i32 + 1,
            None => i as i32 + 1,
        }
    };
    // Nested (list/map) ids start above the max top-level id so they never
    // collide with a slot-based id.
    let max_top = (0..n).map(|i| field_id_of(i)).max().unwrap_or(0);
    let mut next_nested_id = max_top + 1;
    let mut fields = Vec::with_capacity(n);
    for (col_idx, col) in columns.iter().enumerate() {
        let field_id = field_id_of(col_idx);
        let cells: Vec<&Value> = sample_rows
            .iter()
            .filter_map(|r| r.get(col_idx))
            .collect();
        let ty = iceberg_type(&col.data_type, &cells, &mut next_nested_id)?;
        let field = if col_idx == pk_idx || !col.nullable {
            NestedField::required(field_id, &col.name, ty)
        } else {
            NestedField::optional(field_id, &col.name, ty)
        };
        fields.push(field.into());
    }

    let pk_field_id = field_id_of(pk_idx);
```

If `slots` length disagrees with `columns.len()`, that's a caller bug — add a guard at the top of the function:

```rust
    if let Some(s) = slots {
        if s.len() != columns.len() {
            return Err(LakehouseError::Schema(format!(
                "table '{}': slot vector len {} != column count {}",
                schema.table_name,
                s.len(),
                columns.len()
            )));
        }
    }
```

(Place this guard after `columns` is bound.)

- [ ] **Step 4: Update the engine call sites**

In `crates/bluedb-lakehouse/src/engine.rs`, `writer_for` (line 337) currently calls `table_to_iceberg(schema, sample_rows)`. Fetch slots and pass them:

```rust
    pub async fn writer_for(
        &self,
        table: &str,
        schema: &gluesql_core::data::Schema,
        sample_rows: &[&[gluesql_core::data::Value]],
    ) -> Result<LakehouseWriter> {
        let slots = self
            .db
            .connection_for_tenant(&self.tenant)
            .column_slots(table)
            .await
            .map_err(LakehouseError::Sql)?;
        let (iceberg_schema, pk_field_id) =
            table_to_iceberg(schema, sample_rows, slots.as_deref())?;
        let sort_field_ids = self.sort_field_ids(table, schema, pk_field_id, slots.as_deref()).await?;
        LakehouseWriter::open(
            self.file_io.clone(),
            &self.root,
            &self.namespace,
            table,
            iceberg_schema,
            pk_field_id,
            &sort_field_ids,
        )
        .await
    }
```

Update `sort_field_ids` (line 355) to take `slots` and map each component to its slot-based field-id:

```rust
    async fn sort_field_ids(
        &self,
        table: &str,
        schema: &gluesql_core::data::Schema,
        pk_field_id: i32,
        slots: Option<&[u32]>,
    ) -> Result<Vec<i32>> {
        let components = self
            .db
            .connection_for_tenant(&self.tenant)
            .pk_columns(table)
            .await
            .map_err(LakehouseError::Sql)?;
        let Some(components) = components else {
            return Ok(vec![pk_field_id]); // single-column PK
        };
        let names: Vec<&str> = schema
            .column_defs
            .as_ref()
            .map(|defs| defs.iter().map(|c| c.name.as_str()).collect())
            .unwrap_or_default();
        let field_id_of = |pos: usize| -> i32 {
            match slots {
                Some(s) => s[pos] as i32 + 1,
                None => pos as i32 + 1,
            }
        };
        Ok(components
            .iter()
            .filter_map(|c| names.iter().position(|n| n == c).map(field_id_of))
            .collect())
    }
```

- [ ] **Step 5: Run the schema tests + the lakehouse crate**

Run: `cargo test -p bluedb-lakehouse --lib schema 2>&1`
Expected: PASS (the 3 new tests + existing schema tests).

Run: `cargo test -p bluedb-lakehouse 2>&1`
Expected: all pass — existing mirror tests unchanged (identity tables → same field-ids).

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-lakehouse/src/schema.rs crates/bluedb-lakehouse/src/engine.rs
git commit -F - <<'EOF'
feat(schema-evo): derive Iceberg field-ids from colcat slots

table_to_iceberg takes the slot vector (None = identity, byte-identical to
the old positional ids); the engine threads column_slots through and makes
the sort order slot-based so it stays stable across ALTER.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task 4: Pure `reconcile_schema` helper in the writer

**Files:**
- Modify: `crates/bluedb-lakehouse/src/writer.rs` — add `reconcile_schema` + tests

- [ ] **Step 1: Write the failing tests**

Add a `#[cfg(test)] mod reconcile_tests` at the bottom of `crates/bluedb-lakehouse/src/writer.rs`:

```rust
#[cfg(test)]
mod reconcile_tests {
    use super::reconcile_schema;
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};

    fn field(id: i32, name: &str, req: bool) -> NestedField {
        let ty = Type::Primitive(PrimitiveType::String);
        if req {
            NestedField::required(id, name, ty)
        } else {
            NestedField::optional(id, name, ty)
        }
    }

    fn schema(pk: i32, fields: Vec<NestedField>) -> IcebergSchema {
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![pk])
            .with_fields(fields.into_iter().map(|f| f.into()).collect())
            .build()
            .unwrap()
    }

    fn names(s: &IcebergSchema) -> Vec<(i32, String)> {
        s.as_struct()
            .fields()
            .iter()
            .map(|f| (f.id, f.name.clone()))
            .collect()
    }

    #[test]
    fn identical_schema_is_noop() {
        let cur = schema(1, vec![field(1, "id", true), field(2, "a", false)]);
        let des = schema(1, vec![field(1, "id", true), field(2, "a", false)]);
        assert!(reconcile_schema(&cur, &des).unwrap().is_none());
    }

    #[test]
    fn add_column_appends_field() {
        let cur = schema(1, vec![field(1, "id", true), field(2, "a", false)]);
        let des = schema(
            1,
            vec![field(1, "id", true), field(2, "a", false), field(3, "c", false)],
        );
        let out = reconcile_schema(&cur, &des).unwrap().unwrap();
        assert_eq!(
            names(&out),
            vec![(1, "id".into()), (2, "a".into()), (3, "c".into())]
        );
    }

    #[test]
    fn drop_column_removes_field_keeping_ids() {
        // current [id(1), a(2), b(3)] → desired drops `a` → [id(1), b(3)].
        let cur = schema(
            1,
            vec![field(1, "id", true), field(2, "a", false), field(3, "b", false)],
        );
        let des = schema(1, vec![field(1, "id", true), field(3, "b", false)]);
        let out = reconcile_schema(&cur, &des).unwrap().unwrap();
        assert_eq!(names(&out), vec![(1, "id".into()), (3, "b".into())]);
    }

    #[test]
    fn rename_keeps_id_changes_name() {
        let cur = schema(1, vec![field(1, "id", true), field(2, "a", false)]);
        let des = schema(1, vec![field(1, "id", true), field(2, "alpha", false)]);
        let out = reconcile_schema(&cur, &des).unwrap().unwrap();
        assert_eq!(names(&out), vec![(1, "id".into()), (2, "alpha".into())]);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p bluedb-lakehouse --lib reconcile 2>&1`
Expected: FAIL — `reconcile_schema` not found.

- [ ] **Step 3: Implement `reconcile_schema`**

Add as a free function in `crates/bluedb-lakehouse/src/writer.rs` (near `sort_order_on`). It iterates the desired fields in order, reusing the persisted field by id (preserving its type/nested ids; only renaming when the name changed), and returns `None` when the result equals current.

```rust
/// Evolve the persisted Iceberg schema `current` toward the freshly-computed
/// `desired` (slot-based field-ids, current logical column order). Reuse-by-id:
/// a desired field whose id already exists in `current` carries the persisted
/// field's *type* forward (preserving nested list/map child ids), changing only
/// the name when it differs; a desired id absent from `current` is a brand-new
/// column; a current id absent from `desired` is a dropped column (omitted).
///
/// The result's field order is `desired`'s order (the current logical order), so
/// the writer's positional record-batch construction stays correct. Returns
/// `None` when the evolved schema is field-for-field identical to `current` (the
/// never-altered / no-op case), so the caller emits no schema commit.
fn reconcile_schema(
    current: &IcebergSchema,
    desired: &IcebergSchema,
) -> Result<Option<IcebergSchema>> {
    use iceberg::spec::NestedField;

    let cur_struct = current.as_struct();
    let evolved: Vec<_> = desired
        .as_struct()
        .fields()
        .iter()
        .map(|d| {
            match cur_struct.field_by_id(d.id) {
                // Reuse the persisted field (its type, incl. nested ids); rename
                // if the name changed. Keep the desired field's requiredness so a
                // column that became required/optional is honored.
                Some(existing) => {
                    let mut f = NestedField::new(
                        existing.id,
                        d.name.clone(),
                        existing.field_type.as_ref().clone(),
                        d.required,
                    );
                    f.initial_default = existing.initial_default.clone();
                    f.write_default = existing.write_default.clone();
                    Arc::new(f)
                }
                // Brand-new column: take the desired field verbatim.
                None => d.clone(),
            }
        })
        .collect();

    let rebuilt = IcebergSchema::builder()
        .with_schema_id(current.schema_id())
        .with_identifier_field_ids(desired.identifier_field_ids().iter().copied())
        .with_fields(evolved)
        .build()
        .map_err(|e| LakehouseError::Schema(format!("reconciling schema: {e}")))?;

    // No-op when field ids/names/order/types match current exactly.
    if schemas_equivalent(current, &rebuilt) {
        Ok(None)
    } else {
        Ok(Some(rebuilt))
    }
}

/// Two schemas are equivalent for reconcile purposes when their top-level fields
/// match by (id, name, required, type) in the same order. (Schema-id differs by
/// construction, so we compare the struct, not the whole schema.)
fn schemas_equivalent(a: &IcebergSchema, b: &IcebergSchema) -> bool {
    let fa = a.as_struct().fields();
    let fb = b.as_struct().fields();
    fa.len() == fb.len()
        && fa.iter().zip(fb.iter()).all(|(x, y)| {
            x.id == y.id
                && x.name == y.name
                && x.required == y.required
                && x.field_type == y.field_type
        })
}
```

Verify against iceberg-rust 0.9.1 before running:
- `StructType::field_by_id(i32) -> Option<&NestedFieldRef>` exists (check `~/.cargo/registry/src/*/iceberg-0.9.1/src/spec/datatypes.rs`). If the accessor differs (e.g. returns `Arc`), adjust the `match` arm. If there is no `field_by_id`, build a `HashMap<i32, &NestedFieldRef>` from `cur_struct.fields()` once.
- `NestedField` fields: `id`, `name`, `field_type: Type` (or `Arc<Type>`), `required: bool`, `initial_default`, `write_default`. If `field_type` is `Type` (not `Arc`), drop the `.as_ref().clone()` → `.clone()`. If `initial_default`/`write_default` don't exist or aren't `Clone`, omit those two lines.
- `Schema::identifier_field_ids()` returns an iterator/slice of `i32` — adjust `.iter().copied()` to match (it may already be `&[i32]`).

Keep the implementation minimal; the tests pin the behavior.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p bluedb-lakehouse --lib reconcile 2>&1`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-lakehouse/src/writer.rs
git commit -F - <<'EOF'
feat(schema-evo): pure reconcile_schema (reuse-by-id, rename, add, drop)

Evolves the persisted Iceberg schema toward the slot-based desired schema in
logical order; returns None for the no-op (never-altered) case.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task 5: Wire reconcile into `LakehouseWriter::open` (self-authored schema commit)

**Files:**
- Modify: `crates/bluedb-lakehouse/src/writer.rs` — the existing-table branch of `open`

- [ ] **Step 1: Write the failing test**

Add to `crates/bluedb-lakehouse/src/writer.rs` a `#[cfg(test)] mod open_evolve_tests` that creates a table, then reopens it with a desired schema that adds a column, and asserts the reloaded `metadata.current_schema()` has the new field. Use `open_local` plus a direct `open` with a 3-field schema:

```rust
#[cfg(test)]
mod open_evolve_tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};

    fn s(fields: Vec<NestedField>) -> IcebergSchema {
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .with_fields(fields.into_iter().map(|f| f.into()).collect())
            .build()
            .unwrap()
    }
    fn req_int(id: i32, n: &str) -> NestedField {
        NestedField::required(id, n, Type::Primitive(PrimitiveType::Int))
    }
    fn opt_str(id: i32, n: &str) -> NestedField {
        NestedField::optional(id, n, Type::Primitive(PrimitiveType::String))
    }

    #[tokio::test]
    async fn reopen_with_added_column_evolves_schema() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        // Create with [id, a].
        let w = LakehouseWriter::open_local(root, "default", "t", s(vec![req_int(1, "id"), opt_str(2, "a")]), 1)
            .await
            .unwrap();
        drop(w);
        // Reopen with [id, a, c] — `c` added.
        let w2 = LakehouseWriter::open_local(
            root,
            "default",
            "t",
            s(vec![req_int(1, "id"), opt_str(2, "a"), opt_str(3, "c")]),
            1,
        )
        .await
        .unwrap();
        let cols: Vec<String> = w2
            .to_table()
            .unwrap()
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();
        assert_eq!(cols, vec!["id", "a", "c"]);
    }
}
```

Add `tempfile` to `crates/bluedb-lakehouse/Cargo.toml` `[dev-dependencies]` if not present (check first — existing writer tests likely already use a temp dir; copy their pattern instead).

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p bluedb-lakehouse --lib open_evolve 2>&1`
Expected: FAIL — reopened schema is still `[id, a]` (frozen).

- [ ] **Step 3: Wire reconcile into `open`**

In `crates/bluedb-lakehouse/src/writer.rs`, the existing-table branch of `open` (lines 117-137) currently loads metadata and sets `let schema = metadata.current_schema().clone();`. Replace that branch body so it reconciles the loaded schema toward the passed `schema` (the desired, slot-based one):

```rust
        if file_io.exists(&hint_path).await? {
            let raw = file_io.new_input(&hint_path)?.read().await?;
            let version: u64 = String::from_utf8_lossy(&raw)
                .trim()
                .parse()
                .map_err(|e| LakehouseError::Iceberg(format!("bad version-hint: {e}")))?;
            let md_path = format!("{table_root}/metadata/v{version}.metadata.json");
            let bytes = file_io.new_input(&md_path)?.read().await?;
            let metadata: TableMetadata = serde_json::from_slice(&bytes)?;

            let mut writer = Self {
                file_io,
                table_root,
                table_ident,
                schema: metadata.current_schema().clone(),
                pk_field_id,
                metadata,
                version,
                pending_data: Vec::new(),
                pending_deletes: Vec::new(),
            };
            // Reconcile any ALTER since the last seal into the Iceberg schema,
            // self-authoring a schema-only metadata commit. No-op when unchanged.
            writer.evolve_schema_to(&schema).await?;
            Ok(writer)
        } else {
```

Then add the method:

```rust
    /// If `desired` differs from the current Iceberg schema, evolve to it by
    /// self-authoring a schema-only metadata version (`add_current_schema`) and
    /// adopt the evolved schema for subsequent writes. No-op when equal — the
    /// common (never-altered) path, so an unchanged table pays only a comparison.
    async fn evolve_schema_to(&mut self, desired: &IcebergSchema) -> Result<()> {
        let Some(evolved) = reconcile_schema(self.metadata.current_schema(), desired)? else {
            return Ok(());
        };
        let current_md_loc = format!("{}/v{}.metadata.json", self.metadata_dir(), self.version);
        let result = self
            .metadata
            .clone()
            .into_builder(Some(current_md_loc))
            .add_current_schema(evolved)?
            .build()?;
        self.metadata = result.metadata;
        self.version += 1;
        self.schema = self.metadata.current_schema().clone();
        self.write_metadata(self.version).await?;
        Ok(())
    }
```

Note `open` already returns `Result`; `evolve_schema_to`'s `?` propagates. `reconcile_schema` and `add_current_schema` were verified in Task 4 / the de-risk. The `into_builder(...).add_current_schema(...).build()` returns a struct with a `.metadata` field (same shape used by `commit_internal` at line 549-556).

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p bluedb-lakehouse --lib open_evolve 2>&1`
Expected: PASS.

- [ ] **Step 5: Run the full lakehouse crate**

Run: `cargo test -p bluedb-lakehouse 2>&1`
Expected: all pass (existing mirror/compaction/composite tests unaffected — identity tables reconcile to no-op).

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-lakehouse/src/writer.rs crates/bluedb-lakehouse/Cargo.toml
git commit -F - <<'EOF'
feat(schema-evo): reconcile schema on writer open (self-authored commit)

open() now evolves the persisted Iceberg schema toward the freshly-computed
slot-based schema via add_current_schema, so ALTER ADD/DROP/RENAME since the
last seal is reflected before the next data snapshot. No-op when unchanged.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task 6: End-to-end round-trip tests through the seal + iceberg reader

**Files:**
- Create: `crates/bluedb-lakehouse/tests/schema_evolution.rs`

- [ ] **Step 1: Write the tests**

Model the harness on the existing `crates/bluedb-lakehouse/tests/composite_mirror.rs` (open it and reuse its engine/seal/read-back helpers verbatim — same `LakehouseEngine` construction, `seal`, and iceberg-reader read-back). Then add three tests:

```rust
// Pseudocode shape — fill in using composite_mirror.rs's exact helpers.
// Helper assumed: `read_back(engine, table) -> Vec<RecordBatch>` via iceberg reader,
// and `rows_as_strings(batches)` or equivalent already used there.

#[tokio::test]
async fn add_column_appears_in_mirror() {
    // 1. CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT); enable mirror.
    // 2. INSERT (1,'x'); seal.
    // 3. ALTER TABLE t ADD COLUMN c INTEGER; INSERT (2,'y',7); seal.
    // 4. read back: schema has [id,a,c]; row id=2 has c=7; row id=1 has c=NULL.
}

#[tokio::test]
async fn drop_column_removed_from_mirror_without_misalignment() {
    // 1. CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT); mirror on.
    // 2. INSERT (1,'x','keep'); seal.
    // 3. ALTER TABLE t DROP COLUMN a; INSERT (2,'kept2'); seal.
    // 4. read back: schema is [id,b]; b values are 'keep'/'kept2' (NOT shifted
    //    from the dropped `a`), proving no column misalignment.
}

#[tokio::test]
async fn rename_column_uses_new_name_in_mirror() {
    // 1. CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT); mirror on.
    // 2. INSERT (1,'x'); seal.
    // 3. ALTER TABLE t RENAME COLUMN a TO alpha; INSERT (2,'y'); seal.
    // 4. read back: schema field is `alpha` (field-id 2 preserved); values intact.
}
```

The `drop_column_...` test is the critical one: assert the surviving column's values are the original `b` values, which fails today (misalignment) and passes after Task 5.

- [ ] **Step 2: Run to verify the drop test fails on the pre-Task-5 path**

This task runs after Task 5, so all three should pass. To prove the drop test is meaningful, temporarily revert Task 5's `evolve_schema_to` call (comment it out), run, observe the drop test fail with misaligned/!errored data, then restore. (Optional sanity check — skip if confident.)

Run: `cargo test -p bluedb-lakehouse --test schema_evolution 2>&1`
Expected: PASS (3 tests) with Task 5 in place.

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-lakehouse/tests/schema_evolution.rs
git commit -F - <<'EOF'
test(schema-evo): ADD/DROP/RENAME round-trip through seal + iceberg reader

Reads the mirror back through iceberg-rust's own reader after each ALTER; the
DROP case asserts no column misalignment.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task 7: HTTP e2e + guardrail e2e through the server

**Files:**
- Modify/Create: a test in `crates/bluedb-engine/tests/` or `crates/bluedb-server/tests/` — model on the existing lakehouse HTTP e2e (search for the test that enables the mirror via PRAGMA and reads `/catalog/v1/...`).

- [ ] **Step 1: Write the test**

Find the existing lakehouse HTTP e2e (grep `lakehouse_mirror` under `crates/bluedb-server/tests` / `crates/bluedb-engine/tests`) and copy its setup. Add a test that:
1. Creates a mirrored table, inserts a row, triggers a seal (the existing tests show how — `AppState::seal_now` or the debounce wait).
2. Runs `ALTER TABLE t ADD COLUMN c INTEGER`, inserts a row with `c`, seals.
3. Loads the table via the REST catalog (`GET /catalog/v1/namespaces/default/tables/t`) and asserts the returned schema JSON contains a field named `c`.

```rust
// Shape — adapt to the existing harness's request helpers.
#[tokio::test]
async fn altered_column_visible_through_rest_catalog() {
    // ... boot server with lakehouse enabled (copy existing e2e setup) ...
    // POST /sql  PRAGMA lakehouse_mirror_table('t', on)  (after CREATE TABLE t)
    // POST /sql  INSERT ...; wait for seal
    // POST /sql  ALTER TABLE t ADD COLUMN c INTEGER
    // POST /sql  INSERT ... with c; wait for seal
    // GET /catalog/v1/namespaces/default/tables/t -> JSON
    // assert the schema's fields include "c"
}
```

Also add the guardrail e2e (can live in the same file): `ALTER TABLE t DROP COLUMN id` over `/sql` returns an error mentioning the primary key.

- [ ] **Step 2: Run the test**

Run: `cargo test -p bluedb-server --test <file> 2>&1` (or `-p bluedb-engine` depending on where it lands)
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-server/tests/  # or bluedb-engine/tests/
git commit -F - <<'EOF'
test(schema-evo): HTTP e2e — ALTER ADD visible via REST catalog; DROP-key rejected

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task 8: Docs

**Files:**
- Modify: `docs/lakehouse/iceberg-mirror.md` — replace the "Schema evolution" limitation with a working "Schema evolution" subsection
- Modify: `docs/concepts/architecture.md` — note the colcat-slot ↔ Iceberg-field-id link

- [ ] **Step 1: Update `docs/lakehouse/iceberg-mirror.md`**

Remove the first bullet under "## Limitations (v1)" (the "Schema evolution on a mirrored table … avoid `ALTER`" bullet, lines ~203-207) and add a new subsection before "## Limitations (v1)":

```markdown
## Schema evolution

`ALTER TABLE` on a mirrored table is reconciled into Iceberg automatically. Each
column carries a **stable Iceberg field-id** derived from its bluedb column
catalog slot, so:

- **ADD COLUMN** appears as a new Iceberg field on the next seal; existing rows
  read back with `NULL` (or the column's default).
- **DROP COLUMN** removes the field from the current Iceberg schema (old data
  files are still readable — Iceberg projects them through the current schema).
- **RENAME COLUMN** keeps the field-id and changes the name, so warehouse queries
  use the new name with no data rewrite.
- **RENAME TABLE** is an O(1) metadata op and unaffected.

The mirror self-authors a standard Iceberg schema-evolution commit (a new schema
version) before the next data snapshot, so warehouse readers see a normal
evolving Iceberg table.

**Not yet reconciled:** changing a column's *type*, and adding a `LIST`/`MAP`
column to an already-materialized table. Dropping a primary-key (or composite-key
component) column is rejected — it is the merge-on-read identity and clustering
key.
```

Keep the remaining "Compaction rewrites the whole table" limitation bullet.

- [ ] **Step 2: Update `docs/concepts/architecture.md`**

Under "## Design properties", in the "Online schema evolution" bullet (lines ~123-126), append a sentence linking slots to Iceberg:

```markdown
- **Online schema evolution.** Every table is schema'd with a `PRIMARY KEY`, but
  evolving one is cheap: ADD/DROP/RENAME column and RENAME TABLE are O(1) metadata
  ops (stable field-ids + table-ids), never a row rewrite — no migration window.
  The same stable per-column id (the column-catalog *slot*) is what the
  [Iceberg mirror](../lakehouse/iceberg-mirror.md#schema-evolution) maps to an
  Iceberg field-id, so `ALTER` reconciles into the warehouse view without a
  rewrite.
```

- [ ] **Step 3: Commit**

```bash
git add docs/lakehouse/iceberg-mirror.md docs/concepts/architecture.md
git commit -F - <<'EOF'
docs(schema-evo): document ALTER reconciliation into the Iceberg mirror

Replaces the "avoid ALTER on a mirrored table" limitation with a Schema
evolution section; notes the colcat-slot to Iceberg-field-id mapping.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task 9: Full workspace verification

- [ ] **Step 1: Build + test the whole workspace**

Run: `cargo test --workspace 2>&1`
Expected: all suites pass.

- [ ] **Step 2: Clippy on the touched crates**

Run: `cargo clippy -p bluedb-sql -p bluedb-lakehouse -p bluedb-server 2>&1`
Expected: no new warnings on touched files. (Do NOT run `cargo fmt`.)

- [ ] **Step 3: Final commit (only if Step 1/2 produced fixes)**

```bash
git add -A
git commit -F - <<'EOF'
chore(schema-evo): workspace-green + clippy fixes

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

## Self-Review

**Spec coverage:**
- §1 slot-based field-ids → Task 3 (+ accessor Task 1). ✓
- §2 reconcile + self-authored `add_current_schema` → Tasks 4–5. ✓
- §3 schema in logical order / positional batch stays correct → Task 5 (evolved schema adopted as `self.schema`); validated by Task 6 drop test. ✓
- §4 reject DROP of key column → Task 2 (+ e2e in Task 7). ✓
- Scope/non-goals → documented in Task 8. ✓
- Testing strategy items 1–6 → Tasks 3 (unit), 4 (reconcile unit), 6 (round-trip), 7 (e2e + guardrail), 9 (workspace + clippy). ✓

**Placeholder scan:** Task 6/7 use *pseudocode shapes* deliberately — they instruct copying an existing test harness (composite_mirror.rs / the lakehouse HTTP e2e) whose exact helpers must be reused; the assertions to add are spelled out. This is a real instruction, not a TODO, but the executor must open those files. Flagged here so it isn't mistaken for an unfinished step.

**Type consistency:** `column_slots -> Result<Option<Vec<u32>>, SqlError>` (Task 1) is consumed as `slots.as_deref()` → `Option<&[u32]>` by `table_to_iceberg(.., Option<&[u32]>)` (Task 3) and `sort_field_ids(.., Option<&[u32]>)` (Task 3). `reconcile_schema(&IcebergSchema, &IcebergSchema) -> Result<Option<IcebergSchema>>` (Task 4) is called by `evolve_schema_to` (Task 5). Consistent.

**Known verification points (call out during execution, don't assume):**
1. gluesql `ColumnDef` struct literal shape (Task 3 Step 1).
2. `SqlError` → gluesql error conversion in `drop_column` (Task 2 Step 3).
3. iceberg-rust 0.9.1 `StructType::field_by_id`, `NestedField` field names/types, `Schema::identifier_field_ids` (Task 4 Step 3).
4. Test harness helpers in existing crate tests (Tasks 1, 6, 7).
