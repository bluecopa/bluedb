# bluedb Spec B — increment B2c: commit tap + FtsEngine → end-to-end read-your-writes

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Wire the B2a `LiveSegment` to the SQL write+read paths so a `@@` query sees a just-committed row — **read-your-writes through SQL**, the Spec B §5 headline. Two seams: (1) a **commit-observer hook** in `bluedb-sql` that emits committed row changes after the durable write (FTS-agnostic — it just reports `(table, key, row)`); (2) an **`FtsEngine`** in `bluedb-engine` that holds the live segments, observes commits to maintain them, and rewrites `@@` reads against them.

**Architecture:**
- **`bluedb-sql`** gains a `CommitObserver` trait + `RowChange { table, key, row: Option<DataRow> }`. `SlateDbStorage` gets an `Option<Arc<dyn CommitObserver>>` (builder `with_commit_observer`, default `None` → zero behavior change, all existing tests untouched). When set, the `StoreMut` data methods buffer a decoded `RowChange` per row into the txn; `commit()` calls `observer.on_commit(&changes)` **after** the durable `WriteBatch` write succeeds.
- **`bluedb-engine`** gains `FtsEngine`: a registry of `(table, column) → (Arc<LiveSegment>, pk_column, column_ordinal)`. It implements `CommitObserver` (routes each `RowChange` to the right segment: `Key::I64(pk)` + text column → `index(pk, text)`; delete → `tombstone(pk)`). It exposes `rewrite_for(sql)` (pick the segment by the predicate's table/column, run `rewrite_fts_query`) and `execute_fts(glue, sql, params)` (rewrite-or-passthrough → `rest_sql::execute_sql(.., false)`).

**Scope (B2c):** the commit tap, the engine registry/observer/read-wiring, and an end-to-end RYW test over a real `Database`. **Supported shape:** tables with an **`INTEGER PRIMARY KEY`** (so the row `Key` is `Key::I64`, equal to the pk column value, and `pk IN (...)` selects correctly) and a schema'd `DataRow::Vec`. **NOT in B2c:** the durable-split union (`bluedb_fts::FtsIndex` — live-segment-only for now), the HTTP `CREATE FULLTEXT INDEX` endpoint (B2b), background seal/PRAGMA/failover (B4), trigram (B3), non-integer / composite / `DataRow::Map` pks (documented restriction). Persistence of index defs is in-memory in B2c; durable registry is B2b.

**Tech stack:** Rust; `gluesql_core::data::{Key, DataRow, Value}`; `gluesql_core::store::Store` (`fetch_schema`); the B2a `LiveSegment`; the B1 `extract_fts_predicate`/`rewrite_fts_query`.

---

## Task 1: `bluedb-sql` commit-observer seam

**Files:** `crates/bluedb-sql/src/storage.rs` (+ new `commit_tap.rs` module or inline), `crates/bluedb-sql/src/lib.rs` (exports), test in `crates/bluedb-sql/tests/commit_tap.rs`.

- [ ] **Failing test** (`crates/bluedb-sql/tests/commit_tap.rs`): install a recording observer, run an autocommit INSERT then a DELETE through `Glue`, assert the observer saw the right `RowChange`s.
```rust
use std::sync::{Arc, Mutex};
use bluedb_sql::{CommitObserver, Database, RowChange};
use gluesql_core::prelude::Glue;
use gluesql_core::data::{Key, DataRow, Value};
use slatedb::{Db, object_store::memory::InMemory};

#[derive(Default)]
struct Recorder { seen: Mutex<Vec<RowChange>> }
impl CommitObserver for Recorder {
    fn on_commit(&self, changes: &[RowChange]) { self.seen.lock().unwrap().extend_from_slice(changes); }
}

#[tokio::test]
async fn observer_sees_committed_inserts_and_deletes() {
    let db = Arc::new(Db::open("tap", Arc::new(InMemory::new())).await.unwrap());
    let database = Database::new(db);
    let rec = Arc::new(Recorder::default());

    { let mut g = Glue::new(database.connection_serialized());
      g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);").await.unwrap(); }
    { let mut g = Glue::new(database.connection().with_commit_observer(rec.clone()));
      g.execute("INSERT INTO docs (id, body) VALUES (1, 'hello'), (2, 'world');").await.unwrap(); }
    { let mut g = Glue::new(database.connection().with_commit_observer(rec.clone()));
      g.execute("DELETE FROM docs WHERE id = 2;").await.unwrap(); }

    let seen = rec.seen.lock().unwrap();
    // two inserts (row Some) for docs, then one delete (row None) for id=2
    let inserts: Vec<_> = seen.iter().filter(|c| c.table == "docs" && c.row.is_some()).collect();
    assert_eq!(inserts.len(), 2);
    assert!(seen.iter().any(|c| c.table == "docs" && c.row.is_none() && c.key == Key::I64(2)));
}
```
(Note: the `CREATE TABLE` connection has NO observer, so schema writes aren't reported — only the insert/delete connections do. That's fine; assert on what the observer'd connections saw. If gluesql also routes the schema insert through a data method, filter by `table == "docs"` and `row.is_some()/none()` as above.)
Run `cargo test -p bluedb-sql --test commit_tap` → FAIL.

- [ ] **Implement.**
  - Define (in a small `commit_tap.rs` or near the top of `storage.rs`):
```rust
use gluesql_core::data::{DataRow, Key};

/// A single committed row change reported to a [`CommitObserver`] after the
/// durable write. `row` is `Some` for an insert/update (the new row), `None`
/// for a delete.
#[derive(Debug, Clone)]
pub struct RowChange {
    pub table: String,
    pub key: Key,
    pub row: Option<DataRow>,
}

/// Observes committed row changes on a [`SlateDbStorage`] connection — the seam
/// the FTS engine uses to maintain its live index synchronously with SQL commit
/// (Spec B §4.2). Called **after** the durable `WriteBatch` write succeeds, so
/// an observer never sees a change that didn't commit. `on_commit` runs inline
/// on the commit path: keep it cheap (in-memory work only).
pub trait CommitObserver: Send + Sync {
    fn on_commit(&self, changes: &[RowChange]);
}
```
  - `SlateDbStorage`: add field `commit_observer: Option<Arc<dyn CommitObserver>>` (init `None` in `with_substrate`). Add builder mirroring `serialize_writes`:
```rust
/// Install a commit observer (e.g. the FTS live-index maintainer). Reported
/// changes are decoded row mutations, emitted after the durable commit write.
pub fn with_commit_observer(mut self, observer: Arc<dyn CommitObserver>) -> Self {
    self.commit_observer = Some(observer);
    self
}
```
  - `TxnState`: add `changes: Vec<RowChange>` (init empty in `begin`).
  - A helper on `SlateDbStorage`:
```rust
/// Buffer a row change for the commit observer, but only when one is installed
/// and a txn is active (gluesql always runs statements inside a txn).
fn record_change(&mut self, table: &str, key: Key, row: Option<DataRow>) {
    if self.commit_observer.is_none() { return; }
    if let Some(txn) = self.txn.as_mut() {
        txn.changes.push(RowChange { table: table.to_string(), key, row });
    }
}
```
  - Call `record_change` from the three `StoreMut` data methods, cloning the needed data **before** it's moved into `StoredRow`:
    - `append_data`: after forming `key`/`row`, `self.record_change(table_name, key.clone(), Some(row.clone()))` — do the clone only when `self.commit_observer.is_some()` to avoid cost otherwise (guard inside `record_change` already prevents the push, but the `row.clone()` happens at the call site — so wrap the call: `if self.commit_observer.is_some() { self.record_change(table_name, key.clone(), Some(row.clone())); }`). Place it right before `self.write_key(...)`.
    - `insert_data`: same — `Some(row.clone())` with `key.clone()` (covers INSERT and UPDATE; UPDATE re-inserts the same key with the new row → the engine's `index()` supersedes).
    - `delete_data`: `if self.commit_observer.is_some() { self.record_change(table_name, key.clone(), None); }` (use the loop's `key`).
  - `commit()`: after `self.writer()?.write(batch).await...?` succeeds, fire the observer. Restructure the overlay loop to **borrow** so `txn.changes` survives:
```rust
            let mut batch = WriteBatch::new();
            for (key, op) in &txn.overlay {
                match op {
                    Some(value) => batch.put(key, value),
                    None => batch.delete(key),
                }
            }
            self.writer()?.write(batch).await.map_err(SqlError::from)?;
            // Commit tap: durable write succeeded → report changes (Spec B §4.2).
            if let Some(obs) = self.commit_observer.as_ref() {
                if !txn.changes.is_empty() {
                    obs.on_commit(&txn.changes);
                }
            }
```
  - Export from `lib.rs`: `pub use storage::{SlateDbStorage, WriteLease, CommitObserver, RowChange};` (or `pub use commit_tap::{CommitObserver, RowChange};` if you put them in their own module).
- [ ] Run `cargo test -p bluedb-sql` (ALL pass — existing tests have no observer, so unaffected) + `cargo build -p bluedb-sql 2>&1 | grep -i warn` (clean). Commit:
```bash
git commit -am "feat(sql): CommitObserver seam — report committed row changes after durable write"
```

## Task 2: `bluedb-engine` `FtsEngine` — maintain live segments + rewrite reads (RYW)

**Files:** new `crates/bluedb-engine/src/fts_engine.rs`; `mod`/re-export in `lib.rs`; test in `crates/bluedb-engine/tests/ryw.rs`.

- [ ] **Failing end-to-end test** (`crates/bluedb-engine/tests/ryw.rs`):
```rust
use std::sync::Arc;
use bluedb_engine::{FtsEngine, rest_sql};
use bluedb_sql::Database;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::{Db, object_store::memory::InMemory};

#[tokio::test]
async fn read_your_writes_through_sql() {
    let db = Arc::new(Db::open("ryw", Arc::new(InMemory::new())).await.unwrap());
    let database = Database::new(db);
    let fts = FtsEngine::new();

    { let mut g = Glue::new(database.connection_serialized());
      g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);").await.unwrap(); }

    // Declare a fulltext index on docs.body (pk = id), english analyzer.
    fts.create_fulltext_index(&database.connection(), "docs", "body", "id", "english").await.unwrap();

    // Insert WITH the observer installed → live segment maintained on commit.
    { let mut g = Glue::new(database.connection().with_commit_observer(fts.clone()));
      g.execute("INSERT INTO docs (id, body) VALUES (1, 'quarterly invoice overdue'), (2, 'weather sunny skies');").await.unwrap(); }

    // @@ query in a fresh connection sees the just-committed row 1 (RYW via the shared FtsEngine).
    let mut g = Glue::new(database.connection_serialized());
    let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')";
    let out = fts.execute_fts(&mut g, sql, &[]).await.unwrap();
    match out.into_iter().next().unwrap() {
        Payload::Select { rows, .. } => {
            let ids: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
            assert_eq!(ids, vec![Value::I64(1)], "only the matching row, fetched by pk IN (...)");
        }
        other => panic!("{other:?}"),
    }

    // A delete is reflected immediately too.
    { let mut g = Glue::new(database.connection().with_commit_observer(fts.clone()));
      g.execute("DELETE FROM docs WHERE id = 1;").await.unwrap(); }
    let mut g2 = Glue::new(database.connection_serialized());
    let out2 = fts.execute_fts(&mut g2, sql, &[]).await.unwrap();
    match out2.into_iter().next().unwrap() {
        Payload::Select { rows, .. } => assert!(rows.is_empty(), "deleted row must not match"),
        other => panic!("{other:?}"),
    }
}
```
Run `cargo test -p bluedb-engine --test ryw` → FAIL.

- [ ] **Implement `fts_engine.rs`:**
```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bluedb_sql::{CommitObserver, RowChange, SlateDbStorage};
use gluesql_core::data::{DataRow, Key, Value as GValue};
use gluesql_core::prelude::Glue;
use gluesql_core::store::Store;

use crate::error::{EngineError, Result};
use crate::fts_sql::{extract_fts_predicate, rewrite_fts_query};
use crate::live_segment::LiveSegment;
use crate::rest_sql;
use bluedb_rest::Param;
use gluesql_core::prelude::Payload;

struct IndexDef {
    column: String,
    pk_column: String,
    column_ordinal: usize,
    segment: Arc<LiveSegment>,
}

/// Maintains in-memory FTS live segments in lock-step with SQL commits and
/// rewrites `@@` reads against them (Spec B §4.2/§4.3, §5 read-your-writes).
/// B2c: live-segment-only (no durable split union yet), `INTEGER PRIMARY KEY`
/// tables only.
pub struct FtsEngine {
    // table -> its fulltext indexes
    indexes: RwLock<HashMap<String, Vec<IndexDef>>>,
}

impl FtsEngine {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { indexes: RwLock::new(HashMap::new()) })
    }

    /// Declare a fulltext index on `table.text_column`, with `pk_column` the
    /// table's integer primary key and `analyzer` a to_tsvector config string.
    /// Resolves the text column's ordinal from the live schema.
    pub async fn create_fulltext_index(
        &self,
        storage: &SlateDbStorage,
        table: &str,
        text_column: &str,
        pk_column: &str,
        analyzer: &str,
    ) -> Result<()> {
        let schema = Store::fetch_schema(storage, table)
            .await
            .map_err(EngineError::from)?
            .ok_or_else(|| EngineError::Rejected(format!("no such table: {table}")))?;
        let cols = schema.column_defs.ok_or_else(|| {
            EngineError::Rejected(format!("table {table} is schemaless; FTS needs a column schema"))
        })?;
        let ordinal = cols.iter().position(|c| c.name == text_column).ok_or_else(|| {
            EngineError::Rejected(format!("no column {text_column} on {table}"))
        })?;
        let segment = Arc::new(LiveSegment::new(analyzer)?);
        let def = IndexDef { column: text_column.to_string(), pk_column: pk_column.to_string(), column_ordinal: ordinal, segment };
        self.indexes.write().unwrap().entry(table.to_string()).or_default().push(def);
        Ok(())
    }

    /// Rewrite a `@@` query against the matching live segment. `Ok(None)` when
    /// the SQL has no `@@`.
    pub async fn rewrite_for(&self, sql: &str) -> Result<Option<String>> {
        let Some(pred) = extract_fts_predicate(sql)? else { return Ok(None); };
        let (segment, pk_column) = {
            let idx = self.indexes.read().unwrap();
            match idx.get(&pred.table).and_then(|v| v.iter().find(|d| d.column == pred.column)) {
                Some(def) => (def.segment.clone(), def.pk_column.clone()),
                None => return Err(EngineError::Rejected(format!(
                    "no fulltext index on {}.{}", pred.table, pred.column))),
            }
        }; // guard dropped before await
        rewrite_fts_query(sql, &pk_column, &*segment).await
    }

    /// Execute `sql`: rewrite `@@`/`ts_rank` against the live segment if present,
    /// else run unchanged. Goes through the parameterized single-DML surface.
    pub async fn execute_fts(
        &self,
        glue: &mut Glue<SlateDbStorage>,
        sql: &str,
        params: &[Param],
    ) -> Result<Vec<Payload>> {
        let rewritten = self.rewrite_for(sql).await?;
        let final_sql = rewritten.as_deref().unwrap_or(sql);
        rest_sql::execute_sql(glue, final_sql, params, false).await
    }
}

impl CommitObserver for FtsEngine {
    fn on_commit(&self, changes: &[RowChange]) {
        let idx = self.indexes.read().unwrap();
        for ch in changes {
            let Some(defs) = idx.get(&ch.table) else { continue };
            // B2c: integer pk only.
            let Key::I64(pk) = ch.key else { continue };
            for def in defs {
                match &ch.row {
                    Some(DataRow::Vec(values)) => {
                        if let Some(GValue::Str(text)) = values.get(def.column_ordinal) {
                            if let Err(e) = def.segment.index(pk, text) {
                                eprintln!("bluedb-fts: live index failed for {}.{} pk={pk}: {e}", ch.table, def.column);
                            }
                        }
                    }
                    Some(DataRow::Map(_)) => { /* schemaless: unsupported in B2c */ }
                    None => {
                        if let Err(e) = def.segment.tombstone(pk) {
                            eprintln!("bluedb-fts: live tombstone failed for {}.{} pk={pk}: {e}", ch.table, def.column);
                        }
                    }
                }
            }
        }
    }
}
```
  - `lib.rs`: `pub mod fts_engine;` + `pub use fts_engine::FtsEngine;`. Verify `gluesql_core::data::Value`/`DataRow`/`Key` and `Schema.column_defs`/`ColumnDef.name` field paths against gluesql-core 0.19 (adjust if the variant/field names differ — e.g. `DataRow` variants, `Value::Str`). Verify `EngineError: From<gluesql_core::error::Error>` (it is — the `Sql` variant) so `fetch_schema`'s error maps.
- [ ] Run `cargo test -p bluedb-engine --test ryw` (pass) + `cargo test -p bluedb-engine` (all pass) + `cargo build -p bluedb-engine 2>&1 | grep -i warn` (clean). Commit:
```bash
git commit -am "feat(engine): FtsEngine — commit-tap live-index maintenance + @@ rewrite (read-your-writes)"
```

## Self-Review
- Spec B coverage (B2c slice): sync-on-commit tap → live segment (§4.2) ✓; read-your-writes through SQL — insert/delete reflected in a subsequent `@@` with no explicit flush (§5) ✓; the `@@`→`pk IN (...)` rewrite now runs against a **real** maintained index (closes the B1/B2a loop) ✓; durability-first (observer fires only after the durable write) ✓.
- Restrictions (documented): `INTEGER PRIMARY KEY` + schema'd `DataRow::Vec` only; live-segment-only (no durable-split union — a process restart loses the in-memory index until B4's seal/replay); in-memory index defs (durable registry = B2b). `@@` on a column with no declared index → explicit error (not a silent scan).
- Deferred: HTTP `CREATE FULLTEXT INDEX` + server wiring (B2b), durable seal/PRAGMA/failover replay (B4), trigram (B3), `bluedb_fts::FtsIndex` split union into the searcher.
- Isolation: `bluedb-sql` stays FTS-agnostic (the observer reports generic row changes); all FTS knowledge lives in `bluedb-engine`. Observer default-`None` keeps every existing test and the hot path unchanged.
