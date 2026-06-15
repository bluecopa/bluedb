# bluedb Lakehouse Mirror — Iceberg CDC Projection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mirror bluedb tables into object storage as Apache Iceberg v2 tables — full CRUD, minutes-fresh, opt-out-by-default — so Snowflake / Databricks / BigQuery register them and `JOIN` against bluedb's operational data.

**Architecture:** A durable, ordered **CDC log** is written into the same SlateDB `WriteBatch` as each mirrored data commit (exactly-once). A new `bluedb-lakehouse` crate hosts a `LakehouseEngine` (modeled on `FtsEngine`) that, on a **change-triggered debounced seal**, reads CDC entries past a watermark, collapses them last-write-wins per PK, and commits one Iceberg snapshot (Parquet data file + equality-delete file) to the **raw object store** (`object_store::ObjectStore`, not SlateDB-internal blobs, so warehouses can read it). The watermark is stored in the Iceberg snapshot summary for exactly-once recovery. `bluedb-server` binds the engine on `promote`, hosts a read-only Iceberg REST Catalog, and exposes a `PRAGMA lakehouse_mirror` opt-out control.

**Tech Stack:** Rust, `iceberg` (apache/iceberg-rust), `arrow`, `parquet`, `object_store`, `slatedb` 0.13, `gluesql-core` 0.19, axum, `tokio`, `postcard`, `serde_json`.

---

## Prerequisite (hard dependency)

This plan assumes the **schema-direction baseline** is merged first:
[`2026-06-15-drop-schemaless-and-query-guardrail-design.md`](../specs/2026-06-15-drop-schemaless-and-query-guardrail-design.md).
Specifically the mirror relies on: **schemaless removed**, **every table has a PRIMARY KEY**
(so equality-deletes have a key + a clean Iceberg identity column), and **engine-internal
scans are exempt** from the scan/sort guardrail (the seal/backfill full scans).

**Field-ids:** the baseline adds a per-column `field_id` to the persisted schema. Phases 1–5
do **not** need it — they map columns by ordinal for a fixed schema. Only **Phase 6 (ALTER
reconciliation)** needs real field-ids and is therefore deferred until the baseline lands.

Spec: [`2026-06-15-bluedb-lakehouse-iceberg-mirror-design.md`](../specs/2026-06-15-bluedb-lakehouse-iceberg-mirror-design.md).

---

## File structure

**New crate `crates/bluedb-lakehouse/`:**
- `src/lib.rs` — exports + `LakehouseError`.
- `src/schema.rs` — gluesql `Schema`/`ColumnDef` → Iceberg `Schema` (scalar + complex types).
- `src/writer.rs` — `LakehouseWriter`: create/load an Iceberg table, write data + equality-delete files, commit a snapshot with the watermark in its summary.
- `src/cdc.rs` — decode CDC entries, last-write-wins collapse per `(table, pk)`.
- `src/compaction.rs` — merge-on-read → copy-on-write compaction + dead-file GC.
- `src/catalog.rs` — in-process Iceberg table model the REST routes serve (load/list).
- `src/engine.rs` — `LakehouseEngine`: registry, seal + compaction schedulers, `reopen`, backfill.

**Modified `crates/bluedb-sql/`:**
- `src/keyspace.rs` — add `TAG_CDC`.
- `src/cdc.rs` (new) — `CdcEntry` type + key encode/decode + `CdcConfig` (enabled-table set).
- `src/storage.rs` — append CDC entries into the commit `WriteBatch`; generalize `record_change`.
- `src/connection.rs` — CDC seq allocator + `connection_with_cdc`, `scan_cdc`, `gc_cdc`.
- `src/lakehouse.rs` (new) — `parse_lakehouse_pragma` (SET/PRAGMA interception, mirrors `nullorder.rs`).
- `src/lib.rs` — new `pub use`s.

**Modified `crates/bluedb-server/`:**
- `src/lib.rs` — `AppState` gains `lakehouse: RwLock<Arc<LakehouseEngine>>` + `lakehouse_seal_handle`; `promote`/`demote` wiring; PRAGMA routing; config.
- `src/catalog.rs` (new) — `/catalog/v1/*` read-only Iceberg REST routes.
- `src/main.rs` — env-var docs + lakehouse root config.

---

## Phase 1 — Durable CDC log in `bluedb-sql`

### Task 1.1: CDC keyspace tag + entry encode/decode

**Files:**
- Modify: `crates/bluedb-sql/src/keyspace.rs`
- Create: `crates/bluedb-sql/src/cdc.rs`
- Modify: `crates/bluedb-sql/src/lib.rs` (add `mod cdc;` + exports)
- Test: `crates/bluedb-sql/src/cdc.rs` (`#[cfg(test)]`)

- [ ] **Step 1: Add the tag.** In `keyspace.rs`, below the existing external tags (ledger uses `TAG_EXTERNAL_BASE`=0x10 through ~0x15), add:

```rust
/// CDC log for the lakehouse mirror: `external_key(TAG_CDC, seq_be)` → postcard(CdcEntry).
pub const TAG_CDC: u8 = TAG_EXTERNAL_BASE + 6; // 0x16
```

- [ ] **Step 2: Write the failing test** in `crates/bluedb-sql/src/cdc.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use gluesql_core::data::Key;

    #[test]
    fn cdc_entry_round_trips() {
        let e = CdcEntry { table: "docs".into(), key: Key::I64(7), row: None };
        let bytes = e.encode().unwrap();
        assert_eq!(CdcEntry::decode(&bytes).unwrap(), e);
    }
}
```

- [ ] **Step 3: Implement** `crates/bluedb-sql/src/cdc.rs`:

```rust
//! CDC log entries for the lakehouse mirror. One entry per committed row change
//! on a mirror-enabled table, written into the same WriteBatch as the data
//! (see `storage::commit`) so an entry exists iff the data committed.
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use gluesql_core::data::Key;
use gluesql_core::store::DataRow;
use serde::{Deserialize, Serialize};

use crate::error::SqlError;

/// A single committed change in CDC-log form. `row` is `Some` for insert/update,
/// `None` for delete (mirrors `RowChange`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CdcEntry {
    pub table: String,
    pub key: Key,
    pub row: Option<DataRow>,
}

impl CdcEntry {
    pub fn encode(&self) -> Result<Vec<u8>, SqlError> {
        postcard::to_stdvec(self).map_err(|e| SqlError::Internal(e.to_string()))
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, SqlError> {
        postcard::from_bytes(bytes).map_err(|e| SqlError::Internal(e.to_string()))
    }
}

/// Per-connection CDC control: the set of mirror-enabled tables. Shared (clones
/// point at the same set) so a PRAGMA toggle is seen by every live connection.
#[derive(Clone, Default)]
pub struct CdcConfig {
    pub enabled: Arc<RwLock<HashSet<String>>>,
    /// When true, tables not in `enabled` are still mirrored (opt-out default).
    pub default_on: Arc<std::sync::atomic::AtomicBool>,
}

impl CdcConfig {
    pub fn is_enabled(&self, table: &str) -> bool {
        let set = self.enabled.read().unwrap();
        // `enabled` doubles as the override set; interpretation lives in Phase 4.
        // Phase 1 default: mirror when default_on, unless explicitly excluded.
        match self.default_on.load(std::sync::atomic::Ordering::Relaxed) {
            true => !set.contains(&excluded_key(table)),
            false => set.contains(table),
        }
    }
}

/// Phase 4 stores per-table overrides; Phase 1 uses a simple `"-"+table` marker
/// for an explicit opt-out under default-on. Centralized so both agree.
pub fn excluded_key(table: &str) -> String { format!("-{table}") }
```

> Note: `SqlError::Internal(String)` — confirm the variant name in `error.rs`; if it differs (e.g. `SqlError::Encode`), use that. Add a `mod cdc;` and `pub use cdc::{CdcEntry, CdcConfig};` to `lib.rs`, and `pub use keyspace::{... , TAG_CDC};`.

- [ ] **Step 4: Run** `cargo test -p bluedb-sql cdc:: 2>&1` → PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-sql/src/cdc.rs crates/bluedb-sql/src/keyspace.rs crates/bluedb-sql/src/lib.rs
git commit -m "feat(bluedb-sql): CDC entry type + TAG_CDC keyspace for the lakehouse mirror"
```

### Task 1.2: Global CDC sequence allocator on `Database`

**Files:**
- Modify: `crates/bluedb-sql/src/connection.rs` (`Database` struct + ctor + accessor)
- Modify: `crates/bluedb-sql/src/storage.rs` (seed-from-max helper)
- Test: `crates/bluedb-sql/tests/cdc_log.rs` (new)

- [ ] **Step 1: Failing test** in `crates/bluedb-sql/tests/cdc_log.rs`:

```rust
use std::sync::Arc;
use bluedb_sql::Database;
use slatedb::Db;
use object_store::memory::InMemory;

#[tokio::test]
async fn cdc_seq_is_monotonic_and_starts_at_one() {
    let db = Arc::new(Db::open("cdc", Arc::new(InMemory::new())).await.unwrap());
    let database = Database::new(db);
    assert_eq!(database.next_cdc_seq().await.unwrap(), 1);
    assert_eq!(database.next_cdc_seq().await.unwrap(), 2);
}
```

- [ ] **Step 2: Run** `cargo test -p bluedb-sql --test cdc_log 2>&1` → FAIL (`next_cdc_seq` not found).
- [ ] **Step 3: Implement.** Add to `Database` (in `connection.rs`):

```rust
// field:
cdc_seq: Arc<tokio::sync::Mutex<Option<i64>>>, // None until seeded from storage

// in Database::new / new_for_*: cdc_seq: Arc::new(tokio::sync::Mutex::new(None)),

/// Allocate the next global CDC sequence (1-based, monotonic). Seeds lazily from
/// the max persisted CDC key after a (re)start/failover, then increments in-mem.
pub async fn next_cdc_seq(&self) -> Result<i64, SqlError> {
    let mut guard = self.cdc_seq.lock().await;
    let cur = match *guard {
        Some(n) => n,
        None => self.max_persisted_cdc_seq().await?,
    };
    let next = cur + 1;
    *guard = Some(next);
    Ok(next)
}
```

Add `max_persisted_cdc_seq` (scan `Keyspace::external_prefix(TAG_CDC)` for the largest suffix, decode big-endian i64, else 0) using `self.substrate.scan_range(prefix, upper)` — model it on `storage::live_max_i64_key`.

- [ ] **Step 4: Run** `cargo test -p bluedb-sql --test cdc_log 2>&1` → PASS.
- [ ] **Step 5: Commit**

```bash
git commit -am "feat(bluedb-sql): global CDC sequence allocator (lazy-seed, failover-safe)"
```

### Task 1.3: CDC config on the connection + generalized `record_change`

**Files:**
- Modify: `crates/bluedb-sql/src/storage.rs` (`SlateDbStorage` field, `record_change` guard, builder)
- Modify: `crates/bluedb-sql/src/connection.rs` (`connection_with_cdc`)
- Test: `crates/bluedb-sql/tests/cdc_log.rs`

- [ ] **Step 1: Failing test** (append to `cdc_log.rs`):

```rust
#[tokio::test]
async fn changes_recorded_when_cdc_enabled_without_observer() {
    use bluedb_sql::CdcConfig;
    use std::sync::atomic::Ordering;
    let db = Arc::new(Db::open("cdc2", Arc::new(InMemory::new())).await.unwrap());
    let database = Database::new(db);
    let cdc = CdcConfig::default();
    cdc.default_on.store(true, Ordering::Relaxed);

    { let mut g = gluesql_core::prelude::Glue::new(database.connection_serialized());
      g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);").await.unwrap(); }
    { let mut g = gluesql_core::prelude::Glue::new(database.connection_with_cdc(cdc.clone()));
      g.execute("INSERT INTO docs VALUES (1,'a');").await.unwrap(); }

    let entries = database.scan_cdc(0).await.unwrap(); // Task 1.5
    assert_eq!(entries.len(), 1);
}
```

- [ ] **Step 2: Run** → FAIL (`connection_with_cdc` / `scan_cdc` missing).
- [ ] **Step 3: Implement.**
  - Add `cdc: Option<CdcConfig>` to `SlateDbStorage` (default `None` in `with_substrate`).
  - Add builder `pub fn with_cdc(mut self, cdc: CdcConfig) -> Self { self.cdc = Some(cdc); self }`.
  - Generalize the `record_change` guard:

```rust
fn record_change(&mut self, table: &str, key: Key, row: Option<DataRow>) {
    let want = self.commit_observer.is_some()
        || self.cdc.as_ref().map_or(false, |c| c.is_enabled(table));
    if !want { return; }
    if let Some(txn) = self.txn.as_mut() {
        txn.changes.push(RowChange { table: table.to_string(), key, row });
    }
}
```

  - In `connection.rs`: `pub fn connection_with_cdc(&self, cdc: CdcConfig) -> SlateDbStorage { self.connection_serialized().with_cdc(cdc) }` (CDC connections serialize writes so RMW updates are captured correctly; also thread `cdc_seq`/`substrate` so `commit` can allocate — see Task 1.4).

- [ ] **Step 4: Run** `cargo test -p bluedb-sql --test cdc_log 2>&1` (the `changes_recorded…` test still fails on `scan_cdc` until 1.5 — that is expected; this task is green once it compiles and 1.2's test passes). Re-run 1.2's test to confirm no regression.
- [ ] **Step 5: Commit**

```bash
git commit -am "feat(bluedb-sql): CdcConfig on the connection + record changes when CDC enabled"
```

### Task 1.4: Append CDC entries into the commit `WriteBatch` (atomic)

**Files:**
- Modify: `crates/bluedb-sql/src/storage.rs` (`commit`)
- Modify: `crates/bluedb-sql/src/connection.rs` (pass a CDC-seq handle into the storage)
- Test: `crates/bluedb-sql/tests/cdc_log.rs`

- [ ] **Step 1: Failing test:**

```rust
#[tokio::test]
async fn cdc_entries_are_durable_and_ordered() {
    use bluedb_sql::CdcConfig;
    use std::sync::atomic::Ordering;
    let store = Arc::new(InMemory::new());
    let database = Database::new(Arc::new(Db::open("cdc3", store.clone()).await.unwrap()));
    let cdc = CdcConfig::default(); cdc.default_on.store(true, Ordering::Relaxed);
    { let mut g = gluesql_core::prelude::Glue::new(database.connection_serialized());
      g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);").await.unwrap(); }
    { let mut g = gluesql_core::prelude::Glue::new(database.connection_with_cdc(cdc.clone()));
      g.execute("INSERT INTO docs VALUES (1,'a'),(2,'b');").await.unwrap();
      g.execute("UPDATE docs SET body='c' WHERE id=1;").await.unwrap();
      g.execute("DELETE FROM docs WHERE id=2;").await.unwrap(); }
    let entries = database.scan_cdc(0).await.unwrap();
    // 2 inserts + 1 update + 1 delete = 4 entries, seqs strictly increasing
    assert_eq!(entries.len(), 4);
    let seqs: Vec<i64> = entries.iter().map(|(s,_)| *s).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]));
    assert!(entries.last().unwrap().1.row.is_none()); // delete last
}
```

- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement.** In `commit()`, after the `WriteBatch` is built from the overlay and **before** `self.writer()?.write(batch)`, append CDC entries for the buffered changes when CDC is enabled:

```rust
// after building `batch` from txn.overlay, before the durable write:
if let Some(cdc) = self.cdc.clone() {
    for ch in &txn.changes {
        if !cdc.is_enabled(&ch.table) { continue; }
        let seq = self.cdc_seq_next().await?;           // allocates from the shared Database counter
        let entry = CdcEntry { table: ch.table.clone(), key: ch.key.clone(), row: ch.row.clone() };
        let cdc_key = self.keyspace.external_key(TAG_CDC, &seq.to_be_bytes());
        batch.put(&cdc_key, &entry.encode()?);
    }
}
self.writer()?.write(batch).await.map_err(SqlError::from)?;
```

Thread a `cdc_seq` allocator handle into `SlateDbStorage` via `with_substrate`/`connection_with_cdc` (the same `Arc<tokio::sync::Mutex<Option<i64>>>` held by `Database`), and add `async fn cdc_seq_next(&self)` delegating to it (seed via a scan like `Database::max_persisted_cdc_seq`). Keep `txn.changes` flowing to `commit_observer` afterward unchanged (FTS still works).

- [ ] **Step 4: Run** `cargo test -p bluedb-sql --test cdc_log 2>&1` → PASS. Also `cargo test -p bluedb-sql 2>&1` (no regression; commit_tap + gluesql_suite green).
- [ ] **Step 5: Commit**

```bash
git commit -am "feat(bluedb-sql): write CDC entries into the same WriteBatch as the data (exactly-once)"
```

### Task 1.5: `scan_cdc` + `gc_cdc` on `Database`

**Files:** Modify `crates/bluedb-sql/src/connection.rs`; Test `crates/bluedb-sql/tests/cdc_log.rs`.

- [ ] **Step 1: Failing test:**

```rust
#[tokio::test]
async fn gc_cdc_removes_entries_through_watermark() {
    use bluedb_sql::CdcConfig; use std::sync::atomic::Ordering;
    let database = Database::new(Arc::new(Db::open("cdc4", Arc::new(InMemory::new())).await.unwrap()));
    let cdc = CdcConfig::default(); cdc.default_on.store(true, Ordering::Relaxed);
    { let mut g = gluesql_core::prelude::Glue::new(database.connection_serialized());
      g.execute("CREATE TABLE t (id INTEGER PRIMARY KEY);").await.unwrap(); }
    { let mut g = gluesql_core::prelude::Glue::new(database.connection_with_cdc(cdc.clone()));
      g.execute("INSERT INTO t VALUES (1),(2),(3);").await.unwrap(); }
    let all = database.scan_cdc(0).await.unwrap();
    let mid = all[1].0; // second seq
    database.gc_cdc(mid).await.unwrap();
    assert_eq!(database.scan_cdc(0).await.unwrap().len(), 1); // only seq > mid remains
}
```

- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** on `Database`:

```rust
/// Read CDC entries with seq > `after`, in seq order.
pub async fn scan_cdc(&self, after: i64) -> Result<Vec<(i64, CdcEntry)>, SqlError> {
    let ks = Keyspace::new(DEFAULT_TENANT);
    let start = ks.external_key(TAG_CDC, &(after + 1).to_be_bytes());
    let end = crate::keyspace::prefix_upper_bound(&ks.external_prefix(TAG_CDC));
    let mut out = Vec::new();
    let mut it = self.substrate.scan_range(&start, end.as_deref()).await?;
    while let Some(kv) = it.next().await? {
        let seq = i64::from_be_bytes(kv.key[kv.key.len()-8..].try_into().unwrap());
        out.push((seq, CdcEntry::decode(&kv.value)?));
    }
    Ok(out)
}

/// Delete CDC entries with seq <= `through` (after a successful seal).
pub async fn gc_cdc(&self, through: i64) -> Result<(), SqlError> {
    let ks = Keyspace::new(DEFAULT_TENANT);
    let mut batch = slatedb::WriteBatch::new();
    for (seq, _) in self.scan_cdc(0).await? {
        if seq <= through { batch.delete(&ks.external_key(TAG_CDC, &seq.to_be_bytes())); }
    }
    self.substrate.require_writer()?.write(batch).await.map_err(SqlError::from)?;
    Ok(())
}
```

> Confirm `prefix_upper_bound` is reachable (it is used in `storage.rs`; export it from `keyspace` if private). Multi-tenant CDC is out of scope for v1 (default tenant only) — note in code.

- [ ] **Step 4: Run** `cargo test -p bluedb-sql --test cdc_log 2>&1` → PASS (all CDC tests).
- [ ] **Step 5: Commit**

```bash
git commit -am "feat(bluedb-sql): scan_cdc + gc_cdc for the lakehouse seal loop"
```

---

## Phase 2 — `bluedb-lakehouse` crate: schema mapping + Iceberg writer

### Task 2.1: Crate skeleton + dependencies

**Files:** Create `crates/bluedb-lakehouse/Cargo.toml`, `crates/bluedb-lakehouse/src/lib.rs`; Modify root `Cargo.toml` (`[workspace.dependencies]`).

- [ ] **Step 1:** Add to root `Cargo.toml` `[workspace.dependencies]` (pin to current releases at implementation time; check `cargo search`):

```toml
iceberg      = "0.7"          # apache/iceberg-rust (confirm latest in Task 2.2)
arrow-array  = "55"
arrow-schema = "55"
parquet      = "55"
object_store = "0.11"
bluedb-lakehouse = { path = "crates/bluedb-lakehouse" }
```

- [ ] **Step 2:** `crates/bluedb-lakehouse/Cargo.toml`:

```toml
[package]
name = "bluedb-lakehouse"
version = "0.1.0"
edition = "2021"

[dependencies]
bluedb-sql = { workspace = true }
bluedb-storage = { workspace = true }
iceberg = { workspace = true }
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
parquet = { workspace = true }
object_store = { workspace = true }
gluesql-core = { workspace = true }
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
postcard = { workspace = true }
thiserror = { workspace = true }
async-trait = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }
```

- [ ] **Step 3:** `crates/bluedb-lakehouse/src/lib.rs`:

```rust
//! Iceberg CDC mirror for bluedb. See docs/superpowers/specs/2026-06-15-bluedb-lakehouse-iceberg-mirror-design.md
pub mod schema;
pub mod writer;
pub mod cdc;
pub mod compaction;
pub mod catalog;
pub mod engine;

#[derive(Debug, thiserror::Error)]
pub enum LakehouseError {
    #[error("iceberg: {0}")] Iceberg(String),
    #[error("schema: {0}")] Schema(String),
    #[error("sql: {0}")] Sql(#[from] bluedb_sql::SqlError),
    #[error(transparent)] Other(#[from] anyhow::Error),
}
pub type Result<T> = std::result::Result<T, LakehouseError>;

pub use engine::LakehouseEngine;
```

(Create empty `pub mod` stub files so the crate compiles.)

- [ ] **Step 4: Run** `cargo build -p bluedb-lakehouse 2>&1` → builds (empty modules).
- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-lakehouse Cargo.toml
git commit -m "feat(bluedb-lakehouse): crate skeleton + Iceberg/arrow/parquet deps"
```

### Task 2.2: SPIKE — confirm the `iceberg` writer + table API

**Files:** Test `crates/bluedb-lakehouse/tests/spike_iceberg.rs` (deleted after).

- [ ] **Step 1:** Write a throwaway test that, against a `tempfile::tempdir()` + an `object_store::local::LocalFileSystem` (or `MemoryCatalog` + `FileIO`), creates an Iceberg table with schema `(id long, body string)`, appends one row via the Arrow record-batch writer, commits a snapshot, then loads the table and scans it back asserting one row.
- [ ] **Step 2: Run** `cargo test -p bluedb-lakehouse --test spike_iceberg 2>&1`. Resolve the exact API names (catalog type, `TableCreation`, `transaction`/`fast_append`, equality-delete writer availability) from compile errors + `cargo doc -p iceberg --open`.
- [ ] **Step 3: Record findings** as a doc-comment block at the top of `src/writer.rs`: the exact types/methods to use for (a) create table, (b) append data file, (c) **equality-delete file** (CONFIRM it exists; if the writer cannot emit equality deletes in this version, set `WRITER_MODE = CopyOnWrite` and note it — Task 2.6 branches on this), (d) commit snapshot + set a snapshot **summary property**.
- [ ] **Step 4:** Delete the spike test.
- [ ] **Step 5: Commit**

```bash
git commit -am "docs(bluedb-lakehouse): record confirmed iceberg-rust writer API (spike)"
```

### Task 2.3: Scalar type mapping

**Files:** `crates/bluedb-lakehouse/src/schema.rs`; tests inline.

- [ ] **Step 1: Failing test:**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use gluesql_core::ast::DataType;
    use iceberg::spec::PrimitiveType;

    #[test]
    fn maps_scalars() {
        assert_eq!(iceberg_primitive(&DataType::Int).unwrap(), PrimitiveType::Long);
        assert_eq!(iceberg_primitive(&DataType::Text).unwrap(), PrimitiveType::String);
        assert!(matches!(iceberg_primitive(&DataType::Uint128).unwrap(), PrimitiveType::Decimal { precision: 38, scale: 0 }));
    }
}
```

- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `iceberg_primitive(&DataType) -> Result<PrimitiveType>` covering the §7.1 table: int family → `Int`/`Long`; `Uint64` → `Decimal{20,0}`; `Uint128` → `Decimal{38,0}`; `Float`→`Float`, `Double`→`Double`; `Boolean`→`Boolean`; `Text`/`Varchar`→`String`; `Bytea`→`Binary`; `Date`/`Time`/`Timestamp`→ matching; `Decimal(p,s)`→`Decimal{p,s}`; `Uuid`→`String`. (List/Map handled in 2.4.)
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(bluedb-lakehouse): scalar gluesql->iceberg type mapping"`

### Task 2.4: Complex/nested type mapping (list, map, recursion)

**Files:** `crates/bluedb-lakehouse/src/schema.rs`; tests inline.

- [ ] **Step 1: Failing test:** assert a `DataType::List(Box::new(DataType::Int))` column → an Iceberg `Type::List` of `Long`; assert a `Value::List` with mixed element types infers element `String`; assert a `Value::Map` → Iceberg `Map<String, V>`.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `iceberg_type(col_type, sample_values) -> Result<Type>`:
  - `DataType::List(inner)` → `Type::List` with element = `iceberg_type(inner, …)`; if `inner` is untyped (gluesql `List`/`Map` carry no subtype), infer from `sample_values`: homogeneous scalar → that primitive; nested list/map → recurse; empty/heterogeneous → `String` (items JSON-encoded). Same for `DataType::Map` → `Type::Map{ key: String, value: inferred }`. Assign monotonic Iceberg field-ids to nested fields from a counter.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(bluedb-lakehouse): native complex/nested type mapping with inner-type inference"`

### Task 2.5: `LakehouseWriter` — create/load table + write a data file

**Files:** `crates/bluedb-lakehouse/src/writer.rs`; Test `crates/bluedb-lakehouse/tests/writer.rs`.

- [ ] **Step 1: Failing test** (using the API confirmed in 2.2, a `tempdir` + local object store):

```rust
#[tokio::test]
async fn write_then_read_back_rows() {
    // build schema (id long PK, body string) via bluedb_lakehouse::schema
    // writer.upsert(table, rows=[(Key::I64(1), DataRow::Vec[I64(1), Str("a")])]).await
    // writer.commit_snapshot(watermark=1).await
    // read the table back with the iceberg reader → exactly one row {1,"a"}
}
```

- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `LakehouseWriter { object_store, root, table, iceberg_schema, pk_field }` with:
  - `ensure_table()` — create the Iceberg table at `<root>/{tenant}/{table}` if absent, else load.
  - `rows_to_record_batch(&[(Key, DataRow)]) -> RecordBatch` — convert gluesql `Value`s to Arrow arrays per the mapped schema (scalars + the nested builders from 2.4).
  - `write_data_file(batch)` — Parquet data file via the iceberg data-file writer.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(bluedb-lakehouse): LakehouseWriter create/load table + Parquet data file"`

### Task 2.6: Equality-deletes (merge-on-read) — full CRUD

**Files:** `crates/bluedb-lakehouse/src/writer.rs`; Test `crates/bluedb-lakehouse/tests/writer.rs`.

- [ ] **Step 1: Failing test:** insert id=1,2 → commit; then upsert id=1 (new body) + delete id=2 → commit; read back → id=1 has the new body, id=2 absent.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** in `upsert`/`delete`: for each changed PK, emit an **equality-delete** keyed on the PK field id; for `Some(row)` also emit the new row to the data file. If the spike (2.2) found equality-delete writing unavailable, implement `WRITER_MODE = CopyOnWrite`: read the current table, apply the LWW changeset in memory, rewrite the affected data file(s). Same external behavior; note the chosen mode in the module doc.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(bluedb-lakehouse): equality-delete merge-on-read (insert/update/delete)"`

### Task 2.7: Commit snapshot with watermark in the summary

**Files:** `crates/bluedb-lakehouse/src/writer.rs`; Test `crates/bluedb-lakehouse/tests/writer.rs`.

- [ ] **Step 1: Failing test:** `writer.commit_snapshot(watermark=42)`; reload table; assert the latest snapshot's summary contains `bluedb.cdc_watermark = "42"`; add `writer.current_watermark()` returning `Some(42)`.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `commit_snapshot(watermark: i64)` — stage the pending data/delete files into one Iceberg transaction commit, set snapshot summary property `bluedb.cdc_watermark` = watermark, commit. `current_watermark()` parses it from the latest snapshot (None if no snapshot).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(bluedb-lakehouse): store CDC watermark in the Iceberg snapshot summary"`

---

## Phase 3 — `LakehouseEngine`: seal loop, registry, reopen

### Task 3.1: Engine + durable registry (`lakehouse/_registry`)

**Files:** `crates/bluedb-lakehouse/src/engine.rs`; Test `crates/bluedb-lakehouse/tests/engine.rs`.

- [ ] **Step 1: Failing test:** create an engine over a local object store + a `Database`; register table "docs" as mirrored; `reopen` a fresh engine over the same store → it lists "docs" as mirrored.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `LakehouseEngine { object_store, root, db: Database, cdc: CdcConfig, tables: RwLock<HashMap<String, MirrorDef>> }`, modeled on `FtsEngine`:
  - `reopen(object_store, root, db, cdc)` — read `lakehouse/_registry` JSON (the per-table opt-out map + global default) via `object_store.get`, rebuild `tables`, populate `cdc.enabled`/`default_on`.
  - `persist_registry()` — write the JSON back.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(bluedb-lakehouse): LakehouseEngine + durable registry/reopen"`

### Task 3.2: `seal()` — CDC → Iceberg, advance watermark, GC

**Files:** `crates/bluedb-lakehouse/src/engine.rs`, `src/cdc.rs` (LWW collapse); Test `tests/engine.rs`.

- [ ] **Step 1: Failing test:** insert+update+delete across mirrored tables via `connection_with_cdc`; call `engine.seal()`; read each Iceberg table back → matches final bluedb state; second `seal()` with no new CDC is a no-op; `database.scan_cdc(0)` is empty (GC'd through watermark).
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `seal()`:
  1. `watermark = max over mirrored tables of writer.current_watermark()` (or per-table watermark — keep one watermark per table in its snapshot summary; track the min across tables for GC safety).
  2. `entries = db.scan_cdc(global_min_watermark)`.
  3. group by table; `cdc::collapse_lww(entries)` → final `(pk → Some(row)|None)` per table.
  4. for each table: `LakehouseWriter::upsert/delete` then `commit_snapshot(max_seq_for_table)`.
  5. `db.gc_cdc(min committed watermark across mirrored tables)`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(bluedb-lakehouse): seal CDC log into Iceberg snapshots + GC through watermark"`

### Task 3.3: Event-driven (debounced) seal loop

**Files:** `crates/bluedb-lakehouse/src/engine.rs`; Modify `crates/bluedb-sql` commit path (a notify handle on `CdcConfig`); Test `tests/engine.rs`.

- [ ] **Step 1: Failing test:** enable a table; with a short debounce (e.g. 100ms) and NO fixed interval, insert a row → the seal fires within ~debounce and the Iceberg table reflects it; an idle table produces no new snapshot (assert snapshot count unchanged).
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement.** Add a `tokio::sync::Notify` handle to `CdcConfig`; in `storage::commit`, after the durable write, `notify_one()` when a mirror-enabled table committed. `spawn_seal_loop(self: Arc<Self>, debounce: Duration, max_interval: Duration) -> JoinHandle<()>`: loop — `notified().await`, then coalesce further notifies for up to `debounce` (capped at `max_interval` so a steady stream still seals), then `seal()`, log errors. No pending changes ⇒ no seal (never snapshot idle tables). REPLACES the fixed-interval scheduler.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(bluedb-lakehouse): event-driven debounced seal loop (seconds-fresh, skips idle)"`

### Task 3.4: Backfill on enable

**Files:** `crates/bluedb-lakehouse/src/engine.rs`; Test `tests/engine.rs`.

- [ ] **Step 1: Failing test:** create a table + insert 3 rows with CDC **off**; then `engine.enable_table("t")`; assert the next `seal()` produces an Iceberg table with all 3 rows (backfilled by scan, not CDC).
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `enable_table(name)` → set in registry/cdc-enabled, then a one-shot backfill: scan the table (engine-internal, guardrail-exempt) → `upsert` all rows → `commit_snapshot(current_max_cdc_seq)` so subsequent CDC picks up exactly after. `disable_table(name)` stops CDC + seals.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(bluedb-lakehouse): backfill a table into Iceberg on enable"`

### Task 3.5: Memory-bounded compaction (bin-packed, streaming, sort-merge)

**Files:** `crates/bluedb-lakehouse/src/compaction.rs`, `src/engine.rs`; Test `crates/bluedb-lakehouse/tests/compaction.rs`.

**Goal:** peak compaction memory ≈ one target-sized output file, **independent of table size / file count** (spec §5.1). That property is what the test defends — this tree already OOMs Docker, so a load-the-table compactor is unacceptable.

- [ ] **Step 1: Failing test:** seal a table many times to produce ~50 tiny data files + several equality-delete files; run `engine.compact_once("t")`; assert (a) it selects a BOUNDED bin (total input bytes ≤ ~`target_file_bytes`, NOT all 50 files), (b) output is ≤ `target_file_bytes` and far fewer files, (c) correctness vs a full-scan oracle: deleted PKs absent, updated PKs at latest value.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** in `compaction.rs`:
  - `select_bin(files, target_bytes) -> Vec<DataFile>` — bin-pack small files up to ~`target_bytes` of *input* (bounds the working set).
  - `compact_bin(bin)` — stream Arrow `RecordBatch`es from the bin's Parquet files; apply the relevant equality-deletes via a **PK-sorted merge** (rows sorted by PK, delete keys streamed sorted, drop matches) — **O(batch) memory, NO full in-memory PK hash-set**; write ≤ `target_bytes` output file(s); commit an Iceberg replace (new file in; bin + consumed delete files marked for GC).
  - `compact_once(table)` = exactly one bin. `spawn_compaction_worker(self, max_inflight_bytes)` — one throttled background loop running `compact_once` over tables with a backlog, honoring the byte budget, then GC dead files.
  - `backlog(table) -> usize` (unmerged small-file + delete-file count); the seal loop (3.3) reads it and **slows that table's seal cadence when `> max_backlog`** (backpressure so the backlog, hence memory + read cost, stays bounded).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(bluedb-lakehouse): memory-bounded bin-packed sort-merge compaction + seal backpressure"`

---

## Phase 4 — PRAGMA opt-out control

### Task 4.1: `parse_lakehouse_pragma`

**Files:** Create `crates/bluedb-sql/src/lakehouse.rs`; Modify `lib.rs`; tests inline. (Mirrors `nullorder.rs`.)

- [ ] **Step 1: Failing test:**

```rust
#[test]
fn parses_global_and_per_table() {
    assert_eq!(parse_lakehouse_pragma("PRAGMA lakehouse_mirror = off"), Some(LhPragma::GlobalDefault(false)));
    assert_eq!(parse_lakehouse_pragma("PRAGMA lakehouse_mirror_table('docs', off)"), Some(LhPragma::Table("docs".into(), false)));
    assert_eq!(parse_lakehouse_pragma("SELECT 1"), None);
}
```

- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `enum LhPragma { GlobalDefault(bool), Table(String, bool) }` + `parse_lakehouse_pragma(&str) -> Option<LhPragma>` (string-match `set`/`pragma` + `lakehouse_mirror`, parse on/off + optional `('table', …)`). Export from `lib.rs`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(bluedb-sql): parse PRAGMA lakehouse_mirror (global + per-table)"`

### Task 4.2: Apply a PRAGMA to the engine registry

**Files:** `crates/bluedb-lakehouse/src/engine.rs`; Test `tests/engine.rs`.

- [ ] **Step 1: Failing test:** `engine.apply_pragma(LhPragma::GlobalDefault(false))` then `enable`-by-default is off → a new table is not mirrored; `apply_pragma(Table("docs", true))` re-includes "docs"; both survive `reopen` (persisted).
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `apply_pragma(LhPragma)` → mutate `cdc.default_on` / `cdc.enabled` (+ override map) and `persist_registry()`. On a table flipped on, trigger backfill (Task 3.4).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(bluedb-lakehouse): apply lakehouse PRAGMA to the durable registry"`

### Task 4.3: Route PRAGMA interception in the SQL path

**Files:** Modify `crates/bluedb-engine/src/rest_sql.rs` (or wherever `/sql` resolves) + `bluedb-server` exec_sql to call `parse_lakehouse_pragma` before `Glue::execute`, and forward to `engine.apply_pragma`. Test `crates/bluedb-server/tests/lakehouse.rs`.

- [ ] **Step 1: Failing test (HTTP):** `POST /sql {"sql":"PRAGMA lakehouse_mirror = off"}` → 200; then create+insert a table → no Iceberg table appears after a seal.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the interception: if `parse_lakehouse_pragma(sql).is_some()`, apply to the lakehouse engine and return a small ack instead of running it through gluesql (which would reject it).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(server): intercept PRAGMA lakehouse_mirror and apply to the engine"`

---

## Phase 5 — Server wiring + REST catalog

### Task 5.1: Bind the engine on promote / abort on demote

**Files:** Modify `crates/bluedb-server/src/lib.rs`; Test `crates/bluedb-server/tests/lakehouse.rs`.

- [ ] **Step 1: Failing test:** `make_app(promote=true)`; insert rows over HTTP; wait one seal interval; the Iceberg table exists in the test object store.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement.** Add to `Inner`: `lakehouse: RwLock<Arc<LakehouseEngine>>` + `lakehouse_seal_handle: Mutex<Option<JoinHandle<()>>>`. In `promote()`: after the writer `Database` is built, `LakehouseEngine::reopen(self.inner.object_store.clone(), lakehouse_root(), database.clone(), cdc_config)`, spawn its **seal loop + compaction worker** (debounce/budget from config), store the handles; make `connection_with_cdc` the connection used by `/sql`/`/tables` writes so mutations land in the CDC log. In `demote()`: abort both handles, swap to a no-op engine.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(server): bind LakehouseEngine + seal scheduler on promote, abort on demote"`

### Task 5.2: Config (seal debounce / compaction / root)

**Files:** Modify `crates/bluedb-server/src/lib.rs`, `src/main.rs`; tests inline.

- [ ] **Step 1: Failing test:** `parse_lakehouse_debounce_ms(Some("500")) == Duration::from_millis(500)` (default `2000`); `parse_target_file_bytes(None) == 134_217_728`.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** parse helpers (model on `parse_fts_seal_interval_ms`) for `BLUEDB_LAKEHOUSE_SEAL_DEBOUNCE_MS` (default 2000), `BLUEDB_LAKEHOUSE_SEAL_MAX_INTERVAL_MS` (default 10000), `BLUEDB_LAKEHOUSE_TARGET_FILE_BYTES` (default 134217728), `BLUEDB_LAKEHOUSE_MAX_BACKLOG`, plus `lakehouse_root()` from `BLUEDB_LAKEHOUSE_ROOT` (default `"lakehouse"`). Document all in `main.rs`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(server): lakehouse seal-debounce/compaction/root env config"`

### Task 5.3: Read-only Iceberg REST Catalog routes

**Files:** Create `crates/bluedb-server/src/catalog.rs`; Modify `src/lib.rs` (router); Test `tests/lakehouse.rs`.

- [ ] **Step 1: Failing test:** after a table is sealed, `GET /catalog/v1/namespaces/default/tables` lists it; `GET /catalog/v1/namespaces/default/tables/docs` returns a loadTable JSON whose `metadata-location` points into the object store and whose schema has the expected columns.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the read-only subset: `GET /catalog/v1/config`, `/v1/namespaces`, `/v1/namespaces/{ns}`, `/v1/namespaces/{ns}/tables`, `/v1/namespaces/{ns}/tables/{table}` (loadTable: return the current metadata location + metadata JSON from the engine/catalog model). Register on the router in `build_app`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(server): read-only Iceberg REST Catalog (/catalog/v1)"`

### Task 5.4: Authz scope for catalog routes

**Files:** Modify `crates/bluedb-server/src/catalog.rs`, `src/authz.rs`, `src/main.rs` (doc); Test `tests/lakehouse.rs`.

- [ ] **Step 1: Failing test:** with authz configured, `/catalog/v1/*` without a token → 401; with a `catalog:read` (or `data:read`) token → 200.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `state.authorize(&headers, Scope::DataRead)` (reuse `data:read`; or add `Scope::CatalogRead`) at the top of each catalog handler. Document in `main.rs`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(server): bearer-token authz for the Iceberg REST Catalog"`

---

## Phase 6 — End-to-end, emulator, and deferred ALTER

### Task 6.1: End-to-end exactly-once test

**Files:** Test `crates/bluedb-server/tests/lakehouse_e2e.rs`.

- [ ] **Step 1:** Write over HTTP (`/schema/tables` + `/tables`/`/sql` inserts, updates, deletes) → force a seal → read the Iceberg table back via the `iceberg` reader → assert final state. Then build a SECOND `AppState` over the **same** object store, `promote()` it (simulating failover) → it resumes from the watermark; replay a few more writes → no lost/duplicated rows in Iceberg.
- [ ] **Step 2: Run** `cargo test -p bluedb-server --test lakehouse_e2e 2>&1` → PASS.
- [ ] **Step 3: Commit** `git commit -am "test(bluedb-lakehouse): end-to-end CRUD + failover exactly-once"`

### Task 6.2: Object-store emulator round-trip (ignored)

**Files:** Extend `crates/bluedb-server/tests/objstore_emulators.rs`.

- [ ] **Step 1:** Add an `#[ignore]` test mirroring `s3_minio_round_trip`: run the mirror end-to-end against MinIO, then read the Iceberg table back with the `iceberg` reader pointed at the same bucket.
- [ ] **Step 2: Run** (documented) `docker compose -f crates/bluedb-server/tests/emulators/docker-compose.yml up -d && cargo test -p bluedb-server --test objstore_emulators -- --ignored --nocapture`.
- [ ] **Step 3: Commit** `git commit -am "test(bluedb-lakehouse): MinIO emulator Iceberg round-trip (ignored)"`

### Task 6.3: ALTER reconciliation — DEFERRED (needs baseline field-ids)

**Not implemented in this plan.** Once the schema-direction baseline lands per-column
`field_id`s, add a follow-up: at seal time, `schema::reconcile(bluedb_schema, iceberg_schema)`
by field-id → emit an Iceberg `UpdateSchema`/rename (metadata-only) **before** the data
snapshot (spec §7.1). Until then the mirror assumes a fixed schema; an ADD/DROP/RENAME on a
mirrored table is out of scope and should be asserted against (a test that an ALTER on a
mirrored table returns a clear "schema evolution not yet supported" error, so it fails loudly
rather than silently corrupting the mirror).

- [ ] **Step 1:** Add a guard + test: ALTER on a mirrored table → explicit error until 6.3 lands.
- [ ] **Step 2: Commit** `git commit -am "feat(bluedb-lakehouse): reject ALTER on a mirrored table until field-id reconciliation lands"`

---

## Self-review

- **Spec coverage:** §2 format/merge-on-read/opt-out/exactly-once/REST-catalog/active-only/no-gate → Phases 2/3/4/5 (always-compiled crate, no `#[cfg(feature)]`). §3 architecture → Phases 1+3+5. §5 CDC→Iceberg → 2.6 + 3.2; §5.1 freshness + memory-bounded compaction → 3.3 (event-driven seal) + 3.5 (bin-packed sort-merge compaction, backpressure). §6 exactly-once → 1.4 + 2.7 + 3.2 + 6.1. §7.1 type mapping → 2.3/2.4; schema evolution → 6.3 (deferred, baseline-gated). §8 PRAGMA opt-out → Phase 4. §9 REST catalog → 5.3/5.4. §10 config → 5.2. §11 HA → 5.1. §12 testing → each task + 6.1/6.2. §13 risks (iceberg-rust delete maturity) → 2.2 spike + 2.6 CoW fallback.
- **No silent caps:** the iceberg-rust delete-writer risk is handled explicitly (spike → CoW fallback), not hidden.
- **Placeholder scan:** every code step has real code or a precise signature; uncertain iceberg-rust calls are isolated to the 2.2 spike and the writer module, by design.
- **Type consistency:** `CdcEntry`, `CdcConfig`, `LhPragma`, `LakehouseEngine`, `LakehouseWriter`, `connection_with_cdc`, `scan_cdc`/`gc_cdc`, `next_cdc_seq` are used consistently across phases.
