# bluedb-ledger Phase H — integration (SQL projection · HTTP · Jepsen)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Make the now-complete TigerBeetle-parity ledger engine usable and observable: a crash-consistent SQL-readable projection of accounts/transfers, an HTTP `/ledger/*` API returning per-item result codes, and a Jepsen `ledger` workload that asserts double-entry conservation and no-double-apply across faults.

**Architecture:** The native `postcard` records under the ledger's external keyspace tags remain canonical. We add an **atomic dual-write** projection: in the *same* `slatedb::WriteBatch` that commits native records, we also `put` GlueSQL-format rows into two schema'd tables (`ledger_accounts`, `ledger_transfers`) in the default tenant, so `/sql` and `/tables/{ledger_accounts}` see a consistent view with zero lag (a crash can never leave the projection disagreeing with canonical state). GlueSQL coupling lives entirely in a new `bluedb-sql` projection module (`ProjValue`/`ProjectedTable`); `bluedb-ledger` stays GlueSQL-free. The HTTP layer builds a `Ledger` per request from the server's bound `Database` (cloned out of the role `RwLock`, like the SQL path), parses JSON (u128 fields as strings for precision), and returns the engine's per-item result-code vector. Jepsen drives `/ledger/*` and checks conservation against canonical reads.

**Tech Stack:** Rust (bluedb-sql, bluedb-ledger, bluedb-server / Axum 0.8, GlueSQL 0.19, SlateDB), Clojure (Jepsen / lein).

---

## File Structure

- **Create** `crates/bluedb-sql/src/projection.rs` — `ProjValue`, `ProjColumn`, `ProjType`, `ProjectedTable`. Owns all GlueSQL `Key`/`Value`/`DataRow` mapping + DDL + row encoding.
- **Modify** `crates/bluedb-sql/src/storage.rs` — make `StoredRow` and `encode`/`decode` `pub(crate)` so projection.rs reuses the exact on-disk row encoding.
- **Modify** `crates/bluedb-sql/src/lib.rs` — `mod projection; pub use projection::{ProjValue, ProjectedTable, ProjColumn, ProjType};`
- **Create** `crates/bluedb-ledger/src/projection.rs` — `accounts_table()`, `transfers_table()` (the two `ProjectedTable`s), `project_account(&Account) -> Vec<ProjValue>`, `project_transfer(&Transfer) -> Vec<ProjValue>`, and `pub async fn ensure_schema(&Database)`.
- **Modify** `crates/bluedb-ledger/src/ledger.rs` — dual-write projection rows in both commit batches.
- **Modify** `crates/bluedb-ledger/src/model.rs` — `#[derive(Serialize, Deserialize)]` + `serde(rename_all="snake_case")` on `CreateAccountResult`/`CreateTransferResult`; ensure `Account`/`Transfer` are `Serialize`/`Deserialize` (they already are for postcard).
- **Modify** `crates/bluedb-ledger/src/lib.rs` — `pub use` the projection ensure fn + `PendingStatus` if needed; export nothing GlueSQL.
- **Modify** `crates/bluedb-server/Cargo.toml` — add `bluedb-ledger = { workspace = true }`, `serde`.
- **Create** `crates/bluedb-server/src/ledger_api.rs` — `/ledger/*` handlers + JSON DTOs (u128-as-string).
- **Modify** `crates/bluedb-server/src/lib.rs` — `AppState::ledger()` accessor, `ensure_ledger_schema()` in `promote()`, mount routes in `build_app`.
- **Create** `jepsen/src/bluedb/jepsen/ledger.clj` — client + generator + conservation/no-double-apply checker.
- **Modify** `jepsen/src/bluedb/jepsen/core.clj` — register `ledger` workload (case arm + `--workload` validate set + setup DDL/account bootstrap).

---

## Pinned design decisions

- **Atomic dual-write, not post-commit projection.** The projection row `put`s go into the existing `WriteBatch`. Rationale: under Jepsen faults (kill/partition mid-write) a separate projection write could be lost, leaving `SELECT` disagreeing with canonical state — a false conservation violation. One batch = one fate.
- **Projection is put-only.** Accounts and transfers are never deleted; balances overwrite at the same row key. No tombstones in the projection.
- **Schema via DDL, rows hand-encoded.** Hand-building a GlueSQL `Schema`/`ColumnDef` is version-fragile; instead `ProjectedTable::ensure` runs `CREATE TABLE IF NOT EXISTS` through `Glue` once (idempotent, self-healing on failover). Only the *row* bytes are hand-encoded (a `StoredRow{key,row}` via the same private `encode`), which the Task-1 end-to-end test pins against a real `Glue` read.
- **u128 over JSON = string.** `id`, `debit_account_id`, `credit_account_id`, `amount`, `pending_id`, `user_data_128` serialize as JSON strings; input accepts string *or* number. Small fields (`ledger` u32, `code` u16, `flags` u16, `timeout` u32, `user_data_64` u64, `user_data_32`, balances) — balances and `user_data_64` are also u64/u128 so emit those as strings too. Keeps Clojure/`cheshire` from truncating 128-bit ids.
- **Canonical reads for correctness.** Jepsen conservation is checked against `GET /ledger/accounts/{id}` (native canonical), not the SQL projection. The projection is exercised separately by a server test that `SELECT`s through `/sql`.
- **Column order is the contract.** `ProjectedTable.columns` defines both the DDL column order and the `encode_row` value order; they must never drift. Keep them in one place (the `*_table()` constructors in `bluedb-ledger/src/projection.rs`).

---

## Task 1: bluedb-sql projection API

**Files:**
- Create: `crates/bluedb-sql/src/projection.rs`
- Modify: `crates/bluedb-sql/src/storage.rs` (visibility), `crates/bluedb-sql/src/lib.rs` (exports)

- [ ] **Step 1: Expose the row encoding.** In `storage.rs`, change `struct StoredRow` → `pub(crate) struct StoredRow` (and its fields `pub(crate) key`, `pub(crate) row`), and `fn encode`/`fn decode` → `pub(crate) fn encode`/`pub(crate) fn decode`.

- [ ] **Step 2: Write the projection module.** `crates/bluedb-sql/src/projection.rs`:

```rust
//! A minimal "projection" surface: let a layer above bluedb-sql (e.g.
//! bluedb-ledger) maintain a SQL-queryable table whose rows it writes itself
//! into its own atomic batch, while bluedb-sql owns all GlueSQL encoding.
//!
//! A [`ProjectedTable`] knows its column layout; [`ProjectedTable::ensure`]
//! creates the table (idempotent DDL), and [`ProjectedTable::encode_row`]
//! turns a row of [`ProjValue`]s into the exact `(storage_key, value_bytes)`
//! that GlueSQL's own store would have written — so a hand-built
//! `slatedb::WriteBatch` produces rows `SELECT` can read back.

use gluesql_core::data::{Key, Value};
use gluesql_core::prelude::Glue;
use gluesql_core::store::DataRow;

use crate::connection::Database;
use crate::error::SqlError;
use crate::keyspace::Keyspace;
use crate::storage::{encode, StoredRow};

/// A scalar projected into a SQL column. Maps 1:1 onto a GlueSQL [`Value`];
/// the `pk` column additionally maps onto a GlueSQL [`Key`].
#[derive(Clone, Debug, PartialEq)]
pub enum ProjValue {
    U128(u128),
    U64(u64),
    U32(u32),
    U16(u16),
    Str(String),
}

impl ProjValue {
    fn to_value(&self) -> Value {
        match self {
            ProjValue::U128(v) => Value::U128(*v),
            ProjValue::U64(v) => Value::U64(*v),
            ProjValue::U32(v) => Value::U32(*v),
            ProjValue::U16(v) => Value::U16(*v),
            ProjValue::Str(s) => Value::Str(s.clone()),
        }
    }

    fn to_key(&self) -> Key {
        match self {
            ProjValue::U128(v) => Key::U128(*v),
            ProjValue::U64(v) => Key::U64(*v),
            ProjValue::U32(v) => Key::U32(*v),
            ProjValue::U16(v) => Key::U16(*v),
            ProjValue::Str(s) => Key::Str(s.clone()),
        }
    }

    /// The SQL type literal used in `CREATE TABLE`.
    fn sql_type(&self) -> &'static str {
        match self {
            ProjValue::U128(_) => "UINT128",
            ProjValue::U64(_) => "UINT64",
            ProjValue::U32(_) => "UINT32",
            ProjValue::U16(_) => "UINT16",
            ProjValue::Str(_) => "TEXT",
        }
    }
}

/// One column of a projected table: a name + the type (carried as a sample
/// [`ProjValue`] so the DDL type and the encode path can never disagree).
#[derive(Clone, Debug)]
pub struct ProjColumn {
    pub name: &'static str,
    pub sample: ProjValue,
}

impl ProjColumn {
    pub fn new(name: &'static str, sample: ProjValue) -> Self {
        Self { name, sample }
    }
}

/// A SQL table maintained by an external layer. `pk` is the index into
/// `columns` of the PRIMARY KEY column.
#[derive(Clone, Debug)]
pub struct ProjectedTable {
    pub table: String,
    pub columns: Vec<ProjColumn>,
    pub pk: usize,
}

impl ProjectedTable {
    pub fn new(table: impl Into<String>, columns: Vec<ProjColumn>, pk: usize) -> Self {
        let table = table.into();
        assert!(pk < columns.len(), "pk index out of range");
        Self { table, columns, pk }
    }

    /// `CREATE TABLE IF NOT EXISTS <table> (col TYPE [PRIMARY KEY], ...)`.
    pub fn create_table_ddl(&self) -> String {
        let cols = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let pk = if i == self.pk { " PRIMARY KEY" } else { "" };
                format!("{} {}{}", c.name, c.sample.sql_type(), pk)
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("CREATE TABLE IF NOT EXISTS {} ({})", self.table, cols)
    }

    /// Idempotently create the table on `database` (runs the DDL via Glue on a
    /// serialized connection). Requires the active writer.
    pub async fn ensure(&self, database: &Database) -> Result<(), SqlError> {
        let mut glue = Glue::new(database.connection_serialized());
        glue.execute(&self.create_table_ddl())
            .await
            .map_err(|e| SqlError::from(e))?;
        Ok(())
    }

    /// Encode one row into the `(storage_key, value_bytes)` GlueSQL's own store
    /// would have produced, so a raw `WriteBatch::put` of these bytes is
    /// readable by `SELECT`. `values` must match `columns` in length + order.
    pub fn encode_row(
        &self,
        ks: &Keyspace,
        values: &[ProjValue],
    ) -> Result<(Vec<u8>, Vec<u8>), SqlError> {
        assert_eq!(values.len(), self.columns.len(), "row arity mismatch");
        let key = values[self.pk].to_key();
        let row = DataRow::Vec(values.iter().map(ProjValue::to_value).collect());
        let storage_key = ks.row_key(&self.table, &key)?;
        let stored = StoredRow { key, row };
        Ok((storage_key, encode(&stored)?))
    }
}
```

> Note: `SqlError::from(glue_error)` — confirm `SqlError` has a `From<gluesql_core::error::Error>`; if not, map with `SqlError::Sql(e.to_string())` (use whatever the existing variant is — grep `enum SqlError`). Adjust the one line accordingly.

- [ ] **Step 3: Export.** In `crates/bluedb-sql/src/lib.rs` add `mod projection;` and `pub use projection::{ProjColumn, ProjValue, ProjectedTable};`.

- [ ] **Step 4: End-to-end round-trip test** (in `projection.rs` `#[cfg(test)]`). This is the critical test — it proves a hand-encoded row is readable by a real `Glue` and that a U128 PRIMARY KEY works.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use gluesql_core::prelude::{Glue, Payload, Value as SqlValue};
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, WriteBatch};
    use crate::connection::Database;
    use crate::keyspace::{Keyspace, DEFAULT_TENANT};

    async fn db() -> Database {
        let db = Db::open("proj-test", Arc::new(InMemory::new())).await.unwrap();
        Database::new(Arc::new(db))
    }

    fn accounts() -> ProjectedTable {
        ProjectedTable::new(
            "ledger_accounts",
            vec![
                ProjColumn::new("id", ProjValue::U128(0)),
                ProjColumn::new("ledger", ProjValue::U32(0)),
                ProjColumn::new("debits_posted", ProjValue::U128(0)),
                ProjColumn::new("credits_posted", ProjValue::U128(0)),
            ],
            0,
        )
    }

    #[tokio::test]
    async fn hand_written_row_is_readable_by_select() {
        let database = db().await;
        let table = accounts();
        table.ensure(&database).await.unwrap();

        // Hand-write two rows into one atomic batch (as the ledger would).
        let ks = Keyspace::new(DEFAULT_TENANT);
        let mut batch = WriteBatch::new();
        for (id, dp, cp) in [(7u128, 100u128, 0u128), (9u128, 0u128, 100u128)] {
            let (k, v) = table
                .encode_row(
                    &ks,
                    &[ProjValue::U128(id), ProjValue::U32(700), ProjValue::U128(dp), ProjValue::U128(cp)],
                )
                .unwrap();
            batch.put(k, &v);
        }
        database.substrate().require_writer().unwrap().write(batch).await.unwrap();

        // SELECT * sees both rows.
        let mut glue = Glue::new(database.connection());
        let out = glue.execute("SELECT id, debits_posted, credits_posted FROM ledger_accounts ORDER BY id").await.unwrap();
        let Payload::Select { rows, .. } = &out[0] else { panic!("expected select") };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], SqlValue::U128(7));
        assert_eq!(rows[0][1], SqlValue::U128(100));

        // Point lookup on the U128 primary key works.
        let out = glue.execute("SELECT credits_posted FROM ledger_accounts WHERE id = 9").await.unwrap();
        let Payload::Select { rows, .. } = &out[0] else { panic!() };
        assert_eq!(rows[0][0], SqlValue::U128(100));

        // Conservation aggregate computes over the projection.
        let out = glue.execute("SELECT SUM(debits_posted), SUM(credits_posted) FROM ledger_accounts").await.unwrap();
        let Payload::Select { rows, .. } = &out[0] else { panic!() };
        assert_eq!(rows[0][0], rows[0][1]);
    }

    #[test]
    fn ddl_lists_columns_with_pk() {
        let ddl = accounts().create_table_ddl();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS ledger_accounts"));
        assert!(ddl.contains("id UINT128 PRIMARY KEY"));
        assert!(ddl.contains("credits_posted UINT128"));
    }
}
```

- [ ] **Step 5:** `cargo test -p bluedb-sql` + `cargo clippy -p bluedb-sql --all-targets -- -D warnings`. If the U128-PK `SELECT ... WHERE id = N` point lookup fails (GlueSQL planner quirk), keep the full-scan assertions (which must pass) and note the WHERE behavior; the projection's job is `SELECT`-visibility, which the scan proves. Commit: `feat(ledger): bluedb-sql projection API (ProjectedTable + encode_row)`.

---

## Task 2: ledger projection wiring (atomic dual-write)

**Files:**
- Create: `crates/bluedb-ledger/src/projection.rs`
- Modify: `crates/bluedb-ledger/src/ledger.rs`, `crates/bluedb-ledger/src/lib.rs`, `crates/bluedb-ledger/src/model.rs`

- [ ] **Step 1: Projection tables + mappers.** `crates/bluedb-ledger/src/projection.rs`:

```rust
//! SQL projection of ledger records. The native postcard records stay
//! canonical; these `ProjectedTable`s mirror them into `ledger_accounts` /
//! `ledger_transfers` so the data is queryable through bluedb-sql. Rows are
//! written into the *same* atomic batch as the native records (see
//! `ledger.rs`), so the projection can never lag or disagree after a crash.

use bluedb_sql::{Database, ProjColumn, ProjValue, ProjectedTable, DEFAULT_TENANT};

use crate::model::{Account, Transfer};

pub(crate) const ACCOUNTS_TABLE: &str = "ledger_accounts";
pub(crate) const TRANSFERS_TABLE: &str = "ledger_transfers";

pub(crate) fn accounts_table() -> ProjectedTable {
    use ProjValue::{U128, U16, U32, U64};
    ProjectedTable::new(
        ACCOUNTS_TABLE,
        vec![
            ProjColumn::new("id", U128(0)),
            ProjColumn::new("ledger", U32(0)),
            ProjColumn::new("code", U16(0)),
            ProjColumn::new("flags", U16(0)),
            ProjColumn::new("debits_pending", U128(0)),
            ProjColumn::new("debits_posted", U128(0)),
            ProjColumn::new("credits_pending", U128(0)),
            ProjColumn::new("credits_posted", U128(0)),
            ProjColumn::new("user_data_128", U128(0)),
            ProjColumn::new("user_data_64", U64(0)),
            ProjColumn::new("user_data_32", U32(0)),
            ProjColumn::new("timestamp", U64(0)),
        ],
        0,
    )
}

pub(crate) fn transfers_table() -> ProjectedTable {
    use ProjValue::{U128, U16, U32, U64};
    ProjectedTable::new(
        TRANSFERS_TABLE,
        vec![
            ProjColumn::new("id", U128(0)),
            ProjColumn::new("debit_account_id", U128(0)),
            ProjColumn::new("credit_account_id", U128(0)),
            ProjColumn::new("amount", U128(0)),
            ProjColumn::new("pending_id", U128(0)),
            ProjColumn::new("user_data_128", U128(0)),
            ProjColumn::new("user_data_64", U64(0)),
            ProjColumn::new("user_data_32", U32(0)),
            ProjColumn::new("timeout", U32(0)),
            ProjColumn::new("ledger", U32(0)),
            ProjColumn::new("code", U16(0)),
            ProjColumn::new("flags", U16(0)),
            ProjColumn::new("timestamp", U64(0)),
        ],
        0,
    )
}

pub(crate) fn project_account(a: &Account) -> Vec<ProjValue> {
    use ProjValue::{U128, U16, U32, U64};
    vec![
        U128(a.id), U32(a.ledger), U16(a.code), U16(a.flags.0),
        U128(a.debits_pending), U128(a.debits_posted),
        U128(a.credits_pending), U128(a.credits_posted),
        U128(a.user_data_128), U64(a.user_data_64), U32(a.user_data_32),
        U64(a.timestamp),
    ]
}

pub(crate) fn project_transfer(t: &Transfer) -> Vec<ProjValue> {
    use ProjValue::{U128, U16, U32, U64};
    vec![
        U128(t.id), U128(t.debit_account_id), U128(t.credit_account_id),
        U128(t.amount), U128(t.pending_id), U128(t.user_data_128),
        U64(t.user_data_64), U32(t.user_data_32), U32(t.timeout),
        U32(t.ledger), U16(t.code), U16(t.flags.0), U64(t.timestamp),
    ]
}

/// Idempotently create both projection tables on `database` (active writer).
/// Call on writer promotion (and in tests/Jepsen setup) so the tables exist
/// before any `SELECT`.
pub async fn ensure_schema(database: &Database) -> anyhow::Result<()> {
    accounts_table().ensure(database).await?;
    transfers_table().ensure(database).await?;
    Ok(())
}
```

> Confirm the exact `Account`/`Transfer` field names against `model.rs` (e.g. `flags.0` for the `AccountFlags(pub u16)` newtype). Fix any mismatch.

- [ ] **Step 2: Dual-write in `create_accounts`.** In `ledger.rs`, the projection tables + sql keyspace are cheap to build per call. Replace the commit block (currently ledger.rs ~196–205):

```rust
        let mut batch = WriteBatch::new();
        let sql_ks = bluedb_sql::Keyspace::new(bluedb_sql::DEFAULT_TENANT);
        let accounts_tbl = crate::projection::accounts_table();
        for a in &accepted {
            batch.put(self.keyspace.account_key(a.id), &encode(a)?);
            let (k, v) = accounts_tbl.encode_row(&sql_ks, &crate::projection::project_account(a))?;
            batch.put(k, &v);
        }
        if !accepted.is_empty() {
            batch.put(self.keyspace.watermark_key(), ts.last.to_be_bytes());
            writer.write(batch).await?;
        }
```

- [ ] **Step 3: Dual-write in `create_transfers`.** In the commit block (currently ledger.rs ~358–387): for each `dirty` account, also project the (post-mutation) account from `state.working`; for each accepted transfer, also project it:

```rust
        let mut batch = WriteBatch::new();
        let sql_ks = bluedb_sql::Keyspace::new(bluedb_sql::DEFAULT_TENANT);
        let accounts_tbl = crate::projection::accounts_table();
        let transfers_tbl = crate::projection::transfers_table();
        for id in &state.dirty {
            if let Some(account) = state.working.get(id) {
                batch.put(self.keyspace.account_key(*id), &encode(account)?);
                let (k, v) = accounts_tbl.encode_row(&sql_ks, &crate::projection::project_account(account))?;
                batch.put(k, &v);
            }
        }
        for t in &accepted {
            batch.put(self.keyspace.transfer_key(t.id), &encode(t)?);
            let (k, v) = transfers_tbl.encode_row(&sql_ks, &crate::projection::project_transfer(t))?;
            batch.put(k, &v);
            // ... existing expiry-index put stays ...
        }
        // ... existing pending-state puts, expiry deletes, failed-id puts, watermark stay ...
```

> Keep the existing `state.working.get` access pattern — match how the current code reads the dirty account (the summary shows `for id in &state.dirty { ... batch.put(account_key, encode(account)) }`; reuse that exact accessor, just add the two projection lines).

- [ ] **Step 4: Module + exports.** In `lib.rs`: `mod projection;` and `pub use projection::ensure_schema;`. Keep `mod`-private the table/mapper fns.

- [ ] **Step 5: Result-code + record serde.** In `model.rs`, add to both result enums:

```rust
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreateAccountResult { /* ... */ }
```

(same for `CreateTransferResult`). Add `use serde::{Serialize, Deserialize};` if missing. `Account`/`Transfer` already derive `Serialize`/`Deserialize` (postcard) — verify, add if absent.

- [ ] **Step 6: Tests** (in `ledger.rs` `#[cfg(test)]`, reuse `store::test_harness::writer_database`). After `ensure_schema`, drive the engine then read through `Glue` on the *same* `Database`:

```rust
#[tokio::test]
async fn projection_mirrors_native_state_and_conserves() {
    use gluesql_core::prelude::{Glue, Payload, Value as SqlValue};
    let database = crate::store::test_harness::writer_database().await;
    crate::projection::ensure_schema(&database).await.unwrap();
    let ledger = Ledger::new(&database);

    // two accounts in ledger 700
    let r = ledger.create_accounts(&[
        Account::input(1, 700), Account::input(2, 700),
    ]).await.unwrap();
    assert!(matches!(r[0], CreateAccountResult::Created));

    // a transfer of 100 from 1 -> 2
    let r = ledger.create_transfers(&[Transfer::new(10, 1, 2, 100, 700)]).await.unwrap();
    assert!(matches!(r[0], CreateTransferResult::Created));

    // projection agrees with canonical lookups
    let mut glue = Glue::new(database.connection());
    let out = glue.execute("SELECT debits_posted FROM ledger_accounts WHERE id = 1").await.unwrap();
    let Payload::Select { rows, .. } = &out[0] else { panic!() };
    assert_eq!(rows[0][0], SqlValue::U128(100));

    // conservation: sum of debits == sum of credits
    let out = glue.execute("SELECT SUM(debits_posted), SUM(credits_posted) FROM ledger_accounts").await.unwrap();
    let Payload::Select { rows, .. } = &out[0] else { panic!() };
    assert_eq!(rows[0][0], rows[0][1]);

    // the transfer is visible in the transfers projection
    let out = glue.execute("SELECT amount FROM ledger_transfers WHERE id = 10").await.unwrap();
    let Payload::Select { rows, .. } = &out[0] else { panic!() };
    assert_eq!(rows[0][0], SqlValue::U128(100));
}
```

> bluedb-ledger's dev-deps need `gluesql-core` + `slatedb` (already a dep) for this test. Add `gluesql-core = { workspace = true }` under `[dev-dependencies]` in `crates/bluedb-ledger/Cargo.toml` if not present. Confirm `Account::input` / `Transfer::new` constructor names against `model.rs`.

- [ ] **Step 7:** `cargo test -p bluedb-ledger` + `cargo clippy -p bluedb-ledger --all-targets -- -D warnings`. Commit: `feat(ledger): atomic SQL projection of accounts + transfers`.

---

## Task 3: `/ledger/*` HTTP endpoints

**Files:**
- Modify: `crates/bluedb-server/Cargo.toml`
- Create: `crates/bluedb-server/src/ledger_api.rs`
- Modify: `crates/bluedb-server/src/lib.rs`

- [ ] **Step 1: Deps.** In `crates/bluedb-server/Cargo.toml` `[dependencies]` add `bluedb-ledger = { workspace = true }` and `serde = { workspace = true }` (for the DTO derives). `serde_json`/`axum` already present.

- [ ] **Step 2: `AppState` ledger accessor + schema bootstrap.** In `lib.rs`:

```rust
use bluedb_ledger::Ledger;

impl AppState {
    /// Build a `Ledger` over the currently-bound database, or `503` if unbound.
    /// Clones the `Database` (cheap, Arc-based) out of the role lock — like the
    /// SQL connection path — so the handle reflects the node's current role.
    async fn ledger(&self) -> Result<Ledger, AppError> {
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(Ledger::new(db)),
            None => Err(AppError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "node has no database yet (no writer has been promoted)".to_string(),
            }),
        }
    }
}
```

In `promote()`, after `*self.inner.db.write().await = Some(...)`, ensure the ledger schema (best-effort; log on error but don't fail promotion):

```rust
        if let Some(db) = self.inner.db.read().await.as_ref() {
            if let Err(err) = bluedb_ledger::ensure_schema(db).await {
                tracing::warn!("ensure ledger schema: {err}");
            }
        }
```

> `Ledger::new(db)` borrows `&Database` but extracts owned `Substrate`/`WriteLease`, so the returned `Ledger` outlives the read guard. Confirm `ledger()` drops the guard before returning — it must `let db = guard.as_ref()...; Ok(Ledger::new(db))` returns an owned `Ledger`; the guard drops at the end of the match arm. If the borrow checker complains, clone: `let db = self.inner.db.read().await.as_ref().cloned();` then build from `&db`.

- [ ] **Step 3: DTOs + handlers.** `crates/bluedb-server/src/ledger_api.rs`. JSON shape: u128/u64 fields as strings, small fields as numbers; lenient input (`string | number`). Per-item result: `{ "index": i, "id": "..", "result": "created" }`.

```rust
//! `/ledger/*` — the double-entry ledger HTTP surface over [`bluedb_ledger`].
//! Batched `create_accounts`/`create_transfers` return one result code per
//! input item (TigerBeetle-style); `id`/`amount`/128-bit fields cross the wire
//! as decimal strings so large values survive JSON clients.

use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use bluedb_ledger::{Account, AccountFlags, CreateAccountResult, CreateTransferResult, Transfer, TransferFlags};

use crate::{AppError, AppState};

/// Parse a JSON value that may be a decimal string or a number into u128.
fn as_u128(v: &Value, field: &str) -> Result<u128, AppError> {
    match v {
        Value::String(s) => s.parse().map_err(|_| AppError::bad_request(format!("{field}: invalid u128 '{s}'"))),
        Value::Number(n) => n.as_u64().map(|x| x as u128)
            .ok_or_else(|| AppError::bad_request(format!("{field}: not a u128"))),
        Value::Null => Ok(0),
        _ => Err(AppError::bad_request(format!("{field}: expected string or number"))),
    }
}
fn as_u64(v: Option<&Value>, field: &str) -> Result<u64, AppError> { /* same shape, default 0 when None/Null */ }
fn as_u32(v: Option<&Value>) -> u32 { /* number-or-string, default 0 */ }
fn as_u16(v: Option<&Value>) -> u16 { /* number-or-string, default 0 */ }
```

Account input parsing (object → `Account`): required `id`, `ledger`; optional `code`, `flags` (number bitfield), `user_data_*`. Use the model builders (`Account::input(id, ledger).with_code(..).with_flags(AccountFlags(..)).with_user_data_*`). Transfer input: required `id`, `debit_account_id`, `credit_account_id`, `amount`, `ledger`; optional `code`, `flags`, `pending_id`, `timeout`, `user_data_*`.

```rust
fn parse_account(obj: &serde_json::Map<String, Value>) -> Result<Account, AppError> {
    let id = as_u128(obj.get("id").ok_or_else(|| AppError::bad_request("account: missing id"))?, "id")?;
    let ledger = as_u32(obj.get("ledger"));
    let mut a = Account::input(id, ledger)
        .with_code(as_u16(obj.get("code")))
        .with_flags(AccountFlags(as_u16(obj.get("flags"))));
    if let Some(v) = obj.get("user_data_128") { a = a.with_user_data_128(as_u128(v, "user_data_128")?); }
    // user_data_64 / user_data_32 similarly
    Ok(a)
}

fn account_json(a: &Account) -> Value {
    json!({
        "id": a.id.to_string(), "ledger": a.ledger, "code": a.code, "flags": a.flags.0,
        "debits_pending": a.debits_pending.to_string(), "debits_posted": a.debits_posted.to_string(),
        "credits_pending": a.credits_pending.to_string(), "credits_posted": a.credits_posted.to_string(),
        "user_data_128": a.user_data_128.to_string(), "user_data_64": a.user_data_64.to_string(),
        "user_data_32": a.user_data_32, "timestamp": a.timestamp.to_string(),
    })
}
// transfer_json analogous (all 128-bit + timestamp as strings).

/// POST /ledger/accounts — body: array (or single object) of account specs.
pub async fn create_accounts(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    let specs = parse_account_array(body)?;          // Vec<Account>
    let ledger = state.ledger().await?;
    let results = ledger.create_accounts(&specs).await.map_err(|e| AppError::internal(e.to_string()))?;
    let out: Vec<Value> = specs.iter().zip(results).enumerate()
        .map(|(i, (a, r))| json!({ "index": i, "id": a.id.to_string(), "result": result_str_account(r) }))
        .collect();
    Ok(Json(json!({ "results": out })))
}
// create_transfers analogous.

/// GET /ledger/accounts/{id}
pub async fn get_account(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<Value>, AppError> {
    let id: u128 = id.parse().map_err(|_| AppError::bad_request("invalid account id"))?;
    let ledger = state.ledger().await?;
    match ledger.lookup_account(id).await.map_err(|e| AppError::internal(e.to_string()))? {
        Some(a) => Ok(Json(account_json(&a))),
        None => Err(AppError { status: axum::http::StatusCode::NOT_FOUND, message: format!("account {id} not found") }),
    }
}
// get_transfer analogous.
```

Result-code strings: serialize via the `Serialize`+`snake_case` derive added in Task 2, e.g. `serde_json::to_value(r).unwrap()` → a JSON string; or a small `result_str_account`/`result_str_transfer` that does `serde_json::to_value(&r).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_else(|| "unknown".into())`.

> `AppError::bad_request`/`internal` are currently private to `lib.rs`. Make them `pub(crate)` (and `AppError` fields stay private). Add `pub(crate)` to the `AppError` struct if `ledger_api.rs` needs to construct the NOT_FOUND/503 variants — or add `AppError::not_found(msg)` and a `pub(crate)` constructor. Keep it minimal.

- [ ] **Step 4: Mount routes + module.** In `lib.rs`: `mod ledger_api;` and in `build_app`:

```rust
        .route("/ledger/accounts", post(ledger_api::create_accounts))
        .route("/ledger/transfers", post(ledger_api::create_transfers))
        .route("/ledger/accounts/{id}", get(ledger_api::get_account))
        .route("/ledger/transfers/{id}", get(ledger_api::get_transfer))
```

- [ ] **Step 5: Tests** (in `lib.rs` test module or `ledger_api.rs`), driving `build_app` via `tower::ServiceExt::oneshot` like the existing server tests. Need a promoted in-memory `AppState`; reuse the existing test harness that builds one (grep the current `#[cfg(test)]` in `lib.rs` for how it constructs+promotes `AppState`). Tests:
  - POST `/ledger/accounts` with `[{"id":"1","ledger":700},{"id":"2","ledger":700}]` → 200, `results[0].result == "created"`.
  - POST `/ledger/transfers` with `[{"id":"10","debit_account_id":"1","credit_account_id":"2","amount":"100","ledger":700}]` → `results[0].result == "created"`.
  - GET `/ledger/accounts/1` → `debits_posted == "100"`; GET `/ledger/accounts/2` → `credits_posted == "100"`.
  - GET `/ledger/accounts/999` → 404.
  - POST a duplicate-id transfer → `"exists"`; POST a transfer to a missing account (id "3") → the right code string (e.g. `"credit_account_not_found"`).
  - **Projection through the server:** POST to `/sql` with `SELECT SUM(debits_posted), SUM(credits_posted) FROM ledger_accounts` → equal sums (proves the projection is visible end-to-end via HTTP).
  - Passive node (not promoted) → POST `/ledger/transfers` returns 503.

- [ ] **Step 6:** `cargo test -p bluedb-server` + `cargo clippy -p bluedb-server --all-targets -- -D warnings`. Then full `cargo test --workspace` + `cargo clippy --workspace --all-targets -- -D warnings`. Commit: `feat(server): /ledger/* HTTP endpoints with per-item result codes`.

---

## Task 4: Jepsen `ledger` workload

**Files:**
- Create: `jepsen/src/bluedb/jepsen/ledger.clj`
- Modify: `jepsen/src/bluedb/jepsen/core.clj`

> Dispatch this as a subagent task (Clojure, isolated). Provide the subagent: the `/ledger/*` API contract (Task 3), `jepsen/src/bluedb/jepsen/http.clj` (leader-aware HTTP helpers), `jepsen/src/bluedb/jepsen/counter.clj` (closest pattern — a numeric RMW workload), and `jepsen/src/bluedb/jepsen/core.clj` (workload registration). Do NOT have it read this whole plan.

- [ ] **Step 1: Workload namespace** `jepsen/src/bluedb/jepsen/ledger.clj`:
  - **Setup** (`setup!` on the client): create a fixed set of accounts (e.g. ids 1..`n-accounts`, all in `ledger 700`) via `POST /ledger/accounts`. Idempotent — `exists` results are fine on retry.
  - **Operations** the generator emits:
    - `{:f :transfer :value {:id <unique> :from a :to b :amount k}}` → `POST /ledger/transfers` with one transfer; record the returned result code on the op (`:result :created|:exists|...`). Use a globally unique transfer id (e.g. `(swap! counter inc)` seeded per process, or `(+ (* process 1e9) seq)`).
    - `{:f :read}` → `GET /ledger/accounts/{id}` for every account; return `{:f :read :value {a {:debits_posted .. :credits_posted ..} ...}}` (parse the string fields with `bigint`).
  - **Client**: leader-aware routing via `http.clj` (mirror `counter.clj`). On indeterminate errors (timeout/conn-reset under nemesis), return `:info` so Jepsen treats the transfer as indeterminate (it may or may not have applied) — the checker must tolerate this.

- [ ] **Step 2: Checker** (`ledger/checker`): a custom `reify Checker` that, from the final `:read` (taken after the nemesis stops, during a quiescent recovery window):
  - **Conservation:** `(= (sum credits_posted) (sum debits_posted))` across all accounts — must hold absolutely (every applied transfer adds equal amounts to one debit and one credit). A violation = lost or torn write → `:valid? false`.
  - **No-double-apply / accounting:** compute `applied` = sum of amounts of transfers whose op `:result` was `:created`. Indeterminate (`:info`) transfers may or may not be applied, so the conserved total must satisfy `created-sum <= observed-debits-total <= created-sum + indeterminate-sum`. Report the bounds and whether the observed total falls inside.
  - Emit `{:valid? bool :conserved bool :observed-debits .. :observed-credits .. :created-sum .. :indeterminate-sum ..}`.

- [ ] **Step 3: Generator + nemesis**: random `:transfer`s between distinct accounts with small amounts, interspersed `:read`s; standard `nemesis/mix` (kill/partition/pause/skew already in `nemesis.clj`); a final read after a recovery quiesce (use `(gen/phases ... (gen/once {:f :read}))` or the `:final-generator` pattern the other workloads use).

- [ ] **Step 4: Register in `core.clj`:**
  - `(:require ... [bluedb.jepsen.ledger :as ledger])`.
  - Add a `ledger-workload` builder (mirror `counter-workload`) returning `{:client .. :generator .. :final-generator .. :checker (ledger/checker) :nemesis ..}` and the table-setup DDL is **not** needed (the server bootstraps the ledger schema on promote; accounts are created by the client `setup!`).
  - Add `"ledger" ledger-workload` to the workload `case`.
  - Add `"ledger"` to the `--workload` validate set + the option doc string (`set | list-append | counter | unique | ledger`).

- [ ] **Step 5: Verify it compiles + a short smoke run.** `cd jepsen && lein check` (or `lein compile`) must pass. A real cluster run is out of scope for CI; document the command in a comment at the top of `ledger.clj`:

```
lein run test --workload ledger --nemesis mix --time-limit 120 --concurrency 10 ...
```

- [ ] **Step 6:** Commit: `feat(jepsen): ledger workload — double-entry conservation under faults`.

---

## Final review

After all four tasks land green:
- `cargo test --workspace` + `cargo clippy --workspace --all-targets -- -D warnings` clean.
- `cd jepsen && lein check` clean.
- Dispatch a final spec-compliance review (does Phase H match spec §12 H: SQL projection ✓, batched `/ledger/*` per-item result codes ✓, Jepsen conservation + no-double-apply + result-code correctness across faults ✓) and a code-quality review across the diff.
- Then `superpowers:finishing-a-development-branch`.
