//! [`SlateDbStorage`] — a GlueSQL custom storage backed by SlateDB.
//!
//! This wires GlueSQL's pluggable [`Store`]/[`StoreMut`] traits — plus real
//! [`Transaction`], [`Index`]/[`IndexMut`] and [`Metadata`] — onto bluedb's
//! object-storage substrate. Tables (schema'd *and* schemaless), rows, inserts,
//! updates, deletes, ordered scans, secondary indexes and multi-statement
//! transactions all land in one SlateDB keyspace using the encoding documented
//! in [`crate::keyspace`].
//!
//! ## Values
//!
//! Both schemas and rows are serialized with `serde_json` (GlueSQL's `Schema`,
//! `Key` and `DataRow` all derive `serde`). JSON keeps the on-disk form
//! debuggable; swap to `postcard`/`bincode` later if size matters — the seam is
//! [`encode`]/[`decode`].
//!
//! Each stored row value is a [`StoredRow`] holding *both* the GlueSQL `Key`
//! and the `DataRow`. We keep the `Key` in the value (not just baked into the
//! storage key) because [`Key::to_cmp_be_bytes`](gluesql_core::data::Key::to_cmp_be_bytes)
//! is a one-way, order-preserving encoding: a `scan` needs to hand GlueSQL back
//! the original `Key`, which we can only recover by storing it verbatim.
//!
//! ## Multi-tenancy
//!
//! Every key is prefixed with a tenant namespace (see [`crate::keyspace`]), so
//! many tenants can share one [`Db`]. Construct per-tenant storages with
//! [`SlateDbStorage::new_for_tenant`]; [`SlateDbStorage::new`] uses the default
//! tenant [`DEFAULT_TENANT`](crate::keyspace::DEFAULT_TENANT).
//!
//! ## Transactions & isolation
//!
//! GlueSQL drives `BEGIN`/`COMMIT`/`ROLLBACK` through the [`Transaction`] trait.
//! We implement them with a **write-buffer overlay + atomic batch commit** on
//! SlateDB's single-writer model:
//!
//! * `begin` captures a [`DbSnapshot`] (a consistent point-in-time read view)
//!   and opens an empty overlay — an ordered `BTreeMap<Vec<u8>, Option<Vec<u8>>>`
//!   keyed by the encoded storage key (`Some` = put, `None` = tombstone).
//! * **Writes** inside the txn mutate the overlay only; outside a txn they
//!   write through to SlateDB directly (the original autocommit behavior).
//! * **Reads** inside the txn merge the overlay over the *snapshot*: overlay
//!   entries shadow the base, tombstones hide base rows, and `scan_data`
//!   performs an ordered merge so reads see the txn's own writes in correct
//!   key order (read-your-own-writes).
//! * `commit` applies the whole overlay as a single atomic SlateDB
//!   [`WriteBatch`](slatedb::WriteBatch) via [`Db::write`], then clears it.
//!   `rollback` just drops the overlay.
//!
//! **Isolation level: snapshot isolation.** Base reads inside a transaction go
//! through the snapshot captured at `begin`, so a long-running txn never sees
//! writes committed by others after it began, and its own buffered writes are
//! layered on top. Commit is all-or-nothing (one `WriteBatch`). SlateDB is a
//! single-writer store, so there is no concurrent-writer conflict to detect:
//! the `&mut self` GlueSQL hands us, plus SlateDB's single-writer guarantee,
//! means only one transaction mutates a given `Db` at a time. Index entries and
//! schema records flow through the same overlay, so DDL/`CREATE INDEX`/index
//! maintenance inside a txn roll back too.
//!
//! ## Secondary indexes
//!
//! `CREATE INDEX`/`DROP INDEX` and index-backed query plans go through
//! [`IndexMut`]/[`Index`]. Index definitions and entries live in their own
//! keyspace namespaces (see [`crate::keyspace`]); an index entry's key embeds
//! the order-preserving evaluated value followed by the row's primary key, so a
//! byte-ordered range scan yields rows in indexed-value order. Index entries
//! are maintained on every row mutation **through the same overlay**, so they
//! are transactional and stay consistent across INSERT/UPDATE/DELETE.
//!
//! ## Concurrency / `&mut self`
//!
//! Read-path `StoreMut`/`Transaction` methods take `&mut self`; SlateDB's `Db`
//! mutators take `&self`, so we hold an `Arc<Db>` and the `&mut self` borrow is
//! only the GlueSQL-side exclusivity guarantee — no interior locking is needed.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use gluesql_core::ast::{IndexOperator, OrderByExpr};
use gluesql_core::data::{Key, Schema, SchemaIndex, SchemaIndexOrd, Value};
use gluesql_core::error::Result as GlueResult;
use gluesql_core::executor::evaluate_stateless;
use gluesql_core::store::{
    AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexError, IndexMut, Metadata,
    Planner, RowIter, Store, StoreMut, Transaction,
};
use serde::{Deserialize, Serialize};
use slatedb::config::ScanOptions;
use slatedb::{Db, DbSnapshot, WriteBatch};
use tokio::sync::{Mutex, OwnedMutexGuard};

use bluedb_storage::Substrate;

use crate::error::SqlError;
use crate::keyspace::{prefix_upper_bound, Keyspace, DEFAULT_TENANT};

/// A shared **write lease** — the async mutex that serializes write
/// transactions across connections to one [`Db`]. Connections created from the
/// same [`crate::Database`] share one of these; a standalone
/// [`SlateDbStorage::new`] gets its own (it is the sole writer).
pub(crate) type WriteLease = Arc<Mutex<()>>;

/// The stored form of a data row: the primary key plus the row payload.
#[derive(Serialize, Deserialize)]
struct StoredRow {
    key: Key,
    row: DataRow,
}

/// State for an in-flight (non-autocommit) transaction.
struct TxnState {
    /// Buffered mutations keyed by encoded storage key: `Some` = put, `None` =
    /// delete (tombstone). Ordered so range merges over the base are cheap.
    overlay: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Point-in-time read view of SlateDB captured at `begin` (after the write
    /// lease is held), giving snapshot isolation.
    snapshot: Arc<DbSnapshot>,
    /// Exclusive write lease held for the duration of this transaction. Held
    /// from `begin` until `commit`/`rollback`, it serializes write transactions
    /// across connections to the same `Db`: a second connection's `BEGIN`
    /// blocks here until this one ends, so its snapshot sees this txn's commit
    /// and no update is lost. Dropping it (on commit/rollback, or if the
    /// connection is dropped mid-txn) releases the lease.
    _lease: OwnedMutexGuard<()>,
}

/// GlueSQL custom storage over a SlateDB database.
///
/// Construct with [`SlateDbStorage::new`] (default tenant) or
/// [`SlateDbStorage::new_for_tenant`] from an already-open [`Db`], then hand it
/// to `gluesql_core::prelude::Glue::new`.
pub struct SlateDbStorage {
    /// The SlateDB handle this connection reads/writes through: a writer `Db`
    /// (active node) or a read-only `DbReader` (replica). Writes and `BEGIN`
    /// require the writer.
    substrate: Substrate,
    keyspace: Keyspace,
    /// Shared write lease serializing write transactions over this `Db`.
    write_lease: WriteLease,
    /// `Some` while a `BEGIN ... COMMIT/ROLLBACK` block is open.
    txn: Option<TxnState>,
}

impl SlateDbStorage {
    /// Wrap an already-open SlateDB database under the default tenant, as a
    /// **standalone** connection (its own private write lease — it is the sole
    /// writer). To run *several* coordinated connections over one `Db` (so their
    /// write transactions serialize against each other), create them from a
    /// shared [`crate::Database`] instead.
    pub fn new(db: Arc<Db>) -> Self {
        Self::new_for_tenant(db, DEFAULT_TENANT)
    }

    /// Wrap an already-open SlateDB database scoped to `tenant`, standalone.
    ///
    /// Every key this storage reads or writes is namespaced to `tenant`, so two
    /// storages over the same [`Db`] with different tenants share no data.
    pub fn new_for_tenant(db: Arc<Db>, tenant: &str) -> Self {
        Self::with_substrate(Substrate::writer(db), tenant, Arc::new(Mutex::new(())))
    }

    /// Construct over any [`Substrate`] (writer or read replica), sharing
    /// `write_lease`. A reader substrate yields a **read-only** connection:
    /// reads work, but writes and `BEGIN` error (`require_writer`).
    pub(crate) fn with_substrate(substrate: Substrate, tenant: &str, write_lease: WriteLease) -> Self {
        Self {
            substrate,
            keyspace: Keyspace::new(tenant),
            write_lease,
            txn: None,
        }
    }

    /// The writer handle, or a read-only error if this connection is a replica.
    fn writer(&self) -> Result<&Arc<Db>, SqlError> {
        Ok(self.substrate.require_writer()?)
    }

    // --- Unified read/write through the optional transaction overlay. -------

    /// Write a key (put). Buffered in the overlay when a txn is active,
    /// otherwise written straight through to SlateDB.
    async fn write_key(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), SqlError> {
        match self.txn.as_mut() {
            Some(txn) => {
                txn.overlay.insert(key, Some(value));
                Ok(())
            }
            None => {
                self.writer()?.put(&key, &value).await?;
                Ok(())
            }
        }
    }

    /// Delete a key. Buffered as a tombstone in the overlay when a txn is
    /// active, otherwise deleted straight through.
    async fn delete_key(&mut self, key: Vec<u8>) -> Result<(), SqlError> {
        match self.txn.as_mut() {
            Some(txn) => {
                txn.overlay.insert(key, None);
                Ok(())
            }
            None => {
                self.writer()?.delete(&key).await?;
                Ok(())
            }
        }
    }

    /// Read one key, honoring the overlay (and the txn snapshot) when a txn is
    /// active.
    async fn read_key(&self, key: &[u8]) -> Result<Option<Vec<u8>>, SqlError> {
        match self.txn.as_ref() {
            Some(txn) => match txn.overlay.get(key) {
                // Overlay shadows the base: a put or a tombstone wins.
                Some(Some(value)) => Ok(Some(value.clone())),
                Some(None) => Ok(None),
                None => Ok(txn.snapshot.get(key).await?.map(|b| b.to_vec())),
            },
            None => Ok(self.substrate.get(key).await?.map(|b| b.to_vec())),
        }
    }

    /// Scan all `(storage_key, value)` pairs whose key falls in
    /// `[start, end)` (or `[start, ..)` when `end` is `None`), in key order,
    /// merging the overlay over the base. Tombstones drop base rows; overlay
    /// puts shadow base values.
    async fn scan_range(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SqlError> {
        // 1. Pull the base rows (from the snapshot inside a txn, else live db).
        let base = self.scan_range_base(&start, end.as_deref()).await?;

        let txn = match self.txn.as_ref() {
            None => return Ok(base),
            Some(txn) => txn,
        };

        // 2. Ordered merge of base with the overlay slice in the same range.
        let upper = match &end {
            Some(end) => Bound::Excluded(end.clone()),
            None => Bound::Unbounded,
        };
        let overlay_slice = txn
            .overlay
            .range((Bound::Included(start.clone()), upper))
            .map(|(k, v)| (k.clone(), v.clone()));

        Ok(merge_sorted(base, overlay_slice))
    }

    /// Raw base scan (no overlay) over `[start, end)`, reading from the txn
    /// snapshot when one is open and from the live db otherwise.
    async fn scan_range_base(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SqlError> {
        let mut out = Vec::new();
        // SlateDB's snapshot and db share the same scan surface but are distinct
        // types, so we branch and reuse a small drain helper.
        match self.txn.as_ref() {
            Some(txn) => {
                let mut iter = match end {
                    Some(end) => {
                        txn.snapshot
                            .scan_with_options(start.to_vec()..end.to_vec(), &ScanOptions::default())
                            .await?
                    }
                    None => {
                        txn.snapshot
                            .scan_with_options(start.to_vec().., &ScanOptions::default())
                            .await?
                    }
                };
                while let Some(kv) = iter.next().await? {
                    out.push((kv.key.to_vec(), kv.value.to_vec()));
                }
            }
            None => {
                // Live read through the substrate (writer Db or read replica).
                let mut iter = self.substrate.scan_range(start, end).await?;
                while let Some(kv) = iter.next().await? {
                    out.push((kv.key.to_vec(), kv.value.to_vec()));
                }
            }
        }
        Ok(out)
    }

    /// Drain every row of `table_name` in primary-key order, honoring the txn
    /// overlay. Rows come back sorted because storage keys sort by encoded pk.
    async fn collect_rows(&self, table_name: &str) -> Result<Vec<(Key, DataRow)>, SqlError> {
        let prefix = self.keyspace.data_prefix(table_name);
        let end = prefix_upper_bound(&prefix);
        let pairs = self.scan_range(prefix, end).await?;
        let mut rows = Vec::with_capacity(pairs.len());
        for (_, value) in pairs {
            let stored: StoredRow = decode(&value)?;
            rows.push((stored.key, stored.row));
        }
        Ok(rows)
    }

    /// Read a table's schema honoring the overlay (used by index maintenance,
    /// which runs inside `StoreMut` calls that may be within a txn).
    async fn read_schema(&self, table_name: &str) -> Result<Option<Schema>, SqlError> {
        let key = self.keyspace.schema_key(table_name);
        match self.read_key(&key).await? {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }

    // --- Secondary-index helpers. -------------------------------------------

    /// All index definitions for `table_name`, honoring the overlay.
    async fn index_defs(&self, table_name: &str) -> Result<Vec<SchemaIndex>, SqlError> {
        match self.read_schema(table_name).await? {
            Some(schema) => Ok(schema.indexes),
            None => Ok(Vec::new()),
        }
    }

    /// Evaluate one index's expression against a row to get its indexed value,
    /// then convert it to an order-preserving [`Key`].
    async fn index_value(
        index: &SchemaIndex,
        columns: Option<&[String]>,
        row: &DataRow,
    ) -> Result<Key, SqlError> {
        let context = row.as_context(columns);
        let evaluated = evaluate_stateless(Some(context), &index.expr)
            .await
            .map_err(|e| SqlError::IndexEval(e.to_string()))?;
        let value = Value::try_from(evaluated).map_err(|e| SqlError::IndexEval(e.to_string()))?;
        Key::try_from(value).map_err(|e| SqlError::IndexEval(e.to_string()))
    }

    /// Column names for a table (empty for schemaless tables — index exprs on
    /// schemaless tables are rejected by GlueSQL's `validate_index_expr`).
    fn schema_columns(schema: &Schema) -> Vec<String> {
        schema
            .column_defs
            .as_ref()
            .map(|defs| defs.iter().map(|c| c.name.clone()).collect())
            .unwrap_or_default()
    }

    /// Add or remove every index entry for `row` (keyed by `pk`).
    async fn apply_index_entries(
        &mut self,
        table_name: &str,
        pk: &Key,
        row: &DataRow,
        insert: bool,
    ) -> Result<(), SqlError> {
        let defs = self.index_defs(table_name).await?;
        if defs.is_empty() {
            return Ok(());
        }
        let columns = match self.read_schema(table_name).await? {
            Some(schema) => Self::schema_columns(&schema),
            None => Vec::new(),
        };
        let cols = if columns.is_empty() {
            None
        } else {
            Some(columns.as_slice())
        };
        for def in &defs {
            let value = Self::index_value(def, cols, row).await?;
            let entry_key = self
                .keyspace
                .index_entry_key(table_name, &def.name, &value, pk)?;
            if insert {
                self.write_key(entry_key, encode(pk)?).await?;
            } else {
                self.delete_key(entry_key).await?;
            }
        }
        Ok(())
    }
}

/// Serialize a value to bytes (JSON).
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, SqlError> {
    Ok(serde_json::to_vec(value)?)
}

/// Deserialize bytes (JSON) back into a value.
fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, SqlError> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Merge an already-sorted base scan with an already-sorted overlay slice.
///
/// Both inputs are ordered by key. Overlay entries shadow base entries with the
/// same key; an overlay `None` (tombstone) drops the key entirely. The result
/// stays sorted by key.
fn merge_sorted(
    base: Vec<(Vec<u8>, Vec<u8>)>,
    overlay: impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut base = base.into_iter().peekable();
    let mut overlay = overlay.peekable();
    let mut out = Vec::new();

    loop {
        match (base.peek(), overlay.peek()) {
            (Some((bk, _)), Some((ok, _))) => {
                use std::cmp::Ordering::*;
                match bk.cmp(ok) {
                    Less => out.push(base.next().unwrap()),
                    Greater => {
                        let (k, v) = overlay.next().unwrap();
                        if let Some(v) = v {
                            out.push((k, v));
                        }
                    }
                    Equal => {
                        // Overlay wins; drop the base value.
                        base.next();
                        let (k, v) = overlay.next().unwrap();
                        if let Some(v) = v {
                            out.push((k, v));
                        }
                    }
                }
            }
            (Some(_), None) => out.push(base.next().unwrap()),
            (None, Some(_)) => {
                let (k, v) = overlay.next().unwrap();
                if let Some(v) = v {
                    out.push((k, v));
                }
            }
            (None, None) => break,
        }
    }
    out
}

#[async_trait]
impl Store for SlateDbStorage {
    async fn fetch_schema(&self, table_name: &str) -> GlueResult<Option<Schema>> {
        Ok(self.read_schema(table_name).await?)
    }

    async fn fetch_all_schemas(&self) -> GlueResult<Vec<Schema>> {
        let prefix = self.keyspace.schema_prefix();
        let end = prefix_upper_bound(&prefix);
        let pairs = self.scan_range(prefix, end).await?;
        let mut schemas = Vec::with_capacity(pairs.len());
        for (_, value) in pairs {
            schemas.push(decode::<Schema>(&value)?);
        }
        // GlueSQL expects schemas sorted by table name. The scan is already in
        // key order, but the byte order of <table_utf8> matches lexical name
        // order so this is mostly a safety net for the merge path.
        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));
        Ok(schemas)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> GlueResult<Option<DataRow>> {
        let storage_key = self.keyspace.row_key(table_name, key)?;
        match self.read_key(&storage_key).await? {
            Some(bytes) => {
                let stored: StoredRow = decode(&bytes)?;
                Ok(Some(stored.row))
            }
            None => Ok(None),
        }
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> GlueResult<RowIter<'a>> {
        let rows = self.collect_rows(table_name).await?;
        Ok(Box::pin(stream::iter(rows.into_iter().map(Ok))))
    }
}

#[async_trait]
impl StoreMut for SlateDbStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> GlueResult<()> {
        let key = self.keyspace.schema_key(&schema.table_name);
        self.write_key(key, encode(schema)?).await?;
        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> GlueResult<()> {
        // Drop every index entry, every row, then the schema record (which
        // carries the index *definitions*) itself.
        let rows = self.collect_rows(table_name).await?;
        for (key, row) in &rows {
            self.apply_index_entries(table_name, key, row, false).await?;
        }
        for (key, _) in &rows {
            let storage_key = self.keyspace.row_key(table_name, key)?;
            self.delete_key(storage_key).await?;
        }
        self.delete_key(self.keyspace.schema_key(table_name)).await?;
        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> GlueResult<()> {
        // `append_data` feeds schemaless / auto-incremented tables: assign each
        // row a fresh monotonically increasing I64 key. We continue from the
        // current max key (overlay-aware) so appends across calls keep advancing.
        let mut next = self
            .collect_rows(table_name)
            .await?
            .into_iter()
            .filter_map(|(key, _)| match key {
                Key::I64(n) => Some(n),
                _ => None,
            })
            .max()
            .unwrap_or(0);

        for row in rows {
            next += 1;
            let key = Key::I64(next);
            let storage_key = self.keyspace.row_key(table_name, &key)?;
            self.apply_index_entries(table_name, &key, &row, true)
                .await?;
            let stored = StoredRow {
                key: key.clone(),
                row,
            };
            self.write_key(storage_key, encode(&stored)?).await?;
        }
        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> GlueResult<()> {
        // Used for keyed inserts and updates (UPDATE re-inserts the same key).
        for (key, row) in rows {
            // For UPDATE, the old row's index entries must be removed first.
            let storage_key = self.keyspace.row_key(table_name, &key)?;
            if let Some(bytes) = self.read_key(&storage_key).await? {
                let old: StoredRow = decode(&bytes)?;
                self.apply_index_entries(table_name, &key, &old.row, false)
                    .await?;
            }
            self.apply_index_entries(table_name, &key, &row, true)
                .await?;
            let stored = StoredRow {
                key: key.clone(),
                row,
            };
            self.write_key(storage_key, encode(&stored)?).await?;
        }
        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> GlueResult<()> {
        for key in keys {
            let storage_key = self.keyspace.row_key(table_name, &key)?;
            if let Some(bytes) = self.read_key(&storage_key).await? {
                let old: StoredRow = decode(&bytes)?;
                self.apply_index_entries(table_name, &key, &old.row, false)
                    .await?;
            }
            self.delete_key(storage_key).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Transaction for SlateDbStorage {
    async fn begin(&mut self, autocommit: bool) -> GlueResult<bool> {
        if self.txn.is_some() {
            // A transaction is already open. GlueSQL's `execute` wraps every
            // non-transaction statement in `begin(true)` and auto-commits iff
            // that call returns `true` (see gluesql-core executor/execute.rs:
            // `let autocommit = storage.begin(true).await?; ... if autocommit {
            // commit() }`). While an explicit `BEGIN` block is open we must
            // return `false` so each statement's writes stay buffered in the
            // overlay until the user's explicit COMMIT/ROLLBACK — returning
            // `true` here would auto-commit (flush) after every statement and
            // make ROLLBACK a no-op. (For the `StartTransaction` statement the
            // return value is ignored, so nested BEGIN is harmless.)
            return Ok(false);
        }
        if autocommit {
            // Plain statement, no explicit BEGIN: stay in write-through mode.
            return Ok(false);
        }
        // Explicit BEGIN: acquire the exclusive write lease FIRST, then take the
        // snapshot. Taking the snapshot *after* the lease guarantees this txn
        // sees every previously-committed write (a concurrent connection's
        // `BEGIN` blocks on the lease until we commit, then snapshots our
        // result) — so write transactions are serialized and no update is lost.
        let lease = self.write_lease.clone().lock_owned().await;
        let snapshot = self.writer()?.snapshot().await.map_err(SqlError::from)?;
        self.txn = Some(TxnState {
            overlay: BTreeMap::new(),
            snapshot,
            _lease: lease,
        });
        Ok(true)
    }

    async fn rollback(&mut self) -> GlueResult<()> {
        // Drop the overlay; nothing reached SlateDB.
        self.txn = None;
        Ok(())
    }

    async fn commit(&mut self) -> GlueResult<()> {
        if let Some(txn) = self.txn.take() {
            if !txn.overlay.is_empty() {
                let mut batch = WriteBatch::new();
                for (key, op) in txn.overlay {
                    match op {
                        Some(value) => batch.put(&key, &value),
                        None => batch.delete(&key),
                    }
                }
                self.writer()?.write(batch).await.map_err(SqlError::from)?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Index for SlateDbStorage {
    async fn scan_indexed_data<'a>(
        &'a self,
        table_name: &str,
        index_name: &str,
        asc: Option<bool>,
        cmp_value: Option<(&IndexOperator, Value)>,
    ) -> GlueResult<RowIter<'a>> {
        // Resolve the scan byte range from the optional comparison.
        let full_prefix = self.keyspace.index_prefix(table_name, index_name);
        let full_end = prefix_upper_bound(&full_prefix);

        let (start, end) = match &cmp_value {
            None => (full_prefix.clone(), full_end.clone()),
            Some((op, value)) => {
                let key = Key::try_from(value.clone())
                    .map_err(|e| SqlError::IndexEval(e.to_string()))?;
                // The value-bounded prefix: all entries for exactly this value.
                let value_prefix = self
                    .keyspace
                    .index_value_prefix(table_name, index_name, &key)?;
                let value_end = prefix_upper_bound(&value_prefix);
                match op {
                    IndexOperator::Eq => (
                        value_prefix.clone(),
                        value_end.clone().or_else(|| full_end.clone()),
                    ),
                    // value < cmp : [index_prefix, value_prefix)
                    IndexOperator::Lt => (full_prefix.clone(), Some(value_prefix.clone())),
                    // value <= cmp : [index_prefix, value_end)
                    IndexOperator::LtEq => (
                        full_prefix.clone(),
                        value_end.clone().or_else(|| full_end.clone()),
                    ),
                    // value > cmp : [value_end, index_end)
                    IndexOperator::Gt => (
                        value_end.clone().unwrap_or_else(|| full_prefix.clone()),
                        full_end.clone(),
                    ),
                    // value >= cmp : [value_prefix, index_end)
                    IndexOperator::GtEq => (value_prefix.clone(), full_end.clone()),
                }
            }
        };

        // Scan the index-entry range (overlay-aware), each value is the pk.
        let entries = self.scan_range(start, end).await?;

        // Resolve each pk to its row (also overlay-aware).
        let mut rows: Vec<(Key, DataRow)> = Vec::with_capacity(entries.len());
        for (_, pk_bytes) in entries {
            let pk: Key = decode(&pk_bytes)?;
            let row_key = self.keyspace.row_key(table_name, &pk)?;
            if let Some(bytes) = self.read_key(&row_key).await? {
                let stored: StoredRow = decode(&bytes)?;
                rows.push((stored.key, stored.row));
            }
            // A missing row means a tombstoned/stale entry; skip it.
        }

        // `entries` came back in ascending (value, pk) order. Reverse for DESC.
        if asc == Some(false) {
            rows.reverse();
        }

        Ok(Box::pin(stream::iter(rows.into_iter().map(Ok))))
    }
}

#[async_trait]
impl IndexMut for SlateDbStorage {
    async fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &OrderByExpr,
    ) -> GlueResult<()> {
        let mut schema = self
            .read_schema(table_name)
            .await?
            .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()))?;
        if schema.indexes.iter().any(|i| i.name == index_name) {
            return Err(IndexError::IndexNameAlreadyExists(index_name.to_owned()).into());
        }

        let order = column
            .asc
            .map(|asc| {
                if asc {
                    SchemaIndexOrd::Asc
                } else {
                    SchemaIndexOrd::Desc
                }
            })
            .unwrap_or(SchemaIndexOrd::Both);
        let def = SchemaIndex {
            name: index_name.to_owned(),
            expr: column.expr.clone(),
            order,
            created: chrono::Utc::now().naive_utc(),
        };

        // Back-fill entries for every existing row, then persist the new schema.
        let columns = Self::schema_columns(&schema);
        let cols = if columns.is_empty() {
            None
        } else {
            Some(columns.as_slice())
        };
        let rows = self.collect_rows(table_name).await?;
        for (pk, row) in &rows {
            let value = Self::index_value(&def, cols, row).await?;
            let entry_key = self
                .keyspace
                .index_entry_key(table_name, index_name, &value, pk)?;
            self.write_key(entry_key, encode(pk)?).await?;
        }

        schema.indexes.push(def);
        let schema_key = self.keyspace.schema_key(table_name);
        self.write_key(schema_key, encode(&schema)?).await?;
        Ok(())
    }

    async fn drop_index(&mut self, table_name: &str, index_name: &str) -> GlueResult<()> {
        let mut schema = self
            .read_schema(table_name)
            .await?
            .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()))?;
        if !schema.indexes.iter().any(|i| i.name == index_name) {
            return Err(IndexError::IndexNameDoesNotExist(index_name.to_owned()).into());
        }

        // Remove all entries for this index.
        let prefix = self.keyspace.index_prefix(table_name, index_name);
        let end = prefix_upper_bound(&prefix);
        for (k, _) in self.scan_range(prefix, end).await? {
            self.delete_key(k).await?;
        }

        schema.indexes.retain(|i| i.name != index_name);
        let schema_key = self.keyspace.schema_key(table_name);
        self.write_key(schema_key, encode(&schema)?).await?;
        Ok(())
    }
}

// --- Marker traits whose gluesql-core defaults are sufficient. -------------
//
// GlueSQL composes its store bounds as:
//   GStore    = Store + Index + Metadata + CustomFunction
//   GStoreMut = StoreMut + IndexMut + AlterTable + Transaction
//               + CustomFunction + CustomFunctionMut
//   (and Glue additionally requires Planner)
//
// The traits below ship full default method implementations in gluesql-core:
//   * `Metadata`            — `scan_table_meta` defaults to empty.
//   * `CustomFunction(Mut)` — user-defined functions: default "not supported".
//   * `AlterTable`          — default impls drive RENAME/ADD/DROP COLUMN on top
//                             of our Store + StoreMut, so they work for free.
//                             (DROP COLUMN with a dropped index calls
//                             `drop_index`, which we now implement.)
//   * `Planner`             — default query planner; it consults `schema.indexes`
//                             and routes eligible predicates to
//                             `Index::scan_indexed_data`.
impl Metadata for SlateDbStorage {}
impl CustomFunction for SlateDbStorage {}
impl CustomFunctionMut for SlateDbStorage {}
impl AlterTable for SlateDbStorage {}
impl Planner for SlateDbStorage {}
