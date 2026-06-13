//! [`SlateDbStorage`] — a GlueSQL custom storage backed by SlateDB.
//!
//! This wires GlueSQL's pluggable [`Store`]/[`StoreMut`] traits onto bluedb's
//! object-storage substrate. Tables (schema'd *and* schemaless), rows, inserts,
//! updates, deletes and ordered scans all land in one SlateDB keyspace using
//! the encoding documented in [`crate::keyspace`].
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
//! ## Concurrency / `&mut self`
//!
//! `StoreMut` methods take `&mut self`, but SlateDB's `Db` mutators take
//! `&self`, so we hold an `Arc<Db>` and the `&mut self` borrow is only the
//! GlueSQL-side exclusivity guarantee — no interior locking is needed.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use gluesql_core::data::{Key, Schema};
use gluesql_core::error::Result as GlueResult;
use gluesql_core::store::{
    AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexMut, Metadata, Planner,
    RowIter, Store, StoreMut, Transaction,
};
use serde::{Deserialize, Serialize};
use slatedb::config::ScanOptions;
use slatedb::Db;

use crate::error::SqlError;
use crate::keyspace::{data_prefix, prefix_upper_bound, row_key, schema_key};

/// The stored form of a data row: the primary key plus the row payload.
#[derive(Serialize, Deserialize)]
struct StoredRow {
    key: Key,
    row: DataRow,
}

/// GlueSQL custom storage over a SlateDB database.
///
/// Construct with [`SlateDbStorage::new`] from an already-open [`Db`], then
/// hand it to `gluesql_core::prelude::Glue::new`.
#[derive(Clone)]
pub struct SlateDbStorage {
    db: Arc<Db>,
}

impl SlateDbStorage {
    /// Wrap an already-open SlateDB database.
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    /// Borrow the underlying SlateDB handle.
    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), SqlError> {
        self.db.put(&key, &value).await?;
        Ok(())
    }

    async fn delete(&self, key: Vec<u8>) -> Result<(), SqlError> {
        self.db.delete(&key).await?;
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<bytes::Bytes>, SqlError> {
        Ok(self.db.get(key).await?)
    }

    /// Drain every row of `table_name` from SlateDB in primary-key order.
    ///
    /// We resolve the whole table eagerly here: GlueSQL's [`RowIter`] is a
    /// `'a`-bound `Stream`, and threading SlateDB's own async `DbIterator`
    /// (which borrows the scan range) through that lifetime is far more
    /// delicate than just collecting. The reference in-memory storage collects
    /// too. Rows come back already sorted because the storage keys sort by the
    /// encoded primary key (see [`crate::keyspace`]).
    async fn collect_rows(&self, table_name: &str) -> Result<Vec<(Key, DataRow)>, SqlError> {
        let prefix = data_prefix(table_name);
        let mut iter = match prefix_upper_bound(&prefix) {
            Some(end) => {
                self.db
                    .scan_with_options(prefix.clone()..end, &ScanOptions::default())
                    .await?
            }
            // No finite upper bound: scan from the prefix to the end of the keyspace.
            None => {
                self.db
                    .scan_with_options(prefix.clone().., &ScanOptions::default())
                    .await?
            }
        };

        let mut rows = Vec::new();
        while let Some(kv) = iter.next().await? {
            let stored: StoredRow = decode(kv.value.as_ref())?;
            rows.push((stored.key, stored.row));
        }
        Ok(rows)
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

#[async_trait]
impl Store for SlateDbStorage {
    async fn fetch_schema(&self, table_name: &str) -> GlueResult<Option<Schema>> {
        let key = schema_key(table_name);
        match self.get(&key).await? {
            Some(bytes) => Ok(Some(decode(bytes.as_ref())?)),
            None => Ok(None),
        }
    }

    async fn fetch_all_schemas(&self) -> GlueResult<Vec<Schema>> {
        // Schema keys are `[TAG_SCHEMA] <name>`; scan the whole tag partition.
        let prefix = schema_key("");
        let mut iter = match prefix_upper_bound(&prefix) {
            Some(end) => self
                .db
                .scan_with_options(prefix.clone()..end, &ScanOptions::default())
                .await
                .map_err(SqlError::from)?,
            None => self
                .db
                .scan_with_options(prefix.clone().., &ScanOptions::default())
                .await
                .map_err(SqlError::from)?,
        };

        let mut schemas = Vec::new();
        while let Some(kv) = iter.next().await.map_err(SqlError::from)? {
            let schema: Schema = decode(kv.value.as_ref())?;
            schemas.push(schema);
        }
        // GlueSQL expects schemas sorted by table name.
        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));
        Ok(schemas)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> GlueResult<Option<DataRow>> {
        let storage_key = row_key(table_name, key)?;
        match self.get(&storage_key).await? {
            Some(bytes) => {
                let stored: StoredRow = decode(bytes.as_ref())?;
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
        let key = schema_key(&schema.table_name);
        self.put(key, encode(schema)?).await?;
        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> GlueResult<()> {
        // Drop the schema record and every row of the table.
        for (key, _) in self.collect_rows(table_name).await? {
            let storage_key = row_key(table_name, &key)?;
            self.delete(storage_key).await?;
        }
        self.delete(schema_key(table_name)).await?;
        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> GlueResult<()> {
        // `append_data` feeds schemaless / auto-incremented tables: assign each
        // row a fresh monotonically increasing I64 key. We continue from the
        // current max key so appends across calls keep advancing.
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
            let storage_key = row_key(table_name, &key)?;
            let stored = StoredRow { key, row };
            self.put(storage_key, encode(&stored)?).await?;
        }
        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> GlueResult<()> {
        // Used for keyed inserts and updates (UPDATE re-inserts the same key).
        for (key, row) in rows {
            let storage_key = row_key(table_name, &key)?;
            let stored = StoredRow { key, row };
            self.put(storage_key, encode(&stored)?).await?;
        }
        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> GlueResult<()> {
        for key in keys {
            let storage_key = row_key(table_name, &key)?;
            self.delete(storage_key).await?;
        }
        Ok(())
    }
}

// --- Marker traits required by the `GStore`/`GStoreMut` bounds. ------------
//
// GlueSQL composes its store bounds as:
//   GStore    = Store + Index + Metadata + CustomFunction
//   GStoreMut = StoreMut + IndexMut + AlterTable + Transaction
//               + CustomFunction + CustomFunctionMut
//   (and Glue additionally requires Planner)
//
// Every trait below ships full default method implementations in
// gluesql-core, so an empty `impl` is enough:
//   * `Index`/`IndexMut`        — secondary indexes: default to "not supported".
//   * `Metadata`                — `scan_table_meta` defaults to empty.
//   * `CustomFunction(Mut)`     — user-defined functions: default "not supported".
//   * `Transaction`            — `begin(autocommit=true)` => no-op; commit/rollback
//                                 are no-ops. SlateDB writes are immediately
//                                 visible at `DurabilityLevel::Memory`, so
//                                 autocommit semantics hold.
//   * `AlterTable`             — default impls drive RENAME/ADD/DROP COLUMN on
//                                 top of our Store + StoreMut, so they work for
//                                 free (only schemaless ALTER is rejected, by design).
//   * `Planner`               — default query planner over our `Store`.
impl Index for SlateDbStorage {}
impl IndexMut for SlateDbStorage {}
impl Metadata for SlateDbStorage {}
impl CustomFunction for SlateDbStorage {}
impl CustomFunctionMut for SlateDbStorage {}
impl Transaction for SlateDbStorage {}
impl AlterTable for SlateDbStorage {}
impl Planner for SlateDbStorage {}
