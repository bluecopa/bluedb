//! [`SchemaRegistry`] — schemas as queryable data, with write-time validation.
//!
//! GlueSQL normally creates schemas through DDL (`CREATE TABLE`). bluedb's
//! "no migration tax" goal calls for treating a table's schema as ordinary
//! *data* you can list, fetch, and register/replace programmatically — no DDL
//! round-trip — and for cheaply validating a row against the registered schema
//! before it lands.
//!
//! This module is a thin, typed facade over [`SlateDbStorage`]. It reuses the
//! same tenant-namespaced schema keyspace and the same transaction overlay as
//! the SQL path, so a schema registered here is visible to `Glue::execute` (and
//! vice versa), and registrations made inside a `BEGIN` roll back with it.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use slatedb::Db;
//! # use slatedb::object_store::memory::InMemory;
//! use gluesql_core::ast::{ColumnDef, DataType};
//! use gluesql_core::data::{Schema, Value};
//! use gluesql_core::store::DataRow;
//! use bluedb_sql::{SchemaRegistry, SlateDbStorage};
//!
//! # async fn run() -> anyhow::Result<()> {
//! # let db = Db::open("r", Arc::new(InMemory::new())).await?;
//! let mut storage = SlateDbStorage::new(Arc::new(db));
//! let mut registry = SchemaRegistry::new(&mut storage);
//!
//! let schema = Schema {
//!     table_name: "users".to_owned(),
//!     column_defs: Some(vec![
//!         ColumnDef { name: "id".into(), data_type: DataType::Int, nullable: false,
//!                     default: None, unique: None, comment: None },
//!     ]),
//!     indexes: vec![], engine: None, foreign_keys: vec![], comment: None,
//! };
//! registry.register(&schema).await?;
//!
//! let ok = DataRow::Vec(vec![Value::I64(1)]);
//! registry.validate_row("users", &ok).await?; // Ok
//! # Ok(())
//! # }
//! ```

use gluesql_core::data::{Schema, Value};
use gluesql_core::store::{DataRow, Store, StoreMut};

use crate::error::SqlError;
use crate::storage::SlateDbStorage;

/// A schema-as-data view over a [`SlateDbStorage`].
///
/// Borrows the storage mutably for the registry's lifetime; all reads and
/// writes go through the storage's normal (tenant- and transaction-aware) path.
pub struct SchemaRegistry<'a> {
    storage: &'a mut SlateDbStorage,
}

impl<'a> SchemaRegistry<'a> {
    /// Wrap a storage for schema-registry operations.
    pub fn new(storage: &'a mut SlateDbStorage) -> Self {
        Self { storage }
    }

    /// List every registered schema, ordered by table name.
    pub async fn list(&self) -> Result<Vec<Schema>, SqlError> {
        self.storage.fetch_all_schemas().await.map_err(into_sql)
    }

    /// Fetch one schema by table name, or `None` if it isn't registered.
    pub async fn get(&self, table_name: &str) -> Result<Option<Schema>, SqlError> {
        self.storage
            .fetch_schema(table_name)
            .await
            .map_err(into_sql)
    }

    /// Register a new schema, or replace an existing one with the same table
    /// name. Persists into the same schema keyspace the SQL engine reads.
    pub async fn register(&mut self, schema: &Schema) -> Result<(), SqlError> {
        self.storage.insert_schema(schema).await.map_err(into_sql)
    }

    /// Validate a row against the registered schema for `table_name`.
    ///
    /// Checks performed against a schema'd table:
    /// * the table is registered (else [`SqlError::SchemaValidation`]);
    /// * the row is a positional [`DataRow::Vec`] (the registry validates
    ///   columnar rows; map/schemaless rows are accepted as-is);
    /// * the value count matches the column count;
    /// * each value's type matches its column's declared type
    ///   (`NULL` is type-compatible with any column);
    /// * `NULL` only appears in `nullable` columns.
    ///
    /// A **schemaless** table (`column_defs == None`) accepts any row.
    pub async fn validate_row(&self, table_name: &str, row: &DataRow) -> Result<(), SqlError> {
        let schema = self.get(table_name).await?.ok_or_else(|| {
            SqlError::SchemaValidation(format!("table not registered: {table_name}"))
        })?;

        let column_defs = match &schema.column_defs {
            // Schemaless table: nothing to validate against.
            None => return Ok(()),
            Some(defs) => defs,
        };

        let values = match row {
            DataRow::Vec(values) => values,
            // Map rows are schemaless-shaped; we don't positionally validate.
            DataRow::Map(_) => return Ok(()),
        };

        if values.len() != column_defs.len() {
            return Err(SqlError::SchemaValidation(format!(
                "column count mismatch for {table_name}: row has {}, schema has {}",
                values.len(),
                column_defs.len()
            )));
        }

        for (value, column) in values.iter().zip(column_defs.iter()) {
            value
                .validate_null(column.nullable)
                .map_err(|e| validation(table_name, &column.name, e))?;
            // `validate_type` is a no-op for NULL (already covered above).
            if !matches!(value, Value::Null) {
                value
                    .validate_type(&column.data_type)
                    .map_err(|e| validation(table_name, &column.name, e))?;
            }
        }

        Ok(())
    }
}

/// Convert a GlueSQL error surfaced by a `Store`/`StoreMut` call back into a
/// typed [`SqlError`].
fn into_sql(err: gluesql_core::error::Error) -> SqlError {
    SqlError::SchemaValidation(err.to_string())
}

/// Build a [`SqlError::SchemaValidation`] naming the offending column.
fn validation(table: &str, column: &str, err: gluesql_core::error::Error) -> SqlError {
    SqlError::SchemaValidation(format!("{table}.{column}: {err}"))
}
