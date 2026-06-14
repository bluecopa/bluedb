//! A minimal "projection" surface: let a layer above bluedb-sql (e.g.
//! `bluedb-ledger`) maintain a SQL-queryable table whose rows it writes itself
//! into its own atomic batch, while bluedb-sql owns all GlueSQL encoding.
//!
//! A [`ProjectedTable`] knows its column layout. [`ProjectedTable::ensure`]
//! creates the table (idempotent DDL), and [`ProjectedTable::encode_row`] turns
//! a row of [`ProjValue`]s into the exact `(storage_key, value_bytes)` that
//! GlueSQL's own store would have written — so a hand-built
//! [`slatedb::WriteBatch`](slatedb::WriteBatch) of these bytes is readable by a
//! plain `SELECT`. The caller (the ledger) puts those bytes into the same
//! atomic batch as its canonical records, so the SQL view can never lag or
//! disagree after a crash.

use gluesql_core::data::{Key, Value};
use gluesql_core::prelude::Glue;
use gluesql_core::store::DataRow;

use crate::connection::Database;
use crate::error::SqlError;
use crate::keyspace::Keyspace;
use crate::storage::{encode, StoredRow};

/// A scalar projected into a SQL column. Maps 1:1 onto a GlueSQL [`Value`]; the
/// primary-key column additionally maps onto a GlueSQL [`Key`].
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

    /// The SQL type literal used in `CREATE TABLE` for a column of this kind.
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

/// One column of a projected table: a name plus the column type, carried as a
/// sample [`ProjValue`] so the DDL type and the encoded value can never drift
/// apart (both come from the same source of truth).
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
    /// Define a projected table. Panics if `pk` is out of range or `columns` is
    /// empty (both are programming errors in the table definition).
    pub fn new(table: impl Into<String>, columns: Vec<ProjColumn>, pk: usize) -> Self {
        let table = table.into();
        assert!(!columns.is_empty(), "projected table must have columns");
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

    /// Idempotently create the table on `database` (runs the DDL via GlueSQL on
    /// a serialized connection). Requires the active writer.
    pub async fn ensure(&self, database: &Database) -> Result<(), SqlError> {
        let ddl = self.create_table_ddl();
        let mut glue = Glue::new(database.connection_serialized());
        glue.execute(&ddl)
            .await
            .map_err(|e| SqlError::SlateDb(format!("create projection table {}: {e}", self.table)))?;
        Ok(())
    }

    /// Encode one row into the `(storage_key, value_bytes)` GlueSQL's own store
    /// would have produced, so a raw `WriteBatch::put` of these bytes is read
    /// back by `SELECT`. `values` must match `columns` in length and order.
    /// Panics on an arity mismatch (a programming error in the projection
    /// mapper).
    pub fn encode_row(
        &self,
        ks: &Keyspace,
        values: &[ProjValue],
    ) -> Result<(Vec<u8>, Vec<u8>), SqlError> {
        assert_eq!(
            values.len(),
            self.columns.len(),
            "projected row arity mismatch for table {}",
            self.table
        );
        let key = values[self.pk].to_key();
        let row = DataRow::Vec(values.iter().map(ProjValue::to_value).collect());
        let storage_key = ks.row_key(&self.table, &key)?;
        let stored = StoredRow { key, row };
        Ok((storage_key, encode(&stored)?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gluesql_core::prelude::{Glue, Payload, Value as SqlValue};
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, WriteBatch};

    use super::*;
    use crate::connection::Database;
    use crate::keyspace::{Keyspace, DEFAULT_TENANT};

    async fn database() -> Database {
        let db = Db::open("proj-test", Arc::new(InMemory::new()))
            .await
            .expect("open in-memory db");
        Database::new(Arc::new(db))
    }

    fn accounts() -> ProjectedTable {
        use ProjValue::{U128, U32};
        ProjectedTable::new(
            "ledger_accounts",
            vec![
                ProjColumn::new("id", U128(0)),
                ProjColumn::new("ledger", U32(0)),
                ProjColumn::new("debits_posted", U128(0)),
                ProjColumn::new("credits_posted", U128(0)),
            ],
            0,
        )
    }

    #[test]
    fn ddl_lists_columns_with_pk() {
        let ddl = accounts().create_table_ddl();
        assert_eq!(
            ddl,
            "CREATE TABLE IF NOT EXISTS ledger_accounts \
             (id UINT128 PRIMARY KEY, ledger UINT32, debits_posted UINT128, credits_posted UINT128)"
        );
    }

    #[tokio::test]
    async fn hand_written_rows_are_readable_by_select() {
        let database = database().await;
        let table = accounts();
        table.ensure(&database).await.unwrap();

        // Hand-write two rows into one atomic batch, exactly as the ledger will.
        let ks = Keyspace::new(DEFAULT_TENANT);
        let mut batch = WriteBatch::new();
        for (id, dp, cp) in [(7u128, 100u128, 0u128), (9u128, 0u128, 100u128)] {
            let (k, v) = table
                .encode_row(
                    &ks,
                    &[
                        ProjValue::U128(id),
                        ProjValue::U32(700),
                        ProjValue::U128(dp),
                        ProjValue::U128(cp),
                    ],
                )
                .unwrap();
            batch.put(k, &v);
        }
        database
            .substrate()
            .require_writer()
            .unwrap()
            .write(batch)
            .await
            .unwrap();

        let mut glue = Glue::new(database.connection());

        // SELECT * sees both rows in pk order.
        let out = glue
            .execute("SELECT id, debits_posted, credits_posted FROM ledger_accounts ORDER BY id")
            .await
            .unwrap();
        let Payload::Select { rows, .. } = &out[0] else {
            panic!("expected select payload")
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], SqlValue::U128(7));
        assert_eq!(rows[0][1], SqlValue::U128(100));
        assert_eq!(rows[1][0], SqlValue::U128(9));
        assert_eq!(rows[1][2], SqlValue::U128(100));

        // Point lookup on the U128 primary key.
        let out = glue
            .execute("SELECT credits_posted FROM ledger_accounts WHERE id = 9")
            .await
            .unwrap();
        let Payload::Select { rows, .. } = &out[0] else {
            panic!()
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], SqlValue::U128(100));

        // Conservation aggregate over the projection.
        let out = glue
            .execute("SELECT SUM(debits_posted), SUM(credits_posted) FROM ledger_accounts")
            .await
            .unwrap();
        let Payload::Select { rows, .. } = &out[0] else {
            panic!()
        };
        assert_eq!(rows[0][0], rows[0][1]);
    }

    #[tokio::test]
    async fn encoded_row_overwrites_in_place_on_same_pk() {
        let database = database().await;
        let table = accounts();
        table.ensure(&database).await.unwrap();
        let ks = Keyspace::new(DEFAULT_TENANT);

        for posted in [10u128, 25u128] {
            let mut batch = WriteBatch::new();
            let (k, v) = table
                .encode_row(
                    &ks,
                    &[
                        ProjValue::U128(1),
                        ProjValue::U32(700),
                        ProjValue::U128(posted),
                        ProjValue::U128(0),
                    ],
                )
                .unwrap();
            batch.put(k, &v);
            database.substrate().require_writer().unwrap().write(batch).await.unwrap();
        }

        let mut glue = Glue::new(database.connection());
        let out = glue.execute("SELECT debits_posted FROM ledger_accounts").await.unwrap();
        let Payload::Select { rows, .. } = &out[0] else { panic!() };
        assert_eq!(rows.len(), 1, "same pk overwrites, not appends");
        assert_eq!(rows[0][0], SqlValue::U128(25));
    }
}
