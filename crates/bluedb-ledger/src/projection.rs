//! SQL projection of ledger records.
//!
//! The native `postcard` records under the ledger's external keyspace tags stay
//! canonical; these [`ProjectedTable`]s mirror them into the `ledger_accounts`
//! and `ledger_transfers` SQL tables so the data is queryable through bluedb-sql
//! (`/sql`, `/tables/{table}`). Rows are written into the **same atomic batch**
//! as the native records (see the commit blocks in [`crate::ledger`]), so the
//! SQL view can never lag behind — or disagree with — canonical state, even if
//! the writer crashes mid-commit.
//!
//! All GlueSQL encoding lives in bluedb-sql ([`ProjectedTable`]); this module
//! only declares the column layout and maps a record to a row of [`ProjValue`].

use bluedb_sql::{Database, ProjColumn, ProjValue, ProjectedTable};

use crate::model::{Account, Transfer};

/// SQL table name for the accounts projection.
pub(crate) const ACCOUNTS_TABLE: &str = "ledger_accounts";
/// SQL table name for the transfers projection.
pub(crate) const TRANSFERS_TABLE: &str = "ledger_transfers";

/// The `ledger_accounts` projection. Column order here is the single source of
/// truth shared by the DDL and [`project_account`] — they must never drift (a
/// same-typed reorder would silently corrupt rows; the
/// `sql_projection_column_order_is_exact` test guards against it).
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

/// The `ledger_transfers` projection. As with [`accounts_table`], the column
/// order here is the single source of truth shared with [`project_transfer`] —
/// the two must never drift (a same-typed reorder would silently corrupt rows;
/// `sql_projection_column_order_is_exact` guards against it).
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

/// Map an account to its projected row (column order = [`accounts_table`]).
pub(crate) fn project_account(a: &Account) -> Vec<ProjValue> {
    use ProjValue::{U128, U16, U32, U64};
    vec![
        U128(a.id),
        U32(a.ledger),
        U16(a.code),
        U16(a.flags.0),
        U128(a.debits_pending),
        U128(a.debits_posted),
        U128(a.credits_pending),
        U128(a.credits_posted),
        U128(a.user_data_128),
        U64(a.user_data_64),
        U32(a.user_data_32),
        U64(a.timestamp),
    ]
}

/// Map a transfer to its projected row (column order = [`transfers_table`]).
pub(crate) fn project_transfer(t: &Transfer) -> Vec<ProjValue> {
    use ProjValue::{U128, U16, U32, U64};
    vec![
        U128(t.id),
        U128(t.debit_account_id),
        U128(t.credit_account_id),
        U128(t.amount),
        U128(t.pending_id),
        U128(t.user_data_128),
        U64(t.user_data_64),
        U32(t.user_data_32),
        U32(t.timeout),
        U32(t.ledger),
        U16(t.code),
        U16(t.flags.0),
        U64(t.timestamp),
    ]
}

/// Idempotently create both projection tables on `database` (active writer).
/// Call on writer promotion (and in tests / Jepsen setup) so the tables exist
/// before any `SELECT`. Safe to call repeatedly (`CREATE TABLE IF NOT EXISTS`).
pub async fn ensure_schema(database: &Database) -> anyhow::Result<()> {
    accounts_table().ensure(database).await?;
    transfers_table().ensure(database).await?;
    Ok(())
}
