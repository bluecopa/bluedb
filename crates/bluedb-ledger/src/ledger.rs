//! The public [`Ledger`]: built from a [`bluedb_sql::Database`], it runs the
//! double-entry apply state machine inside that database's active writer.

use anyhow::Result;
use bluedb_sql::{Database, WriteLease, DEFAULT_TENANT};
use bluedb_storage::Substrate;

use crate::keyspace::LedgerKeyspace;
use crate::model::{Account, Transfer};
use crate::store::{get_account, get_transfer};

/// A double-entry ledger over one bluedb database.
///
/// Construct one per use from the live [`Database`] (e.g. per request on the
/// server), so it always reflects the node's current writer/replica role. All
/// writes require the active writer; reads work on a replica (eventually
/// consistent).
pub struct Ledger {
    substrate: Substrate,
    write_lease: WriteLease,
    keyspace: LedgerKeyspace,
}

impl Ledger {
    /// Build a ledger over `database` under the default tenant, sharing its
    /// substrate and write lease (so ledger applies serialize against the
    /// database's explicit SQL transactions).
    pub fn new(database: &Database) -> Self {
        Self {
            substrate: database.substrate(),
            write_lease: database.write_lease(),
            keyspace: LedgerKeyspace::new(DEFAULT_TENANT),
        }
    }

    /// Look up one account by id (committed state).
    pub async fn lookup_account(&self, id: u128) -> Result<Option<Account>> {
        get_account(&self.substrate, &self.keyspace, id).await
    }

    /// Look up one transfer by id (committed state).
    pub async fn lookup_transfer(&self, id: u128) -> Result<Option<Transfer>> {
        get_transfer(&self.substrate, &self.keyspace, id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_harness::writer_database;

    #[tokio::test]
    async fn lookups_on_empty_ledger_return_none() {
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        assert!(ledger.lookup_account(1).await.unwrap().is_none());
        assert!(ledger.lookup_transfer(1).await.unwrap().is_none());
    }
}
