//! The public [`Ledger`]: built from a [`bluedb_sql::Database`], it runs the
//! double-entry apply state machine inside that database's active writer.

use std::collections::HashSet;

use anyhow::Result;
use bluedb_sql::{Database, WriteLease, DEFAULT_TENANT};
use bluedb_storage::Substrate;
use slatedb::WriteBatch;

use crate::keyspace::LedgerKeyspace;
use crate::model::{Account, CreateResult, NewAccount, Transfer};
use crate::store::{encode, get_account, get_transfer};

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

    /// Create accounts (batch, idempotent). Each spec whose id already exists
    /// yields [`CreateResult::Exists`] and is left untouched; the rest are
    /// created with zero balances at `timestamp` and committed in one atomic
    /// batch. Requires the active writer.
    pub async fn create_accounts(&self, specs: &[NewAccount], timestamp: u64) -> Result<Vec<CreateResult>> {
        let _lease = self.write_lease.lock().await; // exclusive: we are the sole mutator
        let writer = self.substrate.require_writer()?;

        let mut batch = WriteBatch::new();
        let mut results = Vec::with_capacity(specs.len());
        // Track ids staged in THIS batch so a duplicate id within one call is
        // also treated as Exists (committed-state check can't see them yet).
        let mut staged: HashSet<u128> = HashSet::new();

        for spec in specs {
            if staged.contains(&spec.id)
                || get_account(&self.substrate, &self.keyspace, spec.id).await?.is_some()
            {
                results.push(CreateResult::Exists);
                continue;
            }
            let account = spec.into_account(timestamp);
            batch.put(&self.keyspace.account_key(account.id), &encode(&account)?);
            staged.insert(spec.id);
            results.push(CreateResult::Ok);
        }

        if !batch.is_empty() {
            writer.write(batch).await?;
        }
        Ok(results)
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

    #[tokio::test]
    async fn create_accounts_persists_and_is_idempotent() {
        use crate::model::{AccountFlags, CreateResult, NewAccount};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);

        let specs = [
            NewAccount::new(1, 7),
            NewAccount::new(2, 7).with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS),
        ];
        let results = ledger.create_accounts(&specs, 100).await.unwrap();
        assert_eq!(results, vec![CreateResult::Ok, CreateResult::Ok]);

        let a1 = ledger.lookup_account(1).await.unwrap().unwrap();
        assert_eq!(a1.ledger, 7);
        assert_eq!(a1.timestamp, 100);
        assert_eq!(a1.debits_posted, 0);
        let a2 = ledger.lookup_account(2).await.unwrap().unwrap();
        assert!(a2.flags.contains(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS));

        // Re-creating id 1 is a no-op Exists; the original is untouched.
        let again = ledger.create_accounts(&[NewAccount::new(1, 999)], 200).await.unwrap();
        assert_eq!(again, vec![CreateResult::Exists]);
        let a1b = ledger.lookup_account(1).await.unwrap().unwrap();
        assert_eq!(a1b.ledger, 7, "existing account not overwritten");
        assert_eq!(a1b.timestamp, 100);
    }

    #[tokio::test]
    async fn create_accounts_all_exists_or_empty_writes_nothing() {
        use crate::model::{CreateResult, NewAccount};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        ledger.create_accounts(&[NewAccount::new(42, 1)], 10).await.unwrap();

        // A call where every id already exists produces an all-empty batch; the
        // `is_empty()` guard must skip the write rather than error/round-trip.
        let all_exists = ledger.create_accounts(&[NewAccount::new(42, 2)], 20).await.unwrap();
        assert_eq!(all_exists, vec![CreateResult::Exists]);

        // An empty specs slice is also a no-op that must not error.
        let empty = ledger.create_accounts(&[], 30).await.unwrap();
        assert_eq!(empty, vec![]);

        // The original account is untouched by either no-op call.
        let a = ledger.lookup_account(42).await.unwrap().unwrap();
        assert_eq!(a.ledger, 1);
        assert_eq!(a.timestamp, 10);
    }
}
