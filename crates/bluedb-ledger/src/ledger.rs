//! The public [`Ledger`]: built from a [`bluedb_sql::Database`], it runs the
//! double-entry apply state machine inside that database's active writer.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use bluedb_sql::{Database, WriteLease, DEFAULT_TENANT};
use bluedb_storage::Substrate;
use slatedb::WriteBatch;

use crate::keyspace::LedgerKeyspace;
use crate::model::{Account, CreateResult, LedgerError, NewAccount, Transfer};
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

    /// Apply transfers (batch). Each item is validated and applied independently
    /// (Plan 1: single posted transfers — no linked/pending/balancing flags).
    /// A failing item yields [`CreateResult::Failed`] and applies no change; a
    /// duplicate id yields [`CreateResult::Exists`]. All accepted items commit in
    /// one atomic batch. Requires the active writer.
    pub async fn create_transfers(&self, transfers: &[Transfer], timestamp: u64) -> Result<Vec<CreateResult>> {
        let _lease = self.write_lease.lock().await; // exclusive
        let writer = self.substrate.require_writer()?; // fail fast on a replica; held for the commit

        let mut working: HashMap<u128, Account> = HashMap::new();
        // Ids of accounts an accepted transfer actually mutated — only these are
        // written back (accounts merely read by a rejected transfer are skipped).
        let mut dirty: HashSet<u128> = HashSet::new();
        let mut accepted: Vec<Transfer> = Vec::new();
        let mut results = Vec::with_capacity(transfers.len());

        for t in transfers {
            match self.stage_transfer(t, timestamp, &mut working).await? {
                StageOutcome::Applied(applied) => {
                    dirty.insert(applied.debit_account_id);
                    dirty.insert(applied.credit_account_id);
                    accepted.push(applied);
                    results.push(CreateResult::Ok);
                }
                StageOutcome::Exists => results.push(CreateResult::Exists),
                StageOutcome::Rejected(err) => results.push(CreateResult::Failed(err)),
            }
        }

        // Build one atomic batch: every mutated account + every accepted transfer.
        let mut batch = WriteBatch::new();
        for account in working.values().filter(|a| dirty.contains(&a.id)) {
            batch.put(&self.keyspace.account_key(account.id), &encode(account)?);
        }
        for t in &accepted {
            batch.put(&self.keyspace.transfer_key(t.id), &encode(t)?);
        }
        if !batch.is_empty() {
            writer.write(batch).await?;
        }

        Ok(results)
    }

    /// Validate one transfer and, if accepted, fold its balance deltas into the
    /// `working` set and return the transfer record (stamped with `timestamp`).
    /// `Exists` if the id is already committed; `Rejected` (no change) on a
    /// validation failure. Errors bubble for I/O failures only.
    async fn stage_transfer(
        &self,
        t: &Transfer,
        timestamp: u64,
        working: &mut HashMap<u128, Account>,
    ) -> Result<StageOutcome> {
        if t.debit_account_id == t.credit_account_id {
            return Ok(StageOutcome::Rejected(LedgerError::AccountsMustDiffer));
        }
        // Idempotency: committed transfer with this id already exists.
        if get_transfer(&self.substrate, &self.keyspace, t.id).await?.is_some() {
            return Ok(StageOutcome::Exists);
        }

        // Load both accounts into the working set (read-through, copies).
        let mut debit = match self.load_account(t.debit_account_id, working).await? {
            Some(a) => a,
            None => return Ok(StageOutcome::Rejected(LedgerError::AccountNotFound(t.debit_account_id))),
        };
        let mut credit = match self.load_account(t.credit_account_id, working).await? {
            Some(a) => a,
            None => return Ok(StageOutcome::Rejected(LedgerError::AccountNotFound(t.credit_account_id))),
        };

        if t.ledger != debit.ledger || t.ledger != credit.ledger {
            return Ok(StageOutcome::Rejected(LedgerError::LedgerMismatch));
        }

        // Apply posted deltas with overflow checks.
        debit.debits_posted = match debit.debits_posted.checked_add(t.amount) {
            Some(v) => v,
            None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
        };
        credit.credits_posted = match credit.credits_posted.checked_add(t.amount) {
            Some(v) => v,
            None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
        };

        // (Balance-constraint checks added in Task 10.)

        // Accept: write the mutated copies back into the working set.
        working.insert(debit.id, debit);
        working.insert(credit.id, credit);
        Ok(StageOutcome::Applied(Transfer { timestamp, ..*t }))
    }

    /// Get an account from the working set, loading it read-through on first
    /// touch. Returns a copy; mutations are written back by the caller on accept.
    async fn load_account(
        &self,
        id: u128,
        working: &mut HashMap<u128, Account>,
    ) -> Result<Option<Account>> {
        if let Some(a) = working.get(&id) {
            return Ok(Some(*a));
        }
        match get_account(&self.substrate, &self.keyspace, id).await? {
            Some(a) => {
                working.insert(id, a);
                Ok(Some(a))
            }
            None => Ok(None),
        }
    }
}

/// The outcome of staging one transfer in [`Ledger::stage_transfer`].
enum StageOutcome {
    /// Accepted; deltas folded into the working set. Carries the stamped record.
    Applied(Transfer),
    /// The id already exists (idempotent no-op).
    Exists,
    /// A validation rule rejected it; no change made.
    Rejected(LedgerError),
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

    async fn setup_two_accounts(ledger: &Ledger) {
        use crate::model::NewAccount;
        ledger
            .create_accounts(&[NewAccount::new(1, 7), NewAccount::new(2, 7)], 1)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn single_transfer_moves_balances_and_conserves() {
        use crate::model::{CreateResult, Transfer};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;

        let res = ledger
            .create_transfers(&[Transfer::new(1000, 1, 2, 500, 7)], 50)
            .await
            .unwrap();
        assert_eq!(res, vec![CreateResult::Ok]);

        let debit = ledger.lookup_account(1).await.unwrap().unwrap();
        let credit = ledger.lookup_account(2).await.unwrap().unwrap();
        assert_eq!(debit.debits_posted, 500);
        assert_eq!(credit.credits_posted, 500);
        assert_eq!(debit.debits_posted, credit.credits_posted);

        let t = ledger.lookup_transfer(1000).await.unwrap().unwrap();
        assert_eq!(t.timestamp, 50);
        assert_eq!(t.amount, 500);
    }

    #[tokio::test]
    async fn transfer_validation_rejects() {
        use crate::model::{CreateResult, LedgerError, Transfer};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;

        let r = ledger.create_transfers(&[Transfer::new(1, 1, 1, 10, 7)], 2).await.unwrap();
        assert_eq!(r, vec![CreateResult::Failed(LedgerError::AccountsMustDiffer)]);

        let r = ledger.create_transfers(&[Transfer::new(2, 1, 99, 10, 7)], 2).await.unwrap();
        assert_eq!(r, vec![CreateResult::Failed(LedgerError::AccountNotFound(99))]);

        let r = ledger.create_transfers(&[Transfer::new(3, 1, 2, 10, 8)], 2).await.unwrap();
        assert_eq!(r, vec![CreateResult::Failed(LedgerError::LedgerMismatch)]);

        assert_eq!(ledger.lookup_account(1).await.unwrap().unwrap().debits_posted, 0);
        assert!(ledger.lookup_transfer(1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn duplicate_transfer_id_is_exists() {
        use crate::model::{CreateResult, Transfer};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;

        let t = Transfer::new(7, 1, 2, 100, 7);
        assert_eq!(ledger.create_transfers(&[t], 1).await.unwrap(), vec![CreateResult::Ok]);
        assert_eq!(ledger.create_transfers(&[t], 2).await.unwrap(), vec![CreateResult::Exists]);
        assert_eq!(ledger.lookup_account(1).await.unwrap().unwrap().debits_posted, 100);
    }

    #[tokio::test]
    async fn batch_applies_good_skips_bad() {
        use crate::model::{CreateResult, LedgerError, Transfer};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;

        let batch = [
            Transfer::new(1, 1, 2, 100, 7),
            Transfer::new(2, 1, 99, 50, 7),
            Transfer::new(3, 2, 1, 30, 7),
        ];
        let res = ledger.create_transfers(&batch, 9).await.unwrap();
        assert_eq!(
            res,
            vec![CreateResult::Ok, CreateResult::Failed(LedgerError::AccountNotFound(99)), CreateResult::Ok]
        );

        let a1 = ledger.lookup_account(1).await.unwrap().unwrap();
        let a2 = ledger.lookup_account(2).await.unwrap().unwrap();
        assert_eq!(a1.debits_posted, 100);
        assert_eq!(a1.credits_posted, 30);
        assert_eq!(a2.credits_posted, 100);
        assert_eq!(a2.debits_posted, 30);
        assert_eq!(a1.debits_posted + a2.debits_posted, a1.credits_posted + a2.credits_posted);
    }

    #[tokio::test]
    async fn transfer_amount_overflow_is_rejected() {
        use crate::model::{CreateResult, LedgerError, Transfer};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;
        // Seed a nonzero debits_posted on account 1 so the next add overflows.
        ledger.create_transfers(&[Transfer::new(1, 1, 2, 10, 7)], 1).await.unwrap();

        // 10 + u128::MAX overflows debits_posted → rejected via checked_add.
        let r = ledger.create_transfers(&[Transfer::new(2, 1, 2, u128::MAX, 7)], 2).await.unwrap();
        assert_eq!(r, vec![CreateResult::Failed(LedgerError::Overflow)]);

        // The overflowing transfer applied nothing and was not persisted.
        assert_eq!(ledger.lookup_account(1).await.unwrap().unwrap().debits_posted, 10);
        assert!(ledger.lookup_transfer(2).await.unwrap().is_none());
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
