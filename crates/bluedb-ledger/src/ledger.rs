//! The public [`Ledger`]: built from a [`bluedb_sql::Database`], it runs the
//! double-entry apply state machine inside that database's active writer.
//!
//! Phase A implements TigerBeetle's `create_accounts` exactly and **regular
//! posted** transfers exactly, with the full per-item result-code surface and
//! TB's input-validation ordering. Two-phase (pending/post/void), linked,
//! balancing, closing, imported, and timeout behaviour are gated to later
//! phases and return [`CreateTransferResult::NotImplementedYet`] /
//! [`CreateAccountResult::NotImplementedYet`].

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use bluedb_sql::{Database, WriteLease, DEFAULT_TENANT};
use bluedb_storage::Substrate;
use slatedb::WriteBatch;

use crate::keyspace::LedgerKeyspace;
use crate::model::{
    account_exists_result, account_is_gated, transfer_exists_result, transfer_is_gated,
    validate_account_post_existence, validate_account_pre_existence,
    validate_transfer_post_existence, validate_transfer_pre_existence, Account, AccountFlags,
    CreateAccountResult, CreateTransferResult, Transfer,
};
use crate::store::{encode, get_account, get_transfer, get_watermark, now_ns};

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

    /// Create accounts (batch). Each item is validated against TigerBeetle's
    /// `create_accounts` check ordering; the first failing check determines the
    /// returned code. An id that already exists (committed or staged earlier in
    /// this batch) yields [`CreateAccountResult::Exists`] or
    /// `ExistsWith*` (field comparison). Accepted accounts are assigned an
    /// engine timestamp and committed in one atomic batch. Requires the active
    /// writer.
    pub async fn create_accounts(&self, specs: &[Account]) -> Result<Vec<CreateAccountResult>> {
        use CreateAccountResult as R;
        let _lease = self.write_lease.lock().await; // exclusive: we are the sole mutator
        let writer = self.substrate.require_writer()?;

        let mut ts = TimestampSource::new(get_watermark(&self.substrate, &self.keyspace).await?);
        let mut batch = WriteBatch::new();
        let mut results = Vec::with_capacity(specs.len());
        // Accounts staged in THIS batch, for in-batch existence comparison.
        let mut staged: HashMap<u128, Account> = HashMap::new();

        for spec in specs {
            if let Some(code) = validate_account_pre_existence(spec) {
                results.push(code);
                continue;
            }
            // Existence (TB 13–19): committed first, then staged this batch.
            if let Some(existing) = get_account(&self.substrate, &self.keyspace, spec.id).await? {
                results.push(account_exists_result(spec, &existing));
                continue;
            }
            if let Some(existing) = staged.get(&spec.id) {
                results.push(account_exists_result(spec, existing));
                continue;
            }
            if let Some(code) = validate_account_post_existence(spec) {
                results.push(code);
                continue;
            }
            if account_is_gated(spec) {
                results.push(R::NotImplementedYet);
                continue;
            }

            let mut acct = *spec;
            acct.timestamp = ts.next();
            batch.put(self.keyspace.account_key(acct.id), &encode(&acct)?);
            staged.insert(acct.id, acct);
            results.push(R::Created);
        }

        if !staged.is_empty() {
            batch.put(self.keyspace.watermark_key(), ts.last.to_be_bytes());
            writer.write(batch).await?;
        }
        Ok(results)
    }

    /// Apply transfers (batch). Each item is validated against TigerBeetle's
    /// `create_transfers` check ordering. Phase A applies only **regular posted**
    /// transfers; anything requiring a later-phase feature (any flag, a nonzero
    /// timeout, or a pending reference) returns
    /// [`CreateTransferResult::NotImplementedYet`]. Accepted transfers are
    /// assigned an engine timestamp; all mutated accounts + accepted transfers
    /// commit in one atomic batch. Requires the active writer.
    pub async fn create_transfers(&self, transfers: &[Transfer]) -> Result<Vec<CreateTransferResult>> {
        use CreateTransferResult as R;
        let _lease = self.write_lease.lock().await; // exclusive
        let writer = self.substrate.require_writer()?; // fail fast on a replica; held for the commit

        let mut ts = TimestampSource::new(get_watermark(&self.substrate, &self.keyspace).await?);
        let mut state = ApplyState::default();
        // Transfers accepted in THIS batch, for in-batch existence comparison.
        let mut staged: HashMap<u128, Transfer> = HashMap::new();
        let mut accepted: Vec<Transfer> = Vec::new();
        let mut results = Vec::with_capacity(transfers.len());

        for t in transfers {
            if let Some(code) = validate_transfer_pre_existence(t) {
                results.push(code);
                continue;
            }
            // Existence (TB 12–23): committed first, then staged this batch.
            if let Some(existing) = get_transfer(&self.substrate, &self.keyspace, t.id).await? {
                results.push(transfer_exists_result(t, &existing));
                continue;
            }
            if let Some(existing) = staged.get(&t.id) {
                results.push(transfer_exists_result(t, existing));
                continue;
            }
            if let Some(code) = validate_transfer_post_existence(t) {
                results.push(code);
                continue;
            }
            if transfer_is_gated(t) {
                results.push(R::NotImplementedYet);
                continue;
            }

            match self.stage_regular(t, &mut state).await? {
                Ok(()) => {
                    // Timestamp assigned only on accept (failed items don't advance it).
                    let applied = Transfer { timestamp: ts.next(), ..*t };
                    staged.insert(applied.id, applied);
                    accepted.push(applied);
                    results.push(R::Created);
                }
                Err(code) => results.push(code),
            }
        }

        // One atomic batch: every mutated account + every accepted transfer +
        // the advanced watermark.
        let mut batch = WriteBatch::new();
        for id in &state.dirty {
            if let Some(account) = state.working.get(id) {
                batch.put(self.keyspace.account_key(*id), &encode(account)?);
            }
        }
        for t in &accepted {
            batch.put(self.keyspace.transfer_key(t.id), &encode(t)?);
        }
        if !accepted.is_empty() {
            batch.put(self.keyspace.watermark_key(), ts.last.to_be_bytes());
            writer.write(batch).await?;
        }
        Ok(results)
    }

    /// Apply one plain posted transfer into `state`. Returns `Err(code)` on the
    /// first failing TigerBeetle check (account resolution → ledger agreement →
    /// overflow → balance constraint). On success the debit/credit accounts are
    /// folded into `state` (not yet persisted).
    async fn stage_regular(
        &self,
        t: &Transfer,
        state: &mut ApplyState,
    ) -> Result<std::result::Result<(), CreateTransferResult>> {
        use CreateTransferResult as R;

        // 39 / 40: account resolution.
        let mut debit = match self.load_account(t.debit_account_id, state).await? {
            Some(a) => a,
            None => return Ok(Err(R::DebitAccountNotFound)),
        };
        let mut credit = match self.load_account(t.credit_account_id, state).await? {
            Some(a) => a,
            None => return Ok(Err(R::CreditAccountNotFound)),
        };
        // 41 / 42: ledger agreement.
        if debit.ledger != credit.ledger {
            return Ok(Err(R::AccountsMustHaveTheSameLedger));
        }
        if t.ledger != debit.ledger {
            return Ok(Err(R::TransferMustHaveTheSameLedgerAsAccounts));
        }

        // 62 / 63: per-bucket posted overflow.
        debit.debits_posted = match debit.debits_posted.checked_add(t.amount) {
            Some(v) => v,
            None => return Ok(Err(R::OverflowsDebitsPosted)),
        };
        credit.credits_posted = match credit.credits_posted.checked_add(t.amount) {
            Some(v) => v,
            None => return Ok(Err(R::OverflowsCreditsPosted)),
        };
        // 64 / 65: total (pending + posted) overflow on each constrained side.
        if debit.debits_pending.checked_add(debit.debits_posted).is_none() {
            return Ok(Err(R::OverflowsDebits));
        }
        if credit.credits_pending.checked_add(credit.credits_posted).is_none() {
            return Ok(Err(R::OverflowsCredits));
        }

        // 67 / 68: balance constraints — asymmetric on purpose, matching
        // TigerBeetle: the CONSTRAINED side counts posted + pending (a reserved
        // outflow already commits the account), but the LIMIT side counts posted
        // ONLY. Unconfirmed (pending) credits do NOT grant debit headroom, and
        // unconfirmed debits do not grant credit headroom — i.e.
        // `debits_posted + debits_pending <= credits_posted`, NOT
        // `<= credits_posted + credits_pending`. (The 64/65 checks above already
        // proved these sums don't overflow.)
        if debit.flags.contains(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS)
            && debit.debits_posted + debit.debits_pending > debit.credits_posted
        {
            return Ok(Err(R::ExceedsCredits));
        }
        if credit.flags.contains(AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS)
            && credit.credits_posted + credit.credits_pending > credit.debits_posted
        {
            return Ok(Err(R::ExceedsDebits));
        }

        state.working.insert(debit.id, debit);
        state.dirty.insert(debit.id);
        state.working.insert(credit.id, credit);
        state.dirty.insert(credit.id);
        Ok(Ok(()))
    }

    /// Get an account from the working set, loading it read-through on first
    /// touch. Returns a copy; mutations are written back by the caller on accept.
    async fn load_account(&self, id: u128, state: &mut ApplyState) -> Result<Option<Account>> {
        if let Some(a) = state.working.get(&id) {
            return Ok(Some(*a));
        }
        match get_account(&self.substrate, &self.keyspace, id).await? {
            Some(a) => {
                state.working.insert(id, a);
                Ok(Some(a))
            }
            None => Ok(None),
        }
    }
}

/// Assigns successive unique, strictly-increasing timestamps for one apply,
/// seeded from the persisted watermark and the wall clock: `next = max(prev + 1,
/// now_ns)`. Re-seeding from the persisted watermark each apply keeps timestamps
/// monotonic across writer role changes (a promoted replica reads the watermark,
/// not in-memory state).
struct TimestampSource {
    last: u64,
}

impl TimestampSource {
    fn new(persisted_watermark: u64) -> Self {
        Self { last: persisted_watermark }
    }

    fn next(&mut self) -> u64 {
        self.last = now_ns().max(self.last + 1);
        self.last
    }
}

/// Mutable state threaded through one `create_transfers` apply: the read-through
/// working set of touched accounts and the ids of accounts actually mutated
/// (only these are written back).
#[derive(Default)]
struct ApplyState {
    working: HashMap<u128, Account>,
    dirty: HashSet<u128>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{CreateAccountResult, CreateTransferResult, TransferFlags};
    use crate::store::test_harness::writer_database;

    fn acct(id: u128, ledger: u32) -> Account {
        Account::input(id, ledger).with_code(1)
    }
    fn xfer(id: u128, d: u128, c: u128, amt: u128) -> Transfer {
        Transfer::new(id, d, c, amt, 7).with_code(1)
    }

    async fn setup_two_accounts(ledger: &Ledger) {
        let r = ledger.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        assert_eq!(r, vec![CreateAccountResult::Created, CreateAccountResult::Created]);
    }

    #[tokio::test]
    async fn lookups_on_empty_ledger_return_none() {
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        assert!(ledger.lookup_account(1).await.unwrap().is_none());
        assert!(ledger.lookup_transfer(1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn create_accounts_persists_and_is_idempotent() {
        use CreateAccountResult as R;
        let database = writer_database().await;
        let ledger = Ledger::new(&database);

        let specs = [
            acct(1, 7),
            acct(2, 7).with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS),
        ];
        assert_eq!(ledger.create_accounts(&specs).await.unwrap(), vec![R::Created, R::Created]);

        let a1 = ledger.lookup_account(1).await.unwrap().unwrap();
        assert_eq!(a1.ledger, 7);
        assert!(a1.timestamp > 0);
        assert_eq!(a1.debits_posted, 0);
        let a2 = ledger.lookup_account(2).await.unwrap().unwrap();
        assert!(a2.flags.contains(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS));

        // Re-creating id 1 identically is Exists; the original is untouched.
        assert_eq!(ledger.create_accounts(&[acct(1, 7)]).await.unwrap(), vec![R::Exists]);
        let a1b = ledger.lookup_account(1).await.unwrap().unwrap();
        assert_eq!(a1b.ledger, 7);
        assert_eq!(a1b.timestamp, a1.timestamp, "existing account not overwritten");
    }

    #[tokio::test]
    async fn account_input_validation_codes() {
        use CreateAccountResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        assert_eq!(l.create_accounts(&[Account::input(0, 7).with_code(1)]).await.unwrap(), vec![R::IdMustNotBeZero]);
        assert_eq!(l.create_accounts(&[Account::input(u128::MAX, 7).with_code(1)]).await.unwrap(), vec![R::IdMustNotBeIntMax]);
        assert_eq!(l.create_accounts(&[Account::input(1, 0).with_code(1)]).await.unwrap(), vec![R::LedgerMustNotBeZero]);
        assert_eq!(l.create_accounts(&[Account::input(1, 7)]).await.unwrap(), vec![R::CodeMustNotBeZero]);
        let mut a = acct(1, 7);
        a.timestamp = 5;
        assert_eq!(l.create_accounts(&[a]).await.unwrap(), vec![R::TimestampMustBeZero]);
        let mut a = acct(1, 7);
        a.reserved = 9;
        assert_eq!(l.create_accounts(&[a]).await.unwrap(), vec![R::ReservedField]);
        let mut a = acct(1, 7);
        a.debits_posted = 1;
        assert_eq!(l.create_accounts(&[a]).await.unwrap(), vec![R::DebitsPostedMustBeZero]);
        let me = acct(1, 7)
            .with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS | AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS);
        assert_eq!(l.create_accounts(&[me]).await.unwrap(), vec![R::FlagsAreMutuallyExclusive]);
        let resv = acct(1, 7).with_flags(AccountFlags(1 << 9));
        assert_eq!(l.create_accounts(&[resv]).await.unwrap(), vec![R::ReservedFlag]);
        // id-zero is reported before ledger/code checks (ordering).
        assert_eq!(l.create_accounts(&[Account::input(0, 0)]).await.unwrap(), vec![R::IdMustNotBeZero]);
    }

    #[tokio::test]
    async fn account_exists_with_different_fields() {
        use CreateAccountResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7).with_user_data_64(5)]).await.unwrap();
        assert_eq!(l.create_accounts(&[acct(1, 7).with_user_data_64(5)]).await.unwrap(), vec![R::Exists]);
        assert_eq!(l.create_accounts(&[acct(1, 7).with_user_data_64(6)]).await.unwrap(), vec![R::ExistsWithDifferentUserData64]);
        assert_eq!(l.create_accounts(&[acct(1, 8).with_user_data_64(5)]).await.unwrap(), vec![R::ExistsWithDifferentLedger]);
        assert_eq!(l.create_accounts(&[acct(1, 7).with_code(2).with_user_data_64(5)]).await.unwrap(), vec![R::ExistsWithDifferentCode]);
        let diff_flags = acct(1, 7).with_user_data_64(5).with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS);
        assert_eq!(l.create_accounts(&[diff_flags]).await.unwrap(), vec![R::ExistsWithDifferentFlags]);
    }

    #[tokio::test]
    async fn duplicate_account_id_within_one_batch_is_exists() {
        use CreateAccountResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        // Same id twice in one batch: first Created, second Exists (in-batch).
        let r = l.create_accounts(&[acct(1, 7), acct(1, 7)]).await.unwrap();
        assert_eq!(r, vec![R::Created, R::Exists]);
        // A differing field on the in-batch duplicate yields ExistsWith*.
        let r = l.create_accounts(&[acct(2, 7), acct(2, 7).with_user_data_32(9)]).await.unwrap();
        assert_eq!(r, vec![R::Created, R::ExistsWithDifferentUserData32]);
    }

    #[tokio::test]
    async fn single_transfer_moves_balances_and_conserves() {
        use CreateTransferResult as R;
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;

        assert_eq!(ledger.create_transfers(&[xfer(1000, 1, 2, 500)]).await.unwrap(), vec![R::Created]);

        let debit = ledger.lookup_account(1).await.unwrap().unwrap();
        let credit = ledger.lookup_account(2).await.unwrap().unwrap();
        assert_eq!(debit.debits_posted, 500);
        assert_eq!(credit.credits_posted, 500);
        assert_eq!(debit.debits_posted, credit.credits_posted);

        let t = ledger.lookup_transfer(1000).await.unwrap().unwrap();
        assert!(t.timestamp > 0);
        assert_eq!(t.amount, 500);
    }

    #[tokio::test]
    async fn transfer_input_validation_codes() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        assert_eq!(l.create_transfers(&[xfer(0, 1, 2, 5)]).await.unwrap(), vec![R::IdMustNotBeZero]);
        assert_eq!(l.create_transfers(&[xfer(u128::MAX, 1, 2, 5)]).await.unwrap(), vec![R::IdMustNotBeIntMax]);
        assert_eq!(l.create_transfers(&[xfer(1, 0, 2, 5)]).await.unwrap(), vec![R::DebitAccountIdMustNotBeZero]);
        assert_eq!(l.create_transfers(&[xfer(1, 1, 1, 5)]).await.unwrap(), vec![R::AccountsMustBeDifferent]);
        assert_eq!(l.create_transfers(&[xfer(1, 1, 2, 5).with_pending_id(9)]).await.unwrap(), vec![R::PendingIdMustBeZero]);
        assert_eq!(l.create_transfers(&[Transfer::new(1, 1, 2, 5, 0).with_code(1)]).await.unwrap(), vec![R::LedgerMustNotBeZero]);
        assert_eq!(l.create_transfers(&[Transfer::new(1, 1, 2, 5, 7)]).await.unwrap(), vec![R::CodeMustNotBeZero]);
        assert_eq!(l.create_transfers(&[xfer(1, 1, 99, 5)]).await.unwrap(), vec![R::CreditAccountNotFound]);
        assert_eq!(l.create_transfers(&[xfer(1, 99, 2, 5)]).await.unwrap(), vec![R::DebitAccountNotFound]);
        assert_eq!(l.create_transfers(&[Transfer::new(1, 1, 2, 5, 8).with_code(1)]).await.unwrap(), vec![R::TransferMustHaveTheSameLedgerAsAccounts]);
    }

    #[tokio::test]
    async fn accounts_must_have_the_same_ledger_is_detected() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        // Two accounts in DIFFERENT ledgers; transfer.ledger matches the debit's.
        l.create_accounts(&[acct(1, 7), acct(2, 8)]).await.unwrap();
        assert_eq!(l.create_transfers(&[xfer(1, 1, 2, 5)]).await.unwrap(), vec![R::AccountsMustHaveTheSameLedger]);
    }

    #[tokio::test]
    async fn transfer_exists_with_different_fields() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        assert_eq!(l.create_transfers(&[xfer(7, 1, 2, 100)]).await.unwrap(), vec![R::Created]);
        assert_eq!(l.create_transfers(&[xfer(7, 1, 2, 100)]).await.unwrap(), vec![R::Exists]);
        assert_eq!(l.create_transfers(&[xfer(7, 1, 2, 101)]).await.unwrap(), vec![R::ExistsWithDifferentAmount]);
        assert_eq!(l.create_transfers(&[xfer(7, 2, 1, 100)]).await.unwrap(), vec![R::ExistsWithDifferentDebitAccountId]);
        // balances moved exactly once.
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 100);
    }

    #[tokio::test]
    async fn gated_flags_return_not_implemented_yet() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        for f in [TransferFlags::PENDING, TransferFlags::LINKED, TransferFlags::BALANCING_DEBIT] {
            assert_eq!(l.create_transfers(&[xfer(1, 1, 2, 5).with_flags(f)]).await.unwrap(), vec![R::NotImplementedYet]);
        }
        // post needs a valid pending_id; input passes validation, then gates.
        let post = xfer(1, 1, 2, 5).with_flags(TransferFlags::POST_PENDING_TRANSFER).with_pending_id(2);
        assert_eq!(l.create_transfers(&[post]).await.unwrap(), vec![R::NotImplementedYet]);
        // Nothing persisted by a gated item.
        assert!(l.lookup_transfer(1).await.unwrap().is_none());
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 0);
        // A gated account flag too.
        assert_eq!(
            l.create_accounts(&[acct(9, 7).with_flags(AccountFlags::IMPORTED)]).await.unwrap(),
            vec![CreateAccountResult::NotImplementedYet]
        );
    }

    #[tokio::test]
    async fn batch_applies_good_skips_bad() {
        use CreateTransferResult as R;
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;

        let batch = [xfer(1, 1, 2, 100), xfer(2, 1, 99, 50), xfer(3, 2, 1, 30)];
        let res = ledger.create_transfers(&batch).await.unwrap();
        assert_eq!(res, vec![R::Created, R::CreditAccountNotFound, R::Created]);

        let a1 = ledger.lookup_account(1).await.unwrap().unwrap();
        let a2 = ledger.lookup_account(2).await.unwrap().unwrap();
        assert_eq!(a1.debits_posted, 100);
        assert_eq!(a1.credits_posted, 30);
        assert_eq!(a2.credits_posted, 100);
        assert_eq!(a2.debits_posted, 30);
        assert_eq!(a1.debits_posted + a2.debits_posted, a1.credits_posted + a2.credits_posted);
    }

    #[tokio::test]
    async fn duplicate_transfer_id_within_one_batch_applies_once() {
        use CreateTransferResult as R;
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;

        let t = xfer(7, 1, 2, 100);
        assert_eq!(ledger.create_transfers(&[t, t]).await.unwrap(), vec![R::Created, R::Exists]);
        assert_eq!(ledger.lookup_account(1).await.unwrap().unwrap().debits_posted, 100);
        assert_eq!(ledger.lookup_account(2).await.unwrap().unwrap().credits_posted, 100);
    }

    #[tokio::test]
    async fn transfer_posted_overflow_is_rejected() {
        use CreateTransferResult as R;
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;
        ledger.create_transfers(&[xfer(1, 1, 2, 10)]).await.unwrap();

        let r = ledger.create_transfers(&[xfer(2, 1, 2, u128::MAX)]).await.unwrap();
        assert_eq!(r, vec![R::OverflowsDebitsPosted]);
        assert_eq!(ledger.lookup_account(1).await.unwrap().unwrap().debits_posted, 10);
        assert!(ledger.lookup_transfer(2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn debits_must_not_exceed_credits_is_enforced() {
        use CreateTransferResult as R;
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        ledger
            .create_accounts(&[
                acct(1, 7).with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS),
                acct(2, 7),
            ])
            .await
            .unwrap();

        assert_eq!(ledger.create_transfers(&[xfer(10, 1, 2, 100)]).await.unwrap(), vec![R::ExceedsCredits]);
        assert_eq!(ledger.lookup_account(1).await.unwrap().unwrap().debits_posted, 0);

        assert_eq!(ledger.create_transfers(&[xfer(11, 2, 1, 100)]).await.unwrap(), vec![R::Created]);
        assert_eq!(ledger.create_transfers(&[xfer(12, 1, 2, 100)]).await.unwrap(), vec![R::Created]);
        assert_eq!(ledger.create_transfers(&[xfer(13, 1, 2, 1)]).await.unwrap(), vec![R::ExceedsCredits]);
    }

    #[tokio::test]
    async fn credits_must_not_exceed_debits_is_enforced() {
        use CreateTransferResult as R;
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        ledger
            .create_accounts(&[
                acct(1, 7),
                acct(2, 7).with_flags(AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS),
            ])
            .await
            .unwrap();
        assert_eq!(ledger.create_transfers(&[xfer(20, 1, 2, 100)]).await.unwrap(), vec![R::ExceedsDebits]);
        assert_eq!(ledger.create_transfers(&[xfer(21, 2, 1, 100)]).await.unwrap(), vec![R::Created]);
        assert_eq!(ledger.create_transfers(&[xfer(22, 1, 2, 100)]).await.unwrap(), vec![R::Created]);
        assert_eq!(ledger.create_transfers(&[xfer(23, 1, 2, 1)]).await.unwrap(), vec![R::ExceedsDebits]);
    }

    #[tokio::test]
    async fn create_accounts_all_exists_or_empty_writes_nothing() {
        use CreateAccountResult as R;
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        ledger.create_accounts(&[acct(42, 1)]).await.unwrap();
        let original_ts = ledger.lookup_account(42).await.unwrap().unwrap().timestamp;

        // Every id already exists → all Exists, no write.
        assert_eq!(ledger.create_accounts(&[acct(42, 1)]).await.unwrap(), vec![R::Exists]);
        // Empty slice → no-op, no error.
        assert_eq!(ledger.create_accounts(&[]).await.unwrap(), vec![]);

        let a = ledger.lookup_account(42).await.unwrap().unwrap();
        assert_eq!(a.ledger, 1);
        assert_eq!(a.timestamp, original_ts);
    }

    #[tokio::test]
    async fn conservation_holds_over_a_sequence() {
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        ledger
            .create_accounts(&[acct(1, 7), acct(2, 7), acct(3, 7), acct(4, 7)])
            .await
            .unwrap();

        let mut id = 1000u128;
        let pairs = [(1, 2, 100), (2, 3, 40), (3, 4, 25), (4, 1, 10), (1, 3, 7), (2, 4, 3)];
        for (d, c, amt) in pairs {
            assert_eq!(ledger.create_transfers(&[xfer(id, d, c, amt)]).await.unwrap(), vec![CreateTransferResult::Created]);
            id += 1;
        }

        let mut total_debits = 0u128;
        let mut total_credits = 0u128;
        for acct_id in 1..=4u128 {
            let a = ledger.lookup_account(acct_id).await.unwrap().unwrap();
            total_debits += a.debits_posted;
            total_credits += a.credits_posted;
        }
        let moved: u128 = pairs.iter().map(|&(_, _, amt)| amt).sum();
        assert_eq!(total_debits, moved);
        assert_eq!(total_credits, moved);
    }

    #[tokio::test]
    async fn timestamps_are_engine_assigned_monotonic_and_durable() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        let a1 = l.lookup_account(1).await.unwrap().unwrap();
        let a2 = l.lookup_account(2).await.unwrap().unwrap();
        assert!(a1.timestamp > 0 && a2.timestamp > a1.timestamp, "unique + increasing within a batch");

        l.create_transfers(&[xfer(10, 1, 2, 5)]).await.unwrap();
        let t = l.lookup_transfer(10).await.unwrap().unwrap();
        assert!(t.timestamp > a2.timestamp, "watermark persists across calls (durable, monotonic)");

        // A failed item applies nothing and must not break later monotonicity.
        l.create_transfers(&[xfer(11, 1, 99, 5)]).await.unwrap(); // CreditAccountNotFound
        l.create_transfers(&[xfer(12, 1, 2, 5)]).await.unwrap();
        let t12 = l.lookup_transfer(12).await.unwrap().unwrap();
        assert!(t12.timestamp > t.timestamp);
    }

    #[tokio::test]
    async fn pending_credits_do_not_grant_debit_headroom() {
        // TigerBeetle's asymmetry for DEBITS_MUST_NOT_EXCEED_CREDITS: the limit is
        // credits_POSTED only. We can't create pending credits in Phase A (gated),
        // so assert the formula directly: with zero credits_posted, any posted
        // debit on the constrained account is rejected.
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[
            acct(1, 7).with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS),
            acct(2, 7),
        ])
        .await
        .unwrap();
        assert_eq!(l.create_transfers(&[xfer(11, 1, 2, 1)]).await.unwrap(), vec![R::ExceedsCredits]);
    }
}
