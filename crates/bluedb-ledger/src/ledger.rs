//! The public [`Ledger`]: built from a [`bluedb_sql::Database`], it runs the
//! double-entry apply state machine inside that database's active writer.
//!
//! Implements TigerBeetle's `create_accounts` exactly and, for transfers,
//! **regular posted** plus **two-phase** (pending reserve / post / void)
//! transfers exactly, with the full per-item result-code surface and TB's
//! input-validation ordering. Still gated to later phases (returning
//! [`CreateTransferResult::NotImplementedYet`] / `CreateAccountResult::NotImplementedYet`):
//! linked chains, balancing, closing, imported events, and pending transfers
//! with a nonzero timeout.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use bluedb_sql::{Database, WriteLease, DEFAULT_TENANT};
use bluedb_storage::Substrate;
use slatedb::WriteBatch;

use crate::keyspace::LedgerKeyspace;
use crate::model::{
    account_exists_result, account_is_gated, classify, transfer_exists_result,
    validate_account_post_existence, validate_account_pre_existence,
    validate_transfer_post_existence, validate_transfer_pre_existence, Account, AccountFlags,
    CreateAccountResult, CreateTransferResult, PendingStatus, Transfer, TransferOp, AMOUNT_MAX,
};
use crate::store::{encode, get_account, get_pending_state, get_transfer, get_watermark, now_ns};

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
    /// `create_transfers` check ordering, then classified and applied: regular
    /// posted movements, pending reserves, and post/void resolutions. Features
    /// gated to later phases (linked, balancing, closing, imported, pending with
    /// a nonzero timeout) return [`CreateTransferResult::NotImplementedYet`].
    /// Accepted transfers are assigned an engine timestamp; all mutated accounts,
    /// accepted transfers, and pending-state records commit in one atomic batch.
    /// Requires the active writer.
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

            // Classify and apply. Each arm yields the record to persist (raw for
            // regular/pending; materialized for post/void) or a rejection code.
            let op = classify(t);
            let staged_record: StageRecord = match op {
                TransferOp::Regular => self.stage_regular(t, &mut state).await?.map(|()| *t),
                TransferOp::PendingReserve => self.stage_pending(t, &mut state).await?.map(|()| *t),
                TransferOp::Post | TransferOp::Void => {
                    let post = matches!(op, TransferOp::Post);
                    // The referenced pending may be committed or staged this batch.
                    let pending = match get_transfer(&self.substrate, &self.keyspace, t.pending_id).await? {
                        Some(p) => Some(p),
                        None => staged.get(&t.pending_id).copied(),
                    };
                    self.stage_resolution(t, pending, post, &mut state).await?
                }
                TransferOp::Gated => Err(R::NotImplementedYet),
            };

            match staged_record {
                Ok(record) => {
                    // Timestamp assigned only on accept (failed items don't advance it).
                    let applied = Transfer { timestamp: ts.next(), ..record };
                    staged.insert(applied.id, applied);
                    accepted.push(applied);
                    results.push(R::Created);
                }
                Err(code) => results.push(code),
            }
        }

        // One atomic batch: every mutated account + every accepted transfer +
        // every pending-state record from this batch + the advanced watermark.
        let mut batch = WriteBatch::new();
        for id in &state.dirty {
            if let Some(account) = state.working.get(id) {
                batch.put(self.keyspace.account_key(*id), &encode(account)?);
            }
        }
        for t in &accepted {
            batch.put(self.keyspace.transfer_key(t.id), &encode(t)?);
        }
        for (pending_id, status) in &state.resolved {
            batch.put(self.keyspace.pending_state_key(*pending_id), &encode(status)?);
        }
        if !accepted.is_empty() {
            batch.put(self.keyspace.watermark_key(), ts.last.to_be_bytes());
            writer.write(batch).await?;
        }
        Ok(results)
    }

    /// Apply one plain posted transfer into `state`. The outer `Result` is I/O;
    /// the inner [`StageOutcome`] is the per-item validation result — `Err(code)`
    /// on the first failing TigerBeetle check (account resolution → ledger
    /// agreement → overflow → balance constraint). On success the debit/credit
    /// accounts are folded into `state` (not yet persisted).
    async fn stage_regular(&self, t: &Transfer, state: &mut ApplyState) -> Result<StageOutcome> {
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
        // These also guard the unchecked `posted + pending` in the balance checks
        // below, making those adds panic-free.
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

    /// Stage a two-phase **pending reserve**: move `amount` into the `*_pending`
    /// buckets, with the granular pending-overflow codes (60/61), total-overflow
    /// guards (64/65), and the same asymmetric balance constraint as a post
    /// (the constrained side counts posted + pending). The reservation is later
    /// settled by a post or released by a void.
    async fn stage_pending(&self, t: &Transfer, state: &mut ApplyState) -> Result<StageOutcome> {
        use CreateTransferResult as R;

        let mut debit = match self.load_account(t.debit_account_id, state).await? {
            Some(a) => a,
            None => return Ok(Err(R::DebitAccountNotFound)),
        };
        let mut credit = match self.load_account(t.credit_account_id, state).await? {
            Some(a) => a,
            None => return Ok(Err(R::CreditAccountNotFound)),
        };
        if debit.ledger != credit.ledger {
            return Ok(Err(R::AccountsMustHaveTheSameLedger));
        }
        if t.ledger != debit.ledger {
            return Ok(Err(R::TransferMustHaveTheSameLedgerAsAccounts));
        }

        // 60 / 61: per-bucket pending overflow.
        debit.debits_pending = match debit.debits_pending.checked_add(t.amount) {
            Some(v) => v,
            None => return Ok(Err(R::OverflowsDebitsPending)),
        };
        credit.credits_pending = match credit.credits_pending.checked_add(t.amount) {
            Some(v) => v,
            None => return Ok(Err(R::OverflowsCreditsPending)),
        };
        // 64 / 65: total (pending + posted) overflow; also guards the unchecked
        // adds in the balance checks below.
        if debit.debits_pending.checked_add(debit.debits_posted).is_none() {
            return Ok(Err(R::OverflowsDebits));
        }
        if credit.credits_pending.checked_add(credit.credits_posted).is_none() {
            return Ok(Err(R::OverflowsCredits));
        }

        // 67 / 68: a reserved outflow is constrained exactly like a post.
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

    /// Stage a **post** (`post == true`) or **void** (`false`) of the pending
    /// transfer referenced by `t.pending_id` (`pending`: the looked-up pending,
    /// committed or staged this batch). Runs TigerBeetle's resolution checks
    /// 43→52 in order, then releases the full reservation and (post) moves the
    /// effective amount into `*_posted`. Returns the **materialized** transfer
    /// (inherited fields filled, `amount` = effective) to persist, or a rejection
    /// code. Releasing/posting cannot breach a balance constraint that held at
    /// reserve time, so 67/68 are not re-checked.
    async fn stage_resolution(
        &self,
        t: &Transfer,
        pending: Option<Transfer>,
        post: bool,
        state: &mut ApplyState,
    ) -> Result<StageRecord> {
        use crate::model::TransferFlags as F;
        use CreateTransferResult as R;

        // 43: the pending must exist.
        let pending = match pending {
            Some(p) => p,
            None => return Ok(Err(R::PendingTransferNotFound)),
        };
        // 44: it must actually be a pending transfer.
        if !pending.flags.contains(F::PENDING) {
            return Ok(Err(R::PendingTransferNotPending));
        }
        // 45–48: nonzero fields on the resolution must match the pending.
        if t.debit_account_id != 0 && t.debit_account_id != pending.debit_account_id {
            return Ok(Err(R::PendingTransferHasDifferentDebitAccountId));
        }
        if t.credit_account_id != 0 && t.credit_account_id != pending.credit_account_id {
            return Ok(Err(R::PendingTransferHasDifferentCreditAccountId));
        }
        if t.ledger != 0 && t.ledger != pending.ledger {
            return Ok(Err(R::PendingTransferHasDifferentLedger));
        }
        if t.code != 0 && t.code != pending.code {
            return Ok(Err(R::PendingTransferHasDifferentCode));
        }
        // 49 (post) / 50 (void): amount.
        let effective = if post {
            if t.amount == AMOUNT_MAX {
                pending.amount
            } else if t.amount > pending.amount {
                return Ok(Err(R::ExceedsPendingTransferAmount));
            } else {
                t.amount
            }
        } else {
            if t.amount != 0 && t.amount != pending.amount {
                return Ok(Err(R::PendingTransferHasDifferentAmount));
            }
            pending.amount // released in full; nothing posted
        };
        // 51 / 52: already resolved? in-batch first, then committed.
        let prior = match state.resolved.get(&pending.id).copied() {
            Some(s) => Some(s),
            None => get_pending_state(&self.substrate, &self.keyspace, pending.id).await?,
        };
        match prior {
            Some(PendingStatus::Posted) => return Ok(Err(R::PendingTransferAlreadyPosted)),
            Some(PendingStatus::Voided) => return Ok(Err(R::PendingTransferAlreadyVoided)),
            None => {}
        }

        // Apply against the PENDING transfer's accounts.
        let mut debit = self
            .load_account(pending.debit_account_id, state)
            .await?
            .ok_or_else(|| anyhow::anyhow!("ledger invariant: pending debit account missing"))?;
        let mut credit = self
            .load_account(pending.credit_account_id, state)
            .await?
            .ok_or_else(|| anyhow::anyhow!("ledger invariant: pending credit account missing"))?;

        // Release the full reservation (checked_sub guards the invariant that the
        // reservation is still outstanding).
        debit.debits_pending = debit
            .debits_pending
            .checked_sub(pending.amount)
            .ok_or_else(|| anyhow::anyhow!("ledger invariant: debits_pending underflow on resolve"))?;
        credit.credits_pending = credit
            .credits_pending
            .checked_sub(pending.amount)
            .ok_or_else(|| anyhow::anyhow!("ledger invariant: credits_pending underflow on resolve"))?;
        if post {
            // 62 / 63: posting the effective amount can overflow the posted bucket.
            debit.debits_posted = match debit.debits_posted.checked_add(effective) {
                Some(v) => v,
                None => return Ok(Err(R::OverflowsDebitsPosted)),
            };
            credit.credits_posted = match credit.credits_posted.checked_add(effective) {
                Some(v) => v,
                None => return Ok(Err(R::OverflowsCreditsPosted)),
            };
        }

        state.working.insert(debit.id, debit);
        state.dirty.insert(debit.id);
        state.working.insert(credit.id, credit);
        state.dirty.insert(credit.id);
        state.resolved.insert(
            pending.id,
            if post { PendingStatus::Posted } else { PendingStatus::Voided },
        );

        // Materialize the stored record: inherited fields filled from the pending,
        // `amount` set to the effective posted/voided amount.
        let materialized = Transfer {
            debit_account_id: pending.debit_account_id,
            credit_account_id: pending.credit_account_id,
            amount: effective,
            ledger: pending.ledger,
            code: if t.code != 0 { t.code } else { pending.code },
            user_data_128: if t.user_data_128 != 0 { t.user_data_128 } else { pending.user_data_128 },
            user_data_64: if t.user_data_64 != 0 { t.user_data_64 } else { pending.user_data_64 },
            user_data_32: if t.user_data_32 != 0 { t.user_data_32 } else { pending.user_data_32 },
            ..*t
        };
        Ok(Ok(materialized))
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
        self.last = now_ns().max(self.last.saturating_add(1));
        self.last
    }
}

/// The per-item validation result of staging one transfer: `Ok(())` accepted,
/// `Err(code)` rejected with a TigerBeetle result code. Distinct from the I/O
/// `anyhow::Result` that wraps it.
type StageOutcome = std::result::Result<(), CreateTransferResult>;

/// Like [`StageOutcome`] but carries the (materialized) transfer to persist on
/// accept — used by [`Ledger::stage_resolution`], which fills inherited fields.
type StageRecord = std::result::Result<Transfer, CreateTransferResult>;

/// Mutable state threaded through one `create_transfers` apply: the read-through
/// working set of touched accounts, the ids of accounts actually mutated (only
/// these are written back), and the pending transfers resolved this batch (each
/// gets a pending-state record, and a second resolution in the same batch is
/// rejected).
#[derive(Default)]
struct ApplyState {
    working: HashMap<u128, Account>,
    dirty: HashSet<u128>,
    resolved: HashMap<u128, PendingStatus>,
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
        // Later-phase flags still gate (linked D, balancing E, closing F, imported G).
        for f in [TransferFlags::LINKED, TransferFlags::BALANCING_DEBIT, TransferFlags::IMPORTED] {
            assert_eq!(l.create_transfers(&[xfer(1, 1, 2, 5).with_flags(f)]).await.unwrap(), vec![R::NotImplementedYet]);
        }
        // A pending WITH a timeout is Phase C → still gated.
        let mut timed = xfer(1, 1, 2, 5).with_flags(TransferFlags::PENDING);
        timed.timeout = 30;
        assert_eq!(l.create_transfers(&[timed]).await.unwrap(), vec![R::NotImplementedYet]);
        // Nothing persisted by a gated item.
        assert!(l.lookup_transfer(1).await.unwrap().is_none());
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 0);
        // A gated account flag too.
        assert_eq!(
            l.create_accounts(&[acct(9, 7).with_flags(AccountFlags::IMPORTED)]).await.unwrap(),
            vec![CreateAccountResult::NotImplementedYet]
        );
    }

    // ---- Phase B: two-phase transfers ----

    /// Reserve `amount` (1 → 2, pending) and assert it succeeds.
    async fn reserve(l: &Ledger, id: u128, debit: u128, credit: u128, amount: u128) {
        let p = xfer(id, debit, credit, amount).with_flags(TransferFlags::PENDING);
        assert_eq!(l.create_transfers(&[p]).await.unwrap(), vec![CreateTransferResult::Created]);
    }

    fn post(id: u128, pending_id: u128, amount: u128) -> Transfer {
        // zero accounts/ledger; code 0 inherits — exercises inheritance.
        let mut t = Transfer::new(id, 0, 0, amount, 0);
        t.flags = TransferFlags::POST_PENDING_TRANSFER;
        t.pending_id = pending_id;
        t
    }
    fn void(id: u128, pending_id: u128, amount: u128) -> Transfer {
        let mut t = Transfer::new(id, 0, 0, amount, 0);
        t.flags = TransferFlags::VOID_PENDING_TRANSFER;
        t.pending_id = pending_id;
        t
    }

    #[tokio::test]
    async fn pending_reserve_moves_into_pending_buckets() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        reserve(&l, 500, 1, 2, 100).await;
        let d = l.lookup_account(1).await.unwrap().unwrap();
        let c = l.lookup_account(2).await.unwrap().unwrap();
        assert_eq!((d.debits_pending, d.debits_posted), (100, 0));
        assert_eq!((c.credits_pending, c.credits_posted), (100, 0));
        assert!(l.lookup_transfer(500).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn pending_respects_balance_constraint() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7).with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS), acct(2, 7)]).await.unwrap();
        let p = xfer(1, 1, 2, 50).with_flags(TransferFlags::PENDING);
        assert_eq!(l.create_transfers(&[p]).await.unwrap(), vec![R::ExceedsCredits]);
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 0);
    }

    #[tokio::test]
    async fn post_full_via_amount_max_settles() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        reserve(&l, 500, 1, 2, 100).await;
        assert_eq!(l.create_transfers(&[post(501, 500, AMOUNT_MAX)]).await.unwrap(), vec![CreateTransferResult::Created]);
        let d = l.lookup_account(1).await.unwrap().unwrap();
        let c = l.lookup_account(2).await.unwrap().unwrap();
        assert_eq!((d.debits_pending, d.debits_posted), (0, 100));
        assert_eq!((c.credits_pending, c.credits_posted), (0, 100));
        // Stored post is materialized: inherited accounts/ledger + effective amount.
        let stored = l.lookup_transfer(501).await.unwrap().unwrap();
        assert_eq!((stored.debit_account_id, stored.credit_account_id, stored.ledger, stored.amount), (1, 2, 7, 100));
    }

    #[tokio::test]
    async fn post_partial_releases_remainder() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        reserve(&l, 500, 1, 2, 100).await;
        assert_eq!(l.create_transfers(&[post(501, 500, 60)]).await.unwrap(), vec![CreateTransferResult::Created]);
        let d = l.lookup_account(1).await.unwrap().unwrap();
        let c = l.lookup_account(2).await.unwrap().unwrap();
        assert_eq!((d.debits_pending, d.debits_posted), (0, 60), "remainder released, not posted");
        assert_eq!((c.credits_pending, c.credits_posted), (0, 60));
        assert_eq!(l.lookup_transfer(501).await.unwrap().unwrap().amount, 60);
    }

    #[tokio::test]
    async fn post_exceeding_pending_is_rejected() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        reserve(&l, 500, 1, 2, 100).await;
        assert_eq!(l.create_transfers(&[post(501, 500, 101)]).await.unwrap(), vec![R::ExceedsPendingTransferAmount]);
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 100, "reservation untouched");
        assert!(l.lookup_transfer(501).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn void_releases_reservation() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        reserve(&l, 500, 1, 2, 100).await;
        assert_eq!(l.create_transfers(&[void(501, 500, 0)]).await.unwrap(), vec![CreateTransferResult::Created]);
        let d = l.lookup_account(1).await.unwrap().unwrap();
        assert_eq!((d.debits_pending, d.debits_posted), (0, 0), "released, nothing posted");
        // Void with the exact pending amount also works.
        reserve(&l, 600, 1, 2, 40).await;
        assert_eq!(l.create_transfers(&[void(601, 600, 40)]).await.unwrap(), vec![CreateTransferResult::Created]);
    }

    #[tokio::test]
    async fn void_materialized_record_inherits_pending() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7).with_code(3), acct(2, 7).with_code(3)]).await.unwrap();
        // Reserve with code 3; void with all-zero inheritable fields.
        let mut p = xfer(500, 1, 2, 100).with_code(3);
        p.flags = TransferFlags::PENDING;
        l.create_transfers(&[p]).await.unwrap();
        assert_eq!(l.create_transfers(&[void(501, 500, 0)]).await.unwrap(), vec![CreateTransferResult::Created]);
        let stored = l.lookup_transfer(501).await.unwrap().unwrap();
        // Inherited accounts/ledger/code; amount materialized to the full pending amount.
        assert_eq!(
            (stored.debit_account_id, stored.credit_account_id, stored.ledger, stored.code, stored.amount),
            (1, 2, 7, 3, 100)
        );
    }

    #[tokio::test]
    async fn in_batch_double_resolve_is_rejected() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        // Reserve, post, and post-again all in ONE batch: the in-batch resolved
        // set (not committed state) must reject the second resolution.
        let mut p = xfer(500, 1, 2, 100);
        p.flags = TransferFlags::PENDING;
        let res = l
            .create_transfers(&[p, post(501, 500, AMOUNT_MAX), post(502, 500, AMOUNT_MAX), void(503, 500, 0)])
            .await
            .unwrap();
        assert_eq!(res, vec![R::Created, R::Created, R::PendingTransferAlreadyPosted, R::PendingTransferAlreadyPosted]);
        // Settled exactly once.
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 100);
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 0);
    }

    #[tokio::test]
    async fn void_with_wrong_amount_is_rejected() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        reserve(&l, 500, 1, 2, 100).await;
        assert_eq!(l.create_transfers(&[void(501, 500, 99)]).await.unwrap(), vec![R::PendingTransferHasDifferentAmount]);
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 100);
    }

    #[tokio::test]
    async fn resolution_field_mismatch_codes() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7), acct(3, 7)]).await.unwrap();
        reserve(&l, 500, 1, 2, 100).await;
        // wrong (nonzero) debit account.
        let mut t = post(501, 500, AMOUNT_MAX);
        t.debit_account_id = 3;
        assert_eq!(l.create_transfers(&[t]).await.unwrap(), vec![R::PendingTransferHasDifferentDebitAccountId]);
        // wrong ledger.
        let mut t = post(502, 500, AMOUNT_MAX);
        t.ledger = 8;
        assert_eq!(l.create_transfers(&[t]).await.unwrap(), vec![R::PendingTransferHasDifferentLedger]);
        // wrong code.
        let mut t = post(503, 500, AMOUNT_MAX);
        t.code = 9;
        assert_eq!(l.create_transfers(&[t]).await.unwrap(), vec![R::PendingTransferHasDifferentCode]);
    }

    #[tokio::test]
    async fn resolution_not_found_and_not_pending() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        assert_eq!(l.create_transfers(&[post(501, 999, AMOUNT_MAX)]).await.unwrap(), vec![R::PendingTransferNotFound]);
        // A regular (non-pending) transfer cannot be posted.
        l.create_transfers(&[xfer(600, 1, 2, 5)]).await.unwrap();
        assert_eq!(l.create_transfers(&[post(501, 600, AMOUNT_MAX)]).await.unwrap(), vec![R::PendingTransferNotPending]);
    }

    #[tokio::test]
    async fn double_resolution_is_rejected() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        reserve(&l, 500, 1, 2, 100).await;
        assert_eq!(l.create_transfers(&[post(501, 500, AMOUNT_MAX)]).await.unwrap(), vec![R::Created]);
        // posting again → already posted; voiding → already posted (state is terminal).
        assert_eq!(l.create_transfers(&[post(502, 500, AMOUNT_MAX)]).await.unwrap(), vec![R::PendingTransferAlreadyPosted]);
        assert_eq!(l.create_transfers(&[void(503, 500, 0)]).await.unwrap(), vec![R::PendingTransferAlreadyPosted]);

        // A voided pending reports already_voided.
        reserve(&l, 600, 1, 2, 10).await;
        l.create_transfers(&[void(601, 600, 0)]).await.unwrap();
        assert_eq!(l.create_transfers(&[post(602, 600, AMOUNT_MAX)]).await.unwrap(), vec![R::PendingTransferAlreadyVoided]);
    }

    #[tokio::test]
    async fn reserve_and_post_in_one_batch() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        let mut p = xfer(500, 1, 2, 100);
        p.flags = TransferFlags::PENDING;
        let res = l.create_transfers(&[p, post(501, 500, AMOUNT_MAX)]).await.unwrap();
        assert_eq!(res, vec![CreateTransferResult::Created, CreateTransferResult::Created]);
        let d = l.lookup_account(1).await.unwrap().unwrap();
        assert_eq!((d.debits_pending, d.debits_posted), (0, 100), "settled within the batch");
    }

    #[tokio::test]
    async fn identical_post_retry_is_idempotent() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        reserve(&l, 500, 1, 2, 100).await;
        assert_eq!(l.create_transfers(&[post(501, 500, AMOUNT_MAX)]).await.unwrap(), vec![R::Created]);
        // Same submission (id 501, AMOUNT_MAX, inherited zeros) → Exists, not a mismatch.
        assert_eq!(l.create_transfers(&[post(501, 500, AMOUNT_MAX)]).await.unwrap(), vec![R::Exists]);
        // A differing explicit amount on the same id → mismatch.
        assert_eq!(l.create_transfers(&[post(501, 500, 50)]).await.unwrap(), vec![R::ExistsWithDifferentAmount]);
        // Balances moved exactly once.
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 100);
    }

    #[tokio::test]
    async fn conservation_with_partial_post() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        reserve(&l, 500, 1, 2, 100).await;
        l.create_transfers(&[post(501, 500, 70)]).await.unwrap(); // 70 posted, 30 released
        let d = l.lookup_account(1).await.unwrap().unwrap();
        let c = l.lookup_account(2).await.unwrap().unwrap();
        assert_eq!(d.debits_posted, c.credits_posted, "Σdebits == Σcredits");
        assert_eq!((d.debits_pending, c.credits_pending), (0, 0));
        assert_eq!(d.debits_posted, 70);
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
    async fn debits_limited_to_posted_credits_boundary() {
        // DEBITS_MUST_NOT_EXCEED_CREDITS limit is credits_POSTED. Establish real
        // posted credits, then prove the boundary: a debit of exactly
        // credits_posted is allowed, one unit over is ExceedsCredits.
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[
            acct(1, 7).with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS),
            acct(2, 7),
        ])
        .await
        .unwrap();
        // Give account 1 credits_posted = 100 (2 → 1).
        assert_eq!(l.create_transfers(&[xfer(1, 2, 1, 100)]).await.unwrap(), vec![R::Created]);
        // Debit exactly to the ceiling succeeds.
        assert_eq!(l.create_transfers(&[xfer(2, 1, 2, 100)]).await.unwrap(), vec![R::Created]);
        // One unit over is rejected; no mutation.
        assert_eq!(l.create_transfers(&[xfer(3, 1, 2, 1)]).await.unwrap(), vec![R::ExceedsCredits]);
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 100);
    }

    #[tokio::test]
    async fn credit_side_posted_overflow_is_rejected() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        setup_two_accounts(&l).await;
        // Seed credits_posted on account 2, then overflow it from a third account.
        l.create_accounts(&[acct(3, 7)]).await.unwrap();
        l.create_transfers(&[xfer(1, 1, 2, 10)]).await.unwrap(); // account 2 credits_posted = 10
        let r = l.create_transfers(&[xfer(2, 3, 2, u128::MAX)]).await.unwrap();
        assert_eq!(r, vec![R::OverflowsCreditsPosted]);
        assert_eq!(l.lookup_account(2).await.unwrap().unwrap().credits_posted, 10);
        assert!(l.lookup_transfer(2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn watermark_is_durable_across_reopen() {
        use std::sync::Arc;
        use slatedb::object_store::memory::InMemory;
        use slatedb::Db;

        // Same object store, two separate Db opens — the timestamp watermark must
        // survive a restart so timestamps stay strictly monotonic across it.
        let store = Arc::new(InMemory::new());
        let ts_before;
        {
            let db = Arc::new(Db::open("ledger-reopen", store.clone()).await.unwrap());
            let database = Database::new(db);
            let l = Ledger::new(&database);
            l.create_accounts(&[acct(1, 7)]).await.unwrap();
            ts_before = l.lookup_account(1).await.unwrap().unwrap().timestamp;
            database.flush().await.unwrap(); // durable before we drop it
        }
        let db2 = Arc::new(Db::open("ledger-reopen", store.clone()).await.unwrap());
        let database2 = Database::new(db2);
        let l2 = Ledger::new(&database2);
        assert_eq!(l2.lookup_account(1).await.unwrap().unwrap().timestamp, ts_before, "record survived reopen");
        l2.create_accounts(&[acct(2, 7)]).await.unwrap();
        let ts_after = l2.lookup_account(2).await.unwrap().unwrap().timestamp;
        assert!(ts_after > ts_before, "watermark survived reopen → still monotonic");
    }
}
