//! The public [`Ledger`]: built from a [`bluedb_sql::Database`], it runs the
//! double-entry apply state machine inside that database's active writer.
//!
//! Implements TigerBeetle's `create_accounts` exactly and, for transfers,
//! **regular posted**, **two-phase** (pending reserve / post / void), and
//! **timeouts** (pending expiry via an apply-time sweep) exactly, with the full
//! per-item result-code surface and TB's input-validation ordering. Still gated
//! to later phases (returning [`CreateTransferResult::NotImplementedYet`] /
//! `CreateAccountResult::NotImplementedYet`): linked chains, balancing, closing,
//! and imported events.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bluedb_sql::{Database, WriteLease, DEFAULT_TENANT};
use bluedb_storage::Substrate;
use slatedb::WriteBatch;

use crate::keyspace::LedgerKeyspace;
use crate::model::{
    account_exists_result, account_is_gated, chains, classify, transfer_exists_result,
    validate_account_post_existence, validate_account_pre_existence,
    validate_transfer_post_existence, validate_transfer_pre_existence, Account, AccountFlags,
    CreateAccountResult, CreateTransferResult, PendingStatus, Transfer, TransferFlags, TransferOp,
    AMOUNT_MAX,
};
use crate::store::{
    encode, get_account, get_pending_state, get_transfer, get_watermark, now_ns, scan_expired,
};

/// One second in nanoseconds — the unit of a transfer `timeout`.
const NANOS_PER_SECOND: u64 = 1_000_000_000;
/// TigerBeetle caps a timestamp + timeout at `2^63 - 1` (`overflows_timeout`).
const TIMESTAMP_MAX: u64 = i64::MAX as u64;

/// Time source for timestamp assignment and timeout expiry. `Wall` reads the
/// system clock; `Manual` is an advanceable clock for deterministic tests.
#[derive(Clone)]
pub enum Clock {
    Wall,
    Manual(Arc<AtomicU64>),
}

impl Clock {
    fn now_ns(&self) -> u64 {
        match self {
            Clock::Wall => now_ns(),
            Clock::Manual(a) => a.load(Ordering::SeqCst),
        }
    }
}

/// The absolute expiry timestamp of a timed pending. Saturating: the value was
/// validated not to overflow `2^63 - 1` at reserve time (`overflows_timeout`).
fn expiry_of(timestamp: u64, timeout: u32) -> u64 {
    timestamp.saturating_add((timeout as u64).saturating_mul(NANOS_PER_SECOND))
}

/// The effective amount of a (possibly) balancing transfer against the current
/// debit/credit balances: `t.amount` reduced to the headroom on each balancing
/// side so the constrained sum cannot be exceeded. Non-balancing → `t.amount`.
fn balancing_amount(t: &Transfer, debit: &Account, credit: &Account) -> u128 {
    let mut amount = t.amount;
    if t.flags.contains(TransferFlags::BALANCING_DEBIT) {
        let headroom = debit
            .credits_posted
            .saturating_sub(debit.debits_posted.saturating_add(debit.debits_pending));
        amount = amount.min(headroom);
    }
    if t.flags.contains(TransferFlags::BALANCING_CREDIT) {
        let headroom = credit
            .debits_posted
            .saturating_sub(credit.credits_posted.saturating_add(credit.credits_pending));
        amount = amount.min(headroom);
    }
    amount
}

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
    clock: Clock,
}

impl Ledger {
    /// Build a ledger over `database` under the default tenant, sharing its
    /// substrate and write lease (so ledger applies serialize against the
    /// database's explicit SQL transactions). Uses the wall clock for timestamp
    /// assignment and timeout expiry.
    pub fn new(database: &Database) -> Self {
        Self {
            substrate: database.substrate(),
            write_lease: database.write_lease(),
            keyspace: LedgerKeyspace::new(DEFAULT_TENANT),
            clock: Clock::Wall,
        }
    }

    /// Build a ledger with an injected [`Clock`] — e.g. a [`Clock::Manual`] for
    /// deterministic timeout testing or simulation.
    pub fn with_clock(database: &Database, clock: Clock) -> Self {
        Self {
            substrate: database.substrate(),
            write_lease: database.write_lease(),
            keyspace: LedgerKeyspace::new(DEFAULT_TENANT),
            clock,
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
    /// this batch) yields [`CreateAccountResult::Exists`] or `ExistsWith*` (field
    /// comparison). `flags.linked` chains members all-or-nothing. Accepted
    /// accounts are assigned an engine timestamp and committed in one atomic
    /// batch. Requires the active writer.
    pub async fn create_accounts(&self, specs: &[Account]) -> Result<Vec<CreateAccountResult>> {
        use CreateAccountResult as R;
        let _lease = self.write_lease.lock().await; // exclusive: we are the sole mutator
        let writer = self.substrate.require_writer()?;

        let now = self.clock.now_ns();
        let mut ts = TimestampSource::new(get_watermark(&self.substrate, &self.keyspace).await?);
        let mut staged: HashMap<u128, Account> = HashMap::new();
        let mut accepted: Vec<Account> = Vec::new();
        let mut results = Vec::with_capacity(specs.len());

        let linked: Vec<bool> = specs.iter().map(|a| a.flags.contains(AccountFlags::LINKED)).collect();
        for chain in chains(&linked) {
            // Fast path: an independent (single, non-linked) account.
            if chain.start == chain.end && !chain.open {
                let r = self.stage_account(&specs[chain.start], now, &mut ts, &mut staged, &mut accepted).await?;
                results.push(r);
                continue;
            }
            // Linked chain (or open): tentatively apply, roll back on any failure.
            let staged_snap = staged.clone();
            let accepted_len = accepted.len();
            let ts_snap = ts.last;
            let mut chain_results = Vec::new();
            let mut offender = None;
            if !chain.open {
                for (pos, spec) in specs[chain.start..=chain.end].iter().enumerate() {
                    let r = self.stage_account(spec, now, &mut ts, &mut staged, &mut accepted).await?;
                    let ok = matches!(r, R::Created | R::Exists);
                    chain_results.push(r);
                    if !ok {
                        offender = Some(pos);
                        break;
                    }
                }
            }
            if chain.open || offender.is_some() {
                staged = staged_snap;
                accepted.truncate(accepted_len);
                ts.last = ts_snap;
                for k in chain.start..=chain.end {
                    let pos = k - chain.start;
                    results.push(if chain.open {
                        if k == chain.end { R::LinkedEventChainOpen } else { R::LinkedEventFailed }
                    } else if Some(pos) == offender {
                        chain_results[pos]
                    } else {
                        R::LinkedEventFailed
                    });
                }
            } else {
                results.extend(chain_results);
            }
        }

        let mut batch = WriteBatch::new();
        for a in &accepted {
            batch.put(self.keyspace.account_key(a.id), &encode(a)?);
        }
        if !accepted.is_empty() {
            batch.put(self.keyspace.watermark_key(), ts.last.to_be_bytes());
        }
        if !batch.is_empty() {
            writer.write(batch).await?;
        }
        Ok(results)
    }

    /// Validate + (on success) stage one account into `staged`/`accepted`,
    /// assigning a timestamp. Returns the per-item result; mutates nothing on a
    /// failure/exists result.
    async fn stage_account(
        &self,
        spec: &Account,
        now: u64,
        ts: &mut TimestampSource,
        staged: &mut HashMap<u128, Account>,
        accepted: &mut Vec<Account>,
    ) -> Result<CreateAccountResult> {
        use CreateAccountResult as R;
        if let Some(code) = validate_account_pre_existence(spec) {
            return Ok(code);
        }
        // Existence (TB 13–19): committed first, then staged this batch.
        if let Some(existing) = get_account(&self.substrate, &self.keyspace, spec.id).await? {
            return Ok(account_exists_result(spec, &existing));
        }
        if let Some(existing) = staged.get(&spec.id) {
            return Ok(account_exists_result(spec, existing));
        }
        if let Some(code) = validate_account_post_existence(spec) {
            return Ok(code);
        }
        if account_is_gated(spec) {
            return Ok(R::NotImplementedYet);
        }
        let mut acct = *spec;
        acct.timestamp = ts.next(now);
        staged.insert(acct.id, acct);
        accepted.push(acct);
        Ok(R::Created)
    }

    /// Apply transfers (batch). Begins by sweeping expired pendings, then
    /// validates each item against TigerBeetle's `create_transfers` check ordering
    /// and classifies + applies it: regular posted movements, pending reserves
    /// (incl. timed), and post/void resolutions. Features gated to later phases
    /// (linked, balancing, closing, imported) return
    /// [`CreateTransferResult::NotImplementedYet`]. Accepted transfers are
    /// assigned an engine timestamp; all mutated accounts, accepted transfers,
    /// pending-state records, and expiry-index churn commit in one atomic batch.
    /// Requires the active writer.
    pub async fn create_transfers(&self, transfers: &[Transfer]) -> Result<Vec<CreateTransferResult>> {
        use CreateTransferResult as R;
        let _lease = self.write_lease.lock().await; // exclusive
        let writer = self.substrate.require_writer()?; // fail fast on a replica; held for the commit

        let now = self.clock.now_ns();
        let mut ts = TimestampSource::new(get_watermark(&self.substrate, &self.keyspace).await?);
        let mut state = ApplyState::default();

        // Auto-void every pending whose timeout has elapsed by `now` before
        // processing the batch, so resolutions and re-reads see released funds.
        self.sweep_expired(now, &mut state).await?;

        // Transfers accepted in THIS batch, for in-batch existence comparison.
        let mut staged: HashMap<u128, Transfer> = HashMap::new();
        let mut accepted: Vec<Transfer> = Vec::new();
        let mut results = Vec::with_capacity(transfers.len());

        let linked: Vec<bool> = transfers.iter().map(|t| t.flags.contains(TransferFlags::LINKED)).collect();
        for chain in chains(&linked) {
            // Fast path: an independent (single, non-linked) transfer.
            if chain.start == chain.end && !chain.open {
                let r = self
                    .process_transfer(&transfers[chain.start], now, &mut ts, &mut state, &mut staged, &mut accepted)
                    .await?;
                results.push(r);
                continue;
            }
            // Linked chain (or open): tentatively apply, roll back on any failure.
            let state_snap = state.clone();
            let staged_snap = staged.clone();
            let accepted_len = accepted.len();
            let ts_snap = ts.last;
            let mut chain_results = Vec::new();
            let mut offender = None;
            if !chain.open {
                for (pos, t) in transfers[chain.start..=chain.end].iter().enumerate() {
                    let r = self
                        .process_transfer(t, now, &mut ts, &mut state, &mut staged, &mut accepted)
                        .await?;
                    let ok = matches!(r, R::Created | R::Exists);
                    chain_results.push(r);
                    if !ok {
                        offender = Some(pos);
                        break;
                    }
                }
            }
            if chain.open || offender.is_some() {
                state = state_snap;
                staged = staged_snap;
                accepted.truncate(accepted_len);
                ts.last = ts_snap;
                for k in chain.start..=chain.end {
                    let pos = k - chain.start;
                    results.push(if chain.open {
                        if k == chain.end { R::LinkedEventChainOpen } else { R::LinkedEventFailed }
                    } else if Some(pos) == offender {
                        chain_results[pos]
                    } else {
                        R::LinkedEventFailed
                    });
                }
            } else {
                results.extend(chain_results);
            }
        }

        // One atomic batch: mutated accounts + accepted transfers + pending-state
        // records (incl. swept `Expired`) + expiry-index churn + the watermark.
        let mut batch = WriteBatch::new();
        for id in &state.dirty {
            if let Some(account) = state.working.get(id) {
                batch.put(self.keyspace.account_key(*id), &encode(account)?);
            }
        }
        for t in &accepted {
            batch.put(self.keyspace.transfer_key(t.id), &encode(t)?);
            // A new timed pending gets an expiry-index entry for the sweep.
            if t.flags.contains(TransferFlags::PENDING) && t.timeout != 0 {
                let expires_at = expiry_of(t.timestamp, t.timeout);
                batch.put(self.keyspace.expiry_key(expires_at, t.id), [1u8]);
            }
        }
        for (pending_id, status) in &state.resolved {
            batch.put(self.keyspace.pending_state_key(*pending_id), &encode(status)?);
        }
        // Drop expiry-index entries for pendings resolved or swept this batch.
        for key in &state.expiry_removals {
            batch.delete(key);
        }
        if !accepted.is_empty() {
            batch.put(self.keyspace.watermark_key(), ts.last.to_be_bytes());
        }
        if !batch.is_empty() {
            writer.write(batch).await?;
        }
        Ok(results)
    }

    /// Validate, classify, and (on success) apply one transfer into `state`,
    /// `staged`, and `accepted`, assigning a timestamp on accept. Returns the
    /// per-item result; mutates nothing on a failure/exists result.
    #[allow(clippy::too_many_arguments)]
    async fn process_transfer(
        &self,
        t: &Transfer,
        now: u64,
        ts: &mut TimestampSource,
        state: &mut ApplyState,
        staged: &mut HashMap<u128, Transfer>,
        accepted: &mut Vec<Transfer>,
    ) -> Result<CreateTransferResult> {
        use CreateTransferResult as R;
        if let Some(code) = validate_transfer_pre_existence(t) {
            return Ok(code);
        }
        // Existence (TB 12–23): committed first, then staged this batch.
        if let Some(existing) = get_transfer(&self.substrate, &self.keyspace, t.id).await? {
            return Ok(transfer_exists_result(t, &existing));
        }
        if let Some(existing) = staged.get(&t.id) {
            return Ok(transfer_exists_result(t, existing));
        }
        if let Some(code) = validate_transfer_post_existence(t) {
            return Ok(code);
        }

        // Classify and apply. Each arm yields the record to persist (raw for
        // regular/pending; materialized for post/void) or a rejection code.
        let op = classify(t);
        let staged_record: StageRecord = match op {
            // A balancing transfer records the reduced (effective) amount.
            TransferOp::Regular => self.stage_regular(t, state).await?.map(|amount| Transfer { amount, ..*t }),
            TransferOp::PendingReserve => {
                // The timestamp this item will get on accept (for the
                // timeout-overflow check); equals `ts.next(now)` since no accept
                // happens between this peek and the accept below.
                let prospective_ts = ts.peek(now);
                self.stage_pending(t, prospective_ts, state)
                    .await?
                    .map(|amount| Transfer { amount, ..*t })
            }
            TransferOp::Post | TransferOp::Void => {
                let post = matches!(op, TransferOp::Post);
                // The referenced pending may be committed or staged this batch.
                let pending = match get_transfer(&self.substrate, &self.keyspace, t.pending_id).await? {
                    Some(p) => Some(p),
                    None => staged.get(&t.pending_id).copied(),
                };
                self.stage_resolution(t, pending, post, now, state).await?
            }
            TransferOp::Gated => Err(R::NotImplementedYet),
        };

        match staged_record {
            Ok(record) => {
                // Timestamp assigned only on accept (failed items don't advance it).
                let applied = Transfer { timestamp: ts.next(now), ..record };
                staged.insert(applied.id, applied);
                accepted.push(applied);
                Ok(R::Created)
            }
            Err(code) => Ok(code),
        }
    }

    /// Auto-void every timed pending whose `expires_at <= now`: release the
    /// reservation back to its accounts, record pending-state `Expired`, and
    /// mark the expiry-index entry for deletion. Folded into the caller's batch.
    async fn sweep_expired(&self, now: u64, state: &mut ApplyState) -> Result<()> {
        for (key, pending_id) in scan_expired(&self.substrate, &self.keyspace, now).await? {
            // If it was already resolved, the index entry is stale — just drop it.
            if state.resolved.contains_key(&pending_id)
                || get_pending_state(&self.substrate, &self.keyspace, pending_id).await?.is_some()
            {
                state.expiry_removals.push(key);
                continue;
            }
            let pending = match get_transfer(&self.substrate, &self.keyspace, pending_id).await? {
                Some(p) => p,
                None => {
                    state.expiry_removals.push(key);
                    continue;
                }
            };
            let mut debit = self
                .load_account(pending.debit_account_id, state)
                .await?
                .ok_or_else(|| anyhow::anyhow!("ledger invariant: expired pending debit account missing"))?;
            let mut credit = self
                .load_account(pending.credit_account_id, state)
                .await?
                .ok_or_else(|| anyhow::anyhow!("ledger invariant: expired pending credit account missing"))?;
            debit.debits_pending = debit
                .debits_pending
                .checked_sub(pending.amount)
                .ok_or_else(|| anyhow::anyhow!("ledger invariant: debits_pending underflow on expiry"))?;
            credit.credits_pending = credit
                .credits_pending
                .checked_sub(pending.amount)
                .ok_or_else(|| anyhow::anyhow!("ledger invariant: credits_pending underflow on expiry"))?;
            state.working.insert(debit.id, debit);
            state.dirty.insert(debit.id);
            state.working.insert(credit.id, credit);
            state.dirty.insert(credit.id);
            state.resolved.insert(pending_id, PendingStatus::Expired);
            state.expiry_removals.push(key);
        }
        Ok(())
    }

    /// Apply one plain posted transfer into `state`. The outer `Result` is I/O;
    /// the inner [`StageOutcome`] is the per-item validation result — `Err(code)`
    /// on the first failing TigerBeetle check (account resolution → ledger
    /// agreement → overflow → balance constraint), `Ok(effective_amount)` on
    /// accept (the amount moved, reduced for a balancing transfer). On success
    /// the debit/credit accounts are folded into `state` (not yet persisted).
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

        // Balancing: reduce the amount to the headroom on each balancing side.
        let amount = balancing_amount(t, &debit, &credit);

        // 62 / 63: per-bucket posted overflow.
        debit.debits_posted = match debit.debits_posted.checked_add(amount) {
            Some(v) => v,
            None => return Ok(Err(R::OverflowsDebitsPosted)),
        };
        credit.credits_posted = match credit.credits_posted.checked_add(amount) {
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
        // proved these sums don't overflow.) A balancing side cannot trip its
        // own check (`amount` was reduced to fit); a non-balancing constrained
        // side is still hard-enforced here.
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
        Ok(Ok(amount))
    }

    /// Stage a two-phase **pending reserve**: move `amount` into the `*_pending`
    /// buckets, with the granular pending-overflow codes (60/61), total-overflow
    /// guards (64/65), and the same asymmetric balance constraint as a post
    /// (the constrained side counts posted + pending). The reservation is later
    /// settled by a post or released by a void.
    async fn stage_pending(
        &self,
        t: &Transfer,
        prospective_ts: u64,
        state: &mut ApplyState,
    ) -> Result<StageOutcome> {
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

        // Balancing: reduce the reserved amount to the headroom on each balancing side.
        let amount = balancing_amount(t, &debit, &credit);

        // 60 / 61: per-bucket pending overflow.
        debit.debits_pending = match debit.debits_pending.checked_add(amount) {
            Some(v) => v,
            None => return Ok(Err(R::OverflowsDebitsPending)),
        };
        credit.credits_pending = match credit.credits_pending.checked_add(amount) {
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

        // 66: a timed reservation's expiry (timestamp + timeout·1e9) must fit in
        // TigerBeetle's 2^63 - 1 timestamp ceiling. Checked against the timestamp
        // this item will get on accept (`prospective_ts`).
        if t.timeout != 0 {
            let expires = (t.timeout as u64)
                .checked_mul(NANOS_PER_SECOND)
                .and_then(|d| prospective_ts.checked_add(d));
            match expires {
                Some(e) if e <= TIMESTAMP_MAX => {}
                _ => return Ok(Err(R::OverflowsTimeout)),
            }
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
        Ok(Ok(amount))
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
        now: u64,
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
            Some(PendingStatus::Expired) => return Ok(Err(R::PendingTransferExpired)),
            None => {}
        }
        // 53: lazy expiry backstop. In normal operation the start-of-batch sweep
        // already released any committed expired pending and recorded `Expired`
        // (caught above), and a same-batch reserve always has expiry > now — so
        // this rarely decides the result. It remains as a correctness guard so a
        // resolution can never settle an expired pending regardless of sweep state.
        if pending.timeout != 0 && expiry_of(pending.timestamp, pending.timeout) <= now {
            return Ok(Err(R::PendingTransferExpired));
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
        // A timed pending resolved before expiry: drop its expiry-index entry so
        // the sweep won't touch it later.
        if pending.timeout != 0 {
            state
                .expiry_removals
                .push(self.keyspace.expiry_key(expiry_of(pending.timestamp, pending.timeout), pending.id));
        }

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

    /// The timestamp the next accepted event would get, without consuming it.
    fn peek(&self, now: u64) -> u64 {
        now.max(self.last.saturating_add(1))
    }

    /// Assign (and consume) the next timestamp for an accepted event.
    fn next(&mut self, now: u64) -> u64 {
        self.last = self.peek(now);
        self.last
    }
}

/// The per-item validation result of staging one regular/pending movement:
/// `Ok(effective_amount)` accepted (the amount actually moved, reduced for a
/// balancing transfer), `Err(code)` rejected. Distinct from the I/O
/// `anyhow::Result` that wraps it.
type StageOutcome = std::result::Result<u128, CreateTransferResult>;

/// Like [`StageOutcome`] but carries the (materialized) transfer to persist on
/// accept — used by [`Ledger::stage_resolution`], which fills inherited fields.
type StageRecord = std::result::Result<Transfer, CreateTransferResult>;

/// Mutable state threaded through one `create_transfers` apply: the read-through
/// working set of touched accounts, the ids of accounts actually mutated (only
/// these are written back), and the pending transfers resolved this batch (each
/// gets a pending-state record, and a second resolution in the same batch is
/// rejected).
#[derive(Default, Clone)]
struct ApplyState {
    working: HashMap<u128, Account>,
    dirty: HashSet<u128>,
    resolved: HashMap<u128, PendingStatus>,
    /// Expiry-index keys to delete this batch (resolved-before-expiry + swept).
    expiry_removals: Vec<Vec<u8>>,
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
        // Later-phase flags still gate (imported G; closing F via pending).
        assert_eq!(l.create_transfers(&[xfer(1, 1, 2, 5).with_flags(TransferFlags::IMPORTED)]).await.unwrap(), vec![R::NotImplementedYet]);
        // A closing transfer (requires pending) is gated to Phase F.
        let closing = xfer(1, 1, 2, 5).with_flags(TransferFlags::PENDING | TransferFlags::CLOSING_DEBIT);
        assert_eq!(l.create_transfers(&[closing]).await.unwrap(), vec![R::NotImplementedYet]);
        // Nothing persisted by a gated item.
        assert!(l.lookup_transfer(1).await.unwrap().is_none());
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 0);
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

    // ---- Phase C: timeouts ----

    /// A manual-clock ledger starting at `t0` ns, plus the shared clock handle.
    fn manual_ledger(db: &Database, t0: u64) -> (Ledger, Arc<AtomicU64>) {
        let clock = Arc::new(AtomicU64::new(t0));
        (Ledger::with_clock(db, Clock::Manual(clock.clone())), clock)
    }

    fn timed_pending(id: u128, debit: u128, credit: u128, amount: u128, timeout: u32) -> Transfer {
        let mut p = xfer(id, debit, credit, amount).with_flags(TransferFlags::PENDING);
        p.timeout = timeout;
        p
    }

    #[tokio::test]
    async fn timed_pending_reserves_and_overflow_is_rejected() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        // Clock near the u64 ceiling so timestamp + timeout·1e9 overflows 2^63-1.
        let (l, _clk) = manual_ledger(&db, u64::MAX - 5);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        let res = l.create_transfers(&[timed_pending(500, 1, 2, 100, u32::MAX)]).await.unwrap();
        assert_eq!(res, vec![R::OverflowsTimeout]);
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 0, "not reserved");
    }

    #[tokio::test]
    async fn sweep_auto_voids_expired_pending() {
        let db = writer_database().await;
        let t0 = 1_000_000_000_000; // 1000s in ns
        let (l, clk) = manual_ledger(&db, t0);
        l.create_accounts(&[acct(1, 7), acct(2, 7), acct(3, 7)]).await.unwrap();
        // Reserve with a 10s timeout → expires at t0 + 10e9.
        assert_eq!(l.create_transfers(&[timed_pending(500, 1, 2, 100, 10)]).await.unwrap(), vec![CreateTransferResult::Created]);
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 100);
        // Advance past expiry, then trigger the sweep with an unrelated transfer.
        clk.store(t0 + 11_000_000_000, Ordering::SeqCst);
        l.create_transfers(&[xfer(600, 1, 3, 5)]).await.unwrap();
        let d = l.lookup_account(1).await.unwrap().unwrap();
        assert_eq!(d.debits_pending, 0, "expired reservation released");
        assert_eq!(d.debits_posted, 5, "only the unrelated regular transfer posted");
    }

    #[tokio::test]
    async fn sweep_only_batch_commits_release() {
        let db = writer_database().await;
        let t0 = 1_000_000_000_000;
        let (l, clk) = manual_ledger(&db, t0);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        l.create_transfers(&[timed_pending(500, 1, 2, 100, 10)]).await.unwrap();
        clk.store(t0 + 11_000_000_000, Ordering::SeqCst);
        // An empty batch still runs the sweep and must persist the release.
        l.create_transfers(&[]).await.unwrap();
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 0);
        assert_eq!(l.lookup_account(2).await.unwrap().unwrap().credits_pending, 0);
    }

    #[tokio::test]
    async fn expired_pending_rejects_resolution() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let t0 = 1_000_000_000_000;
        let (l, clk) = manual_ledger(&db, t0);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        l.create_transfers(&[timed_pending(500, 1, 2, 100, 10)]).await.unwrap();
        // Advance past expiry; the same call sweeps first, so the post sees Expired.
        clk.store(t0 + 11_000_000_000, Ordering::SeqCst);
        assert_eq!(l.create_transfers(&[post(501, 500, AMOUNT_MAX)]).await.unwrap(), vec![R::PendingTransferExpired]);
        // Reservation was released by the sweep; nothing posted.
        let d = l.lookup_account(1).await.unwrap().unwrap();
        assert_eq!((d.debits_pending, d.debits_posted), (0, 0));
    }

    #[tokio::test]
    async fn timeout_overflow_in_range_no_u64_overflow() {
        // The middle arm of the overflow check: timestamp + timeout·1e9 fits in
        // u64 but exceeds TigerBeetle's 2^63-1 ceiling → OverflowsTimeout.
        use CreateTransferResult as R;
        let db = writer_database().await;
        let t0 = (i64::MAX as u64) - 5_000_000_000; // ~5s below the ceiling
        let (l, _clk) = manual_ledger(&db, t0);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        // 10s timeout → expiry ≈ ceiling + 5s, within u64 but past 2^63-1.
        let res = l.create_transfers(&[timed_pending(500, 1, 2, 100, 10)]).await.unwrap();
        assert_eq!(res, vec![R::OverflowsTimeout]);
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 0);
    }

    #[tokio::test]
    async fn sweep_releases_multiple_expired_on_same_account() {
        let db = writer_database().await;
        let t0 = 1_000_000_000_000;
        let (l, clk) = manual_ledger(&db, t0);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        // Two timed reservations 1→2 (compounding debits_pending on account 1).
        l.create_transfers(&[timed_pending(500, 1, 2, 30, 10)]).await.unwrap();
        l.create_transfers(&[timed_pending(501, 1, 2, 70, 10)]).await.unwrap();
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 100);
        // Advance past both expiries; one sweep must release both.
        clk.store(t0 + 11_000_000_000, Ordering::SeqCst);
        l.create_transfers(&[]).await.unwrap();
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 0);
        assert_eq!(l.lookup_account(2).await.unwrap().unwrap().credits_pending, 0);
    }

    #[tokio::test]
    async fn resolve_before_expiry_prevents_double_release() {
        let db = writer_database().await;
        let t0 = 1_000_000_000_000;
        let (l, clk) = manual_ledger(&db, t0);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        l.create_transfers(&[timed_pending(500, 1, 2, 100, 10)]).await.unwrap();
        // Post before expiry (its index entry is removed).
        assert_eq!(l.create_transfers(&[post(501, 500, AMOUNT_MAX)]).await.unwrap(), vec![CreateTransferResult::Created]);
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 100);
        // Advance past the old expiry and sweep: nothing to release, balances stable.
        clk.store(t0 + 11_000_000_000, Ordering::SeqCst);
        l.create_transfers(&[]).await.unwrap();
        let d = l.lookup_account(1).await.unwrap().unwrap();
        assert_eq!((d.debits_pending, d.debits_posted), (0, 100), "no double release");
    }

    #[tokio::test]
    async fn zero_timeout_pending_never_expires() {
        let db = writer_database().await;
        let t0 = 1_000_000_000_000;
        let (l, clk) = manual_ledger(&db, t0);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        l.create_transfers(&[xfer(500, 1, 2, 100).with_flags(TransferFlags::PENDING)]).await.unwrap();
        clk.store(t0 + 1_000_000_000_000, Ordering::SeqCst); // far in the future
        l.create_transfers(&[]).await.unwrap(); // sweep finds nothing
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 100, "untimed pending outstanding");
        // And it can still be posted.
        assert_eq!(l.create_transfers(&[post(501, 500, AMOUNT_MAX)]).await.unwrap(), vec![CreateTransferResult::Created]);
    }

    // ---- Phase D: linked chains ----

    fn linked_xfer(id: u128, d: u128, c: u128, amt: u128) -> Transfer {
        xfer(id, d, c, amt).with_flags(TransferFlags::LINKED)
    }

    #[tokio::test]
    async fn linked_accounts_all_succeed() {
        use CreateAccountResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        let a = acct(1, 7).with_flags(AccountFlags::LINKED);
        let res = l.create_accounts(&[a, acct(2, 7)]).await.unwrap();
        assert_eq!(res, vec![R::Created, R::Created]);
        assert!(l.lookup_account(1).await.unwrap().is_some());
        assert!(l.lookup_account(2).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn linked_accounts_one_fails_rolls_back_chain() {
        use CreateAccountResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        // a1 (linked) ok, a2 has ledger 0 (LedgerMustNotBeZero) → whole chain fails.
        let a1 = acct(1, 7).with_flags(AccountFlags::LINKED);
        let a2 = Account::input(2, 0).with_code(1);
        let res = l.create_accounts(&[a1, a2]).await.unwrap();
        assert_eq!(res, vec![R::LinkedEventFailed, R::LedgerMustNotBeZero]);
        // Neither persisted (a1 rolled back).
        assert!(l.lookup_account(1).await.unwrap().is_none());
        assert!(l.lookup_account(2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn open_chain_is_rejected() {
        use CreateAccountResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        // A lone linked account (last batch event linked) → chain open.
        let res = l.create_accounts(&[acct(1, 7).with_flags(AccountFlags::LINKED)]).await.unwrap();
        assert_eq!(res, vec![R::LinkedEventChainOpen]);
        assert!(l.lookup_account(1).await.unwrap().is_none());
        // Two linked with no terminator → first LinkedEventFailed, last ChainOpen.
        let res = l
            .create_accounts(&[acct(2, 7).with_flags(AccountFlags::LINKED), acct(3, 7).with_flags(AccountFlags::LINKED)])
            .await
            .unwrap();
        assert_eq!(res, vec![R::LinkedEventFailed, R::LinkedEventChainOpen]);
        assert!(l.lookup_account(2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn linked_transfers_all_succeed() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7), acct(3, 7)]).await.unwrap();
        // 1→2 (linked) then 2→3 (terminator): both apply.
        let res = l.create_transfers(&[linked_xfer(10, 1, 2, 100), xfer(11, 2, 3, 40)]).await.unwrap();
        assert_eq!(res, vec![R::Created, R::Created]);
        assert_eq!(l.lookup_account(2).await.unwrap().unwrap().debits_posted, 40);
        assert_eq!(l.lookup_account(2).await.unwrap().unwrap().credits_posted, 100);
    }

    #[tokio::test]
    async fn linked_transfers_failure_rolls_back_whole_chain() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        // t1 ok (1→2), t2 references a missing account → chain fails, t1 rolled back.
        let res = l.create_transfers(&[linked_xfer(10, 1, 2, 100), xfer(11, 1, 99, 5)]).await.unwrap();
        assert_eq!(res, vec![R::LinkedEventFailed, R::CreditAccountNotFound]);
        // t1's movement rolled back; nothing persisted.
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 0);
        assert_eq!(l.lookup_account(2).await.unwrap().unwrap().credits_posted, 0);
        assert!(l.lookup_transfer(10).await.unwrap().is_none());
        assert!(l.lookup_transfer(11).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn failure_mid_chain_marks_all_others_linked_failed() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        // t1 ok, t2 bad (missing acct 3), t3 terminator — t2 is the offender.
        let res = l
            .create_transfers(&[linked_xfer(10, 1, 2, 50), linked_xfer(11, 2, 3, 10), xfer(12, 2, 1, 5)])
            .await
            .unwrap();
        assert_eq!(res, vec![R::LinkedEventFailed, R::CreditAccountNotFound, R::LinkedEventFailed]);
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 0);
        assert_eq!(l.lookup_account(2).await.unwrap().unwrap().debits_posted, 0);
    }

    #[tokio::test]
    async fn independent_events_commit_around_a_failed_chain() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        // indep ok | [t2(linked) ok, t3(bad)] chain fails | indep ok
        let batch = [
            xfer(10, 1, 2, 10),                // independent → Created
            linked_xfer(11, 1, 2, 20),         // chain start
            xfer(12, 1, 99, 5),                // terminator, bad → CreditAccountNotFound
            xfer(13, 2, 1, 7),                 // independent → Created
        ];
        let res = l.create_transfers(&batch).await.unwrap();
        assert_eq!(res, vec![R::Created, R::LinkedEventFailed, R::CreditAccountNotFound, R::Created]);
        // Only the two independents applied: 1 debits 10 (+0 from chain), credits 7; 2 credits 10, debits 7.
        let a1 = l.lookup_account(1).await.unwrap().unwrap();
        let a2 = l.lookup_account(2).await.unwrap().unwrap();
        assert_eq!((a1.debits_posted, a1.credits_posted), (10, 7));
        assert_eq!((a2.credits_posted, a2.debits_posted), (10, 7));
    }

    #[tokio::test]
    async fn linked_chain_first_member_is_offender() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        // The FIRST member fails (offender=0); the terminator gets LinkedEventFailed.
        let res = l.create_transfers(&[linked_xfer(10, 1, 99, 5), xfer(11, 1, 2, 5)]).await.unwrap();
        assert_eq!(res, vec![R::CreditAccountNotFound, R::LinkedEventFailed]);
        assert!(l.lookup_transfer(11).await.unwrap().is_none());
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 0);
    }

    #[tokio::test]
    async fn transfer_open_chain_is_rejected() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        // A lone trailing LINKED transfer (no terminator) → chain open, not applied.
        assert_eq!(l.create_transfers(&[linked_xfer(10, 1, 2, 5)]).await.unwrap(), vec![R::LinkedEventChainOpen]);
        assert!(l.lookup_transfer(10).await.unwrap().is_none());
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 0);
    }

    #[tokio::test]
    async fn linked_two_phase_chain() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        // reserve(linked) + post(terminator) committed atomically.
        let mut reserve = xfer(10, 1, 2, 100).with_flags(TransferFlags::PENDING | TransferFlags::LINKED);
        reserve.timeout = 0;
        let res = l.create_transfers(&[reserve, post(11, 10, AMOUNT_MAX)]).await.unwrap();
        assert_eq!(res, vec![R::Created, R::Created]);
        let d = l.lookup_account(1).await.unwrap().unwrap();
        assert_eq!((d.debits_pending, d.debits_posted), (0, 100));
    }

    #[tokio::test]
    async fn idempotent_linked_chain_retry() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7), acct(3, 7)]).await.unwrap();
        let batch = [linked_xfer(10, 1, 2, 100), xfer(11, 2, 3, 40)];
        assert_eq!(l.create_transfers(&batch).await.unwrap(), vec![R::Created, R::Created]);
        // Resubmit identical → both Exists; chain commits (no LinkedEventFailed).
        assert_eq!(l.create_transfers(&batch).await.unwrap(), vec![R::Exists, R::Exists]);
        // Balances unchanged (applied exactly once).
        assert_eq!(l.lookup_account(2).await.unwrap().unwrap().credits_posted, 100);
    }

    // ---- Phase E: balancing transfers ----

    /// Give `account` real `credits_posted` of `amount` by a posted `other → account`.
    async fn fund_credits(l: &Ledger, id: u128, other: u128, account: u128, amount: u128) {
        l.create_transfers(&[xfer(id, other, account, amount)]).await.unwrap();
    }

    #[tokio::test]
    async fn balancing_debit_reduces_to_headroom() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        // Give account 1 credits_posted = 50 (2 → 1).
        fund_credits(&l, 1, 2, 1, 50).await;
        // balancing_debit 1 → 2 of 200 transfers only the 50 of headroom.
        let bd = xfer(2, 1, 2, 200).with_flags(TransferFlags::BALANCING_DEBIT);
        assert_eq!(l.create_transfers(&[bd]).await.unwrap(), vec![R::Created]);
        let a1 = l.lookup_account(1).await.unwrap().unwrap();
        assert_eq!(a1.debits_posted, 50, "reduced to credits_posted headroom");
        assert_eq!(l.lookup_transfer(2).await.unwrap().unwrap().amount, 50, "records reduced amount");
    }

    #[tokio::test]
    async fn balancing_debit_zero_headroom_transfers_zero() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        // No credits on account 1 → headroom 0 → transfers 0, still Created.
        let bd = xfer(1, 1, 2, 100).with_flags(TransferFlags::BALANCING_DEBIT);
        assert_eq!(l.create_transfers(&[bd]).await.unwrap(), vec![R::Created]);
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 0);
        assert_eq!(l.lookup_transfer(1).await.unwrap().unwrap().amount, 0);
    }

    #[tokio::test]
    async fn balancing_credit_reduces_to_headroom() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        // Give account 2 debits_posted = 30 (2 → 1).
        fund_credits(&l, 1, 2, 1, 30).await; // account 2 debits_posted = 30
        // balancing_credit 1 → 2 of 100 transfers only 30 (credit headroom).
        let bc = xfer(2, 1, 2, 100).with_flags(TransferFlags::BALANCING_CREDIT);
        l.create_transfers(&[bc]).await.unwrap();
        assert_eq!(l.lookup_account(2).await.unwrap().unwrap().credits_posted, 30);
        assert_eq!(l.lookup_transfer(2).await.unwrap().unwrap().amount, 30);
    }

    #[tokio::test]
    async fn balancing_both_flags_takes_min_headroom() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7), acct(3, 7)]).await.unwrap();
        // Account 1 credits_posted = 40 (debit headroom 40); account 2 debits_posted = 25 (credit headroom 25).
        fund_credits(&l, 1, 3, 1, 40).await; // 1.credits_posted = 40
        fund_credits(&l, 2, 2, 3, 25).await; // 2.debits_posted = 25
        let b = xfer(3, 1, 2, 100).with_flags(TransferFlags::BALANCING_DEBIT | TransferFlags::BALANCING_CREDIT);
        l.create_transfers(&[b]).await.unwrap();
        assert_eq!(l.lookup_transfer(3).await.unwrap().unwrap().amount, 25, "min(40, 25)");
    }

    #[tokio::test]
    async fn balancing_pending_reserves_reduced_then_posts() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        fund_credits(&l, 1, 2, 1, 60).await; // 1.credits_posted = 60
        // balancing_debit | pending of 100 reserves 60.
        let bp = xfer(2, 1, 2, 100).with_flags(TransferFlags::BALANCING_DEBIT | TransferFlags::PENDING);
        l.create_transfers(&[bp]).await.unwrap();
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_pending, 60);
        assert_eq!(l.lookup_transfer(2).await.unwrap().unwrap().amount, 60, "pending records reduced amount");
        // Post the full reservation (60).
        assert_eq!(l.create_transfers(&[post(3, 2, AMOUNT_MAX)]).await.unwrap(), vec![CreateTransferResult::Created]);
        let a1 = l.lookup_account(1).await.unwrap().unwrap();
        assert_eq!((a1.debits_pending, a1.debits_posted), (0, 60));
    }

    #[tokio::test]
    async fn balancing_debit_still_checks_credit_constraint() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        // 1 = plain (will be the debit), 2 = plain funder, 3 = credit-constrained.
        l.create_accounts(&[
            acct(1, 7),
            acct(2, 7),
            acct(3, 7).with_flags(AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS),
        ])
        .await
        .unwrap();
        // Give account 1 credits_posted = 50 (2 → 1), so balancing_debit has headroom 50.
        fund_credits(&l, 1, 2, 1, 50).await;
        // balancing_debit 1 → 3 of 100 reduces to the debit headroom (1.credits_posted
        // 50 - 1.debits 0 = 50). Account 3 is credits_must_not_exceed_debits with 0
        // debits, so crediting it by 50 → ExceedsDebits (balancing_debit guards only
        // the debit side, not account 3's credit constraint).
        let bd = xfer(2, 1, 3, 100).with_flags(TransferFlags::BALANCING_DEBIT);
        assert_eq!(l.create_transfers(&[bd]).await.unwrap(), vec![R::ExceedsDebits]);
        assert!(l.lookup_transfer(2).await.unwrap().is_none());
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 0, "nothing applied");
    }

    #[tokio::test]
    async fn balancing_no_reduction_when_amount_under_headroom() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        fund_credits(&l, 1, 2, 1, 100).await; // 1.credits_posted = 100 (headroom 100)
        // amount 30 < headroom 100 → no reduction, full 30 transfers.
        let bd = xfer(2, 1, 2, 30).with_flags(TransferFlags::BALANCING_DEBIT);
        l.create_transfers(&[bd]).await.unwrap();
        assert_eq!(l.lookup_transfer(2).await.unwrap().unwrap().amount, 30);
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 30);
    }

    #[tokio::test]
    async fn balancing_linked_chain_rolls_back() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        fund_credits(&l, 1, 2, 1, 50).await; // 1.credits_posted = 50
        let before = l.lookup_account(1).await.unwrap().unwrap().debits_posted;
        // [balancing_debit(linked, would reduce to 50), bad terminator] → chain fails.
        let bd = xfer(2, 1, 2, 200).with_flags(TransferFlags::BALANCING_DEBIT | TransferFlags::LINKED);
        let res = l.create_transfers(&[bd, xfer(3, 1, 99, 5)]).await.unwrap();
        assert_eq!(res, vec![R::LinkedEventFailed, R::CreditAccountNotFound]);
        // The balancing member's reduced movement was rolled back.
        assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, before);
        assert!(l.lookup_transfer(2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn conservation_with_balancing_reduction() {
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7), acct(3, 7)]).await.unwrap();
        fund_credits(&l, 1, 3, 1, 40).await; // 1.credits_posted = 40
        // balancing_debit 1→2 of 1000 reduces to 40; both sides move the same 40.
        let bd = xfer(2, 1, 2, 1000).with_flags(TransferFlags::BALANCING_DEBIT);
        l.create_transfers(&[bd]).await.unwrap();
        let mut total_d = 0u128;
        let mut total_c = 0u128;
        for id in 1..=3u128 {
            let a = l.lookup_account(id).await.unwrap().unwrap();
            total_d += a.debits_posted;
            total_c += a.credits_posted;
        }
        assert_eq!(total_d, total_c, "Σdebits_posted == Σcredits_posted with a reduced transfer");
    }

    #[tokio::test]
    async fn balancing_in_linked_chain() {
        use CreateTransferResult as R;
        let db = writer_database().await;
        let l = Ledger::new(&db);
        l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
        fund_credits(&l, 1, 2, 1, 50).await;
        // [balancing_debit(linked, reduces to 50), terminator] both commit.
        let bd = xfer(2, 1, 2, 200).with_flags(TransferFlags::BALANCING_DEBIT | TransferFlags::LINKED);
        let res = l.create_transfers(&[bd, xfer(3, 2, 1, 5)]).await.unwrap();
        assert_eq!(res, vec![R::Created, R::Created]);
        assert_eq!(l.lookup_transfer(2).await.unwrap().unwrap().amount, 50);
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
