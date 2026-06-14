# bluedb-ledger Two-Phase Transfers Implementation Plan (Plan 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add TigerBeetle-style two-phase transfers — `pending` (reserve) → `post_pending` (settle, full or partial) / `void_pending` (release) — to the `bluedb-ledger` engine, with exactly-once resolution.

**Architecture:** Extend the Plan-1 single-writer apply state machine. Classify each transfer by its flags into `Posted | Pending | PostPending | VoidPending`. A `Pending` transfer reserves into the `*_pending` balance buckets; a `PostPending`/`VoidPending` looks up the referenced pending transfer, releases its reservation, posts the settled amount (post only), and records a *resolved marker* so it can never be resolved twice. All mutations for one batch still commit in a single atomic `WriteBatch` under the write lease.

**Tech Stack:** Rust, SlateDB (`WriteBatch`), `postcard`, the existing `bluedb-ledger` crate.

**Scope of THIS plan (Plan 2 of the ledger series):** two-phase `pending`/`post_pending`/`void_pending` + exactly-once resolution. **Out of scope (later plans):** pending **timeouts**/expiry, **balancing** transfers, **linked** chains, the SQL projection, HTTP routes, and the Jepsen workload. Transfers carrying `timeout != 0`, `LINKED`, or `BALANCING_*` are rejected with `UnsupportedFlag` until their plans land. The `Transfer`/`TransferFlags` model fields for those already exist (Plan-1 scaffolding).

**Reference spec:** `docs/superpowers/specs/2026-06-14-bluedb-ledger-design.md` (§5 two-phase semantics).
**Builds on:** Plan 1 (`docs/superpowers/plans/2026-06-14-bluedb-ledger-core-engine.md`), now merged to `master`.

---

## Background: current code shape (post Plan 1)

`crates/bluedb-ledger/src/ledger.rs` (the only file with the engine):
- `create_transfers(&self, transfers, timestamp)` holds the write lease, loops items through `stage_transfer`, tracks `working: HashMap<u128, Account>`, `dirty: HashSet<u128>`, `staged: HashSet<u128>` (in-batch transfer-id dedup), `accepted: Vec<Transfer>`; builds one `WriteBatch` of dirty accounts + accepted transfers (guarded by `!batch.is_empty()`).
- `stage_transfer(&self, t, timestamp, working: &mut HashMap<u128, Account>) -> Result<StageOutcome>`: rejects same-account; rejects any non-`NONE` flag with `UnsupportedFlag`; idempotency via `get_transfer`; loads accounts via `load_account`; ledger-match; `checked_add` posted deltas; balance constraints; writes back to `working` on accept; returns `Applied(Transfer { timestamp, ..*t })`.
- `load_account(&self, id, working: &mut HashMap<u128, Account>) -> Result<Option<Account>>`: working-set read-through.
- `enum StageOutcome { Applied(Transfer), Exists, Rejected(LedgerError) }` (private, above `mod tests`).
- `model.rs`: `Account`, `Transfer`, `AccountFlags`, `TransferFlags` (NONE, LINKED, PENDING, POST_PENDING_TRANSFER, VOID_PENDING_TRANSFER, BALANCING_DEBIT, BALANCING_CREDIT), `NewAccount`, `CreateResult`, `LedgerError` (AccountsMustDiffer, LedgerMismatch, AccountNotFound(u128), ExceedsCredits, ExceedsDebits, Overflow, UnsupportedFlag).
- `keyspace.rs`: `LedgerKeyspace` with `account_key`/`transfer_key` (tags `0x10`/`0x11` via `bluedb_sql::TAG_EXTERNAL_BASE`), `account_prefix`/`transfer_prefix` (`#[allow(dead_code)]`).
- `store.rs`: `encode`/`decode` (postcard), `get_account`/`get_transfer`, `test_harness::writer_database()`.

---

## File Structure

All changes are within the `bluedb-ledger` crate:
- `crates/bluedb-ledger/src/model.rs` — add `TransferKind` + `Transfer::kind()` classifier + new `LedgerError` variants.
- `crates/bluedb-ledger/src/keyspace.rs` — add the resolved-marker key (tag `0x12`).
- `crates/bluedb-ledger/src/store.rs` — add `is_resolved` point read.
- `crates/bluedb-ledger/src/ledger.rs` — `ApplyState` refactor, kind dispatch, `stage_movement` (posted/pending), `stage_resolution` (post/void), resolved-marker batch writes.

---

## Task 1: Refactor apply locals into `ApplyState` (no behavior change)

Introduce a struct that bundles the per-apply mutable state so later tasks can record resolution side-effects (which mutate the *pending's* accounts, not the transfer's own). This task changes NO behavior — all Plan-1 tests must still pass.

**Files:**
- Modify: `crates/bluedb-ledger/src/ledger.rs`

- [ ] **Step 1: Add the `ApplyState` struct.**

In `ledger.rs`, just above the `enum StageOutcome` definition, add:

```rust
/// Mutable state threaded through one `create_transfers` apply: the read-through
/// working set of touched accounts, the ids of accounts actually mutated (only
/// these are written back), and the ids of pending transfers resolved this batch
/// (each gets a resolved marker). Lets a resolution mutate the *pending's*
/// accounts and still be batched/dirtied correctly.
#[derive(Default)]
struct ApplyState {
    working: std::collections::HashMap<u128, Account>,
    dirty: std::collections::HashSet<u128>,
    resolved: std::collections::HashSet<u128>,
}
```

- [ ] **Step 2: Rewrite `create_transfers` to use `ApplyState`.**

Replace the body of `create_transfers` with:

```rust
    pub async fn create_transfers(&self, transfers: &[Transfer], timestamp: u64) -> Result<Vec<CreateResult>> {
        let _lease = self.write_lease.lock().await; // exclusive
        let writer = self.substrate.require_writer()?; // fail fast on a replica; held for the commit

        let mut state = ApplyState::default();
        // Transfer ids accepted in THIS batch, so a duplicate id within one call
        // is treated as Exists (the committed-state check can't see uncommitted
        // siblings) and cannot double-apply.
        let mut staged: HashSet<u128> = HashSet::new();
        let mut accepted: Vec<Transfer> = Vec::new();
        let mut results = Vec::with_capacity(transfers.len());

        for t in transfers {
            if staged.contains(&t.id) {
                results.push(CreateResult::Exists);
                continue;
            }
            match self.stage_transfer(t, timestamp, &mut state).await? {
                StageOutcome::Applied(applied) => {
                    staged.insert(applied.id);
                    accepted.push(applied);
                    results.push(CreateResult::Ok);
                }
                StageOutcome::Exists => results.push(CreateResult::Exists),
                StageOutcome::Rejected(err) => results.push(CreateResult::Failed(err)),
            }
        }

        // One atomic batch: every mutated account + every accepted transfer +
        // a resolved marker for every pending resolved this batch.
        let mut batch = WriteBatch::new();
        for id in &state.dirty {
            if let Some(account) = state.working.get(id) {
                batch.put(self.keyspace.account_key(*id), &encode(account)?);
            }
        }
        for t in &accepted {
            batch.put(self.keyspace.transfer_key(t.id), &encode(t)?);
        }
        // (Resolved markers are written in Task 3 once the key exists; the set is
        // always empty until then, so this loop is a harmless no-op for now.)
        let _ = &state.resolved;
        if !batch.is_empty() {
            writer.write(batch).await?;
        }

        Ok(results)
    }
```

Note: `dirty` is no longer derived from the transfer's account-id fields in the loop — the stage functions now populate `state.dirty` themselves (Step 3).

- [ ] **Step 3: Update `stage_transfer` and `load_account` to take `&mut ApplyState`.**

Change `stage_transfer`'s signature and its `working` accesses, and its accept step, so it owns the dirty-tracking. Replace the whole `stage_transfer` with:

```rust
    /// Validate one transfer and, if accepted, fold its balance deltas into
    /// `state` (working set + dirty ids) and return the transfer record (stamped
    /// with `timestamp`). `Exists` if the id is already committed; `Rejected` (no
    /// change) on validation failure. Errors bubble for I/O failures only.
    async fn stage_transfer(
        &self,
        t: &Transfer,
        timestamp: u64,
        state: &mut ApplyState,
    ) -> Result<StageOutcome> {
        if t.debit_account_id == t.credit_account_id {
            return Ok(StageOutcome::Rejected(LedgerError::AccountsMustDiffer));
        }
        // Posted transfers only: reject any flag whose semantics aren't
        // implemented yet (linked / two-phase / balancing) so a flagged transfer
        // is never silently persisted as a plain posted one.
        if t.flags != TransferFlags::NONE {
            return Ok(StageOutcome::Rejected(LedgerError::UnsupportedFlag));
        }
        // Idempotency: committed transfer with this id already exists.
        if get_transfer(&self.substrate, &self.keyspace, t.id).await?.is_some() {
            return Ok(StageOutcome::Exists);
        }

        let mut debit = match self.load_account(t.debit_account_id, state).await? {
            Some(a) => a,
            None => return Ok(StageOutcome::Rejected(LedgerError::AccountNotFound(t.debit_account_id))),
        };
        let mut credit = match self.load_account(t.credit_account_id, state).await? {
            Some(a) => a,
            None => return Ok(StageOutcome::Rejected(LedgerError::AccountNotFound(t.credit_account_id))),
        };

        if t.ledger != debit.ledger || t.ledger != credit.ledger {
            return Ok(StageOutcome::Rejected(LedgerError::LedgerMismatch));
        }

        debit.debits_posted = match debit.debits_posted.checked_add(t.amount) {
            Some(v) => v,
            None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
        };
        credit.credits_posted = match credit.credits_posted.checked_add(t.amount) {
            Some(v) => v,
            None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
        };

        if debit.flags.contains(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS) {
            let debits_used = match debit.debits_posted.checked_add(debit.debits_pending) {
                Some(v) => v,
                None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
            };
            if debits_used > debit.credits_posted {
                return Ok(StageOutcome::Rejected(LedgerError::ExceedsCredits));
            }
        }
        if credit.flags.contains(AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS) {
            let credits_used = match credit.credits_posted.checked_add(credit.credits_pending) {
                Some(v) => v,
                None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
            };
            if credits_used > credit.debits_posted {
                return Ok(StageOutcome::Rejected(LedgerError::ExceedsDebits));
            }
        }

        state.working.insert(debit.id, debit);
        state.dirty.insert(debit.id);
        state.working.insert(credit.id, credit);
        state.dirty.insert(credit.id);
        Ok(StageOutcome::Applied(Transfer { timestamp, ..*t }))
    }
```

And replace `load_account` with the `ApplyState` version:

```rust
    /// Get an account from the working set, loading it read-through on first
    /// touch. Returns a copy; mutations are written back by the caller on accept.
    async fn load_account(
        &self,
        id: u128,
        state: &mut ApplyState,
    ) -> Result<Option<Account>> {
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
```

- [ ] **Step 4: Run the full crate test suite (no behavior change).**

Run: `cargo test -p bluedb-ledger`
Expected: all Plan-1 tests still pass (20 tests). Then `cargo build -p bluedb-ledger` clean (the `let _ = &state.resolved;` keeps `resolved` from being an unused-field warning for now).

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-ledger/src/ledger.rs
git commit -m "bluedb-ledger: thread apply state through an ApplyState struct (no behavior change)"
```

---

## Task 2: Transfer classification + new error variants

**Files:**
- Modify: `crates/bluedb-ledger/src/model.rs`

- [ ] **Step 1: Write the failing classification tests.**

Add to the `tests` module in `model.rs`:

```rust
    #[test]
    fn transfer_kind_classifies_and_validates() {
        // Plain posted.
        assert_eq!(Transfer::new(1, 1, 2, 10, 7).kind(), Ok(TransferKind::Posted));

        // Pending reserve.
        let mut p = Transfer::new(1, 1, 2, 10, 7);
        p.flags = TransferFlags::PENDING;
        assert_eq!(p.kind(), Ok(TransferKind::Pending));

        // Post / void need a pending_id.
        let mut post = Transfer::new(1, 0, 0, 10, 7);
        post.flags = TransferFlags::POST_PENDING_TRANSFER;
        assert_eq!(post.kind(), Err(LedgerError::InvalidTransferFlags)); // pending_id == 0
        post.pending_id = 99;
        assert_eq!(post.kind(), Ok(TransferKind::PostPending));

        let mut void = Transfer::new(1, 0, 0, 0, 7);
        void.flags = TransferFlags::VOID_PENDING_TRANSFER;
        void.pending_id = 99;
        assert_eq!(void.kind(), Ok(TransferKind::VoidPending));

        // Mutually exclusive / contradictory flag combos.
        let mut both = Transfer::new(1, 0, 0, 0, 7);
        both.flags = TransferFlags(TransferFlags::POST_PENDING_TRANSFER.0 | TransferFlags::VOID_PENDING_TRANSFER.0);
        both.pending_id = 1;
        assert_eq!(both.kind(), Err(LedgerError::InvalidTransferFlags));

        let mut pend_and_post = Transfer::new(1, 1, 2, 10, 7);
        pend_and_post.flags = TransferFlags(TransferFlags::PENDING.0 | TransferFlags::POST_PENDING_TRANSFER.0);
        pend_and_post.pending_id = 1;
        assert_eq!(pend_and_post.kind(), Err(LedgerError::InvalidTransferFlags));

        // A pending reserve must NOT carry a pending_id; a posted must not either.
        let mut pend_with_id = Transfer::new(1, 1, 2, 10, 7);
        pend_with_id.flags = TransferFlags::PENDING;
        pend_with_id.pending_id = 5;
        assert_eq!(pend_with_id.kind(), Err(LedgerError::InvalidTransferFlags));

        let mut posted_with_id = Transfer::new(1, 1, 2, 10, 7);
        posted_with_id.pending_id = 5;
        assert_eq!(posted_with_id.kind(), Err(LedgerError::InvalidTransferFlags));

        // Not-yet-supported flags (later plans) → UnsupportedFlag.
        let mut linked = Transfer::new(1, 1, 2, 10, 7);
        linked.flags = TransferFlags::LINKED;
        assert_eq!(linked.kind(), Err(LedgerError::UnsupportedFlag));

        let mut balancing = Transfer::new(1, 1, 2, 10, 7);
        balancing.flags = TransferFlags::BALANCING_DEBIT;
        assert_eq!(balancing.kind(), Err(LedgerError::UnsupportedFlag));

        let mut with_timeout = Transfer::new(1, 1, 2, 10, 7);
        with_timeout.flags = TransferFlags::PENDING;
        with_timeout.timeout = 30;
        assert_eq!(with_timeout.kind(), Err(LedgerError::UnsupportedFlag));
    }
```

Run `cargo test -p bluedb-ledger transfer_kind_classifies` → expect FAIL to compile (`TransferKind`, `kind`, `InvalidTransferFlags` don't exist).

- [ ] **Step 2: Add the new `LedgerError` variants.**

In `model.rs`, change the `LedgerError` enum so it ends:

```rust
    #[error("balance arithmetic overflowed u128")]
    Overflow,
    #[error("transfer flag is not supported yet (this engine implements posted + two-phase transfers only)")]
    UnsupportedFlag,
    #[error("contradictory or incomplete transfer flags")]
    InvalidTransferFlags,
    #[error("pending transfer {0:#x} not found (or not a pending transfer)")]
    PendingNotFound(u128),
    #[error("pending transfer {0:#x} was already posted or voided")]
    PendingAlreadyResolved(u128),
    #[error("post amount exceeds the pending transfer's reserved amount")]
    PostExceedsPending,
}
```

(Update the `UnsupportedFlag` message text as shown — it now covers two-phase too.)

- [ ] **Step 3: Add `TransferKind` and `Transfer::kind()`.**

In `model.rs`, after the `Transfer` impl block (after `Transfer::new`), add:

```rust
/// The four kinds of transfer this engine applies, derived from the flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransferKind {
    /// A regular posted movement (debits_posted / credits_posted).
    Posted,
    /// A two-phase reservation (debits_pending / credits_pending).
    Pending,
    /// Settles a pending transfer (referenced by `pending_id`), posting an
    /// amount (≤ the reserved amount; `0` posts the full reserved amount).
    PostPending,
    /// Releases a pending transfer (referenced by `pending_id`), posting nothing.
    VoidPending,
}

impl Transfer {
    /// Classify this transfer by its flags, validating flag combinations and the
    /// `pending_id` rules. Flags not implemented yet (linked, balancing, timeout)
    /// are rejected with [`LedgerError::UnsupportedFlag`].
    pub(crate) fn kind(&self) -> Result<TransferKind, LedgerError> {
        if self.flags.contains(TransferFlags::LINKED)
            || self.flags.contains(TransferFlags::BALANCING_DEBIT)
            || self.flags.contains(TransferFlags::BALANCING_CREDIT)
            || self.timeout != 0
        {
            return Err(LedgerError::UnsupportedFlag);
        }
        let post = self.flags.contains(TransferFlags::POST_PENDING_TRANSFER);
        let void = self.flags.contains(TransferFlags::VOID_PENDING_TRANSFER);
        let pending = self.flags.contains(TransferFlags::PENDING);
        match (post, void, pending) {
            // post + void, or a resolution combined with pending → contradictory.
            (true, true, _) | (true, _, true) | (_, true, true) => {
                Err(LedgerError::InvalidTransferFlags)
            }
            // A resolution references a pending and must carry a pending_id.
            (true, false, false) | (false, true, false) => {
                if self.pending_id == 0 {
                    Err(LedgerError::InvalidTransferFlags)
                } else if post {
                    Ok(TransferKind::PostPending)
                } else {
                    Ok(TransferKind::VoidPending)
                }
            }
            // A reserve or a plain posted transfer must NOT carry a pending_id.
            (false, false, true) => {
                if self.pending_id != 0 {
                    Err(LedgerError::InvalidTransferFlags)
                } else {
                    Ok(TransferKind::Pending)
                }
            }
            (false, false, false) => {
                if self.pending_id != 0 {
                    Err(LedgerError::InvalidTransferFlags)
                } else {
                    Ok(TransferKind::Posted)
                }
            }
        }
    }
}
```

- [ ] **Step 4: Run the tests.**

Run: `cargo test -p bluedb-ledger transfer_kind_classifies` → PASS. Then `cargo test -p bluedb-ledger` → all pass (21 now). `TransferKind` is `pub(crate)` and unused outside tests until Task 4 — if `cargo build -p bluedb-ledger` warns it's unused, that's expected this task; do NOT add `#[allow]` (Task 4 consumes it). The new `LedgerError` variants are likewise consumed in Tasks 4–6.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-ledger/src/model.rs
git commit -m "bluedb-ledger: TransferKind classifier + two-phase error variants"
```

---

## Task 3: Resolved-marker keyspace + store read

**Files:**
- Modify: `crates/bluedb-ledger/src/keyspace.rs`
- Modify: `crates/bluedb-ledger/src/store.rs`
- Modify: `crates/bluedb-ledger/src/ledger.rs`

- [ ] **Step 1: Write the failing keyspace test.**

Add to the `tests` module in `keyspace.rs`:

```rust
    #[test]
    fn resolved_marker_key_is_distinct_namespace() {
        let ks = LedgerKeyspace::new("_");
        let r = ks.pending_resolved_key(5);
        // Distinct from account (0x10) and transfer (0x11) for the same id, and
        // sorts after both (tag 0x12).
        assert_ne!(r, ks.account_key(5));
        assert_ne!(r, ks.transfer_key(5));
        assert!(ks.transfer_key(5) < r);
        // Ordered by id within the namespace.
        assert!(ks.pending_resolved_key(1) < ks.pending_resolved_key(2));
    }
```

Run `cargo test -p bluedb-ledger resolved_marker_key` → FAIL (`no method named pending_resolved_key`).

- [ ] **Step 2: Add the key helper.**

In `keyspace.rs`, add a tag constant after `const TAG_TRANSFER`:

```rust
/// Tag for the "pending transfer resolved" marker (present ⇒ posted/voided).
const TAG_PENDING_RESOLVED: u8 = TAG_EXTERNAL_BASE + 2; // 0x12
```

and a method inside `impl LedgerKeyspace`:

```rust
    pub(crate) fn pending_resolved_key(&self, pending_id: u128) -> Vec<u8> {
        self.ks.external_key(TAG_PENDING_RESOLVED, &pending_id.to_be_bytes())
    }
```

- [ ] **Step 3: Add the `is_resolved` store read.**

In `store.rs`, add (after `get_transfer`):

```rust
/// Whether a pending transfer has already been posted or voided (its resolved
/// marker is present in committed state).
pub(crate) async fn is_resolved(
    substrate: &Substrate,
    ks: &LedgerKeyspace,
    pending_id: u128,
) -> Result<bool> {
    Ok(substrate.get(&ks.pending_resolved_key(pending_id)).await?.is_some())
}
```

- [ ] **Step 4: Write the resolved markers in the batch.**

In `ledger.rs` `create_transfers`, replace the placeholder lines:

```rust
        // (Resolved markers are written in Task 3 once the key exists; the set is
        // always empty until then, so this loop is a harmless no-op for now.)
        let _ = &state.resolved;
```

with:

```rust
        for pending_id in &state.resolved {
            // 1-byte value (SlateDB values must be non-empty); presence is the signal.
            batch.put(self.keyspace.pending_resolved_key(*pending_id), &[1u8]);
        }
```

- [ ] **Step 5: Run tests.**

Run: `cargo test -p bluedb-ledger resolved_marker_key` → PASS. Then `cargo test -p bluedb-ledger` → all pass (21). `is_resolved` is unused until Task 5 — expected dead-code warning this task; do NOT `#[allow]` it.

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-ledger/src/keyspace.rs crates/bluedb-ledger/src/store.rs crates/bluedb-ledger/src/ledger.rs
git commit -m "bluedb-ledger: resolved-marker keyspace + is_resolved read + batch write"
```

---

## Task 4: Kind dispatch + Pending (reserve)

Refactor `stage_transfer` to dispatch on `kind()`, extract the posted logic into a shared `stage_movement`, and add the `Pending` reserve path.

**Files:**
- Modify: `crates/bluedb-ledger/src/ledger.rs`

- [ ] **Step 1: Write the failing tests.**

Add to the `tests` module in `ledger.rs`:

```rust
    #[tokio::test]
    async fn pending_transfer_reserves_into_pending_buckets() {
        use crate::model::{CreateResult, Transfer, TransferFlags};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;

        let mut p = Transfer::new(500, 1, 2, 100, 7);
        p.flags = TransferFlags::PENDING;
        assert_eq!(ledger.create_transfers(&[p], 9).await.unwrap(), vec![CreateResult::Ok]);

        let debit = ledger.lookup_account(1).await.unwrap().unwrap();
        let credit = ledger.lookup_account(2).await.unwrap().unwrap();
        // Reserved in pending, nothing posted yet.
        assert_eq!(debit.debits_pending, 100);
        assert_eq!(debit.debits_posted, 0);
        assert_eq!(credit.credits_pending, 100);
        assert_eq!(credit.credits_posted, 0);
        // Pending conservation.
        assert_eq!(debit.debits_pending, credit.credits_pending);
        // The pending transfer is persisted and looked up by its id.
        assert!(ledger.lookup_transfer(500).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn pending_respects_balance_constraint() {
        use crate::model::{AccountFlags, CreateResult, LedgerError, NewAccount, Transfer, TransferFlags};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        ledger
            .create_accounts(
                &[
                    NewAccount::new(1, 7).with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS),
                    NewAccount::new(2, 7),
                ],
                1,
            )
            .await
            .unwrap();
        // A pending debit on a zero-credit constrained account breaches the
        // constraint just like a posted one (debits_posted + debits_pending).
        let mut p = Transfer::new(1, 1, 2, 50, 7);
        p.flags = TransferFlags::PENDING;
        assert_eq!(
            ledger.create_transfers(&[p], 2).await.unwrap(),
            vec![CreateResult::Failed(LedgerError::ExceedsCredits)]
        );
        assert_eq!(ledger.lookup_account(1).await.unwrap().unwrap().debits_pending, 0);
    }
```

Run `cargo test -p bluedb-ledger pending_transfer_reserves` → FAIL (the `PENDING` flag is currently rejected with `UnsupportedFlag`).

- [ ] **Step 2: Replace `stage_transfer` with a dispatcher, and add `stage_movement`.**

Replace the entire current `stage_transfer` method with these two methods:

```rust
    /// Validate one transfer, dispatch by kind, and (if accepted) fold its
    /// effects into `state`. `Exists` if the id is already committed; `Rejected`
    /// (no change) on validation failure. I/O failures bubble.
    async fn stage_transfer(
        &self,
        t: &Transfer,
        timestamp: u64,
        state: &mut ApplyState,
    ) -> Result<StageOutcome> {
        // Idempotency: committed transfer with this id already exists.
        if get_transfer(&self.substrate, &self.keyspace, t.id).await?.is_some() {
            return Ok(StageOutcome::Exists);
        }
        match t.kind() {
            Err(e) => Ok(StageOutcome::Rejected(e)),
            Ok(TransferKind::Posted) => self.stage_movement(t, timestamp, state, false).await,
            Ok(TransferKind::Pending) => self.stage_movement(t, timestamp, state, true).await,
            Ok(TransferKind::PostPending) => self.stage_resolution(t, timestamp, state, true).await,
            Ok(TransferKind::VoidPending) => self.stage_resolution(t, timestamp, state, false).await,
        }
    }

    /// Stage a posted (`is_pending == false`) or pending (`true`) movement: move
    /// `amount` from the debit account to the credit account, into the posted or
    /// pending buckets respectively, enforcing single-ledger + distinct accounts
    /// + balance constraints. Balance constraints use `posted + pending` on the
    /// constrained side, so a pending reserve is constrained exactly like a post.
    async fn stage_movement(
        &self,
        t: &Transfer,
        timestamp: u64,
        state: &mut ApplyState,
        is_pending: bool,
    ) -> Result<StageOutcome> {
        if t.debit_account_id == t.credit_account_id {
            return Ok(StageOutcome::Rejected(LedgerError::AccountsMustDiffer));
        }
        let mut debit = match self.load_account(t.debit_account_id, state).await? {
            Some(a) => a,
            None => return Ok(StageOutcome::Rejected(LedgerError::AccountNotFound(t.debit_account_id))),
        };
        let mut credit = match self.load_account(t.credit_account_id, state).await? {
            Some(a) => a,
            None => return Ok(StageOutcome::Rejected(LedgerError::AccountNotFound(t.credit_account_id))),
        };
        if t.ledger != debit.ledger || t.ledger != credit.ledger {
            return Ok(StageOutcome::Rejected(LedgerError::LedgerMismatch));
        }

        if is_pending {
            debit.debits_pending = match debit.debits_pending.checked_add(t.amount) {
                Some(v) => v,
                None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
            };
            credit.credits_pending = match credit.credits_pending.checked_add(t.amount) {
                Some(v) => v,
                None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
            };
        } else {
            debit.debits_posted = match debit.debits_posted.checked_add(t.amount) {
                Some(v) => v,
                None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
            };
            credit.credits_posted = match credit.credits_posted.checked_add(t.amount) {
                Some(v) => v,
                None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
            };
        }

        if debit.flags.contains(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS) {
            let debits_used = match debit.debits_posted.checked_add(debit.debits_pending) {
                Some(v) => v,
                None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
            };
            if debits_used > debit.credits_posted {
                return Ok(StageOutcome::Rejected(LedgerError::ExceedsCredits));
            }
        }
        if credit.flags.contains(AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS) {
            let credits_used = match credit.credits_posted.checked_add(credit.credits_pending) {
                Some(v) => v,
                None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
            };
            if credits_used > credit.debits_posted {
                return Ok(StageOutcome::Rejected(LedgerError::ExceedsDebits));
            }
        }

        state.working.insert(debit.id, debit);
        state.dirty.insert(debit.id);
        state.working.insert(credit.id, credit);
        state.dirty.insert(credit.id);
        Ok(StageOutcome::Applied(Transfer { timestamp, ..*t }))
    }
```

- [ ] **Step 3: Add a temporary `stage_resolution` stub so it compiles.**

`stage_transfer` now references `stage_resolution`, implemented fully in Task 5. Add this stub for now (Task 5 replaces its body):

```rust
    /// Stage a post/void of a pending transfer. (Implemented in Task 5.)
    async fn stage_resolution(
        &self,
        _t: &Transfer,
        _timestamp: u64,
        _state: &mut ApplyState,
        _post: bool,
    ) -> Result<StageOutcome> {
        Ok(StageOutcome::Rejected(LedgerError::UnsupportedFlag))
    }
```

- [ ] **Step 4: Run tests.**

Run: `cargo test -p bluedb-ledger` → all pass (23: the 21 prior + 2 new). The existing posted-transfer tests must still pass (the posted path is now `stage_movement(.., false)` — identical behavior). `cargo build -p bluedb-ledger` clean (`stage_resolution` stub uses its params via `_`; `is_resolved` still unused until Task 5 — expected).

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-ledger/src/ledger.rs
git commit -m "bluedb-ledger: dispatch transfers by kind; add pending (two-phase reserve)"
```

---

## Task 5: PostPending (settle a reservation)

**Files:**
- Modify: `crates/bluedb-ledger/src/ledger.rs`

- [ ] **Step 1: Write the failing tests.**

Add to the `tests` module in `ledger.rs`:

```rust
    async fn reserve(ledger: &Ledger, id: u128, debit: u128, credit: u128, amount: u128, ts: u64) {
        use crate::model::{CreateResult, Transfer, TransferFlags};
        let mut p = Transfer::new(id, debit, credit, amount, 7);
        p.flags = TransferFlags::PENDING;
        assert_eq!(ledger.create_transfers(&[p], ts).await.unwrap(), vec![CreateResult::Ok]);
    }

    #[tokio::test]
    async fn post_pending_full_settles_reservation() {
        use crate::model::{CreateResult, Transfer, TransferFlags};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;
        reserve(&ledger, 500, 1, 2, 100, 1).await;

        // amount == 0 posts the FULL pending amount.
        let mut post = Transfer::new(501, 0, 0, 0, 7);
        post.flags = TransferFlags::POST_PENDING_TRANSFER;
        post.pending_id = 500;
        assert_eq!(ledger.create_transfers(&[post], 2).await.unwrap(), vec![CreateResult::Ok]);

        let debit = ledger.lookup_account(1).await.unwrap().unwrap();
        let credit = ledger.lookup_account(2).await.unwrap().unwrap();
        assert_eq!(debit.debits_pending, 0);
        assert_eq!(debit.debits_posted, 100);
        assert_eq!(credit.credits_pending, 0);
        assert_eq!(credit.credits_posted, 100);
    }

    #[tokio::test]
    async fn post_pending_partial_releases_remainder() {
        use crate::model::{CreateResult, Transfer, TransferFlags};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;
        reserve(&ledger, 500, 1, 2, 100, 1).await;

        // Post 60 of the 100 reserved; the remaining 40 is released, not posted.
        let mut post = Transfer::new(501, 0, 0, 60, 7);
        post.flags = TransferFlags::POST_PENDING_TRANSFER;
        post.pending_id = 500;
        assert_eq!(ledger.create_transfers(&[post], 2).await.unwrap(), vec![CreateResult::Ok]);

        let debit = ledger.lookup_account(1).await.unwrap().unwrap();
        let credit = ledger.lookup_account(2).await.unwrap().unwrap();
        assert_eq!(debit.debits_pending, 0);
        assert_eq!(debit.debits_posted, 60);
        assert_eq!(credit.credits_pending, 0);
        assert_eq!(credit.credits_posted, 60);
    }

    #[tokio::test]
    async fn post_pending_rejects_over_amount_and_unknown_pending() {
        use crate::model::{CreateResult, LedgerError, Transfer, TransferFlags};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;
        reserve(&ledger, 500, 1, 2, 100, 1).await;

        // Posting more than reserved is rejected; the reservation is untouched.
        let mut over = Transfer::new(501, 0, 0, 101, 7);
        over.flags = TransferFlags::POST_PENDING_TRANSFER;
        over.pending_id = 500;
        assert_eq!(
            ledger.create_transfers(&[over], 2).await.unwrap(),
            vec![CreateResult::Failed(LedgerError::PostExceedsPending)]
        );
        assert_eq!(ledger.lookup_account(1).await.unwrap().unwrap().debits_pending, 100);

        // Unknown pending id.
        let mut bad = Transfer::new(502, 0, 0, 0, 7);
        bad.flags = TransferFlags::POST_PENDING_TRANSFER;
        bad.pending_id = 999999;
        assert_eq!(
            ledger.create_transfers(&[bad], 3).await.unwrap(),
            vec![CreateResult::Failed(LedgerError::PendingNotFound(999999))]
        );

        // A pending_id that points at a NON-pending (posted) transfer.
        ledger.create_transfers(&[Transfer::new(600, 1, 2, 5, 7)], 4).await.unwrap();
        let mut wrong = Transfer::new(503, 0, 0, 0, 7);
        wrong.flags = TransferFlags::POST_PENDING_TRANSFER;
        wrong.pending_id = 600;
        assert_eq!(
            ledger.create_transfers(&[wrong], 5).await.unwrap(),
            vec![CreateResult::Failed(LedgerError::PendingNotFound(600))]
        );
    }
```

Run `cargo test -p bluedb-ledger post_pending_full` → FAIL (the stub rejects with `UnsupportedFlag`).

- [ ] **Step 2: Implement `stage_resolution` (replace the Task-4 stub).**

Replace the `stage_resolution` stub body with:

```rust
    /// Stage a post (`post == true`) or void (`false`) of the pending transfer
    /// referenced by `t.pending_id`. Releases the full reserved amount from the
    /// `*_pending` buckets; for a post, also moves the settled amount into
    /// `*_posted` (`t.amount == 0` posts the full reserved amount; a non-zero
    /// amount must be ≤ the reserved amount, and the remainder is released).
    /// Records a resolved marker so the pending can never be resolved twice.
    async fn stage_resolution(
        &self,
        t: &Transfer,
        timestamp: u64,
        state: &mut ApplyState,
        post: bool,
    ) -> Result<StageOutcome> {
        // Look up the referenced pending transfer.
        let pending = match get_transfer(&self.substrate, &self.keyspace, t.pending_id).await? {
            Some(p) if p.flags.contains(TransferFlags::PENDING) => p,
            _ => return Ok(StageOutcome::Rejected(LedgerError::PendingNotFound(t.pending_id))),
        };
        // Exactly-once: reject if already resolved (committed marker OR resolved
        // earlier in THIS batch).
        if state.resolved.contains(&t.pending_id)
            || is_resolved(&self.substrate, &self.keyspace, t.pending_id).await?
        {
            return Ok(StageOutcome::Rejected(LedgerError::PendingAlreadyResolved(t.pending_id)));
        }

        // Settled amount: 0 ⇒ post the full reserved amount; else must be ≤ it.
        let posted = if post {
            let amount = if t.amount == 0 { pending.amount } else { t.amount };
            if amount > pending.amount {
                return Ok(StageOutcome::Rejected(LedgerError::PostExceedsPending));
            }
            amount
        } else {
            0
        };

        // Load the PENDING transfer's accounts (not this transfer's fields).
        let mut debit = match self.load_account(pending.debit_account_id, state).await? {
            Some(a) => a,
            None => return Ok(StageOutcome::Rejected(LedgerError::AccountNotFound(pending.debit_account_id))),
        };
        let mut credit = match self.load_account(pending.credit_account_id, state).await? {
            Some(a) => a,
            None => return Ok(StageOutcome::Rejected(LedgerError::AccountNotFound(pending.credit_account_id))),
        };

        // Release the full reservation from the pending buckets (checked_sub
        // guards an invariant violation — the reservation must still be there).
        debit.debits_pending = match debit.debits_pending.checked_sub(pending.amount) {
            Some(v) => v,
            None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
        };
        credit.credits_pending = match credit.credits_pending.checked_sub(pending.amount) {
            Some(v) => v,
            None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
        };
        if post {
            debit.debits_posted = match debit.debits_posted.checked_add(posted) {
                Some(v) => v,
                None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
            };
            credit.credits_posted = match credit.credits_posted.checked_add(posted) {
                Some(v) => v,
                None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
            };
        }
        // No balance-constraint re-check: resolving a hold leaves
        // (posted + pending) non-increasing on each account, so it cannot newly
        // breach a constraint that held when the hold was placed.

        state.working.insert(debit.id, debit);
        state.dirty.insert(debit.id);
        state.working.insert(credit.id, credit);
        state.dirty.insert(credit.id);
        state.resolved.insert(t.pending_id);
        Ok(StageOutcome::Applied(Transfer { timestamp, ..*t }))
    }
```

- [ ] **Step 3: Run tests.**

Run: `cargo test -p bluedb-ledger` → all pass (27: 23 prior + 4 new). `is_resolved` is now used. `cargo build -p bluedb-ledger` clean.

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-ledger/src/ledger.rs
git commit -m "bluedb-ledger: post_pending (settle a two-phase reservation, full or partial)"
```

---

## Task 6: VoidPending + exactly-once guards

**Files:**
- Modify: `crates/bluedb-ledger/src/ledger.rs`

`void_pending` is already implemented by `stage_resolution(.., post=false)` from Task 5. This task adds the tests proving void + the exactly-once / double-resolution guards (including the in-batch case), so the behavior is locked in.

- [ ] **Step 1: Write the tests.**

Add to the `tests` module in `ledger.rs`:

```rust
    #[tokio::test]
    async fn void_pending_releases_reservation() {
        use crate::model::{CreateResult, Transfer, TransferFlags};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;
        reserve(&ledger, 500, 1, 2, 100, 1).await;

        let mut void = Transfer::new(501, 0, 0, 0, 7);
        void.flags = TransferFlags::VOID_PENDING_TRANSFER;
        void.pending_id = 500;
        assert_eq!(ledger.create_transfers(&[void], 2).await.unwrap(), vec![CreateResult::Ok]);

        let debit = ledger.lookup_account(1).await.unwrap().unwrap();
        let credit = ledger.lookup_account(2).await.unwrap().unwrap();
        // Reservation released; nothing posted.
        assert_eq!((debit.debits_pending, debit.debits_posted), (0, 0));
        assert_eq!((credit.credits_pending, credit.credits_posted), (0, 0));
    }

    #[tokio::test]
    async fn pending_cannot_be_resolved_twice() {
        use crate::model::{CreateResult, LedgerError, Transfer, TransferFlags};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;
        reserve(&ledger, 500, 1, 2, 100, 1).await;

        // Post it once.
        let mut post = Transfer::new(501, 0, 0, 0, 7);
        post.flags = TransferFlags::POST_PENDING_TRANSFER;
        post.pending_id = 500;
        assert_eq!(ledger.create_transfers(&[post], 2).await.unwrap(), vec![CreateResult::Ok]);

        // A later void of the same pending is rejected (already resolved).
        let mut void = Transfer::new(502, 0, 0, 0, 7);
        void.flags = TransferFlags::VOID_PENDING_TRANSFER;
        void.pending_id = 500;
        assert_eq!(
            ledger.create_transfers(&[void], 3).await.unwrap(),
            vec![CreateResult::Failed(LedgerError::PendingAlreadyResolved(500))]
        );
        // Balances unchanged by the rejected void.
        assert_eq!(ledger.lookup_account(1).await.unwrap().unwrap().debits_posted, 100);
    }

    #[tokio::test]
    async fn pending_cannot_be_resolved_twice_within_one_batch() {
        use crate::model::{CreateResult, LedgerError, Transfer, TransferFlags};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;
        reserve(&ledger, 500, 1, 2, 100, 1).await;

        // Two resolutions of the same pending in ONE batch: first wins, second
        // is rejected (in-batch resolved set), so the hold settles exactly once.
        let mut post = Transfer::new(501, 0, 0, 0, 7);
        post.flags = TransferFlags::POST_PENDING_TRANSFER;
        post.pending_id = 500;
        let mut void = Transfer::new(502, 0, 0, 0, 7);
        void.flags = TransferFlags::VOID_PENDING_TRANSFER;
        void.pending_id = 500;

        let res = ledger.create_transfers(&[post, void], 2).await.unwrap();
        assert_eq!(
            res,
            vec![CreateResult::Ok, CreateResult::Failed(LedgerError::PendingAlreadyResolved(500))]
        );
        let debit = ledger.lookup_account(1).await.unwrap().unwrap();
        assert_eq!(debit.debits_posted, 100); // posted once
        assert_eq!(debit.debits_pending, 0);
    }
```

Run `cargo test -p bluedb-ledger void_pending_releases` and the two `pending_cannot_be_resolved_twice*` → all should PASS immediately (Task 5 already implements the behavior). If any fails, STOP and report — it means the resolution/guard logic has a gap.

- [ ] **Step 2: Run the full suite.**

Run: `cargo test -p bluedb-ledger` → all pass (30: 27 prior + 3 new).

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-ledger/src/ledger.rs
git commit -m "bluedb-ledger: void_pending + exactly-once resolution tests (incl. in-batch)"
```

---

## Task 7: Two-phase conservation + lints + workspace green

**Files:**
- Modify: `crates/bluedb-ledger/src/ledger.rs`

- [ ] **Step 1: Write a two-phase lifecycle conservation test.**

Add to the `tests` module in `ledger.rs`:

```rust
    #[tokio::test]
    async fn two_phase_lifecycle_conserves() {
        use crate::model::{NewAccount, Transfer, TransferFlags};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        ledger
            .create_accounts(&[NewAccount::new(1, 7), NewAccount::new(2, 7), NewAccount::new(3, 7)], 1)
            .await
            .unwrap();

        // Reserve three holds, then post one fully, post one partially, void one.
        let mut p1 = Transfer::new(10, 1, 2, 100, 7); p1.flags = TransferFlags::PENDING;
        let mut p2 = Transfer::new(11, 2, 3, 50, 7);  p2.flags = TransferFlags::PENDING;
        let mut p3 = Transfer::new(12, 3, 1, 30, 7);  p3.flags = TransferFlags::PENDING;
        ledger.create_transfers(&[p1, p2, p3], 2).await.unwrap();

        let mut post_full = Transfer::new(20, 0, 0, 0, 7);   // post all 100 of hold 10
        post_full.flags = TransferFlags::POST_PENDING_TRANSFER; post_full.pending_id = 10;
        let mut post_part = Transfer::new(21, 0, 0, 20, 7);   // post 20 of hold 11 (30 released)
        post_part.flags = TransferFlags::POST_PENDING_TRANSFER; post_part.pending_id = 11;
        let mut void = Transfer::new(22, 0, 0, 0, 7);         // void hold 12
        void.flags = TransferFlags::VOID_PENDING_TRANSFER; void.pending_id = 12;
        ledger.create_transfers(&[post_full, post_part, void], 3).await.unwrap();

        // All holds resolved ⇒ no pending left anywhere.
        let mut dp = 0u128; let mut cp = 0u128; let mut dpost = 0u128; let mut cpost = 0u128;
        for id in 1..=3u128 {
            let a = ledger.lookup_account(id).await.unwrap().unwrap();
            dp += a.debits_pending; cp += a.credits_pending;
            dpost += a.debits_posted; cpost += a.credits_posted;
        }
        assert_eq!(dp, 0, "no debit reservations remain");
        assert_eq!(cp, 0, "no credit reservations remain");
        // Posted conservation: Σ debits_posted == Σ credits_posted == 100 + 20.
        assert_eq!(dpost, 120);
        assert_eq!(cpost, 120);
        assert_eq!(dpost, cpost);
    }
```

- [ ] **Step 2: Run the crate suite.**

Run: `cargo test -p bluedb-ledger` → all pass (31).

- [ ] **Step 3: Clippy (crate, then workspace) and fix any ledger-crate lint.**

Run: `cargo clippy -p bluedb-ledger --all-targets -- -D warnings` → clean. Then `cargo clippy --workspace --all-targets -- -D warnings` → clean. Fix only lints introduced by this crate (idiomatically, not with blanket `#[allow]`); if clippy flags a PRE-EXISTING issue in another crate, report it instead of fixing.

- [ ] **Step 4: Workspace test (no regressions).**

Run: `cargo test --workspace` → PASS (note any pre-existing/unrelated failure separately; the ledger + sql crates must pass).

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-ledger/src/ledger.rs
git commit -m "bluedb-ledger: two-phase lifecycle conservation test"
```

---

## Self-Review (completed during planning)

**Spec coverage (Plan 2 = two-phase portion of §5):**
- Pending reserve (`*_pending` buckets, constrained like a post) → Task 4.
- Post-pending settle, full (`amount==0`) + partial (remainder released) + over-amount reject → Task 5.
- Void-pending release → Task 5 (logic) + Task 6 (tests).
- Exactly-once resolution (committed marker + in-batch set), `PendingNotFound` for unknown / non-pending target → Tasks 3, 5, 6.
- Atomicity (one `WriteBatch` incl. resolved markers) + serialization (write lease) + idempotency (committed transfer id + in-batch staged) preserved from Plan 1 → Task 1.
- **Correctly deferred:** timeouts (`timeout != 0` → `UnsupportedFlag`), balancing (`BALANCING_*` → `UnsupportedFlag`), linked (`LINKED` → `UnsupportedFlag`), projection/HTTP/Jepsen. Asserted in the Task-2 classification tests.

**Placeholder scan:** No "TBD"/"handle later" — every code step is complete. Task 4 introduces a deliberate `stage_resolution` stub that Task 5 replaces; this is called out explicitly and the stub is valid compiling code (rejects with `UnsupportedFlag`), so the crate builds and tests pass at the Task-4 commit.

**Type/name consistency:** `ApplyState { working, dirty, resolved }` introduced in Task 1 is used by `stage_transfer`/`stage_movement`/`stage_resolution`/`load_account` (Tasks 1, 4, 5). `TransferKind` + `Transfer::kind()` (Task 2) drive the Task-4 dispatch. `LedgerError::{InvalidTransferFlags, PendingNotFound(u128), PendingAlreadyResolved(u128), PostExceedsPending}` defined in Task 2, used in Tasks 2/5/6. `LedgerKeyspace::pending_resolved_key` (Task 3) used by `is_resolved` (Task 3) and the resolved-marker batch write (Task 3). The `0` argument positions of `Transfer::new(id, debit, credit, amount, ledger)` are reused consistently in tests (post/void transfers pass `0` debit/credit since the pending's accounts are used).

---

## Next plans (after Plan 2 lands green)

- **Plan 3 — timeouts:** pending `timeout` (expiry index keyed by `expires_at`, a sweep at apply time that voids expired-unresolved holds, lazy reject of resolutions past expiry). Stop rejecting `timeout != 0`.
- **Plan 4 — balancing transfers:** `BALANCING_DEBIT`/`BALANCING_CREDIT` (transfer `min(amount, available-headroom-under-constraint)`).
- **Plan 5 — linked chains:** all-or-nothing chains (chain-buffer staging in `create_transfers`; generalizes the `ApplyState` fold to a tentative per-chain overlay).
- **Plan 6 — integration:** SQL projection (`ledger_accounts`/`ledger_transfers`), `/ledger/*` HTTP routes, Jepsen `ledger` workload.
