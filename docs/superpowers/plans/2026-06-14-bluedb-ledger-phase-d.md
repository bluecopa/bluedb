# bluedb-ledger Phase D — linked chains (all-or-nothing)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Implement TigerBeetle's `flags.linked` for both accounts and transfers: a maximal run of `linked` events terminated by a non-linked event is committed all-or-nothing; if any member fails, the offender gets its real code and every other member gets `linked_event_failed`; an open chain (the last event in the batch has `linked` set) makes that last event `linked_event_chain_open` and the rest `linked_event_failed`.

**Architecture:** Both `create_accounts` and `create_transfers` are restructured to process the batch chain-by-chain. A shared pure helper `chains(&[bool]) -> Vec<Chain>` groups the batch by the `linked` flags. An independent (single, non-linked) event is processed directly (fast path). A linked chain (length > 1 or open) is processed against a **snapshot** of the apply accumulators; if every member succeeds the snapshot is kept (committed), otherwise it is restored and the chain's results are assigned (`linked_event_failed` to non-offenders, the real code to the first offender, `linked_event_chain_open` to the open-chain terminator).

**Reference:** spec §6; TB `coding/linked-events` + create_accounts/create_transfers codes 2 (`linked_event_failed`) / 3 (`linked_event_chain_open`).

---

## Decisions
- **Chain success predicate:** a member is a *success* (does not fail the chain) iff its result is `Created` **or** `Exists`. `ExistsWithDifferent*` and every error code are failures. Rationale: this preserves idempotent retry of an identical committed chain (every member → `Exists` → chain commits) while treating a genuine divergence/error as a rollback. (Undocumented in TB; flagged for the Phase H real-TB comparison.)
- **Open chain:** the last event of the *batch* with `linked` set has no terminator → `linked_event_chain_open` for that last event, `linked_event_failed` for the rest of its chain; nothing is applied.
- **First-offender rule:** processing stops at the first failing member; that member keeps its real code, all other members of the chain (before and after) get `linked_event_failed`.
- Linking composes with everything already built (regular/pending/post/void/timeout). The sweep still runs once at the start of `create_transfers` (it is not part of any chain).

## Chain grouping (shared helper, `model.rs`)
```rust
/// A contiguous run of batch events forming one linked chain (or one independent
/// event). `start..=end` inclusive; `open` is true iff the last event in the run
/// still has `linked` set (no terminator — i.e. the batch ended mid-chain).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Chain { pub start: usize, pub end: usize, pub open: bool }

/// Group a batch into chains from each event's `linked` flag. A maximal run of
/// `linked==true` events plus the following terminator (`linked==false`) is one
/// chain; a lone `linked==false` event is an independent chain; a trailing run
/// of `linked==true` with no terminator is an `open` chain.
pub(crate) fn chains(linked: &[bool]) -> Vec<Chain> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < linked.len() {
        let mut j = i;
        while j < linked.len() && linked[j] { j += 1; }
        // j is either the terminator index (linked[j]==false) or == len (open).
        if j < linked.len() {
            out.push(Chain { start: i, end: j, open: false });
            i = j + 1;
        } else {
            out.push(Chain { start: i, end: j - 1, open: true });
            i = j;
        }
    }
    out
}
```
Unit-test: `[F]` → one independent; `[T,F]` → one closed chain 0..=1; `[T,T,F,F]` → [0..=2 closed],[3..=3 closed]; `[F,T,T]` → [0],[1..=2 open]; `[T]` → [0..=0 open]; `[]` → [].

> `linked` is read from the flags via `AccountFlags::LINKED` / `TransferFlags::LINKED`. `LINKED` is no longer part of `account_is_gated` / the transfer `Gated` classification (it is handled here).

## Task 1: `chains` helper + un-gate LINKED (model.rs)
- [ ] Add `Chain` + `chains` + unit tests.
- [ ] `account_is_gated`: drop the `LINKED` term (keep `IMPORTED`).
- [ ] `classify`: drop `LINKED` from the gated set. **Important:** `classify` is called per-member during chain processing; a `LINKED` regular transfer must classify as `Regular` (and `LINKED | PENDING` as `PendingReserve`, etc.). Since `LINKED` is orthogonal to the op, strip it before classifying: check the *other* flags only. Update `classify` to ignore `LINKED` when determining the op.

## Task 2: restructure `create_accounts` for chains (ledger.rs)
- [ ] Accumulate `accepted: Vec<Account>` instead of writing into `batch` inline; build the batch at the end (mirrors `create_transfers`). Keep `staged: HashMap<u128, Account>` for in-batch existence.
- [ ] Extract per-account processing into a helper returning `CreateAccountResult` and mutating `(staged, accepted, ts)` — e.g. `fn stage_account(spec, now, &mut ts, &mut staged, &mut accepted) -> Result<CreateAccountResult>` (async; reads committed via `get_account`). Success = `Created | Exists`.
- [ ] Drive by `chains(&specs.iter().map(|a| a.flags.contains(LINKED)).collect())`:
  - independent (`start==end && !open`): process directly, push result.
  - chain/open: snapshot `(staged.clone(), accepted.len(), ts.last)`; if `open`, fail immediately; else process members until first failure; on any failure restore snapshot (`staged = snap; accepted.truncate(len); ts.last = snap_ts`) and assign results (`linked_event_failed` / real code / `linked_event_chain_open` for the open terminator); else push the members' results.
- [ ] Batch: put every `accepted` account; watermark if `!accepted.is_empty()`; `if !batch.is_empty()` write.

## Task 3: restructure `create_transfers` for chains (ledger.rs)
- [ ] Keep the start-of-batch `sweep_expired`.
- [ ] Snapshot type for a transfer chain: `(ApplyState clone, staged.clone(), accepted.len(), ts.last)`. (`ApplyState` derives `Clone`.)
- [ ] Extract per-transfer processing into a helper returning `CreateTransferResult` and mutating `(state, staged, accepted, ts)` — the current loop body (validate pre/post, existence, classify, stage, accept). Success = `Created | Exists`.
- [ ] Drive by `chains(&transfers.iter().map(|t| t.flags.contains(LINKED)).collect())` with the same snapshot/rollback structure as accounts.
- [ ] Batch assembly unchanged (dirty accounts, accepted transfers + expiry-index entries, pending-state records, expiry removals, watermark).

> Derive `Clone` on `ApplyState`.

## Task 4: tests (ledger.rs)
- [ ] **chains() unit tests** (model).
- [ ] **Accounts, all succeed**: `[A(linked), B]` both `Created`; both persisted.
- [ ] **Accounts, one fails**: `[A(linked), B(bad, e.g. ledger 0)]` → `[LinkedEventFailed, LedgerMustNotBeZero]`; NEITHER persisted (rollback).
- [ ] **Open chain**: `[A(linked)]` (last event linked) → `[LinkedEventChainOpen]`; not persisted. `[A(linked), B(linked)]` → `[LinkedEventFailed, LinkedEventChainOpen]`.
- [ ] **Transfers, all succeed**: a linked chain `[t1(linked), t2]` both `Created`, balances reflect both.
- [ ] **Transfers, one fails rolls back the whole chain**: `[t1(linked, ok), t2(bad: CreditAccountNotFound)]` → `[LinkedEventFailed, CreditAccountNotFound]`; t1's balance change is rolled back (debit unchanged), neither transfer persisted.
- [ ] **Failure mid-chain after a real mutation**: `[t1(linked, moves 1→2), t2(linked, moves 2→3 but 3 missing), t3]` → `[LinkedEventFailed, CreditAccountNotFound, LinkedEventFailed]`; account 1 & 2 unchanged.
- [ ] **Independent events around a chain**: `[indep_ok, t1(linked), t2(bad), indep_ok2]` → `[Created, LinkedEventFailed, <code>, Created]`; the two independents commit, the chain rolls back.
- [ ] **Linked + two-phase**: `[reserve(linked), post]` in one chain both `Created`, settled; if the post fails, the reserve rolls back.
- [ ] **Idempotent chain retry**: commit `[t1(linked), t2]`; resubmit identical → `[Exists, Exists]` (chain commits, no `linked_event_failed`).
- [ ] **LINKED no longer gated**: a lone `LINKED` transfer that is the last batch event → `LinkedEventChainOpen` (not `NotImplementedYet`).

Run `cargo test -p bluedb-ledger` + `cargo clippy --workspace --all-targets -- -D warnings`. Commit. Spec-compliance + code-quality review; fix; mark Phase D complete.
