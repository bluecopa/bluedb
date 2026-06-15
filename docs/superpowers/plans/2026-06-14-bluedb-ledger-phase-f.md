# bluedb-ledger Phase F — closing transfers + closed accounts

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Implement TigerBeetle `flags.closing_debit` / `flags.closing_credit` (require pending) and the `Account.flags.closed` state: the named account closes when the closing **pending is created**, a closed account rejects new movements with `debit_account_already_closed` (58) / `credit_account_already_closed` (59) (transient), voiding or expiring the closing pending **reopens** the account, and posting it makes the closure **permanent**.

**Architecture:** Closing requires pending (already validated in Phase A: `closing_transfer_must_be_pending`), so a closing transfer classifies as a pending reserve. After a closing reserve applies, set `closed` on the named account(s). New movements (`stage_regular`, `stage_pending`) gain a closed-account check (58/59) after ledger agreement; resolutions (post/void) are exempt (so the closing pending can be posted/voided and other outstanding pendings unwound). On a closing pending's void (`stage_resolution`) or expiry (`sweep_expired`), reopen the named account(s).

**Reference:** spec §8 (corrected: close on pending-create, not post); TB transfer `closing_debit`/`closing_credit` + account `flags.closed` (fetched 2026-06-14): a closed account "rejects further transfers, except for resolving two-phase transfers that are still pending"; `closed` is "unset by voiding the two-phase pending transfer that closed the account."

## Decisions / clarified semantics
- Closing **pending created** → named account(s) `closed = true` (after the reserve applies). Posting the closing pending keeps it closed (permanent). Voiding or expiring it → `closed = false` (reopen).
- Closed-account check (58/59) applies to NEW movements only: `stage_regular` and `stage_pending` (including a *non-closing* pending on an already-closed account). Resolutions (`stage_resolution` post/void) are exempt. Order: after ledger agreement (41/42), before overflow (60–65).
- For the closing pending itself: at creation the account is not yet closed, so the 58/59 check passes; the reserve applies; then it closes.
- `closing_debit` and `closing_credit` may both be set (close both accounts).
- An account may be created with `flags.closed` directly (TB allows it; `CLOSED` is already in the defined mask). It rejects movements; no special handling beyond storing the flag.
- Closing composes with `linked` (rollback restores `closed`, since `closed` lives in the cloned `ApplyState.working`).

## Task 1: un-gate closing in `classify` (model.rs)
- [ ] Remove `CLOSING_DEBIT`/`CLOSING_CREDIT` from the `Gated` set (leaving only `IMPORTED`). A closing transfer has `PENDING` set (validated), so it classifies as `PendingReserve`. Update `classify_ops` test: `PENDING | CLOSING_DEBIT → PendingReserve`.

## Task 2: closed-account check on new movements (ledger.rs)
- [ ] In `stage_regular`, after the ledger-agreement checks and before the overflow checks, add:
```rust
// 58 / 59: a closed account rejects new movements.
if debit.flags.contains(AccountFlags::CLOSED) { return Ok(Err(R::DebitAccountAlreadyClosed)); }
if credit.flags.contains(AccountFlags::CLOSED) { return Ok(Err(R::CreditAccountAlreadyClosed)); }
```
- [ ] Add the identical check to `stage_pending` (same position). (A closing pending's own accounts are not yet closed, so it passes; a non-closing pending on a closed account is rejected.)

## Task 3: close on closing-reserve; reopen on void/expire (ledger.rs)
- [ ] In `stage_pending`, after the reserve succeeds and before inserting `debit`/`credit` into `state.working`, set the closed flag(s):
```rust
if t.flags.contains(TransferFlags::CLOSING_DEBIT) { debit.flags = debit.flags | AccountFlags::CLOSED; }
if t.flags.contains(TransferFlags::CLOSING_CREDIT) { credit.flags = credit.flags | AccountFlags::CLOSED; }
```
- [ ] In `stage_resolution`, on a **void** (`!post`), after releasing and before recording, reopen the named account(s) per the PENDING's closing flags:
```rust
if !post {
    if pending.flags.contains(TransferFlags::CLOSING_DEBIT) { debit.flags = without_closed(debit.flags); }
    if pending.flags.contains(TransferFlags::CLOSING_CREDIT) { credit.flags = without_closed(credit.flags); }
}
```
(post keeps it closed — no change.) Add a small helper `fn without_closed(f: AccountFlags) -> AccountFlags { AccountFlags(f.0 & !AccountFlags::CLOSED.0) }` (or add `AccountFlags::without` in model.rs).
- [ ] In `sweep_expired`, when auto-voiding an expired pending, apply the same reopen logic for a closing pending (it expired → reopen).

> `AccountFlags` needs a clear-bit op. Add to model.rs: `pub fn without(self, other: Self) -> Self { Self(self.0 & !other.0) }` and use `debit.flags = debit.flags.without(AccountFlags::CLOSED)`.

## Task 4: tests (ledger.rs)
- [ ] **closing reserve closes the account**: `pending | closing_debit` 1→2 → account 1 `flags.closed` set; a subsequent regular 1→3 → `DebitAccountAlreadyClosed`; a regular 3→1 (crediting the closed account) → ... (closed rejects as debit OR credit? It rejects ALL movements; crediting a closed account → `CreditAccountAlreadyClosed`). Assert both.
- [ ] **closing_credit closes the credit account**: `pending | closing_credit` 1→2 → account 2 closed; a transfer crediting 2 → `CreditAccountAlreadyClosed`.
- [ ] **post keeps it closed (permanent)**: closing reserve (closes 1) then post → account 1 still `closed`; new transfer on 1 → `DebitAccountAlreadyClosed`.
- [ ] **void reopens**: closing reserve (closes 1) then void → account 1 `closed` cleared; a regular 1→2 now succeeds.
- [ ] **expiry reopens**: timed closing pending (closes 1); advance clock; sweep → account 1 reopened; a regular transfer succeeds afterward.
- [ ] **resolution exempt from closed-check**: closing reserve closes 1; a void/post of THAT pending still works (not rejected as closed). Also: a pending created on account 1 BEFORE it was closed can still be posted/voided after closing (set up: reserve p1 (1→2), then closing reserve closes 1, then post p1 → succeeds despite 1 being closed).
- [ ] **both closing flags**: `pending | closing_debit | closing_credit` closes both 1 and 2.
- [ ] **close-then-debit in one batch**: `[closing reserve (closes 1), regular 1→2]` → `[Created, DebitAccountAlreadyClosed]`.
- [ ] **closing in a linked chain rolls back**: a linked chain `[closing reserve, bad terminator]` fails → account 1 NOT closed (rollback).
- [ ] **closing no longer gated**: `pending | closing_debit` → `Created` (closes), not `NotImplementedYet`. Update `gated_flags_return_not_implemented_yet` to drop the closing case (keep imported).
- [ ] **account created closed** rejects movements: `Account::input(1,7).with_code(1).with_flags(CLOSED)` created; transfer 1→2 → `DebitAccountAlreadyClosed`.

Run `cargo test -p bluedb-ledger` + `cargo clippy --workspace --all-targets -- -D warnings`. Commit. Spec-compliance + code-quality review; fix; mark Phase F complete.
