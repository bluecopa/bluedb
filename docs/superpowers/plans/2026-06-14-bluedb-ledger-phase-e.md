# bluedb-ledger Phase E — balancing transfers

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Implement TigerBeetle `flags.balancing_debit` / `flags.balancing_credit`: a balancing transfer moves `min(amount, headroom)` on each balancing side, records the actual (possibly reduced) amount, applies regardless of the account's `*_must_not_exceed_*` flag, never fails with `exceeds_*` (reduces to 0 → `Created`), and composes with `pending` and `linked`.

**Architecture:** Un-gate the balancing flags (orthogonal to op, like `linked`). In `stage_regular` and `stage_pending`, after loading the debit/credit accounts, reduce the transfer amount by the balancing headroom(s); use the reduced amount for the movement, overflow, and (non-balancing-side) constraint checks; return the effective amount so the persisted/materialized transfer records it.

**Reference:** spec §7; TB transfer reference (balancing_debit/credit), fetched 2026-06-14.

## Formulas (saturating; computed against the loaded/working account state)
- `balancing_debit` headroom = `debit.credits_posted − (debit.debits_posted + debit.debits_pending)` (saturating at 0).
- `balancing_credit` headroom = `credit.debits_posted − (credit.credits_posted + credit.credits_pending)` (saturating at 0).
- effective amount = `min(t.amount, debit_headroom?, credit_headroom?)` — only the headroom(s) whose flag is set apply. Both set → min of both.
- The reduced amount is what is added to the buckets AND recorded on the transfer. A balancing transfer never returns `exceeds_credits`/`exceeds_debits` for a *balancing* side (it reduced to fit); a non-balancing constrained side is still hard-checked (so e.g. `balancing_debit` into a `credits_must_not_exceed_debits` credit account can still fail `exceeds_debits`).
- 0 headroom → effective 0 → `Created` with amount 0, no balance change (amount 0 is allowed ≥0.16).

## Decisions
- Balancing applies whether or not the account has the matching `*_must_not_exceed_*` flag (it is a per-transfer opt-in to the limit).
- Balancing composes with `pending` (reserve the reduced amount) and `linked`. It is already mutually exclusive with post/void (flag matrix).
- `balancing_debit | balancing_credit` together is allowed (min of both headrooms).

## Task 1: un-gate balancing in `classify` (model.rs)
- [ ] Remove `BALANCING_DEBIT`/`BALANCING_CREDIT` from the `Gated` set in `classify` (leaving `CLOSING_*`, `IMPORTED`). A balancing transfer with no phase flag → `Regular`; with `PENDING` → `PendingReserve`. Update the `classify_ops` test (balancing → Regular; balancing|pending → PendingReserve).

## Task 2: balancing reduction in the apply paths (ledger.rs)
- [ ] Add a helper:
```rust
/// The effective amount of a (possibly) balancing transfer against the current
/// debit/credit balances: reduced to the headroom on each balancing side.
fn balancing_amount(t: &Transfer, debit: &Account, credit: &Account) -> u128 {
    let mut amount = t.amount;
    if t.flags.contains(TransferFlags::BALANCING_DEBIT) {
        let headroom = debit.credits_posted.saturating_sub(debit.debits_posted.saturating_add(debit.debits_pending));
        amount = amount.min(headroom);
    }
    if t.flags.contains(TransferFlags::BALANCING_CREDIT) {
        let headroom = credit.debits_posted.saturating_sub(credit.credits_posted.saturating_add(credit.credits_pending));
        amount = amount.min(headroom);
    }
    amount
}
```
- [ ] Change `stage_regular` and `stage_pending` to return the **effective amount** on success: `type StageOutcome = std::result::Result<u128, CreateTransferResult>;` (repurpose the existing alias). In each, after loading `debit`/`credit`, compute `let amount = balancing_amount(t, &debit, &credit);` and use `amount` everywhere `t.amount` was used (overflow + constraint + bucket adds). Return `Ok(amount)`.
- [ ] In `process_transfer`, materialize the recorded transfer with the effective amount:
  - `TransferOp::Regular => self.stage_regular(t, state).await?.map(|amount| Transfer { amount, ..*t })`
  - `TransferOp::PendingReserve => { let p = ts.peek(now); self.stage_pending(t, p, state).await?.map(|amount| Transfer { amount, ..*t }) }`
  (For non-balancing transfers `amount == t.amount`, so this is a no-op.)

## Task 3: tests (ledger.rs)
- [ ] **balancing_debit reduces to headroom**: debit account 1 has `credits_posted = 50` (via a prior 2→1 of 50); a `balancing_debit` transfer 1→2 of 200 records/moves 50; account 1 `debits_posted == 50`; the stored transfer `amount == 50`.
- [ ] **balancing_debit to 0 headroom → Created, amount 0**: fresh accounts (credits_posted 0); `balancing_debit` 1→2 of 100 → `Created`, stored amount 0, no balance change.
- [ ] **balancing_credit reduces to headroom**: credit account 2 has `debits_posted = 30`; `balancing_credit` 1→2 of 100 records 30.
- [ ] **both flags → min of headrooms**: set up debit headroom 40, credit headroom 25; `balancing_debit|balancing_credit` of 100 → 25.
- [ ] **balancing + pending reserves the reduced amount**: `balancing_debit | pending` 1→2 of 100 with debit headroom 60 → `debits_pending == 60`; the post of it settles 60.
- [ ] **non-balancing side still hard-checked**: `balancing_debit` 1→2 where credit account 2 has `credits_must_not_exceed_debits` and 0 debits → `ExceedsDebits` (balancing_debit doesn't protect the credit side).
- [ ] **balancing in a linked chain**: `[balancing_debit(reduces), terminator]` both `Created`; rollback if the chain fails.
- [ ] **balancing no longer gated**: a lone `balancing_debit` transfer → `Created` (or reduced), not `NotImplementedYet`. Update `gated_flags_return_not_implemented_yet` to drop balancing (keep closing/imported).
- [ ] **conservation with a reduced balancing transfer**: Σdebits_posted == Σcredits_posted using the reduced amount.

Run `cargo test -p bluedb-ledger` + `cargo clippy --workspace --all-targets -- -D warnings`. Commit. Spec-compliance + code-quality review; fix; mark Phase E complete.
