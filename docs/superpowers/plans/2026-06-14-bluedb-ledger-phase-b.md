# bluedb-ledger Phase B — two-phase transfers (pending / post / void), TB-exact

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`).

**Goal:** Un-gate `pending` (reserve), `post_pending_transfer`, and `void_pending_transfer` and implement them exactly per TigerBeetle: `AMOUNT_MAX` post-full sentinel, partial post with remainder release, field inheritance/matching against the pending, distinct already-posted/already-voided, `exceeds_pending_transfer_amount`, and a persisted pending-state record.

**Architecture:** A transfer is classified after input validation into `Regular | PendingReserve | Post | Void | Gated`. `PendingReserve` (PENDING, `timeout == 0`, no other restricted flags) moves into `*_pending`. `Post`/`Void` look up the referenced pending (committed or staged-this-batch), inherit/validate its fields, release the reservation, and (post) move the effective amount into `*_posted`. A pending-state record (tag `0x12`) keyed by `pending_id` records `Posted`/`Voided` so a second resolution returns the right code. Timeouts (`timeout != 0` pendings, expiry, `pending_transfer_expired`) stay gated to Phase C; linked/balancing/closing/imported stay gated.

**Reference:** spec §5, §11; TB transfer reference (inheritance rules) + the create_transfers ordering 43–53 fetched 2026-06-14.

**Storage interpretation (documented decision):** the persisted post/void transfer is **materialized** — zero `debit/credit_account_id`/`ledger`/`code`/`user_data` are filled from the pending, and `amount` is stored as the effective posted/voided amount (so `lookup_transfer` and conservation are self-describing). To keep retries idempotent, `transfer_exists_result` is **inheritance-aware** for resolutions: a zero incoming inheritable field, or a post `amount == AMOUNT_MAX` / void `amount == 0`, matches any stored value (it was a wildcard); a nonzero incoming field must equal the stored value.

---

## TB create_transfers resolution sub-order (43–53), enforced in this order
43 pending_transfer_not_found (transient) · 44 pending_transfer_not_pending · 45 pending_transfer_has_different_debit_account_id · 46 …credit_account_id · 47 …ledger · 48 …code · 49 exceeds_pending_transfer_amount (post) · 50 pending_transfer_has_different_amount (void) · 51 pending_transfer_already_posted · 52 pending_transfer_already_voided · (53 pending_transfer_expired → Phase C). Then apply: release reservation, post effective, overflow 62/63 on the posted bucket.

## Field inheritance / matching (post & void)
- `debit_account_id`/`credit_account_id`: `0` ⇒ inherit pending's; nonzero ⇒ must equal pending's (else 45/46).
- `ledger`: `0` ⇒ inherit; nonzero ⇒ must equal pending's (else 47).
- `code`: `0` ⇒ inherit; nonzero ⇒ must equal pending's (else 48).
- `user_data_128/64/32`: `0` ⇒ inherit; nonzero ⇒ override (no match required, no code).
- `amount` (post): `AMOUNT_MAX` ⇒ full pending amount; else must be `<= pending.amount` (else 49); the posted amount goes to `*_posted`, the remainder `pending.amount - effective` is released (returns to available, i.e. simply not posted).
- `amount` (void): `0` ⇒ full pending amount; nonzero ⇒ must equal pending's (else 50); nothing posted.
- `timeout`: must be `0` (post/void are non-pending → code 35 already covers it).

## Validation changes (resolution-aware) — `model.rs::validate_transfer_post_existence`
For a resolution (post/void), the account-id zero checks (26/28), `accounts_must_be_different` (30), and `ledger`/`code` zero checks (37/38) are **skipped** (those fields inherit). `int_max` account ids on a resolution are not separately checked — a nonzero mismatch is caught by 45/46. `pending_id` rules switch to 32/33/34. Keep 25 (mutual-exclusive) and 35 (timeout) for all.

---

## Task 1: pending-state record + keyspace/store

**Files:** `model.rs` (PendingStatus), `keyspace.rs` (rename), `store.rs` (read)

- [ ] **Step 1: `PendingStatus` enum (model.rs)**
```rust
/// Resolution state of a pending transfer, recorded once it is posted or voided
/// (Phase C adds `Expired`). Persisted under the pending-state keyspace.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PendingStatus {
    Posted,
    Voided,
}
```

- [ ] **Step 2: keyspace — rename `pending_resolved_key` → `pending_state_key` (drop `#[allow(dead_code)]`)**, same tag `0x12`. Update the keyspace test name/uses.

- [ ] **Step 3: store — `get_pending_state`**
```rust
pub(crate) async fn get_pending_state(
    substrate: &Substrate, ks: &LedgerKeyspace, pending_id: u128,
) -> Result<Option<crate::model::PendingStatus>> {
    match substrate.get(&ks.pending_state_key(pending_id)).await? {
        Some(bytes) => Ok(Some(decode(&bytes)?)),
        None => Ok(None),
    }
}
```
Remove the old `is_resolved`. Add a `pending_state` round-trip test.

## Task 2: classification + resolution-aware validation (model.rs)

- [ ] **Step 1: `TransferOp` + `classify`**
```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransferOp { Regular, PendingReserve, Post, Void, Gated }

/// Classify a transfer that has already passed input validation.
pub(crate) fn classify(t: &Transfer) -> TransferOp {
    use TransferFlags as F;
    if t.flags.contains(F::LINKED)
        || t.flags.contains(F::BALANCING_DEBIT) || t.flags.contains(F::BALANCING_CREDIT)
        || t.flags.contains(F::CLOSING_DEBIT) || t.flags.contains(F::CLOSING_CREDIT)
        || t.flags.contains(F::IMPORTED)
    {
        return TransferOp::Gated;
    }
    if t.flags.contains(F::POST_PENDING_TRANSFER) { return TransferOp::Post; }
    if t.flags.contains(F::VOID_PENDING_TRANSFER) { return TransferOp::Void; }
    if t.flags.contains(F::PENDING) {
        if t.timeout != 0 { return TransferOp::Gated; } // Phase C
        return TransferOp::PendingReserve;
    }
    TransferOp::Regular
}
```
Remove `transfer_is_gated`.

- [ ] **Step 2: make `validate_transfer_post_existence` resolution-aware** (skip 26/28/30 and 37/38 for resolutions, per the rules above). Keep the existing non-resolution behaviour identical.

- [ ] **Step 3: make `transfer_exists_result` inheritance-aware for resolutions.** Add a branch: if `incoming.flags` indicates post/void, compare with wildcards for zero inheritable fields and the amount sentinel; else the existing strict comparison. (flags/pending_id/timeout always strict.)

- [ ] **Step 4: unit tests** for `classify` (each variant incl. PENDING+timeout→Gated, PENDING+CLOSING→Gated), resolution-aware validation (post with zero accounts/ledger/code passes input validation), and inheritance-aware exists (post AMOUNT_MAX retry → Exists; post with differing nonzero amount → ExistsWithDifferentAmount).

## Task 3: apply paths (ledger.rs)

- [ ] **Step 1: reinstate `ApplyState.resolved: HashMap<u128, PendingStatus>`** and write pending-state records in the batch.

- [ ] **Step 2: `stage_pending`** — like `stage_regular` but into `*_pending`, with overflow codes 60/61 (`OverflowsDebitsPending`/`OverflowsCreditsPending`), 64/65, and the asymmetric balance constraint 67/68 (posted+pending). Pending reserve constrained exactly like a post.

- [ ] **Step 2b: dispatch in `create_transfers`** by `classify`: Regular→stage_regular; PendingReserve→stage_pending; Post→stage_resolution(post=true); Void→stage_resolution(post=false); Gated→NotImplementedYet. Resolutions must find the pending in committed store **or** the staged-this-batch map (so reserve+resolve in one batch works); pass the staged map into the stage fn or look it up before calling.

- [ ] **Step 3: `stage_resolution(post: bool)`** implementing 43→44→45→46→47→48→(49|50)→51→52, then release + post-effective with overflow 62/63, then record the materialized transfer + pending-state. Use `checked_sub` for the reservation release and bubble an `anyhow` error on underflow (invariant violation — cannot happen). Return the **materialized** transfer (filled inherited fields + effective amount) so the caller persists it and stamps the timestamp.

  Signature: returns `Result<std::result::Result<Transfer, CreateTransferResult>>` (Ok(materialized) | Err(code)); the caller stamps `timestamp` and pushes to `accepted` + records the pending-state into `state.resolved`.

- [ ] **Step 4:** already-resolved check reads `state.resolved` first, then `get_pending_state`. Posted→51, Voided→52.

## Task 4: tests (ledger.rs) + un-gate

- [ ] Update `gated_flags_return_not_implemented_yet` to drop PENDING and the post case (now handled); keep LINKED/BALANCING/CLOSING/IMPORTED and add PENDING+timeout (→ NotImplementedYet, Phase C).
- [ ] **Pending reserve**: reserves into `*_pending`, conserves, respects balance constraint, overflow 60/61.
- [ ] **Post full** (AMOUNT_MAX): pending→posted, `*_pending` cleared, `*_posted` = amount.
- [ ] **Post partial** (amount < pending): posts amount, releases remainder (pending cleared, posted = amount).
- [ ] **Post exceeds** (amount > pending, not AMOUNT_MAX) → `ExceedsPendingTransferAmount`, reservation untouched.
- [ ] **Void full** (amount 0): releases reservation, nothing posted.
- [ ] **Void with matching amount** ok; **void with wrong nonzero amount** → `PendingTransferHasDifferentAmount`.
- [ ] **Inheritance**: post with zero debit/credit/ledger/code inherits; persisted transfer is materialized; lookup shows resolved values.
- [ ] **Field mismatch**: post with nonzero wrong debit_account_id → `PendingTransferHasDifferentDebitAccountId`; wrong ledger → `…DifferentLedger`; wrong code → `…DifferentCode`.
- [ ] **Not found / not pending**: post unknown pending_id → `PendingTransferNotFound`; post pointing at a regular transfer → `PendingTransferNotPending`.
- [ ] **Double resolve**: post then post same pending → `PendingTransferAlreadyPosted`; post then void → `PendingTransferAlreadyVoided` (and void-then-post symmetric).
- [ ] **Same-batch reserve+post**: `[pending, post]` in one batch both `Created`, balances settle.
- [ ] **Idempotent retry**: a committed post retried identically (incl. AMOUNT_MAX) → `Exists`.
- [ ] **Conservation** including a pending that is partially posted.

Run `cargo test -p bluedb-ledger` + `cargo clippy --workspace --all-targets -- -D warnings` green. Commit. Then spec-compliance + code-quality review subagents; fix; mark Phase B complete.
