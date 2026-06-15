# bluedb-ledger — full TigerBeetle parity

- **Date:** 2026-06-14
- **Status:** Draft (supersedes the two-phase-only scope of `2026-06-14-bluedb-ledger-design.md` §5)
- **Directive:** "I need the behaviour to be exactly like TigerBeetle." → match TB's data-plane state machine **exactly**: every accept/reject decision, every balance movement, and the full named result-code set.
- **Reference:** TigerBeetle docs (fetched 2026-06-14): `/reference/account`, `/reference/transfer`, `/reference/requests/create_accounts`, `/reference/requests/create_transfers`. TB is the design; this doc pins what we implement.

## 0. What this changes vs. the current branch

The branch `feat/bluedb-ledger-two-phase` (Plan-1 merged + a simplified two-phase prototype) is a **foundation**, not the final shape. Reworked to be TB-exact:
- `Account`/`Transfer` structs gain the full TB field + flag surface (`reserved`, `closed`, `imported`, `history`, `closing_debit/credit`, etc.).
- `LedgerError` (7 coarse variants) is replaced by **two faithful result enums**: `CreateAccountResult` and `CreateTransferResult` (TB's exact codes).
- The `amount == 0 ⇒ post-full` shortcut is **wrong** and removed — TB's post-full sentinel is `AMOUNT_MAX = 2^128 - 1`.
- Post/void gain **field matching + inheritance** (zero fields inherit from the pending; non-zero must match).
- Timestamps become **engine-assigned** (monotonic, unique); input `timestamp` must be `0` unless `imported`.
- Full **input validation** (`id`/`ledger`/`code` not zero/int-max, flag mutual-exclusivity, …).
- `exists` becomes `exists` vs `exists_with_different_*` (field comparison).
- Carries over unchanged: the crate layout, `LedgerKeyspace`/external tags, `postcard` store, the single-writer + write-lease + atomic `WriteBatch` apply, `ApplyState`, the in-batch staged-id dedup.

## 1. Account model (exact)

```rust
struct Account {
    id: u128,                 // != 0, != 2^128-1, unique
    debits_pending: u128,
    debits_posted: u128,
    credits_pending: u128,
    credits_posted: u128,
    user_data_128: u128,
    user_data_64: u64,
    user_data_32: u32,
    reserved: u32,            // must be 0 on create
    ledger: u32,              // != 0
    code: u16,                // != 0
    flags: AccountFlags,
    timestamp: u64,           // engine-assigned (0 on input unless imported)
}
bitflags AccountFlags: u16 {
    LINKED,
    DEBITS_MUST_NOT_EXCEED_CREDITS,
    CREDITS_MUST_NOT_EXCEED_DEBITS,   // mutually exclusive with the above
    HISTORY,
    IMPORTED,
    CLOSED,                            // set by a closing transfer; rejects further transfers
}
```

## 2. Transfer model (exact)

```rust
struct Transfer {
    id: u128,                 // != 0, != 2^128-1, unique
    debit_account_id: u128,
    credit_account_id: u128,
    amount: u128,             // AMOUNT_MAX = 2^128-1 is the "full pending" sentinel for post
    pending_id: u128,         // != id; set iff post/void
    user_data_128: u128,
    user_data_64: u64,
    user_data_32: u32,
    timeout: u32,             // seconds; only on pending; 0 otherwise
    ledger: u32,              // != 0
    code: u16,                // != 0
    flags: TransferFlags,
    timestamp: u64,           // engine-assigned
}
bitflags TransferFlags: u16 {
    LINKED,
    PENDING,
    POST_PENDING_TRANSFER,
    VOID_PENDING_TRANSFER,
    BALANCING_DEBIT,
    BALANCING_CREDIT,
    CLOSING_DEBIT,            // closes the debit account (requires PENDING)
    CLOSING_CREDIT,           // closes the credit account (requires PENDING)
    IMPORTED,
}
```

**Flag mutual-exclusivity** (TB's matrix, enforced → `flags_are_mutually_exclusive`): `pending` / `post_pending_transfer` / `void_pending_transfer` are mutually exclusive; `balancing_debit`/`balancing_credit` cannot combine with post/void; `closing_debit`/`closing_credit` require `pending` and cannot combine with each other's account side rules; `void` cannot be balancing; etc. (Implement the exact matrix from `/reference/transfer`.)

## 3. Result codes (faithful enums)

### `CreateAccountResult`
`ok`, `linked_event_failed`, `linked_event_chain_open`, `imported_event_expected`, `imported_event_not_expected`, `timestamp_must_be_zero`, `imported_event_timestamp_out_of_range`, `imported_event_timestamp_must_not_advance`, `imported_event_timestamp_must_not_regress`, `reserved_field`, `reserved_flag`, `id_must_not_be_zero`, `id_must_not_be_int_max`, `exists_with_different_flags`, `exists_with_different_user_data_128`, `exists_with_different_user_data_64`, `exists_with_different_user_data_32`, `exists_with_different_ledger`, `exists_with_different_code`, `exists`, `flags_are_mutually_exclusive`, `debits_pending_must_be_zero`, `debits_posted_must_be_zero`, `credits_pending_must_be_zero`, `credits_posted_must_be_zero`, `ledger_must_not_be_zero`, `code_must_not_be_zero`, `imported_event_timestamp_must_not_regress` (accounts variant).

### `CreateTransferResult`
`ok`, `linked_event_failed`, `linked_event_chain_open`, `imported_event_expected`, `imported_event_not_expected`, `timestamp_must_be_zero`, `imported_event_timestamp_out_of_range`, `imported_event_timestamp_must_not_advance`, `imported_event_timestamp_must_not_regress`, `imported_event_timestamp_must_postdate_debit_account`, `imported_event_timestamp_must_postdate_credit_account`, `imported_event_timeout_must_be_zero`, `id_must_not_be_zero`, `id_must_not_be_int_max`, `exists_with_different_flags`, `exists_with_different_pending_id`, `exists_with_different_timeout`, `exists_with_different_debit_account_id`, `exists_with_different_credit_account_id`, `exists_with_different_amount`, `exists_with_different_user_data_128/64/32`, `exists_with_different_ledger`, `exists_with_different_code`, `exists`, `id_already_failed`, `flags_are_mutually_exclusive`, `debit_account_id_must_not_be_zero`, `debit_account_id_must_not_be_int_max`, `credit_account_id_must_not_be_zero`, `credit_account_id_must_not_be_int_max`, `accounts_must_be_different`, `pending_id_must_be_zero`, `pending_id_must_not_be_zero`, `pending_id_must_not_be_int_max`, `pending_id_must_be_different`, `timeout_reserved_for_pending_transfer`, `closing_transfer_must_be_pending`, `ledger_must_not_be_zero`, `code_must_not_be_zero`, `pending_transfer_not_found`, `pending_transfer_not_pending`, `pending_transfer_has_different_debit_account_id`, `pending_transfer_has_different_credit_account_id`, `pending_transfer_has_different_ledger`, `pending_transfer_has_different_code`, `pending_transfer_has_different_amount`, `exceeds_pending_transfer_amount`, `pending_transfer_already_posted`, `pending_transfer_already_voided`, `pending_transfer_expired`, `debit_account_not_found`, `credit_account_not_found`, `accounts_must_have_the_same_ledger`, `transfer_must_have_the_same_ledger_as_accounts`, `debit_account_already_closed`, `credit_account_already_closed`, `overflows_debits_pending`, `overflows_credits_pending`, `overflows_debits_posted`, `overflows_credits_posted`, `overflows_debits`, `overflows_credits`, `overflows_timeout`, `exceeds_credits`, `exceeds_debits`.

(`amount_must_not_be_zero` is deprecated/removed ≥ 0.16 — amounts of 0 are allowed; do not enforce.)

**Transient vs terminal (CORRECTED — verified against TB source `tigerbeetle.zig::transient()` + `state_machine.zig`):** a small set are *transient* (`debit/credit_account_not_found`, `pending_transfer_not_found`, `exceeds_credits/debits`, `debit/credit_account_already_closed`). TB **burns the `id` of a transient failure** — because the outcome depends on point-in-time state, TB locks the id to "failed" so a retry can never produce a different result; a later attempt with that id returns `id_already_failed`, and the logical operation must be resubmitted under a *new* id. **Terminal (deterministic) failures and `linked_event_failed` do NOT burn** — an identical retry deterministically re-fails the same way, and a corrected / unchained retry is allowed. (An earlier draft of this section had this backwards.) We replicate: record only transient-failed ids (tag 0x14) and return `id_already_failed` on reuse.

## 4. Validation + apply order (per transfer)

Mirror TB's order precisely (so the *first* failing check determines the code):
1. flag validity (`reserved_flag`, `flags_are_mutually_exclusive`, `closing_transfer_must_be_pending`, `timeout_reserved_for_pending_transfer`).
2. `id` (`id_must_not_be_zero`/`int_max`).
3. `imported`/`timestamp` rules (`timestamp_must_be_zero` unless imported; imported timestamp range/order).
4. account-id & pending-id field rules (zero/int-max; `accounts_must_be_different`; `pending_id_must_be_zero`/`not_be_zero`/`not_be_int_max`/`be_different`).
5. `ledger`/`code` not zero.
6. existence: if `id` already committed → `exists` or `exists_with_different_*` (compare every field).
7. id_already_failed check (transient-failure history).
8. resolve accounts / pending; field-match + inherit for post/void.
9. ledger agreement (`accounts_must_have_the_same_ledger`, `transfer_must_have_the_same_ledger_as_accounts`).
10. closed-account checks (`debit/credit_account_already_closed`).
11. overflow checks (the granular `overflows_*`).
12. balance constraints (`exceeds_credits`/`exceeds_debits`) — asymmetric (constrained side = posted+pending; limit = posted only). **Verified correct in Plan 1.**
13. apply deltas; for closing transfers set the account's `closed`; assign timestamp; record.

## 5. Two-phase (exact)

- **pending**: reserve into `*_pending`; if `timeout != 0`, store `expires_at = timestamp + timeout*1e9` and an expiry index entry; constraint checked (posted+pending ≤ limit).
- **post_pending**: `amount == AMOUNT_MAX` ⇒ post the full reserved amount; else `amount` must be `≤ pending.amount` (`exceeds_pending_transfer_amount`) and that much is posted, the remainder released. Inherit/zero-or-match `debit/credit/ledger/code/user_data` against the pending (`pending_transfer_has_different_*`). Reject if already posted/voided/expired (`pending_transfer_already_posted`/`already_voided`/`expired`).
- **void_pending**: release the full reservation; `amount` must be `0` or `== pending.amount` (`pending_transfer_has_different_amount`); same field-match + already-resolved + expired checks.
- **resolution state** must distinguish posted vs voided vs expired (so the right `already_*`/`expired` code is returned) → store a small **pending-state record** (status byte + expires_at) keyed by pending id, not a bare marker.
- **timeout expiry**: a pending whose `expires_at <= now` is auto-voided. Implemented as a sweep at apply time (release reservations of expired-unresolved pendings, mark expired) + lazy reject (`pending_transfer_expired`) on a resolution past expiry. Uses an expiry index for the sweep.

## 6. Linked chains (exact)

A maximal run of transfers/accounts where each has `flags.linked` set, terminated by one without it. All-or-nothing: if any member fails, the whole chain fails — the offender gets its real code, the others `linked_event_failed`. `flags.linked` set on the **last** event of the batch → `linked_event_chain_open`. Implemented as a tentative per-chain overlay folded into `ApplyState` only if every member succeeds.

## 7. Balancing transfers (exact)

`balancing_debit` / `balancing_credit`: the transfer moves `min(amount, available)` where `available` is the headroom on the constrained side (per the account's `*_must_not_exceed_*` flag). The persisted transfer records the actual (possibly reduced) amount. Combines with `pending`. Cannot combine with post/void.

## 8. Closing transfers + closed accounts (exact)

`closing_debit`/`closing_credit` (require `pending`): when the pending posts, set the named account's `flags.closed`. A transfer touching a closed account fails `debit/credit_account_already_closed`. (A closed account can be reopened only by voiding the closing pending — TB semantics.)

## 9. Imported events (exact)

`flags.imported`: the whole batch must be imported (else `imported_event_expected`/`not_expected`); `timestamp` must be user-supplied, in range, strictly monotonic vs the last assigned and the referenced accounts (`imported_event_timestamp_*`). For non-imported events the engine assigns the timestamp and input `timestamp` must be 0.

## 10. Timestamp model

The active writer holds a **monotonic timestamp source** (nanoseconds; `max(prev+1, wall_now_ns)`), re-seeded from the max persisted timestamp on promotion (like `SeqAllocator`). Each created object gets a unique, increasing `timestamp`. Input `timestamp` must be 0 (non-imported) — else `timestamp_must_be_zero`. This *replaces* the current "caller passes a `timestamp` param" API: `create_*` no longer take a timestamp argument; they assign internally. (Server/tests observe assigned timestamps via the returned/looked-up records.)

## 11. Persistence changes

- Account/Transfer native records (postcard) gain the new fields — `postcard` handles the struct growth; old records won't exist (pre-merge).
- Resolved marker (tag `0x12`) → **pending-state record** (status: posted/voided/expired + the posted amount + expires_at).
- New **expiry index** (tag `0x13`): `<expires_at::u64-be> <pending_id::u128-be>` for the timeout sweep.
- New **failed-id index** (tag `0x14`): `<transfer_id::u128-be>` → for `id_already_failed`. (Only ids that failed with a *transient* error are recorded.)
- `closed` is a flag on the account record (no separate keyspace).

## 12. Phasing (each phase = its own plan, lands green)

The current simplified two-phase branch is reworked phase by phase. Suggested order (foundational first):

- **Phase A — model + result codes + validation + timestamps.** Rewrite `Account`/`Transfer` to the full surface; replace `LedgerError` with `CreateAccountResult`/`CreateTransferResult`; full input validation + flag matrix + `exists_with_different_*`; engine-assigned monotonic timestamps; `create_accounts` to TB-exact. Regular transfers only (no two-phase yet — rejected by flag matrix or simply the pending paths return a "not-yet" until Phase B; better: keep Phase A regular-only and gate the rest). Establishes the spine everything else hangs on.
- **Phase B — two-phase exact.** pending/post/void with `AMOUNT_MAX`, field-match+inherit, distinct `already_posted`/`already_voided`, `exceeds_pending_transfer_amount`, pending-state record.
- **Phase C — timeouts.** expiry index + sweep + `pending_transfer_expired`.
- **Phase D — linked chains.** all-or-nothing overlay; `linked_event_failed`/`chain_open`.
- **Phase E — balancing transfers.**
- **Phase F — closing transfers + closed accounts.**
- **Phase G — imported events** (user timestamps) + `id_already_failed` terminal-failure tracking.
- **Phase H — integration:** SQL projection, `/ledger/*` HTTP (batched create_accounts/create_transfers returning per-item result codes), Jepsen `ledger` workload (conservation + no-lost + no-double-apply + result-code correctness across faults).

Each phase keeps `cargo test --workspace` + clippy green and is reviewed (spec-compliance + code-quality) before the next.

## 13. Non-goals / explicit deltas from TB

- We do **not** reimplement TB's consensus (VSR), storage engine (LSM/zig), or its wire protocol — bluedb's single-writer + lease + SlateDB provides equivalents. We match the **data-plane semantics + result codes** only.
- Throughput is not a goal (object-storage-bound), per the original spec.
- 128-bit `id` reserved values, AMOUNT_MAX, flag matrix, and result codes follow TB ≥ 0.16 semantics (amount 0 allowed; `amount_must_not_be_zero` not enforced).
