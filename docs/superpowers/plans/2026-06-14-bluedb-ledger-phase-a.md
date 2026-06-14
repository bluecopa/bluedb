# bluedb-ledger Phase A — TB result codes + validation + engine timestamps

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the prototype's coarse `LedgerError`/`CreateResult` with TigerBeetle's exact per-item result codes, full input validation in TB's exact order, engine-assigned monotonic timestamps, TB-exact `create_accounts`, and TB-exact **regular posted** transfers — gating every not-yet-built feature behind a single transitional `NotImplementedYet` code.

**Architecture:** `Account` becomes both the create-input and the stored record (TB's model); `create_accounts(&[Account])` / `create_transfers(&[Transfer])` validate each item against TB's ordered check list, short-circuiting on the first failing code, and apply accepted items in one atomic `WriteBatch`. Timestamps are assigned by the engine from a per-tenant persistent watermark combined with a wall clock (`next = max(persisted_max + 1, now_ns)`); the input `timestamp` must be `0`. Two-phase, linked, balancing, closing, imported, and timeout behaviour are gated to later phases.

**Tech Stack:** Rust, `bluedb-sql` `Database` (substrate + write-lease), `slatedb::WriteBatch`, `postcard`, `serde`.

**Reference:** `docs/superpowers/specs/2026-06-14-bluedb-ledger-tigerbeetle-parity.md` §1–4, §10, §11. TB orderings pinned from `docs.tigerbeetle.com/reference/requests/create_accounts` and `…/create_transfers` (fetched 2026-06-14) — reproduced verbatim in Task 3/4 below.

---

## Constants & sentinels

- `ID_RESERVED_MAX: u128 = u128::MAX` (`2^128 - 1`) — reserved `id`/`pending_id` value.
- `AMOUNT_MAX: u128 = u128::MAX` — TB's "post full pending" sentinel (used in Phase B, defined now).
- Account flag bits: `LINKED=1<<0`, `DEBITS_MUST_NOT_EXCEED_CREDITS=1<<1`, `CREDITS_MUST_NOT_EXCEED_DEBITS=1<<2`, `HISTORY=1<<3`, `IMPORTED=1<<4`, `CLOSED=1<<5`. Defined mask = `0b111111`.
- Transfer flag bits: `LINKED=1<<0`, `PENDING=1<<1`, `POST_PENDING_TRANSFER=1<<2`, `VOID_PENDING_TRANSFER=1<<3`, `BALANCING_DEBIT=1<<4`, `BALANCING_CREDIT=1<<5`, `CLOSING_DEBIT=1<<6`, `CLOSING_CREDIT=1<<7`, `IMPORTED=1<<8`. Defined mask = `0b1_1111_1111`.
- Keyspace tags (reserved here, used per-phase): account `0x10`, transfer `0x11`, pending-state `0x12` (Phase B), expiry-index `0x13` (Phase C), terminal-failure `0x14` (Phase G), **timestamp watermark `0x15` (this phase)**.

## File structure

- `crates/bluedb-ledger/src/model.rs` — rework: flags, `Account` (now input+record), `Transfer`, the two result enums, pure validators. Remove `NewAccount`, `CreateResult`, `LedgerError`, `TransferKind`.
- `crates/bluedb-ledger/src/keyspace.rs` — add `watermark_key()` under tag `0x15`.
- `crates/bluedb-ledger/src/store.rs` — add `get_watermark` + a `now_ns()` wall clock; `encode`/`decode`/point reads unchanged.
- `crates/bluedb-ledger/src/ledger.rs` — rewrite `create_accounts`/`create_transfers` to the new contract; remove the two-phase/`stage_resolution` paths (return Phase B); keep `ApplyState`, `load_account`, lookups.
- `crates/bluedb-ledger/src/lib.rs` — update re-exports.

---

## Task 1: Flag surface + `Account` as input/record + `Transfer` fields

**Files:**
- Modify: `crates/bluedb-ledger/src/model.rs`

- [ ] **Step 1: Expand `AccountFlags` and `TransferFlags` with validity + mutual-exclusion helpers**

```rust
impl AccountFlags {
    pub const NONE: Self = Self(0);
    pub const LINKED: Self = Self(1 << 0);
    pub const DEBITS_MUST_NOT_EXCEED_CREDITS: Self = Self(1 << 1);
    pub const CREDITS_MUST_NOT_EXCEED_DEBITS: Self = Self(1 << 2);
    pub const HISTORY: Self = Self(1 << 3);
    pub const IMPORTED: Self = Self(1 << 4);
    pub const CLOSED: Self = Self(1 << 5);
    const DEFINED: u16 = 0b11_1111;

    pub fn contains(self, other: Self) -> bool { self.0 & other.0 == other.0 }
    /// Any bit outside the defined set → `reserved_flag`.
    pub fn has_reserved_bits(self) -> bool { self.0 & !Self::DEFINED != 0 }
    /// DEBITS_MUST_NOT_EXCEED_CREDITS and CREDITS_MUST_NOT_EXCEED_DEBITS cannot both be set.
    pub fn is_mutually_exclusive_violation(self) -> bool {
        self.contains(Self::DEBITS_MUST_NOT_EXCEED_CREDITS)
            && self.contains(Self::CREDITS_MUST_NOT_EXCEED_DEBITS)
    }
}
```

```rust
impl TransferFlags {
    pub const NONE: Self = Self(0);
    pub const LINKED: Self = Self(1 << 0);
    pub const PENDING: Self = Self(1 << 1);
    pub const POST_PENDING_TRANSFER: Self = Self(1 << 2);
    pub const VOID_PENDING_TRANSFER: Self = Self(1 << 3);
    pub const BALANCING_DEBIT: Self = Self(1 << 4);
    pub const BALANCING_CREDIT: Self = Self(1 << 5);
    pub const CLOSING_DEBIT: Self = Self(1 << 6);
    pub const CLOSING_CREDIT: Self = Self(1 << 7);
    pub const IMPORTED: Self = Self(1 << 8);
    const DEFINED: u16 = 0b1_1111_1111;

    pub fn contains(self, other: Self) -> bool { self.0 & other.0 == other.0 }
    pub fn has_reserved_bits(self) -> bool { self.0 & !Self::DEFINED != 0 }
    /// TB's transfer flag matrix (returns true if the combination is illegal):
    /// - at most one of {pending, post_pending, void_pending};
    /// - post/void cannot combine with balancing_debit/balancing_credit;
    /// - post/void cannot combine with closing_debit/closing_credit;
    /// - post/void cannot combine with pending.
    pub fn is_mutually_exclusive_violation(self) -> bool {
        let pending = self.contains(Self::PENDING);
        let post = self.contains(Self::POST_PENDING_TRANSFER);
        let void = self.contains(Self::VOID_PENDING_TRANSFER);
        let phase_count = [pending, post, void].iter().filter(|b| **b).count();
        if phase_count > 1 { return true; }
        let balancing = self.contains(Self::BALANCING_DEBIT) || self.contains(Self::BALANCING_CREDIT);
        let closing = self.contains(Self::CLOSING_DEBIT) || self.contains(Self::CLOSING_CREDIT);
        if (post || void) && (balancing || closing) { return true; }
        false
    }
    /// A closing transfer (closing_debit|closing_credit) must also be pending.
    pub fn is_closing(self) -> bool {
        self.contains(Self::CLOSING_DEBIT) || self.contains(Self::CLOSING_CREDIT)
    }
}
```

Keep the existing `BitOr` impls.

- [ ] **Step 2: `Account` gains `reserved`; becomes input + record; add builders; drop `NewAccount`**

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    pub id: u128,
    pub debits_pending: u128,
    pub debits_posted: u128,
    pub credits_pending: u128,
    pub credits_posted: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub reserved: u32,
    pub ledger: u32,
    pub code: u16,
    pub flags: AccountFlags,
    pub timestamp: u64,
}

impl Account {
    /// A create-input account: id + ledger, all balances/reserved/timestamp zero.
    pub fn input(id: u128, ledger: u32) -> Self {
        Self {
            id, debits_pending: 0, debits_posted: 0, credits_pending: 0, credits_posted: 0,
            user_data_128: 0, user_data_64: 0, user_data_32: 0, reserved: 0,
            ledger, code: 0, flags: AccountFlags::NONE, timestamp: 0,
        }
    }
    pub fn with_code(mut self, code: u16) -> Self { self.code = code; self }
    pub fn with_flags(mut self, flags: AccountFlags) -> Self { self.flags = flags; self }
    pub fn with_user_data_128(mut self, v: u128) -> Self { self.user_data_128 = v; self }
    pub fn with_user_data_64(mut self, v: u64) -> Self { self.user_data_64 = v; self }
    pub fn with_user_data_32(mut self, v: u32) -> Self { self.user_data_32 = v; self }
}
```

Delete `NewAccount` and its `into_account`. (Tests will use `Account::input(..)`. Note `Account::input` defaults `code` to 0 — tests that need a valid create must set `.with_code(1)` because `code_must_not_be_zero` is enforced. See Task 3.)

- [ ] **Step 3: `Transfer` — keep all fields (already complete); default `code` stays 0**

`Transfer` already has every TB field. Keep `Transfer::new(id, debit, credit, amount, ledger)` (defaults `code=0`, `flags=NONE`, `pending_id=0`, `timeout=0`, `timestamp=0`). Add builders mirroring `Account`:

```rust
impl Transfer {
    pub fn with_code(mut self, code: u16) -> Self { self.code = code; self }
    pub fn with_flags(mut self, flags: TransferFlags) -> Self { self.flags = flags; self }
    pub fn with_pending_id(mut self, id: u128) -> Self { self.pending_id = id; self }
}
```

Remove `TransferKind` and `Transfer::kind()` (replaced by validation + classification in Task 4).

- [ ] **Step 4: Unit tests for flag helpers**

```rust
#[test]
fn account_flag_helpers() {
    assert!(AccountFlags(0b100_0000).has_reserved_bits()); // bit 6 undefined
    assert!(!(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS | AccountFlags::HISTORY).has_reserved_bits());
    assert!((AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS | AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS).is_mutually_exclusive_violation());
    assert!(!AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS.is_mutually_exclusive_violation());
}

#[test]
fn transfer_flag_matrix() {
    use TransferFlags as F;
    assert!((F::PENDING | F::POST_PENDING_TRANSFER).is_mutually_exclusive_violation());
    assert!((F::POST_PENDING_TRANSFER | F::VOID_PENDING_TRANSFER).is_mutually_exclusive_violation());
    assert!((F::POST_PENDING_TRANSFER | F::BALANCING_DEBIT).is_mutually_exclusive_violation());
    assert!((F::VOID_PENDING_TRANSFER | F::CLOSING_DEBIT).is_mutually_exclusive_violation());
    assert!(!F::PENDING.is_mutually_exclusive_violation());
    assert!(!(F::PENDING | F::CLOSING_DEBIT).is_mutually_exclusive_violation()); // closing+pending ok
    assert!(F(1 << 12).has_reserved_bits());
}
```

- [ ] **Step 5: Run model unit tests, commit**

Run: `cargo test -p bluedb-ledger model::tests -- --nocapture` (will not fully build until Task 2 lands the enums and removes old references; if the crate doesn't compile yet because `ledger.rs` still references removed types, proceed to Task 2 and run the suite there). Commit at the end of Task 2 when the crate compiles green.

---

## Task 2: Result-code enums replace `LedgerError`/`CreateResult`

**Files:**
- Modify: `crates/bluedb-ledger/src/model.rs`
- Modify: `crates/bluedb-ledger/src/lib.rs`

- [ ] **Step 1: Define `CreateAccountResult` (TB order; Phase-A subset live, rest reserved)**

```rust
/// Per-item result of `create_accounts`. Variants and their relative order
/// mirror TigerBeetle's create_accounts result codes. `Created` is success;
/// `Exists`/`ExistsWith*` are idempotent (not failures). `NotImplementedYet`
/// is a transitional, non-TigerBeetle code for features gated to later phases
/// (linked, imported) — removed by Phase H.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateAccountResult {
    Created,
    // linked (Phase D) / imported (Phase G) → gated as NotImplementedYet for now
    TimestampMustBeZero,
    ReservedField,
    ReservedFlag,
    IdMustNotBeZero,
    IdMustNotBeIntMax,
    ExistsWithDifferentFlags,
    ExistsWithDifferentUserData128,
    ExistsWithDifferentUserData64,
    ExistsWithDifferentUserData32,
    ExistsWithDifferentLedger,
    ExistsWithDifferentCode,
    Exists,
    FlagsAreMutuallyExclusive,
    DebitsPendingMustBeZero,
    DebitsPostedMustBeZero,
    CreditsPendingMustBeZero,
    CreditsPostedMustBeZero,
    LedgerMustNotBeZero,
    CodeMustNotBeZero,
    NotImplementedYet,
}
```

- [ ] **Step 2: Define `CreateTransferResult` (full TB set; resolution/closed/imported variants reserved for later phases but declared now)**

Declare every variant from TB's create_transfers list so later phases don't reshape the enum. Phase A only *returns* the input-validation, account-resolution, overflow, and balance variants plus `Created`/`Exists*`/`NotImplementedYet`; the rest compile as dead variants until their phase (allow `dead_code` on the enum is unnecessary — enum variants don't warn when unconstructed, but add `#[allow(dead_code)]` if clippy complains).

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateTransferResult {
    Created,
    // linked (D) / imported (G) gated → NotImplementedYet
    TimestampMustBeZero,
    ReservedFlag,
    IdMustNotBeZero,
    IdMustNotBeIntMax,
    ExistsWithDifferentFlags,
    ExistsWithDifferentPendingId,
    ExistsWithDifferentTimeout,
    ExistsWithDifferentDebitAccountId,
    ExistsWithDifferentCreditAccountId,
    ExistsWithDifferentAmount,
    ExistsWithDifferentUserData128,
    ExistsWithDifferentUserData64,
    ExistsWithDifferentUserData32,
    ExistsWithDifferentLedger,
    ExistsWithDifferentCode,
    Exists,
    IdAlreadyFailed,                  // Phase G
    FlagsAreMutuallyExclusive,
    DebitAccountIdMustNotBeZero,
    DebitAccountIdMustNotBeIntMax,
    CreditAccountIdMustNotBeZero,
    CreditAccountIdMustNotBeIntMax,
    AccountsMustBeDifferent,
    PendingIdMustBeZero,
    PendingIdMustNotBeZero,           // Phase B
    PendingIdMustNotBeIntMax,         // Phase B
    PendingIdMustBeDifferent,         // Phase B
    TimeoutReservedForPendingTransfer,
    ClosingTransferMustBePending,
    LedgerMustNotBeZero,
    CodeMustNotBeZero,
    DebitAccountNotFound,             // transient
    CreditAccountNotFound,            // transient
    AccountsMustHaveTheSameLedger,
    TransferMustHaveTheSameLedgerAsAccounts,
    PendingTransferNotFound,          // B (transient)
    PendingTransferNotPending,        // B
    PendingTransferHasDifferentDebitAccountId,    // B
    PendingTransferHasDifferentCreditAccountId,   // B
    PendingTransferHasDifferentLedger,            // B
    PendingTransferHasDifferentCode,              // B
    ExceedsPendingTransferAmount,     // B
    PendingTransferHasDifferentAmount,// B
    PendingTransferAlreadyPosted,     // B
    PendingTransferAlreadyVoided,     // B
    PendingTransferExpired,           // C
    DebitAccountAlreadyClosed,        // F (transient)
    CreditAccountAlreadyClosed,       // F (transient)
    OverflowsDebitsPending,           // B
    OverflowsCreditsPending,          // B
    OverflowsDebitsPosted,
    OverflowsCreditsPosted,
    OverflowsDebits,
    OverflowsCredits,
    OverflowsTimeout,                 // C
    ExceedsCredits,                   // transient
    ExceedsDebits,                    // transient
    NotImplementedYet,
}
```

- [ ] **Step 3: Delete `LedgerError`, `CreateResult`; fix `lib.rs` exports**

`lib.rs`:
```rust
pub use ledger::Ledger;
pub use model::{Account, AccountFlags, CreateAccountResult, CreateTransferResult, Transfer, TransferFlags};
```

- [ ] **Step 4: Build (will fail in `ledger.rs` — expected)**

Run: `cargo build -p bluedb-ledger`
Expected: FAIL — `ledger.rs` references removed `NewAccount`/`CreateResult`/`LedgerError`/`TransferKind`. Fixed in Tasks 3–5. Do not commit yet.

---

## Task 3: `create_accounts` — TB-exact

**Files:**
- Modify: `crates/bluedb-ledger/src/keyspace.rs` (watermark key)
- Modify: `crates/bluedb-ledger/src/store.rs` (`get_watermark`, `now_ns`)
- Modify: `crates/bluedb-ledger/src/ledger.rs` (rewrite `create_accounts`, add timestamp source, drop ts param)

**TB create_accounts order (verbatim, fetched 2026-06-14):**
1 created · 2 linked_event_failed · 3 linked_event_chain_open · 4 imported_event_expected · 5 imported_event_not_expected · 6 timestamp_must_be_zero · 7 imported_event_timestamp_out_of_range · 8 imported_event_timestamp_must_not_advance · 9 reserved_field · 10 reserved_flag · 11 id_must_not_be_zero · 12 id_must_not_be_int_max · 13 exists_with_different_flags · 14 exists_with_different_user_data_128 · 15 …_64 · 16 …_32 · 17 exists_with_different_ledger · 18 exists_with_different_code · 19 exists · 20 flags_are_mutually_exclusive · 21 debits_pending_must_be_zero · 22 debits_posted_must_be_zero · 23 credits_pending_must_be_zero · 24 credits_posted_must_be_zero · 25 ledger_must_not_be_zero · 26 code_must_not_be_zero · 27 imported_event_timestamp_must_not_regress.

Phase A implements 6, 9–26 (linked/imported gated → `NotImplementedYet`).

- [ ] **Step 1: Watermark key in `keyspace.rs`**

```rust
/// Tag for the per-tenant monotonic timestamp watermark (single key).
const TAG_TS_WATERMARK: u8 = TAG_EXTERNAL_BASE + 5; // 0x15

impl LedgerKeyspace {
    pub(crate) fn watermark_key(&self) -> Vec<u8> {
        self.ks.external_key(TAG_TS_WATERMARK, b"ts")
    }
}
```
Add a test that it's distinct from account/transfer keys and constant.

- [ ] **Step 2: `store.rs` — read watermark + wall clock**

```rust
use std::time::{SystemTime, UNIX_EPOCH};

/// Read the persisted timestamp watermark (last assigned ts), 0 if absent.
pub(crate) async fn get_watermark(substrate: &Substrate, ks: &LedgerKeyspace) -> Result<u64> {
    match substrate.get(&ks.watermark_key()).await? {
        Some(bytes) => {
            let arr: [u8; 8] = bytes.as_ref().try_into().context("watermark must be 8 bytes")?;
            Ok(u64::from_be_bytes(arr))
        }
        None => Ok(0),
    }
}

/// Wall clock in nanoseconds since the Unix epoch (engine timestamp source).
pub(crate) fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}
```

- [ ] **Step 3: Timestamp source helper in `ledger.rs`**

```rust
/// Assigns successive unique, strictly-increasing timestamps for one apply,
/// seeded from the persisted watermark and the wall clock. `next = max(prev+1, now_ns)`.
struct TimestampSource { last: u64 }
impl TimestampSource {
    fn new(persisted_watermark: u64) -> Self { Self { last: persisted_watermark } }
    fn next(&mut self) -> u64 {
        let candidate = crate::store::now_ns();
        self.last = candidate.max(self.last + 1);
        self.last
    }
}
```

- [ ] **Step 4: Pure validator `validate_account_input` (model.rs)**

Returns `Some(failure_code)` for checks 6, 9–12, 20–26 (existence handled in `ledger.rs` because it needs storage). Order matters.

```rust
/// Input-only validation (no storage access) for a create-account item, in TB
/// order. Returns the first failing code, or None if input is structurally ok.
/// `exists`/`flags_are_mutually_exclusive`/balances are split: 6,9,10,11,12 run
/// here pre-existence; 20–26 run post-existence (caller invokes
/// `validate_account_post_existence`).
pub(crate) fn validate_account_pre_existence(a: &Account) -> Option<CreateAccountResult> {
    use CreateAccountResult as R;
    if a.timestamp != 0 { return Some(R::TimestampMustBeZero); }      // 6 (non-imported)
    if a.reserved != 0 { return Some(R::ReservedField); }            // 9
    if a.flags.has_reserved_bits() { return Some(R::ReservedFlag); } // 10
    if a.id == 0 { return Some(R::IdMustNotBeZero); }                // 11
    if a.id == u128::MAX { return Some(R::IdMustNotBeIntMax); }      // 12
    None
}

pub(crate) fn validate_account_post_existence(a: &Account) -> Option<CreateAccountResult> {
    use CreateAccountResult as R;
    if a.flags.is_mutually_exclusive_violation() { return Some(R::FlagsAreMutuallyExclusive); } // 20
    if a.debits_pending != 0 { return Some(R::DebitsPendingMustBeZero); }   // 21
    if a.debits_posted != 0 { return Some(R::DebitsPostedMustBeZero); }     // 22
    if a.credits_pending != 0 { return Some(R::CreditsPendingMustBeZero); } // 23
    if a.credits_posted != 0 { return Some(R::CreditsPostedMustBeZero); }   // 24
    if a.ledger == 0 { return Some(R::LedgerMustNotBeZero); }               // 25
    if a.code == 0 { return Some(R::CodeMustNotBeZero); }                   // 26
    None
}

/// Field-by-field comparison against an existing account (checks 13–19), TB order.
pub(crate) fn account_exists_result(incoming: &Account, existing: &Account) -> CreateAccountResult {
    use CreateAccountResult as R;
    if incoming.flags != existing.flags { return R::ExistsWithDifferentFlags; }
    if incoming.user_data_128 != existing.user_data_128 { return R::ExistsWithDifferentUserData128; }
    if incoming.user_data_64 != existing.user_data_64 { return R::ExistsWithDifferentUserData64; }
    if incoming.user_data_32 != existing.user_data_32 { return R::ExistsWithDifferentUserData32; }
    if incoming.ledger != existing.ledger { return R::ExistsWithDifferentLedger; }
    if incoming.code != existing.code { return R::ExistsWithDifferentCode; }
    R::Exists
}

/// LINKED/IMPORTED are gated to later phases.
pub(crate) fn account_is_gated(a: &Account) -> bool {
    a.flags.contains(AccountFlags::LINKED) || a.flags.contains(AccountFlags::IMPORTED)
}
```

- [ ] **Step 5: Rewrite `Ledger::create_accounts` (drop `timestamp` param)**

```rust
pub async fn create_accounts(&self, specs: &[Account]) -> Result<Vec<CreateAccountResult>> {
    use crate::model::{
        account_exists_result, account_is_gated, validate_account_post_existence,
        validate_account_pre_existence, CreateAccountResult as R,
    };
    let _lease = self.write_lease.lock().await;
    let writer = self.substrate.require_writer()?;

    let mut ts = TimestampSource::new(get_watermark(&self.substrate, &self.keyspace).await?);
    let mut batch = WriteBatch::new();
    let mut results = Vec::with_capacity(specs.len());
    let mut staged: HashSet<u128> = HashSet::new();
    let mut any_assigned = false;

    for spec in specs {
        if let Some(code) = validate_account_pre_existence(spec) { results.push(code); continue; }
        // existence (13–19): committed OR staged earlier in THIS batch.
        if staged.contains(&spec.id) {
            // a duplicate in-batch id is "exists" against the just-staged record;
            // re-validate fields against the staged copy (identical id) → Exists or ExistsWith*.
            // Simplit: compare against the staged Account we recorded.
            // (see staged_accounts map below)
        }
        if let Some(existing) = get_account(&self.substrate, &self.keyspace, spec.id).await? {
            results.push(account_exists_result(spec, &existing));
            continue;
        }
        if let Some(staged_acct) = staged_lookup(&staged_accounts, spec.id) {
            results.push(account_exists_result(spec, &staged_acct));
            continue;
        }
        if let Some(code) = validate_account_post_existence(spec) { results.push(code); continue; }
        if account_is_gated(spec) { results.push(R::NotImplementedYet); continue; }

        let mut acct = *spec;
        acct.timestamp = ts.next();
        batch.put(self.keyspace.account_key(acct.id), &encode(&acct)?);
        staged.insert(acct.id);
        staged_accounts.push(acct); // Vec<Account> for in-batch exists comparison
        any_assigned = true;
        results.push(R::Created);
    }

    if any_assigned {
        batch.put(self.keyspace.watermark_key(), &ts.last.to_be_bytes());
        writer.write(batch).await?;
    }
    Ok(results)
}
```

> Implementation note: keep an in-batch `staged_accounts: Vec<Account>` (or `HashMap<u128, Account>`) so a duplicate id within one batch returns `Exists`/`ExistsWith*` by comparing against the staged copy — mirroring committed-existence semantics. Drop the placeholder `staged`-only `HashSet` if the map subsumes it. Simplify the pseudo above into one `HashMap<u128, Account>` lookup that covers both committed and staged.

- [ ] **Step 6: Build green for accounts path; run account tests (added in Task 6); commit Tasks 1–3 together once `create_transfers` (Task 4) also compiles**

Because `ledger.rs` won't compile until `create_transfers` is rewritten (Task 4), the first green build + commit is at the end of Task 4. Tasks 1–4 land as one commit "Phase A: TB result codes, validation, engine timestamps".

---

## Task 4: `create_transfers` — regular posted, TB-exact; gate the rest

**Files:**
- Modify: `crates/bluedb-ledger/src/ledger.rs`
- Modify: `crates/bluedb-ledger/src/model.rs` (transfer validators)

**TB create_transfers order (verbatim, fetched 2026-06-14):** 1 created · 2 linked_event_failed · 3 linked_event_chain_open · 4 imported_event_expected · 5 imported_event_not_expected · 6 timestamp_must_be_zero · 7 imported_event_timestamp_out_of_range · 8 imported_event_timestamp_must_not_advance · 9 reserved_flag · 10 id_must_not_be_zero · 11 id_must_not_be_int_max · 12–22 exists_with_different_{flags,pending_id,timeout,debit_account_id,credit_account_id,amount,user_data_128,user_data_64,user_data_32,ledger,code} · 23 exists · 24 id_already_failed · 25 flags_are_mutually_exclusive · 26 debit_account_id_must_not_be_zero · 27 debit_account_id_must_not_be_int_max · 28 credit_account_id_must_not_be_zero · 29 credit_account_id_must_not_be_int_max · 30 accounts_must_be_different · 31 pending_id_must_be_zero · 32 pending_id_must_not_be_zero · 33 pending_id_must_not_be_int_max · 34 pending_id_must_be_different · 35 timeout_reserved_for_pending_transfer · 36 closing_transfer_must_be_pending · 37 ledger_must_not_be_zero · 38 code_must_not_be_zero · 39 debit_account_not_found · 40 credit_account_not_found · 41 accounts_must_have_the_same_ledger · 42 transfer_must_have_the_same_ledger_as_accounts · 43–53 pending-resolution · 54–57 imported · 58–59 account_already_closed · 60–65 overflows_* · 66 overflows_timeout · 67 exceeds_credits · 68 exceeds_debits.

Phase A implements 6, 9–11, 12–23 (exists family), 25–31, 35–42, 60(only posted/credits), 62–65, 67–68. **Gate**: a transfer that is anything other than a *plain regular posted transfer* (`flags == NONE && timeout == 0 && pending_id == 0`) returns `NotImplementedYet` — placed after input validation (post-38), before account resolution (pre-39).

- [ ] **Step 1: Pure validators `validate_transfer_pre_existence` and `validate_transfer_post_existence` (model.rs)**

```rust
pub(crate) fn validate_transfer_pre_existence(t: &Transfer) -> Option<CreateTransferResult> {
    use CreateTransferResult as R;
    if t.timestamp != 0 { return Some(R::TimestampMustBeZero); }      // 6 (non-imported)
    if t.flags.has_reserved_bits() { return Some(R::ReservedFlag); } // 9
    if t.id == 0 { return Some(R::IdMustNotBeZero); }                // 10
    if t.id == u128::MAX { return Some(R::IdMustNotBeIntMax); }      // 11
    None
}

/// Checks 25–38 (post-existence input validation). Closing/pending validity is
/// checked here; the gate (NotImplementedYet) is applied by the caller AFTER this.
pub(crate) fn validate_transfer_post_existence(t: &Transfer) -> Option<CreateTransferResult> {
    use CreateTransferResult as R;
    use TransferFlags as F;
    if t.flags.is_mutually_exclusive_violation() { return Some(R::FlagsAreMutuallyExclusive); }  // 25
    if t.debit_account_id == 0 { return Some(R::DebitAccountIdMustNotBeZero); }                   // 26
    if t.debit_account_id == u128::MAX { return Some(R::DebitAccountIdMustNotBeIntMax); }         // 27
    if t.credit_account_id == 0 { return Some(R::CreditAccountIdMustNotBeZero); }                 // 28
    if t.credit_account_id == u128::MAX { return Some(R::CreditAccountIdMustNotBeIntMax); }       // 29
    if t.debit_account_id == t.credit_account_id { return Some(R::AccountsMustBeDifferent); }     // 30

    let is_resolution = t.flags.contains(F::POST_PENDING_TRANSFER) || t.flags.contains(F::VOID_PENDING_TRANSFER);
    if is_resolution {
        if t.pending_id == 0 { return Some(R::PendingIdMustNotBeZero); }            // 32
        if t.pending_id == u128::MAX { return Some(R::PendingIdMustNotBeIntMax); }  // 33
        if t.pending_id == t.id { return Some(R::PendingIdMustBeDifferent); }       // 34
    } else if t.pending_id != 0 {
        return Some(R::PendingIdMustBeZero);                                        // 31
    }

    if !t.flags.contains(F::PENDING) && t.timeout != 0 { return Some(R::TimeoutReservedForPendingTransfer); } // 35
    if t.flags.is_closing() && !t.flags.contains(F::PENDING) { return Some(R::ClosingTransferMustBePending); } // 36
    if t.ledger == 0 { return Some(R::LedgerMustNotBeZero); }   // 37
    if t.code == 0 { return Some(R::CodeMustNotBeZero); }       // 38
    None
}

pub(crate) fn transfer_exists_result(incoming: &Transfer, existing: &Transfer) -> CreateTransferResult {
    use CreateTransferResult as R;
    if incoming.flags != existing.flags { return R::ExistsWithDifferentFlags; }
    if incoming.pending_id != existing.pending_id { return R::ExistsWithDifferentPendingId; }
    if incoming.timeout != existing.timeout { return R::ExistsWithDifferentTimeout; }
    if incoming.debit_account_id != existing.debit_account_id { return R::ExistsWithDifferentDebitAccountId; }
    if incoming.credit_account_id != existing.credit_account_id { return R::ExistsWithDifferentCreditAccountId; }
    if incoming.amount != existing.amount { return R::ExistsWithDifferentAmount; }
    if incoming.user_data_128 != existing.user_data_128 { return R::ExistsWithDifferentUserData128; }
    if incoming.user_data_64 != existing.user_data_64 { return R::ExistsWithDifferentUserData64; }
    if incoming.user_data_32 != existing.user_data_32 { return R::ExistsWithDifferentUserData32; }
    if incoming.ledger != existing.ledger { return R::ExistsWithDifferentLedger; }
    if incoming.code != existing.code { return R::ExistsWithDifferentCode; }
    R::Exists
}

/// True if this transfer needs a feature gated to a later phase.
pub(crate) fn transfer_is_gated(t: &Transfer) -> bool {
    t.flags != TransferFlags::NONE || t.timeout != 0 || t.pending_id != 0
}
```

- [ ] **Step 2: Rewrite `create_transfers` (drop ts param) + `stage_regular`**

Replace `stage_transfer`/`stage_movement`/`stage_resolution` with one regular path. Keep `ApplyState` (drop its `resolved` field — reintroduced in Phase B) and `load_account`.

```rust
pub async fn create_transfers(&self, transfers: &[Transfer]) -> Result<Vec<CreateTransferResult>> {
    use crate::model::{
        transfer_exists_result, transfer_is_gated, validate_transfer_post_existence,
        validate_transfer_pre_existence, CreateTransferResult as R,
    };
    let _lease = self.write_lease.lock().await;
    let writer = self.substrate.require_writer()?;

    let mut ts = TimestampSource::new(get_watermark(&self.substrate, &self.keyspace).await?);
    let mut state = ApplyState::default();
    let mut staged: HashMap<u128, Transfer> = HashMap::new();
    let mut accepted: Vec<Transfer> = Vec::new();
    let mut results = Vec::with_capacity(transfers.len());

    for t in transfers {
        if let Some(code) = validate_transfer_pre_existence(t) { results.push(code); continue; }
        // existence (12–23): committed or staged this batch.
        if let Some(existing) = get_transfer(&self.substrate, &self.keyspace, t.id).await? {
            results.push(transfer_exists_result(t, &existing)); continue;
        }
        if let Some(existing) = staged.get(&t.id) {
            results.push(transfer_exists_result(t, existing)); continue;
        }
        if let Some(code) = validate_transfer_post_existence(t) { results.push(code); continue; }
        if transfer_is_gated(t) { results.push(R::NotImplementedYet); continue; }

        match self.stage_regular(t, &mut state).await? {
            Ok(()) => {
                let applied = Transfer { timestamp: ts.next(), ..*t };
                staged.insert(applied.id, applied);
                accepted.push(applied);
                results.push(R::Created);
            }
            Err(code) => results.push(code),
        }
    }

    let mut batch = WriteBatch::new();
    for id in &state.dirty {
        if let Some(a) = state.working.get(id) { batch.put(self.keyspace.account_key(*id), &encode(a)?); }
    }
    for t in &accepted { batch.put(self.keyspace.transfer_key(t.id), &encode(t)?); }
    if !accepted.is_empty() {
        batch.put(self.keyspace.watermark_key(), &ts.last.to_be_bytes());
        writer.write(batch).await?;
    }
    Ok(results)
}
```

> Note: timestamp is assigned **only on accept** (`ts.next()` inside the `Ok` arm) so failed/gated items don't consume timestamps — matching TB (only created events advance the assigned-timestamp watermark). The watermark is written iff `accepted` is non-empty.

- [ ] **Step 3: `stage_regular` — checks 39–42, 62–65, 67–68 (posted only)**

```rust
/// Apply a plain posted transfer into `state`. Returns Err(code) on the first
/// failing TB check (account resolution → ledger agreement → overflow → balance).
async fn stage_regular(&self, t: &Transfer, state: &mut ApplyState) -> Result<std::result::Result<(), CreateTransferResult>> {
    use crate::model::{AccountFlags, CreateTransferResult as R};
    let mut debit = match self.load_account(t.debit_account_id, state).await? {
        Some(a) => a, None => return Ok(Err(R::DebitAccountNotFound)),   // 39
    };
    let mut credit = match self.load_account(t.credit_account_id, state).await? {
        Some(a) => a, None => return Ok(Err(R::CreditAccountNotFound)),  // 40
    };
    if debit.ledger != credit.ledger { return Ok(Err(R::AccountsMustHaveTheSameLedger)); }       // 41
    if t.ledger != debit.ledger { return Ok(Err(R::TransferMustHaveTheSameLedgerAsAccounts)); }  // 42

    // 62: overflows_debits_posted
    debit.debits_posted = match debit.debits_posted.checked_add(t.amount) {
        Some(v) => v, None => return Ok(Err(R::OverflowsDebitsPosted)),
    };
    // 63: overflows_credits_posted
    credit.credits_posted = match credit.credits_posted.checked_add(t.amount) {
        Some(v) => v, None => return Ok(Err(R::OverflowsCreditsPosted)),
    };
    // 64: overflows_debits (pending + posted on debit account)
    if debit.debits_pending.checked_add(debit.debits_posted).is_none() { return Ok(Err(R::OverflowsDebits)); }
    // 65: overflows_credits (pending + posted on credit account)
    if credit.credits_pending.checked_add(credit.credits_posted).is_none() { return Ok(Err(R::OverflowsCredits)); }

    // 67: exceeds_credits — debit side, asymmetric (posted+pending <= credits_posted). Verified in Plan 1.
    if debit.flags.contains(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS) {
        let used = debit.debits_posted.checked_add(debit.debits_pending).ok_or(()).map_err(|_| ()); // already guarded above
        if debit.debits_posted + debit.debits_pending > debit.credits_posted {
            return Ok(Err(R::ExceedsCredits));
        }
        let _ = used;
    }
    // 68: exceeds_debits — credit side.
    if credit.flags.contains(AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS) {
        if credit.credits_posted + credit.credits_pending > credit.debits_posted {
            return Ok(Err(R::ExceedsDebits));
        }
    }

    state.working.insert(debit.id, debit); state.dirty.insert(debit.id);
    state.working.insert(credit.id, credit); state.dirty.insert(credit.id);
    Ok(Ok(()))
}
```

> Clean up the `used`/`map_err` placeholder above — the `overflows_debits/credits` checks (64/65) already guarantee `debits_posted + debits_pending` doesn't overflow, so the balance comparison can add directly. Keep the comment explaining the asymmetry (carry the verified Plan-1 doc comment).

- [ ] **Step 4: Trim `ApplyState`**

```rust
#[derive(Default)]
struct ApplyState {
    working: std::collections::HashMap<u128, Account>,
    dirty: std::collections::HashSet<u128>,
}
```
Remove `resolved` (Phase B reintroduces a richer pending-state path). Remove now-unused `is_resolved`/`pending_resolved_key` imports — but KEEP `pending_resolved_key` in `keyspace.rs` under `#[allow(dead_code)]` (Phase B). Remove the `is_resolved` import in `ledger.rs`.

- [ ] **Step 5: Build + run full suite (after Task 6 migrates tests). First green commit.**

Run: `cargo test -p bluedb-ledger`
Then: `cargo clippy -p bluedb-ledger --all-targets -- -D warnings`
Commit: `git add -A && git commit` — "bluedb-ledger Phase A: TB result codes, full input validation, engine timestamps; regular transfers exact, rest gated".

---

## Task 5: `store.rs` test harness + watermark round-trip

**Files:**
- Modify: `crates/bluedb-ledger/src/store.rs`

- [ ] **Step 1: Update the `account_round_trips_through_postcard` test for the new `reserved` field**

```rust
let a = Account {
    id: 99, debits_pending: 1, debits_posted: 2, credits_pending: 3, credits_posted: 4,
    user_data_128: u128::MAX, user_data_64: 64, user_data_32: 32,
    reserved: 0, ledger: 1, code: 7, flags: AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS, timestamp: 123,
};
```
Update `get_account_reads_what_was_written` similarly (add `reserved: 0`).

- [ ] **Step 2: Watermark round-trip test**

```rust
#[tokio::test]
async fn watermark_round_trips() {
    let database = test_harness::writer_database().await;
    let substrate = database.substrate();
    let ks = LedgerKeyspace::new(bluedb_sql::DEFAULT_TENANT);
    assert_eq!(get_watermark(&substrate, &ks).await.unwrap(), 0);
    let writer = substrate.require_writer().unwrap();
    writer.put(&ks.watermark_key(), &1234u64.to_be_bytes()).await.unwrap();
    assert_eq!(get_watermark(&substrate, &ks).await.unwrap(), 1234);
}
```

- [ ] **Step 3: Run store tests** — `cargo test -p bluedb-ledger store::`

---

## Task 6: Migrate + expand `ledger.rs` tests to the new contract

**Files:**
- Modify: `crates/bluedb-ledger/src/ledger.rs` (tests module)

Rewrite every existing test to the new API (`Account::input`, `.with_code(1)`, no ts param, `CreateAccountResult`/`CreateTransferResult`). Replace the two-phase tests (`post_pending_*`, `pending_*`, `reserve`, `unsupported_transfer_flag`) with gating tests (return `NotImplementedYet`) — full two-phase tests return in Phase B.

- [ ] **Step 1: Helpers**

```rust
fn acct(id: u128, ledger: u32) -> Account { Account::input(id, ledger).with_code(1) }

async fn setup_two_accounts(ledger: &Ledger) {
    let r = ledger.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
    assert_eq!(r, vec![CreateAccountResult::Created, CreateAccountResult::Created]);
}
fn xfer(id: u128, d: u128, c: u128, amt: u128) -> Transfer { Transfer::new(id, d, c, amt, 7).with_code(1) }
```

- [ ] **Step 2: Core behaviour tests (rewrites)**

Cover: create_accounts persists + idempotent (`Created` then `Exists`); single transfer moves balances + conserves; batch applies-good-skips-bad; duplicate id in separate call → `Exists`; duplicate id in one batch → applies once + `Exists`; conservation over a sequence; DEBITS_MUST_NOT_EXCEED_CREDITS + CREDITS_MUST_NOT_EXCEED_DEBITS enforced (`ExceedsCredits`/`ExceedsDebits`); pending-credits-do-not-grant-debit-headroom (carry forward, now via a *posted* setup since pending is gated — give account 1 credits via a posted 2→1 then assert the asymmetry still holds with zero pending). Each asserts no partial mutation on failure.

- [ ] **Step 3: Validation-code tests (TB order)**

```rust
#[tokio::test]
async fn account_input_validation_codes() {
    use CreateAccountResult as R;
    let db = writer_database().await; let l = Ledger::new(&db);
    assert_eq!(l.create_accounts(&[Account::input(0, 7).with_code(1)]).await.unwrap(), vec![R::IdMustNotBeZero]);
    assert_eq!(l.create_accounts(&[Account::input(u128::MAX, 7).with_code(1)]).await.unwrap(), vec![R::IdMustNotBeIntMax]);
    assert_eq!(l.create_accounts(&[Account::input(1, 0).with_code(1)]).await.unwrap(), vec![R::LedgerMustNotBeZero]);
    assert_eq!(l.create_accounts(&[Account::input(1, 7)]).await.unwrap(), vec![R::CodeMustNotBeZero]); // code 0
    let mut a = Account::input(1, 7).with_code(1); a.timestamp = 5;
    assert_eq!(l.create_accounts(&[a]).await.unwrap(), vec![R::TimestampMustBeZero]);
    let mut a = Account::input(1, 7).with_code(1); a.reserved = 9;
    assert_eq!(l.create_accounts(&[a]).await.unwrap(), vec![R::ReservedField]);
    let mut a = Account::input(1, 7).with_code(1); a.debits_posted = 1;
    assert_eq!(l.create_accounts(&[a]).await.unwrap(), vec![R::DebitsPostedMustBeZero]);
    let me = Account::input(1, 7).with_code(1).with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS | AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS);
    assert_eq!(l.create_accounts(&[me]).await.unwrap(), vec![R::FlagsAreMutuallyExclusive]);
    let resv = Account::input(1, 7).with_code(1).with_flags(AccountFlags(1 << 9));
    assert_eq!(l.create_accounts(&[resv]).await.unwrap(), vec![R::ReservedFlag]);
}

#[tokio::test]
async fn account_exists_with_different_fields() {
    use CreateAccountResult as R;
    let db = writer_database().await; let l = Ledger::new(&db);
    l.create_accounts(&[Account::input(1, 7).with_code(1).with_user_data_64(5)]).await.unwrap();
    assert_eq!(l.create_accounts(&[Account::input(1, 7).with_code(1).with_user_data_64(5)]).await.unwrap(), vec![R::Exists]);
    assert_eq!(l.create_accounts(&[Account::input(1, 7).with_code(1).with_user_data_64(6)]).await.unwrap(), vec![R::ExistsWithDifferentUserData64]);
    assert_eq!(l.create_accounts(&[Account::input(1, 8).with_code(1).with_user_data_64(5)]).await.unwrap(), vec![R::ExistsWithDifferentLedger]);
    assert_eq!(l.create_accounts(&[Account::input(1, 7).with_code(2).with_user_data_64(5)]).await.unwrap(), vec![R::ExistsWithDifferentCode]);
}

#[tokio::test]
async fn transfer_input_validation_codes() {
    use CreateTransferResult as R;
    let db = writer_database().await; let l = Ledger::new(&db); setup_two_accounts(&l).await;
    assert_eq!(l.create_transfers(&[xfer(0, 1, 2, 5)]).await.unwrap(), vec![R::IdMustNotBeZero]);
    assert_eq!(l.create_transfers(&[xfer(u128::MAX, 1, 2, 5)]).await.unwrap(), vec![R::IdMustNotBeIntMax]);
    assert_eq!(l.create_transfers(&[xfer(1, 0, 2, 5)]).await.unwrap(), vec![R::DebitAccountIdMustNotBeZero]);
    assert_eq!(l.create_transfers(&[xfer(1, 1, 1, 5)]).await.unwrap(), vec![R::AccountsMustBeDifferent]);
    assert_eq!(l.create_transfers(&[xfer(1, 1, 2, 5).with_pending_id(9)]).await.unwrap(), vec![R::PendingIdMustBeZero]);
    assert_eq!(l.create_transfers(&[Transfer::new(1, 1, 2, 5, 0).with_code(1)]).await.unwrap(), vec![R::LedgerMustNotBeZero]);
    assert_eq!(l.create_transfers(&[xfer(1, 1, 2, 5).with_code(0)]).await.unwrap()[0], R::CodeMustNotBeZero);
    assert_eq!(l.create_transfers(&[xfer(1, 1, 99, 5)]).await.unwrap(), vec![R::CreditAccountNotFound]);
    assert_eq!(l.create_transfers(&[Transfer::new(1, 1, 2, 5, 8).with_code(1)]).await.unwrap(), vec![R::TransferMustHaveTheSameLedgerAsAccounts]);
}

#[tokio::test]
async fn gated_flags_return_not_implemented_yet() {
    use CreateTransferResult as R;
    let db = writer_database().await; let l = Ledger::new(&db); setup_two_accounts(&l).await;
    for f in [TransferFlags::PENDING, TransferFlags::LINKED, TransferFlags::BALANCING_DEBIT] {
        let r = l.create_transfers(&[xfer(1, 1, 2, 5).with_flags(f)]).await.unwrap();
        assert_eq!(r, vec![R::NotImplementedYet]);
    }
    // post/void need a pending_id; that input passes validation then gates.
    let r = l.create_transfers(&[xfer(1, 1, 2, 5).with_flags(TransferFlags::POST_PENDING_TRANSFER).with_pending_id(2)]).await.unwrap();
    assert_eq!(r, vec![R::NotImplementedYet]);
    // nothing persisted
    assert!(l.lookup_transfer(1).await.unwrap().is_none());
    assert_eq!(l.lookup_account(1).await.unwrap().unwrap().debits_posted, 0);
}
```

- [ ] **Step 4: Engine-timestamp tests**

```rust
#[tokio::test]
async fn timestamps_are_engine_assigned_monotonic_and_durable() {
    let db = writer_database().await; let l = Ledger::new(&db);
    l.create_accounts(&[acct(1, 7), acct(2, 7)]).await.unwrap();
    let a1 = l.lookup_account(1).await.unwrap().unwrap();
    let a2 = l.lookup_account(2).await.unwrap().unwrap();
    assert!(a1.timestamp > 0 && a2.timestamp > a1.timestamp, "unique + increasing within a batch");

    l.create_transfers(&[xfer(10, 1, 2, 5)]).await.unwrap();
    let t = l.lookup_transfer(10).await.unwrap().unwrap();
    assert!(t.timestamp > a2.timestamp, "watermark persists across calls (durable, monotonic)");

    // Failed/gated items don't consume a timestamp gap that breaks monotonicity:
    l.create_transfers(&[xfer(11, 1, 99, 5)]).await.unwrap(); // CreditAccountNotFound, applies nothing
    l.create_transfers(&[xfer(12, 1, 2, 5)]).await.unwrap();
    let t12 = l.lookup_transfer(12).await.unwrap().unwrap();
    assert!(t12.timestamp > t.timestamp);
}
```

- [ ] **Step 5: Run full suite + clippy**

Run: `cargo test -p bluedb-ledger`
Run: `cargo test --workspace` (ensure nothing else broke — no other crate depends on the removed types; confirm)
Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: all green.

- [ ] **Step 6: Commit**

`git commit` — "bluedb-ledger Phase A: migrate + expand tests for TB result codes and engine timestamps".

---

## Self-review checklist (run before review subagents)

1. **Spec coverage:** every check in TB's create_accounts (6, 9–26) and create_transfers (6, 9–11, 12–23, 25–42, 62–65, 67–68) order is implemented; linked/imported/pending/post/void/balancing/closing/timeout gated to `NotImplementedYet`. ✔ map each to a test.
2. **Ordering fidelity:** validators return the *first* matching code top-to-bottom; existence short-circuits before post-existence checks; the gate sits after input validation, before account resolution.
3. **Timestamp invariants:** strictly increasing, unique, durable across calls; assigned only on accept; input `timestamp != 0` → `TimestampMustBeZero`.
4. **No partial mutation:** a failed/gated item folds nothing into `state`; the batch only contains accepted items + dirty accounts + watermark.
5. **Atomicity preserved:** single `WriteBatch`, written under the lease via the active writer; empty-batch guard kept.
6. **No placeholders:** remove the `used`/`map_err` scaffolding in `stage_regular`; no `todo!()`.

## Review (subagent-driven)

After Task 6 is green: dispatch a **spec-compliance** review subagent (does the code match this plan + the TB orderings?) then a **code-quality** review subagent. Fix findings, re-review, then mark Phase A complete and move to Phase B.
