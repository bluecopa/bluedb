//! Ledger data model: TigerBeetle-faithful fixed records with u128 amounts,
//! plus the exact per-item result codes and the input validators that mirror
//! TigerBeetle's `create_accounts` / `create_transfers` check ordering.

use serde::{Deserialize, Serialize};

/// TigerBeetle's "full pending" sentinel for `post_pending_transfer.amount`
/// (and the reserved `id`/`pending_id` value): `2^128 - 1`.
pub const AMOUNT_MAX: u128 = u128::MAX;

/// Account behavior flags (bitset). Mirrors TigerBeetle's account flags.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountFlags(pub u16);

impl AccountFlags {
    pub const NONE: Self = Self(0);
    pub const LINKED: Self = Self(1 << 0);
    pub const DEBITS_MUST_NOT_EXCEED_CREDITS: Self = Self(1 << 1);
    pub const CREDITS_MUST_NOT_EXCEED_DEBITS: Self = Self(1 << 2);
    pub const HISTORY: Self = Self(1 << 3);
    pub const IMPORTED: Self = Self(1 << 4);
    pub const CLOSED: Self = Self(1 << 5);
    /// Every defined flag bit; bits outside this mask are `reserved_flag`.
    const DEFINED: u16 = 0b11_1111;

    /// True if every bit in `other` is set in `self`.
    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Any bit outside the defined set is set → `reserved_flag`.
    pub fn has_reserved_bits(self) -> bool {
        self.0 & !Self::DEFINED != 0
    }

    /// `DEBITS_MUST_NOT_EXCEED_CREDITS` and `CREDITS_MUST_NOT_EXCEED_DEBITS`
    /// cannot both be set → `flags_are_mutually_exclusive`.
    pub fn is_mutually_exclusive_violation(self) -> bool {
        self.contains(Self::DEBITS_MUST_NOT_EXCEED_CREDITS)
            && self.contains(Self::CREDITS_MUST_NOT_EXCEED_DEBITS)
    }
}

impl std::ops::BitOr for AccountFlags {
    type Output = Self;
    /// Combine flag sets (bitwise OR), e.g. `AccountFlags::LINKED | AccountFlags::HISTORY`.
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// Transfer behavior flags (bitset). Mirrors TigerBeetle's transfer flags.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferFlags(pub u16);

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
    /// Every defined flag bit; bits outside this mask are `reserved_flag`.
    const DEFINED: u16 = 0b1_1111_1111;

    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn has_reserved_bits(self) -> bool {
        self.0 & !Self::DEFINED != 0
    }

    /// TigerBeetle's transfer flag matrix (true ⇒ illegal combination):
    /// - at most one of {pending, post_pending, void_pending};
    /// - post/void cannot combine with balancing_debit/balancing_credit;
    /// - post/void cannot combine with closing_debit/closing_credit.
    pub fn is_mutually_exclusive_violation(self) -> bool {
        let pending = self.contains(Self::PENDING);
        let post = self.contains(Self::POST_PENDING_TRANSFER);
        let void = self.contains(Self::VOID_PENDING_TRANSFER);
        let phase_count = [pending, post, void].into_iter().filter(|b| *b).count();
        if phase_count > 1 {
            return true;
        }
        let balancing = self.contains(Self::BALANCING_DEBIT) || self.contains(Self::BALANCING_CREDIT);
        let closing = self.is_closing();
        if (post || void) && (balancing || closing) {
            return true;
        }
        false
    }

    /// A closing transfer closes the debit and/or credit account; it must also
    /// be pending (`closing_transfer_must_be_pending` otherwise).
    pub fn is_closing(self) -> bool {
        self.contains(Self::CLOSING_DEBIT) || self.contains(Self::CLOSING_CREDIT)
    }
}

impl std::ops::BitOr for TransferFlags {
    type Output = Self;
    /// Combine flag sets (bitwise OR), e.g. `TransferFlags::PENDING | TransferFlags::LINKED`.
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// A ledger account — both the create-input shape and the stored record, exactly
/// as in TigerBeetle. Clients fill `id`/`ledger`/`code`/`flags`/`user_data_*`
/// and leave balances, `reserved`, and `timestamp` zero; the engine validates
/// the zeros, assigns `timestamp`, and persists.
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
    /// A create-input account: `id` + `ledger`, all balances / `reserved` /
    /// `timestamp` zero and `code` zero (set a nonzero `code` before creating —
    /// `code_must_not_be_zero` is enforced).
    pub fn input(id: u128, ledger: u32) -> Self {
        Self {
            id,
            debits_pending: 0,
            debits_posted: 0,
            credits_pending: 0,
            credits_posted: 0,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            reserved: 0,
            ledger,
            code: 0,
            flags: AccountFlags::NONE,
            timestamp: 0,
        }
    }

    pub fn with_code(mut self, code: u16) -> Self {
        self.code = code;
        self
    }
    pub fn with_flags(mut self, flags: AccountFlags) -> Self {
        self.flags = flags;
        self
    }
    pub fn with_user_data_128(mut self, v: u128) -> Self {
        self.user_data_128 = v;
        self
    }
    pub fn with_user_data_64(mut self, v: u64) -> Self {
        self.user_data_64 = v;
        self
    }
    pub fn with_user_data_32(mut self, v: u32) -> Self {
        self.user_data_32 = v;
        self
    }
}

/// A transfer request / record. `timestamp` is assigned by the engine at apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transfer {
    pub id: u128,
    pub debit_account_id: u128,
    pub credit_account_id: u128,
    pub amount: u128,
    pub pending_id: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub ledger: u32,
    pub code: u16,
    pub flags: TransferFlags,
    pub timeout: u32,
    pub timestamp: u64,
}

impl Transfer {
    /// A minimal posted transfer: id, debit, credit, amount, ledger; everything
    /// else default (set a nonzero `code` before creating).
    pub fn new(id: u128, debit_account_id: u128, credit_account_id: u128, amount: u128, ledger: u32) -> Self {
        Self {
            id,
            debit_account_id,
            credit_account_id,
            amount,
            pending_id: 0,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            ledger,
            code: 0,
            flags: TransferFlags::NONE,
            timeout: 0,
            timestamp: 0,
        }
    }

    pub fn with_code(mut self, code: u16) -> Self {
        self.code = code;
        self
    }
    pub fn with_flags(mut self, flags: TransferFlags) -> Self {
        self.flags = flags;
        self
    }
    pub fn with_pending_id(mut self, id: u128) -> Self {
        self.pending_id = id;
        self
    }
}

/// Per-item result of `create_accounts`. Variant ordering mirrors TigerBeetle's
/// create_accounts result codes. `Created` is success; `Exists` / `ExistsWith*`
/// are idempotent (not failures). `NotImplementedYet` is a transitional,
/// non-TigerBeetle code for features gated to later phases (linked, imported);
/// it is removed once every phase lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateAccountResult {
    Created,
    LinkedEventFailed,
    LinkedEventChainOpen,
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
    /// Transitional, non-TigerBeetle: a feature gated to a later phase.
    NotImplementedYet,
}

/// Per-item result of `create_transfers`. Variant ordering mirrors TigerBeetle's
/// create_transfers result codes. Many variants are reserved for later phases
/// (pending resolution, closed accounts, imported, timeout) but are declared now
/// so the enum's shape is stable. `NotImplementedYet` is transitional (removed
/// once every phase lands).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateTransferResult {
    Created,
    LinkedEventFailed,
    LinkedEventChainOpen,
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
    IdAlreadyFailed,
    FlagsAreMutuallyExclusive,
    DebitAccountIdMustNotBeZero,
    DebitAccountIdMustNotBeIntMax,
    CreditAccountIdMustNotBeZero,
    CreditAccountIdMustNotBeIntMax,
    AccountsMustBeDifferent,
    PendingIdMustBeZero,
    PendingIdMustNotBeZero,
    PendingIdMustNotBeIntMax,
    PendingIdMustBeDifferent,
    TimeoutReservedForPendingTransfer,
    ClosingTransferMustBePending,
    LedgerMustNotBeZero,
    CodeMustNotBeZero,
    DebitAccountNotFound,
    CreditAccountNotFound,
    AccountsMustHaveTheSameLedger,
    TransferMustHaveTheSameLedgerAsAccounts,
    PendingTransferNotFound,
    PendingTransferNotPending,
    PendingTransferHasDifferentDebitAccountId,
    PendingTransferHasDifferentCreditAccountId,
    PendingTransferHasDifferentLedger,
    PendingTransferHasDifferentCode,
    ExceedsPendingTransferAmount,
    PendingTransferHasDifferentAmount,
    PendingTransferAlreadyPosted,
    PendingTransferAlreadyVoided,
    PendingTransferExpired,
    DebitAccountAlreadyClosed,
    CreditAccountAlreadyClosed,
    OverflowsDebitsPending,
    OverflowsCreditsPending,
    OverflowsDebitsPosted,
    OverflowsCreditsPosted,
    OverflowsDebits,
    OverflowsCredits,
    OverflowsTimeout,
    ExceedsCredits,
    ExceedsDebits,
    /// Transitional, non-TigerBeetle: a feature gated to a later phase.
    NotImplementedYet,
}

/// Resolution state of a pending transfer, recorded once it is posted, voided,
/// or expired. Persisted under the pending-state keyspace and used to return
/// `pending_transfer_already_posted` / `_already_voided` / `_expired` on a later
/// resolution attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PendingStatus {
    Posted,
    Voided,
    Expired,
}

/// How a validated transfer is applied. `Gated` is a later-phase feature
/// (linked, balancing, closing, imported, or a pending with a timeout).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransferOp {
    Regular,
    PendingReserve,
    Post,
    Void,
    Gated,
}

/// Classify a transfer that has already passed input validation. `LINKED` and
/// the `BALANCING_*` flags are orthogonal to the op (handled by the chain driver
/// and the apply paths respectively), so they do not affect the classification.
pub(crate) fn classify(t: &Transfer) -> TransferOp {
    use TransferFlags as F;
    if t.flags.contains(F::CLOSING_DEBIT) || t.flags.contains(F::CLOSING_CREDIT) || t.flags.contains(F::IMPORTED) {
        return TransferOp::Gated;
    }
    if t.flags.contains(F::POST_PENDING_TRANSFER) {
        return TransferOp::Post;
    }
    if t.flags.contains(F::VOID_PENDING_TRANSFER) {
        return TransferOp::Void;
    }
    if t.flags.contains(F::PENDING) {
        return TransferOp::PendingReserve; // timeout handled in Phase C
    }
    TransferOp::Regular
}

/// A contiguous run of batch events forming one linked chain (or one independent
/// event). `start..=end` inclusive; `open` is true iff the run's last event still
/// has `linked` set — i.e. the batch ended mid-chain (no terminator).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Chain {
    pub start: usize,
    pub end: usize,
    pub open: bool,
}

/// Group a batch into chains from each event's `linked` flag. A maximal run of
/// `linked == true` events plus its terminator (`linked == false`) is one chain;
/// a lone `linked == false` event is an independent chain; a trailing run of
/// `linked == true` with no terminator is an `open` chain.
pub(crate) fn chains(linked: &[bool]) -> Vec<Chain> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < linked.len() {
        let mut j = i;
        while j < linked.len() && linked[j] {
            j += 1;
        }
        if j < linked.len() {
            // linked[j] == false → terminator; chain is i..=j.
            out.push(Chain { start: i, end: j, open: false });
            i = j + 1;
        } else {
            // ran off the end while still linked → open chain i..=len-1.
            out.push(Chain { start: i, end: j - 1, open: true });
            i = j;
        }
    }
    out
}

// ---- Account input validators (mirror TB create_accounts order) ----

/// Input-only checks that run *before* the existence lookup (TB codes 6, 9–12).
/// Returns the first failing code, or `None` if the pre-existence input is ok.
pub(crate) fn validate_account_pre_existence(a: &Account) -> Option<CreateAccountResult> {
    use CreateAccountResult as R;
    if a.timestamp != 0 {
        return Some(R::TimestampMustBeZero); // 6 (non-imported)
    }
    if a.reserved != 0 {
        return Some(R::ReservedField); // 9
    }
    if a.flags.has_reserved_bits() {
        return Some(R::ReservedFlag); // 10
    }
    if a.id == 0 {
        return Some(R::IdMustNotBeZero); // 11
    }
    if a.id == u128::MAX {
        return Some(R::IdMustNotBeIntMax); // 12
    }
    None
}

/// Input-only checks that run *after* the existence lookup, only for a new id
/// (TB codes 20–26). Returns the first failing code, or `None`.
pub(crate) fn validate_account_post_existence(a: &Account) -> Option<CreateAccountResult> {
    use CreateAccountResult as R;
    if a.flags.is_mutually_exclusive_violation() {
        return Some(R::FlagsAreMutuallyExclusive); // 20
    }
    if a.debits_pending != 0 {
        return Some(R::DebitsPendingMustBeZero); // 21
    }
    if a.debits_posted != 0 {
        return Some(R::DebitsPostedMustBeZero); // 22
    }
    if a.credits_pending != 0 {
        return Some(R::CreditsPendingMustBeZero); // 23
    }
    if a.credits_posted != 0 {
        return Some(R::CreditsPostedMustBeZero); // 24
    }
    if a.ledger == 0 {
        return Some(R::LedgerMustNotBeZero); // 25
    }
    if a.code == 0 {
        return Some(R::CodeMustNotBeZero); // 26
    }
    None
}

/// Field-by-field comparison of an incoming account against the existing record
/// with the same id (TB codes 13–19), in TB order.
pub(crate) fn account_exists_result(incoming: &Account, existing: &Account) -> CreateAccountResult {
    use CreateAccountResult as R;
    if incoming.flags != existing.flags {
        return R::ExistsWithDifferentFlags;
    }
    if incoming.user_data_128 != existing.user_data_128 {
        return R::ExistsWithDifferentUserData128;
    }
    if incoming.user_data_64 != existing.user_data_64 {
        return R::ExistsWithDifferentUserData64;
    }
    if incoming.user_data_32 != existing.user_data_32 {
        return R::ExistsWithDifferentUserData32;
    }
    if incoming.ledger != existing.ledger {
        return R::ExistsWithDifferentLedger;
    }
    if incoming.code != existing.code {
        return R::ExistsWithDifferentCode;
    }
    R::Exists
}

/// IMPORTED (Phase G) account creation is gated for now. (LINKED is handled by
/// the chain driver, not gated.)
pub(crate) fn account_is_gated(a: &Account) -> bool {
    a.flags.contains(AccountFlags::IMPORTED)
}

// ---- Transfer input validators (mirror TB create_transfers order) ----

/// Input-only checks that run *before* the existence lookup (TB codes 6, 9–11).
pub(crate) fn validate_transfer_pre_existence(t: &Transfer) -> Option<CreateTransferResult> {
    use CreateTransferResult as R;
    if t.timestamp != 0 {
        return Some(R::TimestampMustBeZero); // 6 (non-imported)
    }
    if t.flags.has_reserved_bits() {
        return Some(R::ReservedFlag); // 9
    }
    if t.id == 0 {
        return Some(R::IdMustNotBeZero); // 10
    }
    if t.id == u128::MAX {
        return Some(R::IdMustNotBeIntMax); // 11
    }
    None
}

/// Input-only checks that run *after* the existence lookup, only for a new id
/// (TB codes 25–38). The gate (`NotImplementedYet`) is applied by the caller
/// *after* this returns `None`.
pub(crate) fn validate_transfer_post_existence(t: &Transfer) -> Option<CreateTransferResult> {
    use CreateTransferResult as R;
    use TransferFlags as F;
    if t.flags.is_mutually_exclusive_violation() {
        return Some(R::FlagsAreMutuallyExclusive); // 25
    }

    let is_resolution =
        t.flags.contains(F::POST_PENDING_TRANSFER) || t.flags.contains(F::VOID_PENDING_TRANSFER);

    // Account-id zero/int-max + distinctness (26–30) apply only to non-resolution
    // transfers: post/void inherit zero account ids from the pending, and a
    // nonzero mismatch is reported later as pending_transfer_has_different_* (45/46).
    if !is_resolution {
        if t.debit_account_id == 0 {
            return Some(R::DebitAccountIdMustNotBeZero); // 26
        }
        if t.debit_account_id == u128::MAX {
            return Some(R::DebitAccountIdMustNotBeIntMax); // 27
        }
        if t.credit_account_id == 0 {
            return Some(R::CreditAccountIdMustNotBeZero); // 28
        }
        if t.credit_account_id == u128::MAX {
            return Some(R::CreditAccountIdMustNotBeIntMax); // 29
        }
        if t.debit_account_id == t.credit_account_id {
            return Some(R::AccountsMustBeDifferent); // 30
        }
    }

    if is_resolution {
        if t.pending_id == 0 {
            return Some(R::PendingIdMustNotBeZero); // 32
        }
        if t.pending_id == u128::MAX {
            return Some(R::PendingIdMustNotBeIntMax); // 33
        }
        if t.pending_id == t.id {
            return Some(R::PendingIdMustBeDifferent); // 34
        }
    } else if t.pending_id != 0 {
        return Some(R::PendingIdMustBeZero); // 31
    }

    if !t.flags.contains(F::PENDING) && t.timeout != 0 {
        return Some(R::TimeoutReservedForPendingTransfer); // 35
    }
    if t.flags.is_closing() && !t.flags.contains(F::PENDING) {
        return Some(R::ClosingTransferMustBePending); // 36
    }

    // ledger/code zero (37/38) apply only to non-resolution transfers: post/void
    // inherit a zero ledger/code from the pending.
    if !is_resolution {
        if t.ledger == 0 {
            return Some(R::LedgerMustNotBeZero); // 37
        }
        if t.code == 0 {
            return Some(R::CodeMustNotBeZero); // 38
        }
    }
    None
}

/// Field-by-field comparison of an incoming transfer against the existing record
/// with the same id (TB codes 12–23), in TB order.
pub(crate) fn transfer_exists_result(incoming: &Transfer, existing: &Transfer) -> CreateTransferResult {
    use CreateTransferResult as R;
    use TransferFlags as F;
    // For a resolution (post/void), zero inheritable fields and the amount
    // sentinel (`AMOUNT_MAX` for post, `0` for void) were wildcards at apply time
    // and match the stored materialized value — so an identical retry is `Exists`.
    let resolution =
        incoming.flags.contains(F::POST_PENDING_TRANSFER) || incoming.flags.contains(F::VOID_PENDING_TRANSFER);

    /// A field differs: for a resolution a zero incoming is a wildcard (inherited).
    fn differs<T: PartialEq + Default>(resolution: bool, inc: T, ex: T) -> bool {
        if resolution {
            inc != T::default() && inc != ex
        } else {
            inc != ex
        }
    }

    if incoming.flags != existing.flags {
        return R::ExistsWithDifferentFlags;
    }
    if incoming.pending_id != existing.pending_id {
        return R::ExistsWithDifferentPendingId;
    }
    if incoming.timeout != existing.timeout {
        return R::ExistsWithDifferentTimeout;
    }
    if differs(resolution, incoming.debit_account_id, existing.debit_account_id) {
        return R::ExistsWithDifferentDebitAccountId;
    }
    if differs(resolution, incoming.credit_account_id, existing.credit_account_id) {
        return R::ExistsWithDifferentCreditAccountId;
    }
    // amount is special-cased because its resolution wildcard is a sentinel, not
    // zero: `AMOUNT_MAX` for a post (void's wildcard happens to be 0, so it could
    // use `differs`, but both are handled here for symmetry).
    let amount_wildcard = if incoming.flags.contains(F::POST_PENDING_TRANSFER) { AMOUNT_MAX } else { 0 };
    let amount_differs = if resolution {
        incoming.amount != amount_wildcard && incoming.amount != existing.amount
    } else {
        incoming.amount != existing.amount
    };
    if amount_differs {
        return R::ExistsWithDifferentAmount;
    }
    if differs(resolution, incoming.user_data_128, existing.user_data_128) {
        return R::ExistsWithDifferentUserData128;
    }
    if differs(resolution, incoming.user_data_64, existing.user_data_64) {
        return R::ExistsWithDifferentUserData64;
    }
    if differs(resolution, incoming.user_data_32, existing.user_data_32) {
        return R::ExistsWithDifferentUserData32;
    }
    if differs(resolution, incoming.ledger, existing.ledger) {
        return R::ExistsWithDifferentLedger;
    }
    if differs(resolution, incoming.code, existing.code) {
        return R::ExistsWithDifferentCode;
    }
    R::Exists
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_flag_helpers() {
        assert!(AccountFlags(0b100_0000).has_reserved_bits()); // bit 6 undefined
        assert!(!(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS | AccountFlags::HISTORY).has_reserved_bits());
        assert!((AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS
            | AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS)
            .is_mutually_exclusive_violation());
        assert!(!AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS.is_mutually_exclusive_violation());
        let f = AccountFlags::LINKED | AccountFlags::HISTORY;
        assert!(f.contains(AccountFlags::LINKED) && f.contains(AccountFlags::HISTORY));
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
        assert!(!F::IMPORTED.has_reserved_bits());
    }

    #[test]
    fn account_pre_existence_order() {
        use CreateAccountResult as R;
        let mut a = Account::input(1, 7).with_code(1);
        a.timestamp = 5;
        assert_eq!(validate_account_pre_existence(&a), Some(R::TimestampMustBeZero));
        let mut a = Account::input(1, 7).with_code(1);
        a.reserved = 1;
        assert_eq!(validate_account_pre_existence(&a), Some(R::ReservedField));
        let a = Account::input(0, 7).with_code(1);
        assert_eq!(validate_account_pre_existence(&a), Some(R::IdMustNotBeZero));
        let a = Account::input(u128::MAX, 7).with_code(1);
        assert_eq!(validate_account_pre_existence(&a), Some(R::IdMustNotBeIntMax));
        assert_eq!(validate_account_pre_existence(&Account::input(1, 7).with_code(1)), None);
    }

    #[test]
    fn transfer_post_existence_pending_id_rules() {
        use CreateTransferResult as R;
        use TransferFlags as F;
        // regular transfer must not carry a pending_id.
        let t = Transfer::new(1, 1, 2, 5, 7).with_code(1).with_pending_id(9);
        assert_eq!(validate_transfer_post_existence(&t), Some(R::PendingIdMustBeZero));
        // post/void must carry one.
        let t = Transfer::new(1, 1, 2, 5, 7).with_code(1).with_flags(F::POST_PENDING_TRANSFER);
        assert_eq!(validate_transfer_post_existence(&t), Some(R::PendingIdMustNotBeZero));
        // pending_id must differ from id.
        let t = Transfer::new(9, 1, 2, 5, 7).with_code(1).with_flags(F::VOID_PENDING_TRANSFER).with_pending_id(9);
        assert_eq!(validate_transfer_post_existence(&t), Some(R::PendingIdMustBeDifferent));
        // closing must be pending.
        let t = Transfer::new(1, 1, 2, 5, 7).with_code(1).with_flags(F::CLOSING_DEBIT);
        assert_eq!(validate_transfer_post_existence(&t), Some(R::ClosingTransferMustBePending));
        // timeout only on pending.
        let mut t = Transfer::new(1, 1, 2, 5, 7).with_code(1);
        t.timeout = 10;
        assert_eq!(validate_transfer_post_existence(&t), Some(R::TimeoutReservedForPendingTransfer));
        // clean regular transfer passes.
        assert_eq!(validate_transfer_post_existence(&Transfer::new(1, 1, 2, 5, 7).with_code(1)), None);
    }

    #[test]
    fn classify_ops() {
        use TransferFlags as F;
        let t = |f: F, timeout: u32| {
            let mut x = Transfer::new(1, 1, 2, 5, 7).with_code(1).with_flags(f);
            x.timeout = timeout;
            x
        };
        assert_eq!(classify(&Transfer::new(1, 1, 2, 5, 7).with_code(1)), TransferOp::Regular);
        assert_eq!(classify(&t(F::PENDING, 0)), TransferOp::PendingReserve);
        assert_eq!(classify(&t(F::PENDING, 30)), TransferOp::PendingReserve); // pending+timeout handled in C
        assert_eq!(classify(&t(F::POST_PENDING_TRANSFER, 0)), TransferOp::Post);
        assert_eq!(classify(&t(F::VOID_PENDING_TRANSFER, 0)), TransferOp::Void);
        assert_eq!(classify(&t(F::LINKED, 0)), TransferOp::Regular); // LINKED is orthogonal
        assert_eq!(classify(&t(F::LINKED | F::PENDING, 0)), TransferOp::PendingReserve);
        assert_eq!(classify(&t(F::BALANCING_DEBIT, 0)), TransferOp::Regular); // balancing is orthogonal
        assert_eq!(classify(&t(F::BALANCING_CREDIT | F::PENDING, 0)), TransferOp::PendingReserve);
        assert_eq!(classify(&t(F::IMPORTED, 0)), TransferOp::Gated); // imported → Phase G
        assert_eq!(classify(&t(F::PENDING | F::CLOSING_DEBIT, 0)), TransferOp::Gated); // closing → Phase F
    }

    #[test]
    fn chains_grouping() {
        let c = |s, e, o| Chain { start: s, end: e, open: o };
        assert_eq!(chains(&[]), vec![]);
        assert_eq!(chains(&[false]), vec![c(0, 0, false)]);
        assert_eq!(chains(&[true]), vec![c(0, 0, true)]); // lone open
        assert_eq!(chains(&[true, false]), vec![c(0, 1, false)]);
        assert_eq!(chains(&[true, true, false, false]), vec![c(0, 2, false), c(3, 3, false)]);
        assert_eq!(chains(&[false, true, true]), vec![c(0, 0, false), c(1, 2, true)]);
        assert_eq!(
            chains(&[true, false, true, false]),
            vec![c(0, 1, false), c(2, 3, false)]
        );
    }

    #[test]
    fn account_gating_predicate() {
        assert!(!account_is_gated(&Account::input(1, 7).with_code(1)));
        assert!(account_is_gated(&Account::input(1, 7).with_code(1).with_flags(AccountFlags::IMPORTED)));
    }

    #[test]
    fn resolution_validation_allows_inherited_zero_fields() {
        // A post with zero accounts/ledger/code passes input validation (those
        // fields inherit from the pending). pending_id rules still apply.
        use CreateTransferResult as R;
        let mut post = Transfer::new(5, 0, 0, AMOUNT_MAX, 0);
        post.flags = TransferFlags::POST_PENDING_TRANSFER;
        post.pending_id = 9;
        assert_eq!(validate_transfer_post_existence(&post), None);
        post.pending_id = 0;
        assert_eq!(validate_transfer_post_existence(&post), Some(R::PendingIdMustNotBeZero));
    }

    #[test]
    fn resolution_exists_is_inheritance_aware() {
        use CreateTransferResult as R;
        use TransferFlags as F;
        // Stored materialized post: accounts 1→2, ledger 7, code 3, amount 100.
        let mut stored = Transfer::new(5, 1, 2, 100, 7).with_code(3);
        stored.flags = F::POST_PENDING_TRANSFER;
        stored.pending_id = 9;
        stored.timestamp = 42;
        // Retry with AMOUNT_MAX + inherited zeros → Exists.
        let mut retry = Transfer::new(5, 0, 0, AMOUNT_MAX, 0);
        retry.flags = F::POST_PENDING_TRANSFER;
        retry.pending_id = 9;
        assert_eq!(transfer_exists_result(&retry, &stored), R::Exists);
        // Retry with a differing explicit amount → ExistsWithDifferentAmount.
        let mut retry2 = retry;
        retry2.amount = 50;
        assert_eq!(transfer_exists_result(&retry2, &stored), R::ExistsWithDifferentAmount);
        // Retry with a differing explicit debit account → mismatch.
        let mut retry3 = retry;
        retry3.debit_account_id = 99;
        assert_eq!(transfer_exists_result(&retry3, &stored), R::ExistsWithDifferentDebitAccountId);
    }
}
