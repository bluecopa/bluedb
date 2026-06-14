//! Ledger data model: TigerBeetle-faithful fixed records with u128 amounts.

use serde::{Deserialize, Serialize};

/// Account behavior flags (bitset). Mirrors TigerBeetle's account flags.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountFlags(pub u16);

impl AccountFlags {
    pub const NONE: Self = Self(0);
    pub const LINKED: Self = Self(1 << 0);
    pub const DEBITS_MUST_NOT_EXCEED_CREDITS: Self = Self(1 << 1);
    pub const CREDITS_MUST_NOT_EXCEED_DEBITS: Self = Self(1 << 2);
    pub const HISTORY: Self = Self(1 << 3);

    /// True if every bit in `other` is set in `self`.
    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
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
/// Plan 1 honors none of these directly (single posted transfers); later plans
/// implement linked / two-phase / balancing.
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

    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for TransferFlags {
    type Output = Self;
    /// Combine flag sets (bitwise OR), e.g. `TransferFlags::PENDING | TransferFlags::LINKED`.
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// A persisted account with its four balance buckets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    pub id: u128,
    pub ledger: u32,
    pub code: u16,
    pub flags: AccountFlags,
    pub debits_pending: u128,
    pub debits_posted: u128,
    pub credits_pending: u128,
    pub credits_posted: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub timestamp: u64,
}

/// The caller-supplied form for creating an account (no balances; the engine
/// initializes them to zero and assigns `timestamp`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewAccount {
    pub id: u128,
    pub ledger: u32,
    pub code: u16,
    pub flags: AccountFlags,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
}

impl NewAccount {
    /// A minimal account spec: id + ledger, everything else default.
    pub fn new(id: u128, ledger: u32) -> Self {
        Self { id, ledger, code: 0, flags: AccountFlags::NONE, user_data_128: 0, user_data_64: 0, user_data_32: 0 }
    }

    /// Set behavior flags (builder).
    pub fn with_flags(mut self, flags: AccountFlags) -> Self {
        self.flags = flags;
        self
    }

    /// Materialize a zero-balance [`Account`] at `timestamp`.
    pub(crate) fn into_account(self, timestamp: u64) -> Account {
        Account {
            id: self.id,
            ledger: self.ledger,
            code: self.code,
            flags: self.flags,
            debits_pending: 0,
            debits_posted: 0,
            credits_pending: 0,
            credits_posted: 0,
            user_data_128: self.user_data_128,
            user_data_64: self.user_data_64,
            user_data_32: self.user_data_32,
            timestamp,
        }
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
    /// else default.
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
}

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

/// Per-item outcome of a batch create. Mirrors TigerBeetle's per-event results.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateResult {
    /// Created/applied successfully.
    Ok,
    /// An item with this `id` already existed; no change made (idempotent).
    Exists,
    /// Rejected by a validation rule; no change made.
    Failed(LedgerError),
}

/// A per-transfer / per-account validation failure (does not abort the batch;
/// surfaced in [`CreateResult::Failed`]). I/O failures are surfaced separately
/// as `anyhow::Error` from the `Ledger` methods.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LedgerError {
    #[error("debit and credit accounts must differ")]
    AccountsMustDiffer,
    #[error("transfer ledger must match both accounts' ledger")]
    LedgerMismatch,
    #[error("account {0:#x} not found")]
    AccountNotFound(u128),
    #[error("debit account would breach debits_must_not_exceed_credits")]
    ExceedsCredits,
    #[error("credit account would breach credits_must_not_exceed_debits")]
    ExceedsDebits,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_flags_contains() {
        let f = AccountFlags::LINKED | AccountFlags::HISTORY;
        assert!(f.contains(AccountFlags::LINKED));
        assert!(f.contains(AccountFlags::HISTORY));
        assert!(!f.contains(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS));
        assert!(AccountFlags::NONE.contains(AccountFlags::NONE));
    }

    #[test]
    fn new_account_materializes_zero_balances() {
        let a = NewAccount::new(7, 1)
            .with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS)
            .into_account(42);
        assert_eq!(a.id, 7);
        assert_eq!(a.ledger, 1);
        assert_eq!(a.timestamp, 42);
        assert_eq!((a.debits_posted, a.credits_posted, a.debits_pending, a.credits_pending), (0, 0, 0, 0));
        assert!(a.flags.contains(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS));
    }

    #[test]
    fn transfer_new_defaults() {
        let t = Transfer::new(1, 10, 20, 500, 1);
        assert_eq!((t.debit_account_id, t.credit_account_id, t.amount), (10, 20, 500));
        assert_eq!(t.flags, TransferFlags::NONE);
        assert_eq!(t.pending_id, 0);
    }

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
}
