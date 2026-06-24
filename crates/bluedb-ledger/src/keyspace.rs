//! Native-record key encoding for the ledger, layered on bluedb-sql's
//! tenant-namespaced [`Keyspace`] via its external-namespace tags.

use bluedb_sql::{Keyspace, TAG_EXTERNAL_BASE};

/// Tag for native account records.
const TAG_ACCOUNT: u8 = TAG_EXTERNAL_BASE; // 0x10
/// Tag for native transfer records.
const TAG_TRANSFER: u8 = TAG_EXTERNAL_BASE + 1; // 0x11
/// Tag for the pending-state record (present ⇒ posted/voided; holds the status).
const TAG_PENDING_STATE: u8 = TAG_EXTERNAL_BASE + 2; // 0x12
/// Tag for the timeout expiry index (`<expires_at::u64-be> <pending_id::u128-be>`).
const TAG_EXPIRY: u8 = TAG_EXTERNAL_BASE + 3; // 0x13
/// Tag for the terminal-failure index (present ⇒ this transfer id is burned).
const TAG_FAILED: u8 = TAG_EXTERNAL_BASE + 4; // 0x14
/// Tag for the per-tenant monotonic timestamp watermark (a single key).
const TAG_TS_WATERMARK: u8 = TAG_EXTERNAL_BASE + 5; // 0x15

/// Builds the storage keys for ledger records within one tenant. Account and
/// transfer ids are encoded big-endian so a range scan yields them in id order.
pub(crate) struct LedgerKeyspace {
    ks: Keyspace,
}

impl LedgerKeyspace {
    pub(crate) fn new(tenant: &str) -> Self {
        Self {
            ks: Keyspace::new(tenant),
        }
    }

    pub(crate) fn account_key(&self, id: u128) -> Vec<u8> {
        self.ks.external_key(TAG_ACCOUNT, &id.to_be_bytes())
    }

    pub(crate) fn transfer_key(&self, id: u128) -> Vec<u8> {
        self.ks.external_key(TAG_TRANSFER, &id.to_be_bytes())
    }

    pub(crate) fn pending_state_key(&self, pending_id: u128) -> Vec<u8> {
        self.ks
            .external_key(TAG_PENDING_STATE, &pending_id.to_be_bytes())
    }

    /// The single per-tenant key holding the monotonic timestamp watermark.
    pub(crate) fn watermark_key(&self) -> Vec<u8> {
        self.ks.external_key(TAG_TS_WATERMARK, b"ts")
    }

    /// Expiry-index key for a timed pending: `<expires_at::u64-be> <pending_id::u128-be>`,
    /// so a range scan yields entries in ascending `expires_at` then id order.
    pub(crate) fn expiry_key(&self, expires_at: u64, pending_id: u128) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(24);
        suffix.extend_from_slice(&expires_at.to_be_bytes());
        suffix.extend_from_slice(&pending_id.to_be_bytes());
        self.ks.external_key(TAG_EXPIRY, &suffix)
    }

    /// The prefix shared by every expiry-index entry (scan lower bound).
    pub(crate) fn expiry_prefix(&self) -> Vec<u8> {
        self.ks.external_prefix(TAG_EXPIRY)
    }

    /// Key for the terminal-failure index of a burned transfer id.
    pub(crate) fn failed_key(&self, id: u128) -> Vec<u8> {
        self.ks.external_key(TAG_FAILED, &id.to_be_bytes())
    }

    /// Exclusive scan upper bound covering exactly the entries with
    /// `expires_at <= now` (i.e. everything that has expired by `now`).
    pub(crate) fn expiry_scan_end(&self, now: u64) -> Vec<u8> {
        self.ks
            .external_key(TAG_EXPIRY, &now.saturating_add(1).to_be_bytes())
    }

    #[allow(dead_code)] // used by range scans in later plans (lookup-all / sweeps)
    pub(crate) fn account_prefix(&self) -> Vec<u8> {
        self.ks.external_prefix(TAG_ACCOUNT)
    }

    #[allow(dead_code)]
    pub(crate) fn transfer_prefix(&self) -> Vec<u8> {
        self.ks.external_prefix(TAG_TRANSFER)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_and_transfer_keys_are_distinct_and_ordered() {
        let ks = LedgerKeyspace::new("_");
        let a = ks.account_key(5);
        let x = ks.transfer_key(5);
        assert_ne!(a, x, "same id, different namespace → different keys");
        assert!(a < x, "account tag (0x10) sorts before transfer tag (0x11)");
        assert!(a.starts_with(&ks.account_prefix()));
        assert!(x.starts_with(&ks.transfer_prefix()));
        assert!(!x.starts_with(&ks.account_prefix()));
    }

    #[test]
    fn account_keys_sort_by_id() {
        let ks = LedgerKeyspace::new("_");
        assert!(ks.account_key(1) < ks.account_key(2));
        assert!(ks.account_key(2) < ks.account_key(u128::MAX));
    }

    #[test]
    fn pending_state_key_is_distinct_namespace() {
        let ks = LedgerKeyspace::new("_");
        let r = ks.pending_state_key(5);
        // Distinct from account (0x10) and transfer (0x11) for the same id, and
        // sorts after both (tag 0x12).
        assert_ne!(r, ks.account_key(5));
        assert_ne!(r, ks.transfer_key(5));
        assert!(ks.transfer_key(5) < r);
        // Ordered by id within the namespace.
        assert!(ks.pending_state_key(1) < ks.pending_state_key(2));
    }

    #[test]
    fn expiry_keys_sort_and_bound_correctly() {
        let ks = LedgerKeyspace::new("_");
        // Ordered by expires_at, then by pending_id.
        assert!(ks.expiry_key(100, 5) < ks.expiry_key(100, 6));
        assert!(ks.expiry_key(100, u128::MAX) < ks.expiry_key(101, 0));
        assert!(ks.expiry_key(100, 5).starts_with(&ks.expiry_prefix()));
        // expiry_scan_end(now) excludes entries with expires_at > now, includes <= now.
        let end = ks.expiry_scan_end(100);
        assert!(
            ks.expiry_key(100, u128::MAX) < end,
            "expires_at == now is included"
        );
        assert!(
            end <= ks.expiry_key(101, 0),
            "expires_at == now+1 is excluded"
        );
    }

    #[test]
    fn failed_key_is_distinct_namespace() {
        let ks = LedgerKeyspace::new("_");
        let f = ks.failed_key(5);
        assert_ne!(f, ks.account_key(5));
        assert_ne!(f, ks.transfer_key(5));
        assert_ne!(f, ks.pending_state_key(5));
        assert!(ks.failed_key(1) < ks.failed_key(2));
    }

    #[test]
    fn watermark_key_is_constant_and_distinct() {
        let ks = LedgerKeyspace::new("_");
        let w = ks.watermark_key();
        assert_eq!(w, ks.watermark_key(), "watermark is a single fixed key");
        assert_ne!(w, ks.account_key(0));
        assert_ne!(w, ks.transfer_key(0));
        // tag 0x15 sorts after account (0x10) / transfer (0x11).
        assert!(ks.transfer_key(u128::MAX) < w);
    }
}
