//! Native-record key encoding for the ledger, layered on bluedb-sql's
//! tenant-namespaced [`Keyspace`] via its external-namespace tags.

use bluedb_sql::{Keyspace, TAG_EXTERNAL_BASE};

/// Tag for native account records.
const TAG_ACCOUNT: u8 = TAG_EXTERNAL_BASE; // 0x10
/// Tag for native transfer records.
const TAG_TRANSFER: u8 = TAG_EXTERNAL_BASE + 1; // 0x11
/// Tag for the "pending transfer resolved" marker (present ⇒ posted/voided).
const TAG_PENDING_RESOLVED: u8 = TAG_EXTERNAL_BASE + 2; // 0x12
/// Tag for the per-tenant monotonic timestamp watermark (a single key).
const TAG_TS_WATERMARK: u8 = TAG_EXTERNAL_BASE + 5; // 0x15

/// Builds the storage keys for ledger records within one tenant. Account and
/// transfer ids are encoded big-endian so a range scan yields them in id order.
pub(crate) struct LedgerKeyspace {
    ks: Keyspace,
}

impl LedgerKeyspace {
    pub(crate) fn new(tenant: &str) -> Self {
        Self { ks: Keyspace::new(tenant) }
    }

    pub(crate) fn account_key(&self, id: u128) -> Vec<u8> {
        self.ks.external_key(TAG_ACCOUNT, &id.to_be_bytes())
    }

    pub(crate) fn transfer_key(&self, id: u128) -> Vec<u8> {
        self.ks.external_key(TAG_TRANSFER, &id.to_be_bytes())
    }

    #[allow(dead_code)] // reinstated in Phase B (pending-state record)
    pub(crate) fn pending_resolved_key(&self, pending_id: u128) -> Vec<u8> {
        self.ks.external_key(TAG_PENDING_RESOLVED, &pending_id.to_be_bytes())
    }

    /// The single per-tenant key holding the monotonic timestamp watermark.
    pub(crate) fn watermark_key(&self) -> Vec<u8> {
        self.ks.external_key(TAG_TS_WATERMARK, b"ts")
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
