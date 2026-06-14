//! Native-record key encoding for the ledger, layered on bluedb-sql's
//! tenant-namespaced [`Keyspace`] via its external-namespace tags.

use bluedb_sql::{Keyspace, TAG_EXTERNAL_BASE};

/// Tag for native account records.
const TAG_ACCOUNT: u8 = TAG_EXTERNAL_BASE; // 0x10
/// Tag for native transfer records.
const TAG_TRANSFER: u8 = TAG_EXTERNAL_BASE + 1; // 0x11

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
}
