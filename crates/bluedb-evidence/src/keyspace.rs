//! Native-record key encoding for the evidence substrate, layered on
//! bluedb-sql's tenant-namespaced [`Keyspace`] via its external-namespace tags.

use bluedb_sql::{Keyspace, TAG_EXTERNAL_BASE};

// Ledger uses 0x10-0x15, CDC 0x16, so evidence starts at 0x17.
pub(crate) const TAG_EVIDENCE_ENTRY: u8 = TAG_EXTERNAL_BASE + 7; // 0x17
pub(crate) const TAG_EVIDENCE_SEQ: u8 = TAG_EXTERNAL_BASE + 8; // 0x18
pub(crate) const TAG_EVIDENCE_IDEM: u8 = TAG_EXTERNAL_BASE + 9; // 0x19
pub(crate) const TAG_EVIDENCE_CHAIN: u8 = TAG_EXTERNAL_BASE + 11; // 0x1B
// 0x1A (Merkle) and 0x1C-0x1E (graph) reserved for later plans.

/// Compute the exclusive upper bound for a prefix scan.
///
/// Copied from `bluedb_sql::keyspace` (private there). Increments the last
/// non-`0xFF` byte, dropping trailing `0xFF`s. Returns `None` if all bytes
/// are `0xFF` or the slice is empty.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(end);
        }
        end.pop();
    }
    None
}

/// Builds storage keys for evidence records within one tenant.
pub(crate) struct EvidenceKeyspace {
    ks: Keyspace,
}

impl EvidenceKeyspace {
    pub(crate) fn new(tenant: &str) -> Self {
        Self { ks: Keyspace::new(tenant) }
    }

    /// `<len(chain)::u32-be> <chain_utf8>` — length-prefixed so names form
    /// disjoint, self-delimiting ranges regardless of chain name content.
    fn chain_suffix(chain: &str) -> Vec<u8> {
        let b = chain.as_bytes();
        let mut s = Vec::with_capacity(4 + b.len());
        s.extend_from_slice(&(b.len() as u32).to_be_bytes());
        s.extend_from_slice(b);
        s
    }

    /// Key for one entry: `TAG_EVIDENCE_ENTRY <chain_suffix> <seq::i64-be>`.
    /// Big-endian `i64` makes byte order match numeric order (positive domain).
    pub(crate) fn entry_key(&self, chain: &str, seq: i64) -> Vec<u8> {
        let mut s = Self::chain_suffix(chain);
        s.extend_from_slice(&seq.to_be_bytes());
        self.ks.external_key(TAG_EVIDENCE_ENTRY, &s)
    }

    /// Shared prefix of every entry key for `chain`.
    pub(crate) fn entry_prefix(&self, chain: &str) -> Vec<u8> {
        self.ks.external_key(TAG_EVIDENCE_ENTRY, &Self::chain_suffix(chain))
    }

    /// Exclusive upper bound for a range scan over all entries of `chain`.
    pub(crate) fn entry_prefix_end(&self, chain: &str) -> Vec<u8> {
        prefix_upper_bound(&self.entry_prefix(chain)).expect("non-0xFF prefix")
    }

    /// Key for the per-chain monotonic sequence counter.
    pub(crate) fn seq_key(&self, chain: &str) -> Vec<u8> {
        self.ks.external_key(TAG_EVIDENCE_SEQ, &Self::chain_suffix(chain))
    }

    /// Key for an idempotency record `(chain, idem_token)`.
    pub(crate) fn idem_key(&self, chain: &str, idem: &str) -> Vec<u8> {
        let mut s = Self::chain_suffix(chain);
        s.extend_from_slice(idem.as_bytes());
        self.ks.external_key(TAG_EVIDENCE_IDEM, &s)
    }

    /// Key for the chain-level metadata record.
    pub(crate) fn chain_meta_key(&self, chain: &str) -> Vec<u8> {
        self.ks.external_key(TAG_EVIDENCE_CHAIN, &Self::chain_suffix(chain))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_keys_sort_numerically_not_lexically() {
        let ks = EvidenceKeyspace::new("acme");
        assert!(ks.entry_key("c", 9) < ks.entry_key("c", 10));
        assert!(ks.entry_key("c", 999) < ks.entry_key("c", 1000));
        assert!(ks.entry_key("c", 1) < ks.entry_key("c", i64::MAX));
    }

    #[test]
    fn chain_names_do_not_bleed_into_each_other() {
        let ks = EvidenceKeyspace::new("acme");
        let ab_hi = ks.entry_key("ab", i64::MAX);
        let abc_lo = ks.entry_key("abc", 1);
        assert!(ab_hi < abc_lo);
    }

    #[test]
    fn tenant_isolates_keys() {
        let a = EvidenceKeyspace::new("acme").entry_key("c", 1);
        let b = EvidenceKeyspace::new("globex").entry_key("c", 1);
        assert_ne!(a, b);
    }

    #[test]
    fn entry_prefix_is_exact_prefix_of_entry_keys() {
        let ks = EvidenceKeyspace::new("acme");
        let prefix = ks.entry_prefix("chain1");
        let end = ks.entry_prefix_end("chain1");
        let k1 = ks.entry_key("chain1", 1);
        let k2 = ks.entry_key("chain1", i64::MAX);
        assert!(k1.starts_with(&prefix));
        assert!(k2.starts_with(&prefix));
        assert!(k1 >= prefix && k1 < end);
        assert!(k2 >= prefix && k2 < end);
        // Different chain must not fall inside this range.
        let other = ks.entry_key("chain2", 1);
        assert!(!(other >= prefix && other < end));
    }

    #[test]
    fn seq_and_idem_and_chain_meta_are_distinct_namespaces() {
        let ks = EvidenceKeyspace::new("acme");
        let entry = ks.entry_key("c", 1);
        let seq = ks.seq_key("c");
        let idem = ks.idem_key("c", "tok");
        let meta = ks.chain_meta_key("c");
        // All distinct.
        assert_ne!(entry, seq);
        assert_ne!(entry, idem);
        assert_ne!(entry, meta);
        assert_ne!(seq, idem);
        assert_ne!(seq, meta);
        assert_ne!(idem, meta);
        // Tags sort in order: ENTRY(0x17) < SEQ(0x18) < IDEM(0x19) < CHAIN(0x1B).
        assert!(entry < seq);
        assert!(seq < idem);
        assert!(idem < meta);
    }
}
