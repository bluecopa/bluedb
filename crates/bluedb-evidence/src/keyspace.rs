//! Native-record key encoding for the evidence substrate, layered on
//! bluedb-sql's tenant-namespaced [`Keyspace`] via its external-namespace tags.

use bluedb_sql::{prefix_upper_bound, Keyspace, TAG_EXTERNAL_BASE};

// Ledger uses 0x10-0x15, CDC 0x16, so evidence starts at 0x17.
pub(crate) const TAG_EVIDENCE_ENTRY: u8 = TAG_EXTERNAL_BASE + 7; // 0x17
pub(crate) const TAG_EVIDENCE_SEQ: u8 = TAG_EXTERNAL_BASE + 8; // 0x18
pub(crate) const TAG_EVIDENCE_IDEM: u8 = TAG_EXTERNAL_BASE + 9; // 0x19
pub(crate) const TAG_EVIDENCE_MERKLE: u8 = TAG_EXTERNAL_BASE + 10; // 0x1A
pub(crate) const TAG_EVIDENCE_CHAIN: u8 = TAG_EXTERNAL_BASE + 11; // 0x1B
pub(crate) const TAG_GRAPH_EDGE: u8 = TAG_EXTERNAL_BASE + 12; // 0x1C  canonical edge
pub(crate) const TAG_GRAPH_OUT: u8 = TAG_EXTERNAL_BASE + 13; // 0x1D  out-adjacency (by weight asc)
pub(crate) const TAG_GRAPH_IN: u8 = TAG_EXTERNAL_BASE + 14; // 0x1E  in-adjacency (by weight asc)

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

    /// Key for the per-chain Merkle frontier (verified chains only).
    pub(crate) fn merkle_key(&self, chain: &str) -> Vec<u8> {
        self.ks.external_key(TAG_EVIDENCE_MERKLE, &Self::chain_suffix(chain))
    }

    /// Canonical edge key: identity `(graph, src, dst, type)`. Value = `weight_obe`.
    pub(crate) fn graph_edge_key(&self, graph: &str, src: &str, dst: &str, etype: &str) -> Vec<u8> {
        let mut s = Vec::new();
        push_lp(&mut s, graph);
        push_lp(&mut s, src);
        push_lp(&mut s, dst);
        push_lp(&mut s, etype);
        self.ks.external_key(TAG_GRAPH_EDGE, &s)
    }

    /// Out-adjacency key: `graph ‖ src ‖ weight_obe ‖ dst ‖ type`. Value = `[1u8]`.
    /// A node's out-edges sort by weight ascending under the `(graph, src)` prefix.
    pub(crate) fn graph_out_key(&self, graph: &str, src: &str, weight: i64, dst: &str, etype: &str) -> Vec<u8> {
        let mut s = Vec::new();
        push_lp(&mut s, graph);
        push_lp(&mut s, src);
        s.extend_from_slice(&weight_obe(weight));
        push_lp(&mut s, dst);
        push_lp(&mut s, etype);
        self.ks.external_key(TAG_GRAPH_OUT, &s)
    }

    /// In-adjacency key: `graph ‖ dst ‖ weight_obe ‖ src ‖ type`. Value = `[1u8]`.
    pub(crate) fn graph_in_key(&self, graph: &str, dst: &str, weight: i64, src: &str, etype: &str) -> Vec<u8> {
        let mut s = Vec::new();
        push_lp(&mut s, graph);
        push_lp(&mut s, dst);
        s.extend_from_slice(&weight_obe(weight));
        push_lp(&mut s, src);
        push_lp(&mut s, etype);
        self.ks.external_key(TAG_GRAPH_IN, &s)
    }
}

/// Order-preserving big-endian encoding of a signed weight: flips the sign bit
/// so two's-complement `i64`s sort numerically as unsigned bytes.
pub(crate) fn weight_obe(w: i64) -> [u8; 8] {
    ((w as u64) ^ (i64::MIN as u64)).to_be_bytes()
}

/// Inverse of [`weight_obe`].
pub(crate) fn weight_from_obe(b: &[u8; 8]) -> i64 {
    (u64::from_be_bytes(*b) ^ (i64::MIN as u64)) as i64
}

/// Append `<len::u32-be> <bytes>` to `buf` (self-delimiting component).
fn push_lp(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
    buf.extend_from_slice(b);
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
    fn weight_obe_is_order_preserving() {
        let ws = [i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];
        for pair in ws.windows(2) {
            assert!(weight_obe(pair[0]) < weight_obe(pair[1]), "{} vs {}", pair[0], pair[1]);
        }
        for w in ws {
            assert_eq!(weight_from_obe(&weight_obe(w)), w);
        }
    }

    #[test]
    fn out_index_orders_a_nodes_edges_by_weight_ascending() {
        let ks = EvidenceKeyspace::new("acme");
        let lo = ks.graph_out_key("g", "u", 1, "a", "");
        let hi = ks.graph_out_key("g", "u", 100, "a", "");
        assert!(lo < hi);
        let neg = ks.graph_out_key("g", "u", -5, "a", "");
        assert!(neg < lo);
    }

    #[test]
    fn graph_tags_are_distinct_namespaces_and_ordered() {
        let ks = EvidenceKeyspace::new("acme");
        let edge = ks.graph_edge_key("g", "u", "v", "");
        let out = ks.graph_out_key("g", "u", 1, "v", "");
        let inn = ks.graph_in_key("g", "v", 1, "u", "");
        assert_ne!(edge, out);
        assert_ne!(edge, inn);
        assert_ne!(out, inn);
        // Tags sort EDGE(0x1C) < OUT(0x1D) < IN(0x1E), and all sort after CHAIN(0x1B).
        assert!(ks.chain_meta_key("g") < edge);
        assert!(edge < out);
        assert!(out < inn);
    }

    #[test]
    fn node_ids_do_not_bleed_via_length_prefix() {
        let ks = EvidenceKeyspace::new("acme");
        let ab = ks.graph_out_key("g", "ab", 1, "x", "");
        let abc = ks.graph_out_key("g", "abc", 1, "x", "");
        assert_ne!(ab, abc);
        let g1 = ks.graph_edge_key("g1", "u", "v", "");
        let g2 = ks.graph_edge_key("g2", "u", "v", "");
        assert_ne!(g1, g2);
    }

    #[test]
    fn graph_keys_are_tenant_isolated() {
        let a = EvidenceKeyspace::new("acme").graph_edge_key("g", "u", "v", "");
        let b = EvidenceKeyspace::new("globex").graph_edge_key("g", "u", "v", "");
        assert_ne!(a, b);
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
        let merkle = ks.merkle_key("c");
        assert_ne!(merkle, entry);
        assert_ne!(merkle, seq);
        assert_ne!(merkle, idem);
        assert_ne!(merkle, meta);
        // Tags sort in order: ENTRY(0x17) < SEQ(0x18) < IDEM(0x19) < MERKLE(0x1A) < CHAIN(0x1B).
        assert!(entry < seq);
        assert!(seq < idem);
        assert!(idem < merkle);
        assert!(merkle < meta);
    }
}
