//! Pure RFC 6962 (Certificate Transparency / Trillian) Merkle tree math over
//! 32-byte hashes. No I/O. Leaves passed to `merkle_root`/proofs are already
//! `leaf_hash`es, so the Merkle Tree Hash of a single leaf is the leaf itself.
//!
//! Domain separation: leaf prefix `0x00`, node prefix `0x01` (RFC 6962 §2.1).
//! Indices here are 0-based; the public `seq` is 1-based (leaf index = seq - 1).

use sha2::{Digest, Sha256};

use crate::model::{EdgeDelta, EdgeOp, Merge};

/// Stable 1-byte discriminant for an edge op, used in the leaf framing so the
/// hash attests the exact op. Matches the idempotency fingerprint in `chain`.
fn op_discriminant(op: &EdgeOp) -> u8 {
    match op {
        EdgeOp::Upsert { merge: Merge::Set } => 0,
        EdgeOp::Upsert { merge: Merge::Max } => 1,
        EdgeOp::Delete => 2,
    }
}

/// Digest of an empty tree: `SHA256(<empty>)` (RFC 6962 §2.1).
pub(crate) fn empty_root() -> [u8; 32] {
    Sha256::new().finalize().into()
}

/// Internal node hash: `SHA256(0x01 ‖ left ‖ right)`.
pub(crate) fn node_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x01]);
    h.update(left);
    h.update(right);
    h.finalize().into()
}

/// Leaf hash: `SHA256(0x00 ‖ frame(type, payload, at, edges))`. Each field is
/// length-delimited (u64-be length, then bytes); edges are sorted by
/// `(graph, src, dst, etype, op)` then framed (each field length-delimited,
/// then `weight` as 8 BE bytes, then the op discriminant). A copy of the exact
/// bytes — no payload normalization (R3 byte-exactness preserved).
pub(crate) fn leaf_hash(etype: &str, payload: &[u8], at: &str, edges: &[EdgeDelta]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x00]);
    for field in [etype.as_bytes(), payload, at.as_bytes()] {
        h.update((field.len() as u64).to_be_bytes());
        h.update(field);
    }
    let mut sorted: Vec<&EdgeDelta> = edges.iter().collect();
    sorted.sort_by(|a, b| {
        (&a.graph, &a.src, &a.dst, &a.etype, op_discriminant(&a.op))
            .cmp(&(&b.graph, &b.src, &b.dst, &b.etype, op_discriminant(&b.op)))
    });
    h.update((sorted.len() as u64).to_be_bytes());
    for e in sorted {
        for field in [e.graph.as_bytes(), e.src.as_bytes(), e.dst.as_bytes(), e.etype.as_bytes()] {
            h.update((field.len() as u64).to_be_bytes());
            h.update(field);
        }
        h.update(e.weight.to_be_bytes());
        h.update([op_discriminant(&e.op)]);
    }
    h.finalize().into()
}

/// Largest power of two strictly less than `n`. Requires `n >= 2`.
pub(crate) fn largest_pow2_lt(n: usize) -> usize {
    debug_assert!(n >= 2);
    let mut k = 1;
    while k << 1 < n {
        k <<= 1;
    }
    k
}

/// RFC 6962 Merkle Tree Hash over a slice of leaf hashes (the from-scratch
/// reference; O(N)). Empty → `empty_root`; single → that leaf. Kept as the
/// in-crate test reference for the storage-backed proofs (test-only since the
/// production proof path now assembles from persisted nodes).
#[cfg(test)]
pub(crate) fn merkle_root(leaves: &[[u8; 32]]) -> [u8; 32] {
    match leaves.len() {
        0 => empty_root(),
        1 => leaves[0],
        n => {
            let k = largest_pow2_lt(n);
            node_hash(&merkle_root(&leaves[..k]), &merkle_root(&leaves[k..]))
        }
    }
}

impl crate::model::Frontier {
    /// Like [`push`], but returns the complete-subtree parent nodes created by the
    /// carry-merge, each as `(level, index, hash)` where the node is the root over
    /// leaves `[index·2^level, (index+1)·2^level)`. Used to persist internal nodes.
    pub(crate) fn push_emit(&mut self, leaf: [u8; 32]) -> Vec<(u8, u64, [u8; 32])> {
        let mut emitted = Vec::new();
        let mut carry = leaf;
        let mut carry_level: u8 = 0;
        let mut carry_start: u64 = self.size as u64; // leaf offset of the carry's range
        let mut s = self.size;
        while s & 1 == 1 {
            let left = self.peaks.pop().expect("frontier peak underflow");
            carry = node_hash(&left, &carry);
            carry_start -= 1u64 << carry_level; // left covers 2^carry_level leaves to the left
            carry_level += 1;
            let index = carry_start >> carry_level;
            emitted.push((carry_level, index, carry));
            s >>= 1;
        }
        self.peaks.push(carry);
        self.size += 1;
        emitted
    }

    /// Fold one new leaf into the frontier (RFC 6962 incremental append): push
    /// it as a height-0 peak, then carry-merge equal-height peaks. The number of
    /// merges equals the count of trailing 1-bits in the old size. O(log N).
    /// Delegates to [`push_emit`]; kept for the in-crate frontier tests (the
    /// append path uses `push_emit` directly to persist the merged nodes).
    #[cfg(test)]
    pub(crate) fn push(&mut self, leaf: [u8; 32]) {
        let _ = self.push_emit(leaf);
    }

    /// Digest at the current size: "bag the peaks" right→left. O(log N).
    pub(crate) fn root(&self) -> [u8; 32] {
        let mut iter = self.peaks.iter().rev();
        match iter.next() {
            None => empty_root(),
            Some(&last) => {
                let mut acc = last;
                for p in iter {
                    acc = node_hash(p, &acc);
                }
                acc
            }
        }
    }
}

/// RFC 6962 inclusion proof (PATH(m, D[n])) for the 0-based `index` into
/// `leaves`. The audit path lists sibling subtree roots bottom-up. O(N).
/// Panics if `index >= leaves.len()`. Test-only reference for the storage-backed
/// `proof::inclusion`.
#[cfg(test)]
pub(crate) fn inclusion_proof(leaves: &[[u8; 32]], index: usize) -> Vec<[u8; 32]> {
    let n = leaves.len();
    assert!(index < n, "inclusion index out of range");
    if n == 1 {
        return Vec::new();
    }
    let k = largest_pow2_lt(n);
    if index < k {
        let mut p = inclusion_proof(&leaves[..k], index);
        p.push(merkle_root(&leaves[k..]));
        p
    } else {
        let mut p = inclusion_proof(&leaves[k..], index - k);
        p.push(merkle_root(&leaves[..k]));
        p
    }
}

/// RFC 6962 consistency proof that the tree at size `first` is a prefix of the
/// tree formed by all of `leaves` (size = `leaves.len()`). `first` is a leaf
/// count, `1 <= first <= leaves.len()`. O(N). `first == 0` → empty proof.
/// Test-only reference for the storage-backed `proof::consistency`.
#[cfg(test)]
pub(crate) fn consistency_proof(leaves: &[[u8; 32]], first: usize) -> Vec<[u8; 32]> {
    if first == 0 {
        return Vec::new();
    }
    subproof(first, leaves, true)
}

#[cfg(test)]
fn subproof(m: usize, leaves: &[[u8; 32]], b: bool) -> Vec<[u8; 32]> {
    let n = leaves.len();
    if m == n {
        // The subtree is complete: include its root unless it's the original
        // tree the verifier already knows (`b == true`).
        if b {
            Vec::new()
        } else {
            vec![merkle_root(leaves)]
        }
    } else {
        let k = largest_pow2_lt(n);
        if m <= k {
            let mut p = subproof(m, &leaves[..k], b);
            p.push(merkle_root(&leaves[k..]));
            p
        } else {
            let mut p = subproof(m - k, &leaves[k..], false);
            p.push(merkle_root(&leaves[..k]));
            p
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // SHA256("") — the RFC 6962 empty-tree digest. Golden vector.
    const EMPTY_SHA256_HEX: &str =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    fn hex(b: &[u8; 32]) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        for x in b {
            write!(s, "{x:02x}").unwrap();
        }
        s
    }

    #[test]
    fn empty_root_matches_sha256_of_empty() {
        assert_eq!(hex(&empty_root()), EMPTY_SHA256_HEX);
    }

    #[test]
    fn node_hash_is_order_sensitive() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        assert_ne!(node_hash(&a, &b), node_hash(&b, &a));
    }

    #[test]
    fn leaf_hash_is_deterministic_and_field_sensitive() {
        let h1 = leaf_hash("t", b"payload", "at", &[]);
        let h2 = leaf_hash("t", b"payload", "at", &[]);
        assert_eq!(h1, h2);
        assert_ne!(h1, leaf_hash("t", b"payloaD", "at", &[]));
        assert_ne!(h1, leaf_hash("t", b"payload", "AT", &[]));
        // The 0x00 leaf prefix separates leaf and node domains.
        assert_ne!(leaf_hash("", b"", "", &[]), empty_root());
    }

    #[test]
    fn leaf_hash_edge_order_invariant() {
        let e = |dst: &str| EdgeDelta {
            graph: "g".into(),
            src: "a".into(),
            dst: dst.into(),
            weight: 1,
            etype: String::new(),
            op: EdgeOp::Upsert { merge: Merge::Set },
        };
        // Same edge set, different input order → identical leaf hash.
        assert_eq!(
            leaf_hash("t", b"p", "at", &[e("b"), e("c")]),
            leaf_hash("t", b"p", "at", &[e("c"), e("b")]),
        );
        // Different edge content → different hash.
        assert_ne!(
            leaf_hash("t", b"p", "at", &[e("b")]),
            leaf_hash("t", b"p", "at", &[e("b"), e("c")]),
        );
    }

    // Deterministic pseudo-random leaves (no Math.random; derive from index).
    fn leaf_n(i: usize) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update((i as u64).to_be_bytes());
        h.finalize().into()
    }

    #[test]
    fn push_emit_nodes_match_reference_subtree_roots() {
        use crate::model::Frontier;
        let leaves: Vec<[u8; 32]> = (0..300).map(leaf_n).collect();
        let mut f = Frontier::default();
        for n in 0..leaves.len() {
            for (level, index, hash) in f.push_emit(leaves[n]) {
                let lo = (index as usize) << level;
                let hi = lo + (1usize << level);
                assert!(hi <= n + 1, "node (L{level},{index}) exceeds appended leaves at size {}", n + 1);
                assert_eq!(hash, merkle_root(&leaves[lo..hi]), "node (L{level},{index}) wrong at size {}", n + 1);
            }
        }
        // push (delegating) still yields the same root as the reference.
        let mut g = Frontier::default();
        for (i, &lh) in leaves.iter().enumerate() {
            assert_eq!(g.root(), merkle_root(&leaves[..i]));
            g.push(lh);
        }
    }

    #[test]
    fn frontier_root_agrees_with_reference_root_for_all_sizes() {
        use crate::model::Frontier;
        let leaves: Vec<[u8; 32]> = (0..300).map(leaf_n).collect();
        let mut f = Frontier::default();
        assert_eq!(f.root(), empty_root());
        for n in 0..leaves.len() {
            assert_eq!(
                f.root(),
                merkle_root(&leaves[..n]),
                "frontier root diverged at size {n}"
            );
            // peak count == popcount(size)
            assert_eq!(f.peaks.len(), (f.size as u64).count_ones() as usize);
            f.push(leaves[n]);
        }
    }

    #[test]
    fn merkle_root_small_cases_are_explicit() {
        let h: Vec<[u8; 32]> = (0..4).map(leaf_n).collect();
        // 2 leaves: node(h0, h1)
        assert_eq!(merkle_root(&h[..2]), node_hash(&h[0], &h[1]));
        // 3 leaves: node(node(h0,h1), h2)   (k = 2)
        assert_eq!(
            merkle_root(&h[..3]),
            node_hash(&node_hash(&h[0], &h[1]), &h[2])
        );
        // 4 leaves: node(node(h0,h1), node(h2,h3))
        assert_eq!(
            merkle_root(&h[..4]),
            node_hash(&node_hash(&h[0], &h[1]), &node_hash(&h[2], &h[3]))
        );
    }

    /// Independent RFC 6962 inclusion verifier (Trillian formulation). Returns
    /// the reconstructed root; the caller compares to the trusted root.
    fn root_from_inclusion(leaf: [u8; 32], index: usize, size: usize, proof: &[[u8; 32]]) -> Option<[u8; 32]> {
        if index >= size {
            return None;
        }
        let x = (index ^ (size - 1)) as u64;
        let inner = (64 - x.leading_zeros()) as usize; // bits.Len64
        let border = ((index >> inner) as u64).count_ones() as usize;
        if proof.len() != inner + border {
            return None;
        }
        let mut res = leaf;
        for (i, h) in proof[..inner].iter().enumerate() {
            if (index >> i) & 1 == 0 {
                res = node_hash(&res, h);
            } else {
                res = node_hash(h, &res);
            }
        }
        for h in &proof[inner..] {
            res = node_hash(h, &res);
        }
        Some(res)
    }

    #[test]
    fn inclusion_proofs_verify_for_every_index_and_size() {
        let leaves: Vec<[u8; 32]> = (0..130).map(leaf_n).collect();
        for size in 1..=leaves.len() {
            let root = merkle_root(&leaves[..size]);
            for index in 0..size {
                let proof = inclusion_proof(&leaves[..size], index);
                assert_eq!(
                    root_from_inclusion(leaves[index], index, size, &proof),
                    Some(root),
                    "inclusion failed: index {index} of size {size}"
                );
                // Tamper: flip a byte of the first proof node → must NOT verify.
                if let Some(first) = proof.first() {
                    let mut bad = proof.clone();
                    bad[0][0] ^= 0xff;
                    let _ = first;
                    assert_ne!(
                        root_from_inclusion(leaves[index], index, size, &bad),
                        Some(root),
                        "tampered inclusion verified: index {index} of size {size}"
                    );
                }
            }
        }
    }

    fn chain_border_right(mut seed: [u8; 32], proof: &[[u8; 32]]) -> [u8; 32] {
        for h in proof {
            seed = node_hash(h, &seed);
        }
        seed
    }
    fn chain_inner(mut seed: [u8; 32], proof: &[[u8; 32]], index: usize) -> [u8; 32] {
        for (i, h) in proof.iter().enumerate() {
            if (index >> i) & 1 == 0 {
                seed = node_hash(&seed, h);
            } else {
                seed = node_hash(h, &seed);
            }
        }
        seed
    }
    fn chain_inner_right(mut seed: [u8; 32], proof: &[[u8; 32]], index: usize) -> [u8; 32] {
        for (i, h) in proof.iter().enumerate() {
            if (index >> i) & 1 == 1 {
                seed = node_hash(h, &seed);
            }
        }
        seed
    }

    /// Independent RFC 6962 consistency verifier. True iff `proof` proves the
    /// size-`first` tree (root `root1`) is a prefix of the size-`second` tree
    /// (root `root2`).
    fn verify_consistency(
        first: usize,
        second: usize,
        proof: &[[u8; 32]],
        root1: [u8; 32],
        root2: [u8; 32],
    ) -> bool {
        if first > second {
            return false;
        }
        if first == second {
            return proof.is_empty() && root1 == root2;
        }
        if first == 0 {
            return proof.is_empty();
        }
        // decompInclProof(first-1, second)
        let x = ((first - 1) ^ (second - 1)) as u64;
        let mut inner = (64 - x.leading_zeros()) as usize;
        let border = (((first - 1) >> inner) as u64).count_ones() as usize;
        let shift = (first as u64).trailing_zeros() as usize;
        inner -= shift;
        let (seed, start) = if first == (1usize << shift) {
            (root1, 0)
        } else {
            (proof[0], 1)
        };
        if proof.len() != start + inner + border {
            return false;
        }
        let proof = &proof[start..];
        let mask = (first - 1) >> shift;
        let hash1 = chain_border_right(chain_inner_right(seed, &proof[..inner], mask), &proof[inner..]);
        let hash2 = chain_border_right(chain_inner(seed, &proof[..inner], mask), &proof[inner..]);
        hash1 == root1 && hash2 == root2
    }

    #[test]
    fn consistency_proofs_verify_for_all_first_le_second() {
        let leaves: Vec<[u8; 32]> = (0..70).map(leaf_n).collect();
        for second in 1..=leaves.len() {
            let root2 = merkle_root(&leaves[..second]);
            for first in 1..=second {
                let root1 = merkle_root(&leaves[..first]);
                let proof = consistency_proof(&leaves[..second], first);
                assert!(
                    verify_consistency(first, second, &proof, root1, root2),
                    "consistency failed: {first} -> {second}"
                );
                if !proof.is_empty() {
                    let mut bad = proof.clone();
                    bad[0][0] ^= 0xff;
                    assert!(
                        !verify_consistency(first, second, &bad, root1, root2),
                        "tampered consistency verified: {first} -> {second}"
                    );
                }
            }
        }
    }
}
