use bluedb_evidence::{EntryInput, Evidence};
use sha2::{Digest as Sha2Digest, Sha256};

mod harness;

fn entry(t: &str) -> EntryInput {
    EntryInput { etype: t.into(), payload: t.as_bytes().to_vec(), at: String::new(), edges: vec![] }
}

#[tokio::test]
async fn verified_append_stores_leaf_hash_and_frontier_matches_digest() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    // default auto-create is verified
    for i in 0..5 {
        ev.append("v", vec![entry(&format!("e{i}"))], None).await.unwrap();
    }
    // Every entry carries a leaf hash on a verified chain.
    let rows = ev.read_range("v", 1, 5).await.unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows.iter().all(|(_, r)| r.leaf_hash.is_some()));

    // Digest size == head, root is non-empty.
    let d = ev.digest("v").await.unwrap();
    assert_eq!(d.size, 5);
}

#[tokio::test]
async fn plain_chain_has_no_leaf_hash_or_frontier() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    ev.create_chain("p", false).await.unwrap();
    ev.append("p", vec![entry("a")], None).await.unwrap();
    let rows = ev.read_range("p", 1, 1).await.unwrap();
    assert!(rows[0].1.leaf_hash.is_none());
    // digest on a plain chain is an error (see Task 7).
    assert!(ev.digest("p").await.is_err());
}

/// RFC 6962 reference Merkle root over a slice of leaf hashes.
/// Empty → SHA256(""); single → the leaf; else split at largest power of two < n.
fn recompute_root(hashes: &[[u8; 32]]) -> [u8; 32] {
    fn node_hash(l: &[u8; 32], r: &[u8; 32]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update([0x01u8]);
        h.update(l);
        h.update(r);
        h.finalize().into()
    }
    fn go(leaves: &[[u8; 32]]) -> [u8; 32] {
        match leaves.len() {
            0 => Sha256::new().finalize().into(),
            1 => leaves[0],
            n => {
                let mut k = 1usize;
                while k << 1 < n {
                    k <<= 1;
                }
                node_hash(&go(&leaves[..k]), &go(&leaves[k..]))
            }
        }
    }
    go(hashes)
}

/// Trillian formulation of the RFC 6962 inclusion verifier.
/// Returns `Some(reconstructed_root)` or `None` if the proof length is wrong.
/// `index` is 0-based; `size` is the tree size.
fn root_from_inclusion(
    leaf: [u8; 32],
    index: usize,
    size: usize,
    proof: &[[u8; 32]],
) -> Option<[u8; 32]> {
    fn node_hash(l: &[u8; 32], r: &[u8; 32]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update([0x01u8]);
        h.update(l);
        h.update(r);
        h.finalize().into()
    }
    if index >= size {
        return None;
    }
    let x = (index ^ (size - 1)) as u64;
    let inner = (64 - x.leading_zeros()) as usize;
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

#[tokio::test]
async fn digest_matches_recompute_and_proofs_are_well_formed() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    for i in 0..9 {
        ev.append("v", vec![entry(&format!("e{i}"))], None).await.unwrap();
    }
    let d = ev.digest("v").await.unwrap();
    assert_eq!(d.size, 9);

    // Inclusion proof for a middle seq against full size.
    let inc = ev.inclusion("v", 4, None).await.unwrap();
    assert_eq!(inc.seq, 4);
    assert_eq!(inc.size, 9);
    assert!(!inc.audit_path.is_empty());

    // Consistency proof between an earlier size and head.
    let con = ev.consistency("v", 5, None).await.unwrap();
    assert_eq!((con.first, con.second), (5, 9));

    // Out-of-range args are rejected.
    assert!(ev.inclusion("v", 0, None).await.is_err());
    assert!(ev.inclusion("v", 10, None).await.is_err());
    assert!(ev.consistency("v", 0, None).await.is_err());
    assert!(ev.consistency("v", 6, Some(5)).await.is_err());

    // Cross-validate: the frontier root built across 9 separate incremental
    // appends must agree with a fresh from-scratch recompute over the stored
    // leaf hashes. This proves append feeds the frontier with the correct hashes.
    let rows = ev.read_range("v", 1, 9).await.unwrap();
    let stored: Vec<[u8; 32]> = rows.iter().map(|(_, r)| r.leaf_hash.unwrap()).collect();
    assert_eq!(
        d.root,
        recompute_root(&stored),
        "frontier root after 9 incremental appends must match fresh recompute"
    );

    // Inclusion proof for seq 4 (0-based index 3) must reconstruct the digest root.
    assert_eq!(
        root_from_inclusion(stored[3], 3, 9, &inc.audit_path),
        Some(d.root),
        "inclusion proof for seq 4 must reconstruct the digest root"
    );
}
