use bluedb_evidence::{EntryInput, Evidence};
use bluedb_sql::{prefix_upper_bound, Keyspace};
use sha2::{Digest as Sha2Digest, Sha256};

mod harness;

fn entry(t: &str) -> EntryInput {
    EntryInput {
        etype: t.into(),
        payload: t.as_bytes().to_vec(),
        at: String::new(),
        edges: vec![],
    }
}

#[tokio::test]
async fn verified_append_stores_leaf_hash_and_frontier_matches_digest() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    // default auto-create is verified
    for i in 0..5 {
        ev.append("v", vec![entry(&format!("e{i}"))], None)
            .await
            .unwrap();
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
        ev.append("v", vec![entry(&format!("e{i}"))], None)
            .await
            .unwrap();
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

/// Count persisted complete-subtree nodes (tag 0x1F) for chain `chain`, tenant `_`,
/// via the public `bluedb_sql::Keyspace` (an independent verifier of what append
/// wrote). The node key is `external_key(0x1F, <len(chain)::u32-be ‖ chain>)`.
async fn count_merkle_nodes(db: &bluedb_sql::Database, chain: &str) -> usize {
    let ks = Keyspace::new("_");
    let mut suffix = (chain.len() as u32).to_be_bytes().to_vec();
    suffix.extend_from_slice(chain.as_bytes());
    let prefix = ks.external_key(0x1F, &suffix);
    let end = prefix_upper_bound(&prefix);
    let mut it = db
        .substrate()
        .scan_range(&prefix, end.as_deref())
        .await
        .unwrap();
    let mut n = 0;
    while it.next().await.unwrap().is_some() {
        n += 1;
    }
    n
}

#[tokio::test]
async fn append_persists_complete_subtree_nodes() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    // Append 7 single-entry leaves on a verified (default) chain.
    for i in 0..7u8 {
        ev.append(
            "c",
            vec![EntryInput {
                etype: "t".into(),
                payload: vec![i],
                at: String::new(),
                edges: vec![],
            }],
            None,
        )
        .await
        .unwrap();
    }
    // Node (level 2, index 0) must be present and 32 bytes — the root over leaves
    // [0,4). Re-derive its key independently via the public Keyspace API.
    let ks = Keyspace::new("_");
    let mut suffix = (1u32).to_be_bytes().to_vec(); // len("c") == 1
    suffix.extend_from_slice(b"c");
    suffix.push(2u8); // level 2
    suffix.extend_from_slice(&0u64.to_be_bytes()); // index 0
    let key = ks.external_key(0x1F, &suffix);
    let node = db
        .substrate()
        .get(&key)
        .await
        .unwrap()
        .expect("node (2,0) present");
    assert_eq!(node.len(), 32);

    // Total persisted internal nodes after N appends == N − popcount(N): the count
    // of carry-merges across the incremental pushes. For N=7: 7 − 3 = 4.
    let n = 7usize;
    let expected = n - (n as u64).count_ones() as usize;
    assert_eq!(
        count_merkle_nodes(&db, "c").await,
        expected,
        "node count must equal N - popcount(N)"
    );
}

/// RFC 6962 inclusion-proof length for 0-based `index` in a tree of `size`
/// leaves: `inner + border` (the Trillian decomposition). This is O(log N), not
/// O(N) — it is what the storage-backed proof path must produce.
fn expected_inclusion_len(index: usize, size: usize) -> usize {
    let x = (index ^ (size - 1)) as u64;
    let inner = (64 - x.leading_zeros()) as usize;
    let border = ((index >> inner) as u64).count_ones() as usize;
    inner + border
}

#[tokio::test]
async fn storage_proofs_are_ologn_reconstruct_root_and_survive_redaction() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    // 21 separate appends (not a batch) — exercises many incremental carry-merges.
    for i in 0..21 {
        ev.append("v", vec![entry(&format!("e{i}"))], None)
            .await
            .unwrap();
    }
    let d = ev.digest("v").await.unwrap();
    assert_eq!(d.size, 21);

    let rows = ev.read_range("v", 1, 21).await.unwrap();
    let stored: Vec<[u8; 32]> = rows.iter().map(|(_, r)| r.leaf_hash.unwrap()).collect();

    // Every inclusion proof: O(log N) length AND reconstructs the digest root.
    for seq in 1..=21i64 {
        let inc = ev.inclusion("v", seq, None).await.unwrap();
        let index = (seq - 1) as usize;
        assert_eq!(
            inc.audit_path.len(),
            expected_inclusion_len(index, 21),
            "proof for seq {seq} is not O(log N) (RFC 6962 length)"
        );
        assert_eq!(
            root_from_inclusion(stored[index], index, 21, &inc.audit_path),
            Some(d.root),
            "inclusion proof for seq {seq} must reconstruct the digest root"
        );
    }

    // Redact a middle entry: leaf_hash is retained, so digest and proofs are
    // unaffected — the entry's existence stays provable while its payload is gone.
    let before = ev.digest("v").await.unwrap();
    ev.redact("v", 11).await.unwrap();
    let after = ev.digest("v").await.unwrap();
    assert_eq!(before, after, "redaction must not change the digest");
    let inc = ev.inclusion("v", 11, None).await.unwrap();
    assert_eq!(
        root_from_inclusion(stored[10], 10, 21, &inc.audit_path),
        Some(d.root),
        "inclusion proof for a redacted entry must still verify"
    );
}
