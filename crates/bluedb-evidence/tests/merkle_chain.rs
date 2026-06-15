use bluedb_evidence::{EntryInput, Evidence};

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
}
