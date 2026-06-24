use bluedb_evidence::{EdgeDelta, EdgeOp, EntryInput, Evidence, Merge};
mod harness;

fn ev_entry(t: &str) -> EntryInput {
    EntryInput {
        etype: t.into(),
        payload: t.as_bytes().to_vec(),
        at: String::new(),
        edges: vec![],
    }
}

#[tokio::test]
async fn single_appends_yield_dense_seqs() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    for i in 1..=5i64 {
        let r = ev.append("c", vec![ev_entry("e")], None).await.unwrap();
        assert_eq!(r.base_seq, i - 1);
        assert_eq!(r.seqs, vec![i]);
    }
    assert_eq!(ev.head("c").await.unwrap(), 5);
}

#[tokio::test]
async fn batch_yields_contiguous_seqs() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    let r = ev
        .append("c", vec![ev_entry("a"), ev_entry("b"), ev_entry("c")], None)
        .await
        .unwrap();
    assert_eq!(r.base_seq, 0);
    assert_eq!(r.seqs, vec![1, 2, 3]);
    assert_eq!(ev.head("c").await.unwrap(), 3);
}

#[tokio::test]
async fn idempotent_retry_returns_same_seqs_and_conflicts_on_change() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    let first = ev
        .append("c", vec![ev_entry("x")], Some("k1"))
        .await
        .unwrap();
    let again = ev
        .append("c", vec![ev_entry("x")], Some("k1"))
        .await
        .unwrap();
    assert_eq!(first.seqs, again.seqs);
    assert_eq!(ev.head("c").await.unwrap(), 1);
    let err = ev
        .append("c", vec![ev_entry("y")], Some("k1"))
        .await
        .unwrap_err();
    assert!(matches!(err, bluedb_evidence::EvidenceError::IdemConflict));
}

// Fix 1: same etype/payload/at but different edges → IdemConflict.
#[tokio::test]
async fn idem_conflict_on_edge_change() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    let edge_a = EdgeDelta {
        graph: "g".into(),
        src: "A".into(),
        dst: "B".into(),
        weight: 1,
        etype: String::new(),
        op: EdgeOp::Upsert { merge: Merge::Set },
    };
    let edge_b = EdgeDelta {
        graph: "g".into(),
        src: "A".into(),
        dst: "C".into(), // different dst
        weight: 1,
        etype: String::new(),
        op: EdgeOp::Upsert { merge: Merge::Set },
    };
    let entry_with_edge_a = EntryInput {
        etype: "ev".into(),
        payload: b"data".to_vec(),
        at: "2026-01-01".into(),
        edges: vec![edge_a],
    };
    let entry_with_edge_b = EntryInput {
        etype: "ev".into(),
        payload: b"data".to_vec(),
        at: "2026-01-01".into(),
        edges: vec![edge_b],
    };
    ev.append("c", vec![entry_with_edge_a], Some("k-edges"))
        .await
        .unwrap();
    let err = ev
        .append("c", vec![entry_with_edge_b], Some("k-edges"))
        .await
        .unwrap_err();
    assert!(matches!(err, bluedb_evidence::EvidenceError::IdemConflict));
}

// Fix 4: empty append leaves head unchanged and returns empty seqs.
#[tokio::test]
async fn empty_append_is_noop() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    ev.append("c", vec![ev_entry("x")], None).await.unwrap();
    let head_before = ev.head("c").await.unwrap();
    let r = ev.append("c", vec![], None).await.unwrap();
    assert_eq!(r.seqs, Vec::<i64>::new());
    assert_eq!(r.base_seq, head_before);
    assert_eq!(ev.head("c").await.unwrap(), head_before);
}
