use bluedb_evidence::{EntryInput, Evidence};
mod harness;

fn ev_entry(t: &str) -> EntryInput {
    EntryInput { etype: t.into(), payload: t.as_bytes().to_vec(), at: String::new(), edges: vec![] }
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
    let r = ev.append("c", vec![ev_entry("a"), ev_entry("b"), ev_entry("c")], None).await.unwrap();
    assert_eq!(r.base_seq, 0);
    assert_eq!(r.seqs, vec![1, 2, 3]);
    assert_eq!(ev.head("c").await.unwrap(), 3);
}

#[tokio::test]
async fn idempotent_retry_returns_same_seqs_and_conflicts_on_change() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    let first = ev.append("c", vec![ev_entry("x")], Some("k1")).await.unwrap();
    let again = ev.append("c", vec![ev_entry("x")], Some("k1")).await.unwrap();
    assert_eq!(first.seqs, again.seqs);
    assert_eq!(ev.head("c").await.unwrap(), 1);
    let err = ev.append("c", vec![ev_entry("y")], Some("k1")).await.unwrap_err();
    assert!(matches!(err, bluedb_evidence::EvidenceError::IdemConflict));
}
