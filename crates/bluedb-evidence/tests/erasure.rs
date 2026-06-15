use bluedb_evidence::{EntryInput, Evidence};

mod harness;

fn entry(t: &str) -> EntryInput {
    EntryInput { etype: t.into(), payload: t.as_bytes().to_vec(), at: "T".into(), edges: vec![] }
}

#[tokio::test]
async fn redaction_keeps_seq_meta_and_digest_on_verified_chain() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    for i in 0..5 {
        ev.append("v", vec![entry(&format!("e{i}"))], None).await.unwrap();
    }
    let before = ev.digest("v").await.unwrap();

    ev.redact("v", 3).await.unwrap();
    ev.redact("v", 3).await.unwrap(); // idempotent

    let rows = ev.read_range("v", 1, 5).await.unwrap();
    assert_eq!(rows.len(), 5, "redaction keeps the slot");
    let (_, r3) = &rows[2];
    assert_eq!(r3.payload, Vec::<u8>::new(), "payload blanked");
    assert!(r3.redacted);
    assert_eq!(r3.etype, "e2"); // type/at retained
    assert_eq!(r3.at, "T");
    assert!(r3.leaf_hash.is_some(), "leaf_hash retained");

    assert_eq!(ev.head("v").await.unwrap(), 5, "head unchanged");
    let after = ev.digest("v").await.unwrap();
    assert_eq!(before, after, "digest unchanged by redaction");

    // Proofs still well-formed after redaction.
    assert!(ev.inclusion("v", 3, None).await.is_ok());
}

#[tokio::test]
async fn hard_delete_plain_leaves_gap_but_keeps_head() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    ev.create_chain("p", false).await.unwrap();
    for i in 0..3 {
        ev.append("p", vec![entry(&format!("e{i}"))], None).await.unwrap();
    }
    ev.hard_delete("p", 2, true).await.unwrap();
    let rows = ev.read_range("p", 1, 3).await.unwrap();
    let seqs: Vec<i64> = rows.iter().map(|(s, _)| *s).collect();
    assert_eq!(seqs, vec![1, 3], "seq 2 is a gap");
    assert_eq!(ev.head("p").await.unwrap(), 3, "head unchanged");
}

#[tokio::test]
async fn hard_delete_rejected_on_verified_chain() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    ev.append("v", vec![entry("a")], None).await.unwrap(); // verified by default
    let err = ev.hard_delete("v", 1, true).await.unwrap_err();
    assert!(matches!(err, bluedb_evidence::EvidenceError::VerifiedNoDelete(_)));
    // Entry remains.
    assert_eq!(ev.read_range("v", 1, 1).await.unwrap().len(), 1);
}

#[tokio::test]
async fn redact_and_delete_missing_entry_is_not_found() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    ev.create_chain("p", false).await.unwrap();
    assert!(matches!(
        ev.redact("p", 99).await.unwrap_err(),
        bluedb_evidence::EvidenceError::EntryNotFound { .. }
    ));
    assert!(matches!(
        ev.hard_delete("p", 99, true).await.unwrap_err(),
        bluedb_evidence::EvidenceError::EntryNotFound { .. }
    ));
}
