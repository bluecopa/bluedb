use bluedb_evidence::{ChainMeta, Evidence};
mod harness;

#[tokio::test]
async fn create_chain_is_idempotent_and_rejects_mode_change() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    ev.create_chain("audit", true).await.unwrap();
    ev.create_chain("audit", true).await.unwrap(); // same mode → ok
    let err = ev.create_chain("audit", false).await.unwrap_err();
    assert!(matches!(err, bluedb_evidence::EvidenceError::ChainModeConflict(_)));
    assert_eq!(ev.chain_meta("audit").await.unwrap(), Some(ChainMeta { verified: true }));
    assert_eq!(ev.chain_meta("missing").await.unwrap(), None);
}
