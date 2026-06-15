use std::collections::BTreeSet;
use std::sync::Arc;
use bluedb_evidence::{EntryInput, Evidence};

mod harness;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_are_dense_and_gap_free() {
    for _iter in 0..5 {
        let db = Arc::new(harness::memory_db().await);
        let clients = 8usize;
        let per = 100i64;
        let mut handles = Vec::new();
        for _ in 0..clients {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                let ev = Evidence::new(&db, "_");
                let mut got = Vec::new();
                for _ in 0..per {
                    let r = ev
                        .append(
                            "c",
                            vec![EntryInput {
                                etype: "e".into(),
                                payload: vec![],
                                at: String::new(),
                                edges: vec![],
                            }],
                            None,
                        )
                        .await
                        .unwrap();
                    got.push(r.seqs[0]);
                }
                got
            }));
        }
        let mut all = BTreeSet::new();
        for h in handles {
            for s in h.await.unwrap() {
                assert!(all.insert(s), "duplicate seq {s}");
            }
        }
        let total = (clients as i64) * per;
        assert_eq!(
            all,
            (1..=total).collect::<BTreeSet<_>>(),
            "must be exactly 1..=total, no gaps"
        );
    }
}
