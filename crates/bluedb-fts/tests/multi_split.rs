//! Multi-split search across an index made of 2+ splits stored in SlateDB,
//! driven by a manifest, plus a manifest load-from-SlateDB round trip.

use std::sync::Arc;

use bluedb_fts::indexer::Indexer;
use bluedb_fts::manifest::{Manifest, SplitMeta};
use bluedb_fts::open::open_split_lazy;
use bluedb_fts::search::{multi_split_count, multi_split_search, SplitHandle};
use bluedb_storage::SlateDbBlobStore;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::Db;
use tantivy::schema::{Field, Schema, STORED, TEXT};
use tantivy::TantivyDocument;

const INDEX_ID: &str = "recon-2026";

fn schema() -> (Schema, Field, Field) {
    let mut sb = Schema::builder();
    let title = sb.add_text_field("title", TEXT | STORED);
    let body = sb.add_text_field("body", TEXT);
    (sb.build(), title, body)
}

fn doc(title: Field, t: &str, body: Field, b: &str) -> TantivyDocument {
    let mut d = TantivyDocument::default();
    d.add_text(title, t);
    d.add_text(body, b);
    d
}

#[tokio::test]
async fn search_across_two_splits_in_slatedb() {
    let (schema_a, title, body) = schema();
    let (schema_b, _t2, _b2) = schema();

    // Split A: two docs, "financial" appears once.
    let split_a = Indexer::new()
        .build_with_hotcache(
            schema_a,
            vec![
                doc(title, "ledger reconciliation", body, "match invoices financial"),
                doc(title, "payroll run", body, "salaries wages employees"),
            ],
        )
        .expect("build split A");

    // Split B: two docs, "financial" appears once, "revenue" once.
    let split_b = Indexer::new()
        .build_with_hotcache(
            schema_b,
            vec![
                doc(title, "quarterly revenue report", body, "revenue deferred financial"),
                doc(title, "audit notes", body, "controls testing samples"),
            ],
        )
        .expect("build split B");

    // ---- store both splits + a manifest in SlateDB ----
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("bluedb-fts-multi-test", object_store)
            .await
            .expect("open slatedb"),
    );

    let meta_a = SplitMeta::new("split-a", 2, split_a.len() as u64, 1);
    let meta_b = SplitMeta::new("split-b", 2, split_b.len() as u64, 1);
    db.put(meta_a.blob_key(INDEX_ID).as_bytes(), &split_a[..])
        .await
        .expect("put A");
    db.put(meta_b.blob_key(INDEX_ID).as_bytes(), &split_b[..])
        .await
        .expect("put B");

    let mut manifest = Manifest::new(INDEX_ID);
    manifest.push(meta_a).push(meta_b);
    db.put(manifest.blob_key().as_bytes(), &manifest.to_bytes().unwrap())
        .await
        .expect("put manifest");

    // ---- reload the manifest from SlateDB and open every split lazily ----
    let blob = Arc::new(SlateDbBlobStore::new(db));
    let loaded = Manifest::load_for(blob.as_ref(), INDEX_ID)
        .await
        .expect("load manifest");
    assert_eq!(loaded.splits.len(), 2);
    assert_eq!(loaded.num_docs(), 4);

    let mut indexes = Vec::new();
    for sm in &loaded.splits {
        let idx = open_split_lazy(blob.clone(), &sm.blob_key(INDEX_ID))
            .await
            .expect("open split lazily");
        indexes.push((sm.split_id.clone(), idx));
    }
    let handles: Vec<SplitHandle> = indexes
        .iter()
        .map(|(id, idx)| SplitHandle::new(id.clone(), idx))
        .collect();

    // ---- search across both splits ----
    let fields = vec![title, body];

    // "financial" matches one doc in each split -> 2 merged hits.
    let financial = multi_split_search(&handles, "financial", &fields, 10).expect("search");
    assert_eq!(financial.len(), 2, "'financial' once per split");
    assert!(
        financial[0].split_ord != financial[1].split_ord,
        "the two 'financial' hits come from different splits"
    );
    // Scores are descending (merged ordering).
    assert!(
        financial[0].score >= financial[1].score,
        "merged hits must be sorted by descending score"
    );
    assert_eq!(
        multi_split_count(&handles, "financial", &fields).expect("count"),
        2
    );

    // "revenue" only in split B -> 1 hit from that split.
    let revenue = multi_split_search(&handles, "revenue", &fields, 10).expect("search");
    assert_eq!(revenue.len(), 1, "'revenue' only in split B");
    let split_b_ord = handles
        .iter()
        .position(|h| h.split_id == "split-b")
        .unwrap();
    assert_eq!(revenue[0].split_ord, split_b_ord);

    // Term in neither split.
    assert_eq!(
        multi_split_search(&handles, "nonexistentterm", &fields, 10)
            .expect("search")
            .len(),
        0
    );

    // limit truncates the merged set.
    let limited = multi_split_search(&handles, "financial", &fields, 1).expect("search");
    assert_eq!(limited.len(), 1, "limit=1 truncates merged hits");
}

#[tokio::test]
async fn manifest_round_trips_through_slatedb() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("bluedb-fts-manifest-test", object_store)
            .await
            .expect("open slatedb"),
    );

    let mut manifest = Manifest::new("books-2026");
    manifest
        .push(SplitMeta::new("s1", 100, 4096, 3))
        .push(SplitMeta::new("s2", 50, 2048, 3).with_time_range(1, 99));
    db.put(manifest.blob_key().as_bytes(), &manifest.to_bytes().unwrap())
        .await
        .expect("put manifest");

    let blob = SlateDbBlobStore::new(db);
    let loaded = Manifest::load(&blob, &Manifest::blob_key_for("books-2026"))
        .await
        .expect("load manifest");
    assert_eq!(loaded, manifest);
    assert_eq!(loaded.num_docs(), 150);
    assert_eq!(loaded.splits[1].time_range, Some((1, 99)));
}
