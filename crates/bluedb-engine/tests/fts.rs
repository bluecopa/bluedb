//! End-to-end FTS engine facade: ingest → search → delete → policy-driven
//! compaction (+ GC), and the background compaction scheduler.

use std::sync::Arc;
use std::time::Duration;

use bluedb_engine::FtsIndex;
use bluedb_fts::manifest::Manifest;
use bluedb_fts::policy::CompactionPolicy;
use bluedb_fts::IdField;
use bluedb_storage::SlateDbBlobStore;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use tantivy::schema::{Field, Schema, STORED, STRING, TEXT};
use tantivy::TantivyDocument;

const INDEX_ID: &str = "recon-2026";

fn schema() -> (Schema, Field, Field) {
    let mut sb = Schema::builder();
    let id = sb.add_text_field("id", STRING | STORED);
    let body = sb.add_text_field("body", TEXT | STORED);
    (sb.build(), id, body)
}

fn doc(id_f: Field, id: &str, body_f: Field, body: &str) -> TantivyDocument {
    let mut d = TantivyDocument::default();
    d.add_text(id_f, id);
    d.add_text(body_f, body);
    d
}

async fn new_blob(name: &str) -> Arc<SlateDbBlobStore> {
    let db = Db::open(name, Arc::new(InMemory::new())).await.expect("open slatedb");
    Arc::new(SlateDbBlobStore::new(Arc::new(db)))
}

/// Compact-eager policy: more than 2 splits (or >30% tombstoned) triggers.
fn eager_policy() -> CompactionPolicy {
    CompactionPolicy {
        max_splits: 2,
        max_tombstone_ratio: 0.3,
        min_splits_to_merge: 2,
    }
}

#[tokio::test]
async fn append_search_delete_and_compact() {
    let (schema, id_f, body_f) = schema();
    let blob = new_blob("engine-fts-lifecycle").await;
    let index = FtsIndex::new(INDEX_ID, blob.clone(), schema, IdField(id_f), eager_policy());
    let fields = vec![body_f];

    // Three appends → three splits, all matching "financial".
    index.append([doc(id_f, "a", body_f, "alpha financial")]).await.unwrap();
    index.append([doc(id_f, "b", body_f, "beta financial")]).await.unwrap();
    index.append([doc(id_f, "c", body_f, "gamma financial")]).await.unwrap();
    assert_eq!(index.search("financial", &fields, 10).await.unwrap().len(), 3);

    // Logical delete of "b" hides it from search (still physically present).
    index.delete(["b"]).await.unwrap();
    assert_eq!(index.search("financial", &fields, 10).await.unwrap().len(), 2);

    // Policy fires (3 splits > max_splits 2): compaction folds all into one,
    // physically drops "b", and GCs the 3 superseded split blobs.
    let summary = index.maybe_compact().await.unwrap().expect("policy should trigger compaction");
    assert_eq!(summary.superseded, 3, "all three input splits superseded");
    assert_eq!(summary.deleted_blobs, 3, "their blobs are GC'd");

    // Manifest is now a single compacted split.
    let manifest = Manifest::load_for(blob.as_ref(), INDEX_ID).await.unwrap();
    assert_eq!(manifest.splits.len(), 1, "folded into one split");

    // Live docs survive, the deleted one is physically gone.
    assert_eq!(index.search("financial", &fields, 10).await.unwrap().len(), 2);
    assert_eq!(index.search("alpha", &fields, 10).await.unwrap().len(), 1);
    assert_eq!(index.search("beta", &fields, 10).await.unwrap().len(), 0, "deleted 'b' physically dropped");

    // Below threshold now (1 split, 0 tombstones) → no further compaction.
    assert!(index.maybe_compact().await.unwrap().is_none());
}

#[tokio::test]
async fn same_id_update_through_the_engine_is_an_in_place_replace() {
    let (schema, id_f, body_f) = schema();
    let blob = new_blob("engine-fts-update").await;
    let index = FtsIndex::new(INDEX_ID, blob, schema, IdField(id_f), CompactionPolicy::default());
    let fields = vec![body_f];

    index.append([doc(id_f, "x", body_f, "oldterm")]).await.unwrap();
    index.update(["x"], [doc(id_f, "x", body_f, "newterm")]).await.unwrap();

    assert_eq!(index.search("newterm", &fields, 10).await.unwrap().len(), 1, "new version live");
    assert_eq!(index.search("oldterm", &fields, 10).await.unwrap().len(), 0, "old version hidden");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_scheduler_compacts() {
    let (schema, id_f, body_f) = schema();
    let blob = new_blob("engine-fts-scheduler").await;
    let index = Arc::new(FtsIndex::new(INDEX_ID, blob.clone(), schema, IdField(id_f), eager_policy()));

    // Three splits — over the policy's max.
    index.append([doc(id_f, "a", body_f, "alpha")]).await.unwrap();
    index.append([doc(id_f, "b", body_f, "beta")]).await.unwrap();
    index.append([doc(id_f, "c", body_f, "gamma")]).await.unwrap();

    let handle = index.clone().spawn_compaction_scheduler(Duration::from_millis(20));

    // Poll until the scheduler folds the 3 splits into 1 (bounded ~5s).
    let mut compacted = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let manifest = Manifest::load_for(blob.as_ref(), INDEX_ID).await.unwrap();
        if manifest.splits.len() == 1 {
            compacted = true;
            break;
        }
    }
    handle.abort();
    assert!(compacted, "background scheduler should have compacted 3 splits into 1");
}
