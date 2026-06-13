//! GC executor over real SlateDB: build several splits, compact them, then prove
//! that `gc_keys(superseded_keys)` and `gc_orphaned_splits` physically delete the
//! right blobs while the referenced split survives.

use std::sync::Arc;

use bluedb_fts::gc::{gc_keys, gc_orphaned_splits, splits_below_generation};
use bluedb_fts::manifest::Manifest;
use bluedb_fts::merge::Compactor;
use bluedb_fts::open::open_split_lazy;
use bluedb_fts::tombstones::Tombstones;
use bluedb_fts::writer::IndexWriter;
use bluedb_fts::IdField;
use bluedb_storage::{BlobStore, BlobStoreMut, SlateDbBlobStore};
use slatedb::object_store::{memory::InMemory, ObjectStore};
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

async fn open_db(name: &str) -> Arc<Db> {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open(name, object_store).await.expect("open slatedb"))
}

async fn key_exists(blob: &SlateDbBlobStore, key: &str) -> bool {
    blob.get_all(key).await.is_ok()
}

#[tokio::test]
async fn gc_keys_deletes_superseded_splits_after_compaction() {
    let (schema, id_f, body_f) = schema();
    let db = open_db("fts-gc-superseded").await;
    let blob = Arc::new(SlateDbBlobStore::new(db.clone()));

    let mut writer = IndexWriter::new();
    let mut manifest = Manifest::new(INDEX_ID);
    let tombs = Tombstones::new(INDEX_ID);

    // Two appended splits.
    let s1 = writer
        .append(&mut manifest, schema.clone(), vec![doc(id_f, "a", body_f, "alpha")])
        .expect("s1");
    blob.put(&s1.blob_key, s1.split_bytes.clone().into()).await.expect("put s1");
    let s2 = writer
        .append(&mut manifest, schema.clone(), vec![doc(id_f, "b", body_f, "beta")])
        .expect("s2");
    blob.put(&s2.blob_key, s2.split_bytes.clone().into()).await.expect("put s2");

    // Compact both into one.
    let opened = {
        let mut v = Vec::new();
        for sm in &manifest.splits {
            let idx = open_split_lazy(blob.clone(), &sm.blob_key(INDEX_ID)).await.unwrap();
            v.push((sm.split_id.clone(), sm.generation, idx));
        }
        v
    };
    let inputs: Vec<(String, u64, &tantivy::Index)> =
        opened.iter().map(|(id, g, idx)| (id.clone(), *g, idx)).collect();
    let result = Compactor::new()
        .compact(&manifest, &tombs, &inputs, schema.clone(), IdField(id_f))
        .expect("compact");

    // Persist the compacted split + advance the manifest BEFORE GC.
    blob.put(&result.blob_key, result.split_bytes.clone().into()).await.expect("put compacted");
    let new_manifest = result.manifest;
    assert_eq!(result.superseded_keys.len(), 2);

    // Both inputs still physically present until GC.
    assert!(key_exists(&blob, &s1.blob_key).await);
    assert!(key_exists(&blob, &s2.blob_key).await);

    // GC the superseded inputs.
    let n = gc_keys(blob.as_ref(), &result.superseded_keys).await.expect("gc");
    assert_eq!(n, 2, "two superseded splits deleted");

    // Inputs gone; the compacted (referenced) split survives.
    assert!(!key_exists(&blob, &s1.blob_key).await, "s1 GC'd");
    assert!(!key_exists(&blob, &s2.blob_key).await, "s2 GC'd");
    assert!(key_exists(&blob, &result.blob_key).await, "compacted split kept");

    // GC is idempotent: re-running over the same keys (now missing) is a no-op
    // that still "counts" them but fails nothing.
    let n2 = gc_keys(blob.as_ref(), &result.superseded_keys).await.expect("gc retry");
    assert_eq!(n2, 2, "delete of a missing key is a no-op, not an error");

    // The advanced manifest now references only the compacted split: no orphans.
    let orphans = gc_orphaned_splits(blob.as_ref(), &new_manifest).await.expect("orphan scan");
    assert!(orphans.is_empty(), "nothing orphaned after a clean GC");
}

#[tokio::test]
async fn gc_orphaned_splits_deletes_unreferenced_blobs() {
    let (schema, id_f, body_f) = schema();
    let db = open_db("fts-gc-orphans").await;
    let blob = Arc::new(SlateDbBlobStore::new(db.clone()));

    let mut writer = IndexWriter::new();
    let mut manifest = Manifest::new(INDEX_ID);

    // Three appended splits, all on disk.
    let mut keys = Vec::new();
    for (id, term) in [("a", "alpha"), ("b", "beta"), ("c", "gamma")] {
        let r = writer
            .append(&mut manifest, schema.clone(), vec![doc(id_f, id, body_f, term)])
            .expect("append");
        blob.put(&r.blob_key, r.split_bytes.clone().into()).await.expect("put");
        keys.push(r.blob_key);
    }

    // Simulate a manifest that was advanced to drop the first two splits (e.g. a
    // compaction whose GC never ran), leaving them orphaned on disk.
    let mut advanced = Manifest::new(INDEX_ID);
    advanced.push(manifest.splits[2].clone()); // keep only "c"

    let orphans = gc_orphaned_splits(blob.as_ref(), &advanced).await.expect("gc orphans");
    assert_eq!(
        orphans,
        vec![keys[0].clone(), keys[1].clone()],
        "the two unreferenced splits are deleted, in sorted order"
    );

    // The referenced split survives; the orphans are gone.
    assert!(!key_exists(&blob, &keys[0]).await);
    assert!(!key_exists(&blob, &keys[1]).await);
    assert!(key_exists(&blob, &keys[2]).await, "referenced split kept");

    // Re-running finds nothing to do.
    assert!(gc_orphaned_splits(blob.as_ref(), &advanced).await.unwrap().is_empty());
}

#[tokio::test]
async fn retention_policy_feeds_gc_keys() {
    let (schema, id_f, body_f) = schema();
    let db = open_db("fts-gc-retention").await;
    let blob = Arc::new(SlateDbBlobStore::new(db.clone()));

    let mut writer = IndexWriter::new();
    let mut manifest = Manifest::new(INDEX_ID);

    // gen1, gen2, gen3 (each append bumps the generation).
    for (id, term) in [("a", "alpha"), ("b", "beta"), ("c", "gamma")] {
        let r = writer
            .append(&mut manifest, schema.clone(), vec![doc(id_f, id, body_f, term)])
            .expect("append");
        blob.put(&r.blob_key, r.split_bytes.clone().into()).await.expect("put");
    }
    assert_eq!(manifest.max_generation(), 3);

    // Retention: drop everything strictly below the current max generation.
    let stale = splits_below_generation(&manifest, manifest.max_generation());
    assert_eq!(stale.len(), 2, "gen1 + gen2 are below the gen3 watermark");

    let n = gc_keys(blob.as_ref(), &stale).await.expect("gc retention");
    assert_eq!(n, 2);

    // The gen3 split remains; the two older ones are gone.
    for sm in &manifest.splits {
        let exists = key_exists(&blob, &sm.blob_key(INDEX_ID)).await;
        if sm.generation == 3 {
            assert!(exists, "gen3 split kept");
        } else {
            assert!(!exists, "older split GC'd");
        }
    }
}
