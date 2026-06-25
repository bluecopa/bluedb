//! End-to-end index lifecycle over real SlateDB: logical deletes (tombstones),
//! incremental appends, same-id updates, and merge/compaction — all driven
//! through the `IndexWriter`/`Compactor` coordinators with splits stored in,
//! and reopened from, an in-memory SlateDB.
//!
//! These are the proofs that the generation-scoped delete model is correct:
//! a same-id update (tombstone old id at the current generation, re-append the
//! new doc in a newer split) leaves exactly ONE live copy carrying the new
//! content — both at query time (filtered search) and after compaction.

use std::sync::Arc;

use bluedb_fts::manifest::Manifest;
use bluedb_fts::merge::Compactor;
use bluedb_fts::open::open_split_lazy;
use bluedb_fts::search::{
    multi_split_search, multi_split_search_filtered, multi_split_search_filtered_ids, SplitHandle,
};
use bluedb_fts::tombstones::Tombstones;
use bluedb_fts::writer::IndexWriter;
use bluedb_fts::IdField;
use bluedb_storage::SlateDbBlobStore;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::Db;
use tantivy::schema::{Field, Schema, STORED, STRING, TEXT};
use tantivy::TantivyDocument;

const INDEX_ID: &str = "recon-2026";

/// Schema: a STORED string `id` (the doc-id field) + a STORED `body` (STORED so
/// it survives compaction's re-index).
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

/// Open every split in `manifest` lazily, returning `(split_id, generation, Index)`.
async fn open_all(
    blob: &Arc<SlateDbBlobStore>,
    manifest: &Manifest,
) -> Vec<(String, u64, tantivy::Index)> {
    let mut out = Vec::new();
    for sm in &manifest.splits {
        let idx = open_split_lazy(blob.clone(), &sm.blob_key(&manifest.index_id))
            .await
            .expect("open split lazily");
        out.push((sm.split_id.clone(), sm.generation, idx));
    }
    out
}

fn handles(opened: &[(String, u64, tantivy::Index)]) -> Vec<SplitHandle<'_>> {
    opened
        .iter()
        .map(|(id, gen, idx)| SplitHandle::with_generation(id.clone(), *gen, idx))
        .collect()
}

#[tokio::test]
async fn tombstone_hides_a_doc_that_is_still_physically_present() {
    let (schema, id_f, body_f) = schema();
    let db = open_db("fts-lifecycle-delete").await;

    let mut writer = IndexWriter::new();
    let mut manifest = Manifest::new(INDEX_ID);
    let appended = writer
        .append(
            &mut manifest,
            schema.clone(),
            vec![
                doc(id_f, "a", body_f, "alpha financial"),
                doc(id_f, "b", body_f, "beta financial"),
            ],
        )
        .expect("append split");
    db.put(appended.blob_key.as_bytes(), &appended.split_bytes[..])
        .await
        .expect("put split");

    let blob = Arc::new(SlateDbBlobStore::new(db));
    let opened = open_all(&blob, &manifest).await;
    let hs = handles(&opened);
    let fields = vec![body_f];

    // Tombstone "a" at the split's generation -> hidden by the filtered search.
    let mut tombs = Tombstones::new(INDEX_ID);
    tombs.delete_doc_at("a", manifest.max_generation());

    let plain = multi_split_search(&hs, "financial", &fields, 10).expect("plain");
    assert_eq!(plain.len(), 2, "both docs still physically present");

    let filtered =
        multi_split_search_filtered(&hs, "financial", &fields, 10, IdField(id_f), &tombs)
            .expect("filtered");
    assert_eq!(
        filtered.len(),
        1,
        "tombstoned 'a' is hidden, only 'b' lives"
    );
}

#[tokio::test]
async fn filtered_ids_returns_id_score_pairs_deduped_and_tombstone_filtered() {
    let (schema, id_f, body_f) = schema();
    let db = open_db("fts-lifecycle-filtered-ids").await;

    let mut writer = IndexWriter::new();
    let mut manifest = Manifest::new(INDEX_ID);
    let mut tombs = Tombstones::new(INDEX_ID);

    // split gen1: a, b, c — all match "financial".
    let s1 = writer
        .append(
            &mut manifest,
            schema.clone(),
            vec![
                doc(id_f, "a", body_f, "alpha financial report"),
                doc(id_f, "b", body_f, "beta financial"),
                doc(id_f, "c", body_f, "gamma financial"),
            ],
        )
        .expect("s1");
    db.put(s1.blob_key.as_bytes(), &s1.split_bytes[..])
        .await
        .expect("put s1");

    // Update "a" in a strictly newer split (still matches "financial").
    let s2 = writer
        .update(
            &mut manifest,
            &mut tombs,
            ["a"],
            schema.clone(),
            vec![doc(id_f, "a", body_f, "alpha financial updated")],
        )
        .expect("s2");
    db.put(s2.blob_key.as_bytes(), &s2.split_bytes[..])
        .await
        .expect("put s2");

    // Delete "b" outright at the current max generation.
    tombs.delete_doc_at("b", manifest.max_generation());

    let blob = Arc::new(SlateDbBlobStore::new(db));
    let opened = open_all(&blob, &manifest).await;
    let hs = handles(&opened);
    let fields = vec![body_f];

    let ids: Vec<(String, f32)> =
        multi_split_search_filtered_ids(&hs, "financial", &fields, 10, IdField(id_f), &tombs)
            .expect("filtered ids");

    // Tombstoned "b" is excluded; "a" appears exactly once (last-write-wins
    // dedup across the original + updated split).
    let just_ids: Vec<&str> = ids.iter().map(|(id, _)| id.as_str()).collect();
    let mut sorted = just_ids.clone();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        vec!["a", "c"],
        "live, deduped ids only ('b' tombstoned)"
    );
    assert_eq!(
        just_ids.len(),
        2,
        "no duplicate 'a' from the same-id update"
    );

    // Scores are positive and the result is sorted by descending score.
    for (_, score) in &ids {
        assert!(*score > 0.0, "BM25 score must be positive");
    }
    assert!(
        ids[0].1 >= ids[1].1,
        "results sorted by descending score: {ids:?}"
    );
}

#[tokio::test]
async fn incremental_append_is_searchable_across_splits() {
    let (schema, id_f, body_f) = schema();
    let db = open_db("fts-lifecycle-incremental").await;

    let mut writer = IndexWriter::new();
    let mut manifest = Manifest::new(INDEX_ID);

    for r in writer
        .append(
            &mut manifest,
            schema.clone(),
            vec![doc(id_f, "a", body_f, "alpha financial")],
        )
        .into_iter()
        .chain(writer.append(
            &mut manifest,
            schema.clone(),
            vec![doc(id_f, "b", body_f, "beta financial")],
        ))
    {
        db.put(r.blob_key.as_bytes(), &r.split_bytes[..])
            .await
            .expect("put");
    }

    assert_eq!(manifest.splits.len(), 2, "two splits after two appends");
    assert!(
        manifest.splits[1].generation > manifest.splits[0].generation,
        "each append mints a strictly newer generation"
    );

    let blob = Arc::new(SlateDbBlobStore::new(db));
    let opened = open_all(&blob, &manifest).await;
    let hs = handles(&opened);
    let fields = vec![body_f];

    let hits = multi_split_search(&hs, "financial", &fields, 10).expect("search");
    assert_eq!(
        hits.len(),
        2,
        "'financial' matches one doc in each appended split"
    );
}

#[tokio::test]
async fn same_id_update_yields_exactly_one_live_copy_with_new_content() {
    let (schema, id_f, body_f) = schema();
    let db = open_db("fts-lifecycle-update").await;

    let mut writer = IndexWriter::new();
    let mut manifest = Manifest::new(INDEX_ID);
    let mut tombs = Tombstones::new(INDEX_ID);

    // Original: id=x with "oldterm".
    let first = writer
        .append(
            &mut manifest,
            schema.clone(),
            vec![doc(id_f, "x", body_f, "oldterm shared")],
        )
        .expect("append v1");
    db.put(first.blob_key.as_bytes(), &first.split_bytes[..])
        .await
        .expect("put v1");

    // Update in place: tombstone old x (at current max gen) + append new x in a
    // strictly newer split.
    let second = writer
        .update(
            &mut manifest,
            &mut tombs,
            ["x"],
            schema.clone(),
            vec![doc(id_f, "x", body_f, "newterm shared")],
        )
        .expect("update x");
    db.put(second.blob_key.as_bytes(), &second.split_bytes[..])
        .await
        .expect("put v2");

    let blob = Arc::new(SlateDbBlobStore::new(db));
    let opened = open_all(&blob, &manifest).await;
    let hs = handles(&opened);
    let fields = vec![body_f];

    // The new content is live...
    let new_hits = multi_split_search_filtered(&hs, "newterm", &fields, 10, IdField(id_f), &tombs)
        .expect("new");
    assert_eq!(new_hits.len(), 1, "the updated 'x' is live exactly once");

    // ...the old content is gone...
    let old_hits = multi_split_search_filtered(&hs, "oldterm", &fields, 10, IdField(id_f), &tombs)
        .expect("old");
    assert_eq!(old_hits.len(), 0, "the old version of 'x' is hidden");

    // ...and a term shared by both versions still yields exactly ONE hit (the
    // new one): generation-scoped delete + last-write-wins dedup, not a double.
    let shared = multi_split_search_filtered(&hs, "shared", &fields, 10, IdField(id_f), &tombs)
        .expect("shared");
    assert_eq!(
        shared.len(),
        1,
        "same-id update collapses to a single live copy"
    );
}

#[tokio::test]
async fn compaction_physically_drops_dead_docs_and_keeps_updated_ones() {
    let (schema, id_f, body_f) = schema();
    let db = open_db("fts-lifecycle-compact").await;

    let mut writer = IndexWriter::new();
    let mut manifest = Manifest::new(INDEX_ID);
    let mut tombs = Tombstones::new(INDEX_ID);

    // split gen1: a("alpha"), b("beta")
    let s1 = writer
        .append(
            &mut manifest,
            schema.clone(),
            vec![
                doc(id_f, "a", body_f, "alpha"),
                doc(id_f, "b", body_f, "beta"),
            ],
        )
        .expect("s1");
    db.put(s1.blob_key.as_bytes(), &s1.split_bytes[..])
        .await
        .expect("put s1");

    // split gen2: c("gamma")
    let s2 = writer
        .append(
            &mut manifest,
            schema.clone(),
            vec![doc(id_f, "c", body_f, "gamma")],
        )
        .expect("s2");
    db.put(s2.blob_key.as_bytes(), &s2.split_bytes[..])
        .await
        .expect("put s2");

    // update a -> "alpha2" (tombstone old a, append split gen3 with new a)
    let s3 = writer
        .update(
            &mut manifest,
            &mut tombs,
            ["a"],
            schema.clone(),
            vec![doc(id_f, "a", body_f, "alpha2")],
        )
        .expect("update a");
    db.put(s3.blob_key.as_bytes(), &s3.split_bytes[..])
        .await
        .expect("put s3");

    // delete b outright (at the current max generation).
    tombs.delete_doc_at("b", manifest.max_generation());

    assert_eq!(manifest.splits.len(), 3, "three splits before compaction");

    // ---- compact ALL splits into one ----
    let blob = Arc::new(SlateDbBlobStore::new(db.clone()));
    let opened = open_all(&blob, &manifest).await;
    let inputs: Vec<(String, u64, &tantivy::Index)> = opened
        .iter()
        .map(|(id, gen, idx)| (id.clone(), *gen, idx))
        .collect();

    let result = Compactor::new()
        .compact(&manifest, &tombs, &inputs, schema.clone(), IdField(id_f))
        .expect("compact");

    // The compacted manifest has a single split; the three inputs are superseded.
    assert_eq!(
        result.manifest.splits.len(),
        1,
        "all inputs folded into one split"
    );
    assert_eq!(result.superseded_keys.len(), 3, "three input splits to GC");
    assert_eq!(
        result.split_meta.num_docs, 2,
        "live docs: updated 'a' + 'c'; dead 'a','b' dropped"
    );

    // "b" was fully dropped -> its tombstone is cleared; "a" is kept (live new
    // copy) so its tombstone stays (harmless: the compacted copy is newer).
    assert!(
        !result.tombstones.is_deleted("b"),
        "fully-dropped id's tombstone is cleared"
    );

    // ---- reopen ONLY the compacted split and confirm physical contents ----
    db.put(result.blob_key.as_bytes(), &result.split_bytes[..])
        .await
        .expect("put compacted");
    let compacted = open_split_lazy(blob.clone(), &result.blob_key)
        .await
        .expect("open compacted");
    let opened2 = vec![(
        result.split_meta.split_id.clone(),
        result.split_meta.generation,
        compacted,
    )];
    let hs = handles(&opened2);
    let fields = vec![body_f];

    // Plain search over the compacted split (dead docs are physically gone, so
    // no tombstone filtering is needed to exclude them).
    assert_eq!(
        multi_split_search(&hs, "alpha2", &fields, 10)
            .unwrap()
            .len(),
        1,
        "updated 'a' present"
    );
    assert_eq!(
        multi_split_search(&hs, "gamma", &fields, 10).unwrap().len(),
        1,
        "'c' present"
    );
    assert_eq!(
        multi_split_search(&hs, "alpha", &fields, 10).unwrap().len(),
        0,
        "old 'a' physically gone"
    );
    assert_eq!(
        multi_split_search(&hs, "beta", &fields, 10).unwrap().len(),
        0,
        "deleted 'b' physically gone"
    );
}
