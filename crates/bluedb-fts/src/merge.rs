//! Merge / compaction — fold N splits into ONE, physically dropping tombstones.
//!
//! Incremental indexing ([`crate::writer`]) grows the split count by one per
//! append, and logical deletes ([`crate::tombstones`]) leave dead documents
//! physically present. Both inflate query fan-out (one tantivy `Index::open` +
//! search per split) and storage. Compaction bounds them: it rewrites a set of
//! input splits into a single new split that contains only the **live**
//! documents, then the caller can garbage-collect the now-superseded inputs.
//!
//! ## Approach: re-index from STORED fields
//! Splits are immutable bundles, so we can't edit them in place. We compact by
//! **re-indexing**: open each input split, walk every live (non-tombstoned)
//! document via the searcher, read its **stored** fields, and add the
//! reconstructed document to a fresh tantivy index — then pack that index into
//! one new split (with a hotcache, like every other split).
//!
//! ### Requirement: fields must be STORED to survive compaction
//! Re-indexing can only reconstruct a document from what tantivy kept in its
//! **stored** section. A field that is indexed/`FAST` but NOT `STORED` has no
//! retrievable value and is dropped on compaction. This is inherent to the
//! re-index strategy: **any field you need downstream (including the doc-id
//! field) must be `STORED`.** The compactor surfaces this by carrying every
//! stored field forward verbatim and leaving non-stored fields to be recomputed
//! by tantivy from the stored values at index time.
//!
//! ### Why not tantivy-native segment merge?
//! tantivy *can* merge segments within a single index without re-tokenizing,
//! and a future optimization could pack multiple input splits' segments into a
//! managed directory and call `IndexWriter::merge` to fold them — preserving
//! non-stored fields and avoiding re-tokenization. That path needs careful
//! handling of the fork's segment-id collision and `meta.json` merge semantics
//! across independently-built splits; the re-index approach here is simpler,
//! clearly testable, and correct. Native cross-split segment merge is noted as
//! a **future optimization**.

use tantivy::schema::{Schema, Value};
use tantivy::{DocAddress, Index, TantivyDocument};

use crate::indexer::{read_index_files, Indexer};
use crate::manifest::{Manifest, SplitMeta};
use crate::split::pack_split_with_hotcache;
use crate::tombstones::Tombstones;
use crate::IdField;

/// The product of a compaction.
pub struct CompactionResult {
    /// Packed bytes of the single new compacted split (with a real hotcache).
    pub split_bytes: Vec<u8>,
    /// Catalog metadata for the compacted split.
    pub split_meta: SplitMeta,
    /// Conventional blob key the caller should `put` the new split under.
    pub blob_key: String,
    /// The new manifest: the input splits removed, the compacted split added,
    /// `generation` bumped. Persist this in place of the old manifest.
    pub manifest: Manifest,
    /// The new tombstones: ids that were physically dropped by this compaction
    /// are cleared. Persist in place of the old tombstones.
    pub tombstones: Tombstones,
    /// Blob keys of the now-**superseded** input splits. The engine can't delete
    /// through the frozen read-only [`bluedb_storage::BlobStore`]; the caller
    /// garbage-collects these (e.g. via the write seam) after persisting the
    /// new manifest.
    pub superseded_keys: Vec<String>,
}

/// Compaction coordinator. Stateless aside from the [`Indexer`] heap budget.
pub struct Compactor {
    indexer: Indexer,
}

impl Default for Compactor {
    fn default() -> Self {
        Self::new()
    }
}

impl Compactor {
    /// New compactor with the default [`Indexer`] heap budget.
    pub fn new() -> Self {
        Self {
            indexer: Indexer::new(),
        }
    }

    /// Use a custom [`Indexer`].
    pub fn with_indexer(indexer: Indexer) -> Self {
        Self { indexer }
    }

    /// Compact the splits named by `input_split_ids` (a subset of
    /// `manifest.splits`) into ONE new split, given each input split's opened
    /// tantivy [`Index`].
    ///
    /// `inputs` is a `(split_id, generation, opened index)` per input split. The
    /// index may be opened whole-split (via
    /// [`crate::vendor::BundleDirectory::open_split`]) or lazily (via
    /// [`crate::open::open_split_lazy`]) — either works; the searcher reads
    /// stored fields the same way. The `generation` (from each split's
    /// [`SplitMeta`](crate::manifest::SplitMeta)) is required so liveness is
    /// **generation-scoped**, exactly as in the filtered search: a copy is dead
    /// only if its id was tombstoned at a generation `>=` its own split's, so a
    /// same-id update keeps the newer copy and drops the older. `schema` is the
    /// shared index schema; the new split is built with it. `id_field` is the
    /// STORED doc-id field used to test liveness against `tombstones`.
    ///
    /// Live documents (those NOT hidden by a generation-scoped tombstone) are
    /// re-indexed into the new split. Dead documents are physically excluded. The
    /// returned [`CompactionResult::manifest`] replaces the input splits with
    /// the single compacted split (`generation` bumped), and
    /// [`CompactionResult::tombstones`] has the now-physically-removed ids
    /// cleared.
    ///
    /// Input splits not present in `manifest` are ignored for the manifest
    /// rewrite but still contribute their live docs if passed in `inputs`; in
    /// practice `inputs` and `input_split_ids` describe the same set.
    pub fn compact(
        &self,
        manifest: &Manifest,
        tombstones: &Tombstones,
        inputs: &[(String, u64, &Index)],
        schema: Schema,
        id_field: IdField,
    ) -> anyhow::Result<CompactionResult> {
        let input_ids: std::collections::BTreeSet<&str> =
            inputs.iter().map(|(id, _, _)| id.as_str()).collect();

        // ---- 1. re-index live docs into a fresh index ----
        let dir = tempfile::tempdir()?;
        let index = Index::create_in_dir(dir.path(), schema.clone())?;
        // Ids physically carried into the new split — used to clear tombstones
        // (a tombstoned id that we DROP is the common case; this set tracks ids
        // that survive, which is what lets us prove we didn't accidentally keep
        // a dead doc, and to leave alone tombstones for ids in OTHER splits).
        let mut kept_ids: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut dropped_ids: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut num_docs: u64 = 0;

        {
            let mut writer = index.writer(self.writer_heap_bytes())?;
            for (split_id, generation, input_index) in inputs {
                let reader = input_index.reader()?;
                let searcher = reader.searcher();
                for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
                    for doc_id in segment_reader.doc_ids_alive() {
                        let addr = DocAddress {
                            segment_ord: segment_ord as u32,
                            doc_id,
                        };
                        let stored: TantivyDocument = searcher.doc(addr)?;
                        let id = id_field.extract(&stored).ok_or_else(|| {
                            anyhow::anyhow!(
                                "split {split_id} doc {doc_id} has no stored value for the id \
                                 field; the id field must be STORED to survive compaction"
                            )
                        })?;
                        // Generation-scoped liveness, identical to the rule the
                        // filtered search applies (see `tombstones::is_deleted_at`):
                        // a copy is dead only if the delete is as new as, or newer
                        // than, ITS split. So a same-id update — old copy
                        // tombstoned at gen g, new copy re-appended in a split of
                        // gen > g — drops the old copy here while carrying the new
                        // one forward, instead of erroneously dropping both.
                        if tombstones.is_deleted_at(&id, *generation) {
                            dropped_ids.insert(id);
                            continue; // physically excluded
                        }
                        kept_ids.insert(id);
                        // Re-add the reconstructed doc. Only its STORED fields
                        // are present; tantivy re-derives indexed/fast columns.
                        writer.add_document(reproject(&schema, &stored))?;
                        num_docs += 1;
                    }
                }
            }
            writer.commit()?;
        } // writer dropped -> files flushed

        // ---- 2. pack the compacted index into one new split ----
        let files = read_index_files(dir.path())?;
        let mmap_directory = tantivy::directory::MmapDirectory::open(dir.path())?;
        let mut hotcache: Vec<u8> = Vec::new();
        crate::vendor::write_hotcache(mmap_directory, &mut hotcache)?;
        let split_bytes = pack_split_with_hotcache(&files, &hotcache);
        drop(index);

        // ---- 3. build the new manifest: drop inputs, add compacted split ----
        let generation = manifest
            .splits
            .iter()
            .map(|s| s.generation)
            .max()
            .unwrap_or(0)
            + 1;
        let compacted_id = format!("compacted-{generation:06}");
        let split_meta = SplitMeta::new(
            &compacted_id,
            num_docs,
            split_bytes.len() as u64,
            generation,
        );
        let blob_key = split_meta.blob_key(&manifest.index_id);

        let mut new_manifest = Manifest::new(&manifest.index_id);
        let mut superseded_keys = Vec::new();
        for sm in &manifest.splits {
            if input_ids.contains(sm.split_id.as_str()) {
                superseded_keys.push(sm.blob_key(&manifest.index_id));
            } else {
                new_manifest.push(sm.clone());
            }
        }
        new_manifest.push(split_meta.clone());

        // ---- 4. clear tombstones for ids that were physically dropped ----
        // A tombstone may only be cleared once NO live-or-dead physical copy of
        // its id can still be hidden by it. We clear an id iff:
        //   * it was physically dropped here (`dropped_ids`), AND
        //   * it was NOT also carried forward as a live copy (`kept_ids`) — a
        //     same-id update keeps the new version, whose tombstone (at the old,
        //     lower generation) is harmless but must stay so it keeps hiding any
        //     straggler old copy, AND
        //   * this compaction covered EVERY split in the manifest — otherwise a
        //     non-input split might still hold a (correctly hidden) copy of the
        //     id, and clearing the tombstone would wrongly revive it.
        // When those don't hold we simply leave the tombstone in place: that is
        // always safe (it hides nothing that isn't meant to be hidden), at the
        // cost of a little tombstone-set growth until a full compaction.
        let covered_all_splits = manifest
            .splits
            .iter()
            .all(|sm| input_ids.contains(sm.split_id.as_str()));
        let mut new_tombstones = tombstones.clone();
        if covered_all_splits {
            for id in &dropped_ids {
                if !kept_ids.contains(id) {
                    new_tombstones.clear_id(id);
                }
            }
        }

        Ok(CompactionResult {
            split_bytes,
            split_meta,
            blob_key,
            manifest: new_manifest,
            tombstones: new_tombstones,
            superseded_keys,
        })
    }

    fn writer_heap_bytes(&self) -> usize {
        self.indexer.writer_heap_bytes()
    }
}

/// Reproject a retrieved document onto `schema`, copying every **stored** field
/// value verbatim. This is the document we re-add during compaction.
///
/// Because the retrieved [`TantivyDocument`] only contains stored values (that
/// is all tantivy persisted), re-adding it preserves exactly the stored fields;
/// tantivy re-derives indexed/columnar data at write time. Multi-valued fields
/// are preserved (every value of every field is copied), and nested
/// object/array values (e.g. JSON fields) survive intact because each value is
/// round-tripped through [`tantivy::schema::OwnedValue`].
fn reproject(schema: &Schema, stored: &TantivyDocument) -> TantivyDocument {
    let mut out = TantivyDocument::default();
    for (field, _entry) in schema.fields() {
        for value in stored.get_all(field) {
            // Any leaf/array/object value converts losslessly to OwnedValue,
            // and `&OwnedValue: Value`, so this one path handles every type.
            let owned: tantivy::schema::OwnedValue = value.as_value().into();
            out.add_field_value(field, &owned);
        }
    }
    out
}
