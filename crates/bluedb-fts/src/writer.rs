//! Incremental indexing — **append** a new split without rebuilding.
//!
//! A logical index is a set of immutable splits (see [`crate::manifest`]) that
//! already compose under [`crate::search::multi_split_search`]. So "add more
//! documents" is just: build one fresh single-segment split from the new batch,
//! mint a `split_id`, and append its [`SplitMeta`] to the manifest — the
//! existing splits are never touched or re-read.
//!
//! [`IndexWriter`] is the thin coordinator for that flow. It is stateless aside
//! from an [`Indexer`] (writer-heap budget) and a monotonically-incrementing
//! split counter used only to mint readable default split-ids; the source of
//! truth is always the [`Manifest`] the caller passes in and persists. As with
//! the rest of the engine, the writer **returns bytes** (split blob + updated
//! manifest) for the caller to `put` — it never writes to the frozen
//! [`bluedb_storage::BlobStore`].
//!
//! ## Updates
//! Splits are immutable, so an update is modeled as **tombstone the old id +
//! append the new document** (which may carry the same id or a new one). See
//! [`IndexWriter::update`]. The old version stays physically present until a
//! merge ([`crate::merge`]) compacts it away; query-time filtering
//! ([`crate::search::multi_split_search_filtered`]) hides it in the meantime.
//!
//! ### Same-id updates are generation-scoped
//! The subtlety is a **same-id** update — replacing `X` with a new version of
//! `X`. A plain delete-by-id would hide BOTH the old and the new copy (they
//! share the id), so the row would vanish. Tombstones are therefore
//! generation-scoped (see [`crate::tombstones`]): [`IndexWriter::update`] records
//! the delete at the manifest's **current** max generation, then appends the new
//! doc in a split at `max + 1`. The old copies (generation `<= max`) are hidden;
//! the re-append (generation `max + 1`) survives. The result is exactly one live
//! `X`, the new content.

use tantivy::schema::Schema;
use tantivy::TantivyDocument;

use crate::indexer::{read_index_files, Indexer};
use crate::manifest::{Manifest, SplitMeta};
use crate::split::pack_split_with_hotcache;
use crate::tombstones::Tombstones;

/// The product of an append: the new split's packed bytes, its catalog entry,
/// and the conventional blob key the caller should `put` it under.
///
/// The caller is expected to: `put(blob_key, split_bytes)`, then persist
/// [`AppendResult::manifest`] (also returned by the append call).
pub struct AppendResult {
    /// Packed split blob (built with a real hotcache, so it can be opened
    /// lazily via [`crate::open::open_split_lazy`]).
    pub split_bytes: Vec<u8>,
    /// Catalog metadata for the new split.
    pub split_meta: SplitMeta,
    /// Conventional blob key for the new split:
    /// `indexes/<index_id>/splits/<split_id>.split`.
    pub blob_key: String,
}

/// Coordinator for incremental writes against a logical index.
pub struct IndexWriter {
    indexer: Indexer,
    /// Monotone counter used only to mint readable default split-ids
    /// (`split-000001`, ...). Not authoritative — the manifest is.
    next_split_seq: u64,
}

impl Default for IndexWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexWriter {
    /// New writer with the default [`Indexer`] heap budget.
    pub fn new() -> Self {
        Self {
            indexer: Indexer::new(),
            next_split_seq: 0,
        }
    }

    /// Use a custom [`Indexer`] (e.g. a larger writer-heap budget).
    pub fn with_indexer(indexer: Indexer) -> Self {
        Self {
            indexer,
            next_split_seq: 0,
        }
    }

    /// Mint a default split-id. Format `split-NNNNNN`, zero-padded, so ids sort
    /// lexicographically in creation order. Seeded past any existing
    /// numeric-suffixed splits in the manifest to avoid collisions.
    fn mint_split_id(&mut self, manifest: &Manifest) -> String {
        // Seed the counter past the highest existing `split-NNNNNN` id so that
        // appending to a reloaded manifest doesn't reuse an id.
        let highest = manifest
            .splits
            .iter()
            .filter_map(|s| s.split_id.strip_prefix("split-"))
            .filter_map(|n| n.parse::<u64>().ok())
            .max()
            .map(|n| n + 1)
            .unwrap_or(0);
        if highest > self.next_split_seq {
            self.next_split_seq = highest;
        }
        let id = format!("split-{:06}", self.next_split_seq);
        self.next_split_seq += 1;
        id
    }

    /// Build a split from `docs` and append it to `manifest` (mutated in place).
    ///
    /// Mints a default split-id, builds a hotcache-carrying split, sets the new
    /// split's `generation` to `manifest`'s current max generation + 1 (so the
    /// catalog records that a write happened), pushes its [`SplitMeta`], and
    /// returns the [`AppendResult`]. Existing splits are untouched.
    pub fn append(
        &mut self,
        manifest: &mut Manifest,
        schema: Schema,
        docs: impl IntoIterator<Item = TantivyDocument>,
    ) -> anyhow::Result<AppendResult> {
        let split_id = self.mint_split_id(manifest);
        self.append_with_id(manifest, &split_id, schema, docs)
    }

    /// Like [`IndexWriter::append`], but with a caller-chosen `split_id`.
    pub fn append_with_id(
        &mut self,
        manifest: &mut Manifest,
        split_id: &str,
        schema: Schema,
        docs: impl IntoIterator<Item = TantivyDocument>,
    ) -> anyhow::Result<AppendResult> {
        // Build the index, count docs, and pack with a real hotcache so the new
        // split is lazily openable like every other split in the index.
        let (index, dir, num_docs) = self.indexer.build_index_counted(schema, docs)?;
        let files = read_index_files(dir.path())?;
        let mmap_directory = tantivy::directory::MmapDirectory::open(dir.path())?;
        let mut hotcache: Vec<u8> = Vec::new();
        crate::vendor::write_hotcache(mmap_directory, &mut hotcache)?;
        let split_bytes = pack_split_with_hotcache(&files, &hotcache);
        drop(index); // index borrows dir; drop before dir falls out of scope

        let generation = manifest.max_generation() + 1;
        let split_meta = SplitMeta::new(split_id, num_docs, split_bytes.len() as u64, generation);
        let blob_key = split_meta.blob_key(&manifest.index_id);
        manifest.push(split_meta.clone());

        Ok(AppendResult {
            split_bytes,
            split_meta,
            blob_key,
        })
    }

    /// Model an **update** as *tombstone the old id, then append a new batch*.
    ///
    /// `old_ids` are marked deleted in `tombstones` **at the manifest's current
    /// max generation** (so every existing copy stops showing up in filtered
    /// search); `docs` is the new batch — which may carry the same ids (a true
    /// in-place update) or new ones. The new split is appended at
    /// `max_generation + 1`, so a re-appended same-id doc lands in a split
    /// *newer* than the recorded deletion generation and stays live. Returns the
    /// [`AppendResult`] for the appended split. The caller persists the updated
    /// `manifest` and `tombstones`.
    ///
    /// Replacing a doc in place is just `update([old_id], [new_doc_with_same_id])`:
    /// the result is exactly ONE live copy of the id, carrying the new content.
    pub fn update<I, S>(
        &mut self,
        manifest: &mut Manifest,
        tombstones: &mut Tombstones,
        old_ids: I,
        schema: Schema,
        docs: impl IntoIterator<Item = TantivyDocument>,
    ) -> anyhow::Result<AppendResult>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        // Tombstone the old versions at the CURRENT max generation: this hides
        // every existing split's copy (generation <= current max) but NOT the
        // new split we are about to append (generation = current max + 1). That
        // generation gap is exactly what makes a same-id re-append a true
        // in-place replace rather than a vanish.
        let delete_generation = manifest.max_generation();
        for id in old_ids {
            tombstones.delete_doc_at(id, delete_generation);
        }
        self.append(manifest, schema, docs)
    }
}
