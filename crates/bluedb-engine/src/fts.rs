//! [`FtsIndex`] — the full-text engine facade: ingest, delete/update, search,
//! and a **policy-driven compaction coordinator** + background scheduler over
//! one logical index in the substrate.
//!
//! The FTS pillar ([`bluedb_fts`]) ships the pieces — `IndexWriter` (append),
//! `Tombstones` (generation-scoped deletes), `multi_split_search_filtered`,
//! `CompactionPolicy`, `Compactor`, and the `gc` executor — but leaves the
//! orchestration (load manifest/tombstones → act → persist → GC) to the caller.
//! `FtsIndex` is that orchestrator, scoped to one `index_id` over a
//! [`SlateDbBlobStore`] (which is both the read [`BlobStore`] and the write
//! `BlobStoreMut`).
//!
//! ## Concurrency
//! All mutators (`append`/`update`/`delete`/`maybe_compact`) load the manifest,
//! mutate it, and persist it — a read-modify-write that must not interleave with
//! another mutator. They serialize on an in-process write lock (mirroring the
//! SQL pillar's write lease). Searches are lock-free: each takes its own
//! manifest+tombstones snapshot. (Cross-*process* coordination — CAS on the
//! manifest blob — is an M4/HA concern; today one process owns the index.)

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tantivy::schema::{Field, Schema};
use tantivy::TantivyDocument;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use bluedb_fts::gc;
use bluedb_fts::manifest::Manifest;
use bluedb_fts::merge::Compactor;
use bluedb_fts::open::open_split_lazy;
use bluedb_fts::policy::CompactionPolicy;
use bluedb_fts::search::{
    multi_split_count_query_filtered, multi_split_search_filtered, multi_split_search_filtered_ids,
    multi_split_search_query_filtered_ids, multi_split_search_query_sorted_ids, MultiSplitHit,
    SplitHandle,
};
use bluedb_fts::tombstones::Tombstones;
use bluedb_fts::writer::IndexWriter;
use bluedb_fts::IdField;
use bluedb_storage::{BlobStore, BlobStoreMut, SlateDbBlobStore};
use tantivy::query::Query;

use crate::error::Result;

/// What a compaction did (for logging / the control API).
#[derive(Debug, Clone)]
pub struct CompactionSummary {
    /// Id of the single split the inputs were folded into.
    pub compacted_split_id: String,
    /// Number of input splits superseded (and queued for GC).
    pub superseded: usize,
    /// Number of superseded split blobs actually deleted by GC.
    pub deleted_blobs: usize,
}

/// A full-text index over one logical `index_id` in the substrate.
pub struct FtsIndex {
    index_id: String,
    blob: Arc<SlateDbBlobStore>,
    schema: Schema,
    id_field: IdField,
    policy: CompactionPolicy,
    /// Serializes manifest read-modify-write across mutators.
    write_lock: Mutex<()>,
}

impl FtsIndex {
    /// Open (or lazily create) the index `index_id`. `schema` is the shared
    /// tantivy schema, `id_field` the STORED doc-id field, `policy` drives
    /// [`FtsIndex::maybe_compact`].
    pub fn new(
        index_id: impl Into<String>,
        blob: Arc<SlateDbBlobStore>,
        schema: Schema,
        id_field: IdField,
        policy: CompactionPolicy,
    ) -> Self {
        Self {
            index_id: index_id.into(),
            blob,
            schema,
            id_field,
            policy,
            write_lock: Mutex::new(()),
        }
    }

    /// The logical index id.
    pub fn index_id(&self) -> &str {
        &self.index_id
    }

    /// The shared tantivy schema (so callers can resolve fields to build
    /// [`TantivyDocument`]s for [`FtsIndex::append`]/[`FtsIndex::update`]).
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    // --- manifest / tombstones persistence -----------------------------------

    async fn load_manifest(&self) -> Result<Manifest> {
        let key = Manifest::blob_key_for(&self.index_id);
        match self.blob.get_all(&key).await {
            // Present: parse (a parse error IS propagated — never silently reset).
            Ok(bytes) => Ok(Manifest::from_bytes(&bytes)?),
            // Absent: a fresh index.
            Err(_) => Ok(Manifest::new(&self.index_id)),
        }
    }

    async fn load_tombstones(&self) -> Result<Tombstones> {
        let key = Tombstones::blob_key_for(&self.index_id);
        match self.blob.get_all(&key).await {
            Ok(bytes) => Ok(Tombstones::from_bytes(&bytes)?),
            Err(_) => Ok(Tombstones::new(&self.index_id)),
        }
    }

    async fn persist_manifest(&self, manifest: &Manifest) -> Result<()> {
        self.blob
            .put(&manifest.blob_key(), Bytes::from(manifest.to_bytes()?))
            .await?;
        Ok(())
    }

    async fn persist_tombstones(&self, tombstones: &Tombstones) -> Result<()> {
        self.blob
            .put(&tombstones.blob_key(), Bytes::from(tombstones.to_bytes()?))
            .await?;
        Ok(())
    }

    /// Open every split named in `splits` lazily, returning
    /// `(split_id, generation, Index)` for search/compaction.
    async fn open_splits(
        &self,
        splits: impl Iterator<Item = &bluedb_fts::manifest::SplitMeta>,
    ) -> Result<Vec<(String, u64, tantivy::Index)>> {
        let dyn_blob: Arc<dyn BlobStore> = self.blob.clone();
        let mut out = Vec::new();
        for sm in splits {
            let index = open_split_lazy(dyn_blob.clone(), &sm.blob_key(&self.index_id)).await?;
            out.push((sm.split_id.clone(), sm.generation, index));
        }
        Ok(out)
    }

    // --- ingest --------------------------------------------------------------

    /// Append `docs` as a new split and advance the manifest. Returns the new
    /// split id.
    pub async fn append<I>(&self, docs: I) -> Result<String>
    where
        I: IntoIterator<Item = TantivyDocument>,
    {
        let _guard = self.write_lock.lock().await;
        let mut manifest = self.load_manifest().await?;
        let mut writer = IndexWriter::new();
        let result = writer.append(&mut manifest, self.schema.clone(), docs)?;
        self.blob
            .put(&result.blob_key, Bytes::from(result.split_bytes))
            .await?;
        self.persist_manifest(&manifest).await?;
        Ok(result.split_meta.split_id)
    }

    /// In-place update: tombstone `old_ids` (at the current generation), then
    /// append `docs` (which may reuse those ids) in a strictly newer split, so a
    /// same-id update is a true replace. Returns the new split id.
    pub async fn update<I, S, D>(&self, old_ids: I, docs: D) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
        D: IntoIterator<Item = TantivyDocument>,
    {
        let _guard = self.write_lock.lock().await;
        let mut manifest = self.load_manifest().await?;
        let mut tombstones = self.load_tombstones().await?;
        let mut writer = IndexWriter::new();
        let result = writer.update(
            &mut manifest,
            &mut tombstones,
            old_ids,
            self.schema.clone(),
            docs,
        )?;
        self.blob
            .put(&result.blob_key, Bytes::from(result.split_bytes))
            .await?;
        self.persist_manifest(&manifest).await?;
        self.persist_tombstones(&tombstones).await?;
        Ok(result.split_meta.split_id)
    }

    /// Logically delete `ids` (tombstoned at the current generation; hidden from
    /// search and physically dropped at the next compaction).
    pub async fn delete<I, S>(&self, ids: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let _guard = self.write_lock.lock().await;
        let manifest = self.load_manifest().await?;
        let mut tombstones = self.load_tombstones().await?;
        let generation = manifest.max_generation();
        for id in ids {
            tombstones.delete_doc_at(id, generation);
        }
        self.persist_tombstones(&tombstones).await?;
        Ok(())
    }

    // --- query ---------------------------------------------------------------

    /// Search the index for `query` over `fields`, returning the top `limit`
    /// live hits (tombstoned docs excluded, generation-scoped + deduped).
    pub async fn search(
        &self,
        query: &str,
        fields: &[Field],
        limit: usize,
    ) -> Result<Vec<MultiSplitHit>> {
        let manifest = self.load_manifest().await?;
        let tombstones = self.load_tombstones().await?;
        let opened = self.open_splits(manifest.splits.iter()).await?;
        let handles: Vec<SplitHandle> = opened
            .iter()
            .map(|(id, generation, index)| {
                SplitHandle::with_generation(id.clone(), *generation, index)
            })
            .collect();
        Ok(multi_split_search_filtered(
            &handles,
            query,
            fields,
            limit,
            self.id_field,
            &tombstones,
        )?)
    }

    /// Like [`FtsIndex::search`], but returns the top `limit` live hits as
    /// `(id, score)` pairs (the stored doc-id + BM25 score) instead of
    /// `MultiSplitHit`. Used by the union searcher, which needs each durable
    /// hit's id (pk) to mask any pk the live tier already covers.
    pub async fn search_ids(
        &self,
        query: &str,
        fields: &[Field],
        limit: usize,
    ) -> Result<Vec<(String, f32)>> {
        let manifest = self.load_manifest().await?;
        let tombstones = self.load_tombstones().await?;
        let opened = self.open_splits(manifest.splits.iter()).await?;
        let handles: Vec<SplitHandle> = opened
            .iter()
            .map(|(id, generation, index)| {
                SplitHandle::with_generation(id.clone(), *generation, index)
            })
            .collect();
        Ok(multi_split_search_filtered_ids(
            &handles,
            query,
            fields,
            limit,
            self.id_field,
            &tombstones,
        )?)
    }

    /// BM25 search with a pre-built tantivy [`Query`]; returns `(id, score)`.
    pub async fn search_query_ids(
        &self,
        query: &dyn Query,
        limit: usize,
    ) -> Result<Vec<(String, f32)>> {
        let manifest = self.load_manifest().await?;
        let tombstones = self.load_tombstones().await?;
        let opened = self.open_splits(manifest.splits.iter()).await?;
        let handles: Vec<SplitHandle> = opened
            .iter()
            .map(|(id, generation, index)| {
                SplitHandle::with_generation(id.clone(), *generation, index)
            })
            .collect();
        Ok(multi_split_search_query_filtered_ids(
            &handles,
            query,
            limit,
            self.id_field,
            &tombstones,
        )?)
    }

    /// Total live matches for a pre-built tantivy [`Query`].
    pub async fn count_query(&self, query: &dyn Query) -> Result<usize> {
        let manifest = self.load_manifest().await?;
        let tombstones = self.load_tombstones().await?;
        let opened = self.open_splits(manifest.splits.iter()).await?;
        let handles: Vec<SplitHandle> = opened
            .iter()
            .map(|(id, generation, index)| {
                SplitHandle::with_generation(id.clone(), *generation, index)
            })
            .collect();
        Ok(multi_split_count_query_filtered(
            &handles,
            query,
            self.id_field,
            &tombstones,
        )?)
    }

    /// Search with a pre-built [`Query`] ordered by an `i64` fast field.
    pub async fn search_query_sorted_ids(
        &self,
        query: &dyn Query,
        sort_field_name: &str,
        descending: bool,
        limit: usize,
    ) -> Result<Vec<(String, f32)>> {
        let manifest = self.load_manifest().await?;
        let tombstones = self.load_tombstones().await?;
        let opened = self.open_splits(manifest.splits.iter()).await?;
        let handles: Vec<SplitHandle> = opened
            .iter()
            .map(|(id, generation, index)| {
                SplitHandle::with_generation(id.clone(), *generation, index)
            })
            .collect();
        Ok(multi_split_search_query_sorted_ids(
            &handles,
            query,
            sort_field_name,
            descending,
            limit,
            self.id_field,
            &tombstones,
        )?)
    }

    // --- compaction ----------------------------------------------------------

    /// If the [`CompactionPolicy`] says so, compact the planned splits into one,
    /// persist the advanced manifest + tombstones, and GC the superseded blobs.
    /// Returns `None` when no compaction was needed.
    pub async fn maybe_compact(&self) -> Result<Option<CompactionSummary>> {
        let _guard = self.write_lock.lock().await;
        let manifest = self.load_manifest().await?;
        let tombstones = self.load_tombstones().await?;

        if !self.policy.should_compact(&manifest, &tombstones) {
            return Ok(None);
        }
        let plan = self.policy.plan_compaction(&manifest, &tombstones);
        if plan.is_empty() {
            return Ok(None);
        }

        let opened = self
            .open_splits(
                manifest
                    .splits
                    .iter()
                    .filter(|sm| plan.contains(&sm.split_id)),
            )
            .await?;
        let inputs: Vec<(String, u64, &tantivy::Index)> = opened
            .iter()
            .map(|(id, generation, index)| (id.clone(), *generation, index))
            .collect();

        let result = Compactor::new().compact(
            &manifest,
            &tombstones,
            &inputs,
            self.schema.clone(),
            self.id_field,
        )?;

        // Persist the new split + advanced manifest/tombstones BEFORE GC: a
        // reader must never load the new manifest and miss a not-yet-written
        // split, nor read a split GC already removed.
        self.blob
            .put(&result.blob_key, Bytes::from(result.split_bytes))
            .await?;
        self.persist_manifest(&result.manifest).await?;
        self.persist_tombstones(&result.tombstones).await?;
        let deleted_blobs = gc::gc_keys(self.blob.as_ref(), &result.superseded_keys).await?;

        Ok(Some(CompactionSummary {
            compacted_split_id: result.split_meta.split_id,
            superseded: result.superseded_keys.len(),
            deleted_blobs,
        }))
    }

    /// Spawn a background task that calls [`maybe_compact`](FtsIndex::maybe_compact)
    /// every `interval`. Compaction errors are logged and the loop continues
    /// (a transient blob error shouldn't kill the scheduler). The returned
    /// [`JoinHandle`] runs until aborted or the process exits.
    pub fn spawn_compaction_scheduler(self: Arc<Self>, interval: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // Skip the immediate first tick so we don't compact a just-opened index.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match self.maybe_compact().await {
                    Ok(Some(summary)) => {
                        tracing_log(&format!(
                            "bluedb-engine[{}]: compacted into {} ({} superseded, {} blobs GC'd)",
                            self.index_id,
                            summary.compacted_split_id,
                            summary.superseded,
                            summary.deleted_blobs
                        ));
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing_log(&format!(
                            "bluedb-engine[{}]: compaction error: {err}",
                            self.index_id
                        ));
                    }
                }
            }
        })
    }
}

/// Minimal log sink. Replaced by `core.observalibity`/OpenTelemetry when the
/// service wires real tracing (M3); kept dependency-free here.
fn tracing_log(message: &str) {
    eprintln!("{message}");
}
