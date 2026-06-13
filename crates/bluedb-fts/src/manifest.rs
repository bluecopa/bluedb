//! Split **manifest** — the catalog of splits that make up a logical index.
//!
//! Search is stateless and horizontal: a searcher needs to know *which* splits
//! exist for an index and where they live. The manifest is that list — a small
//! serde-serializable blob persisted alongside the splits in the substrate.
//!
//! The manifest is read through [`bluedb_storage::BlobStore`] (the frozen
//! read-only seam). Writing it is the indexer/coordinator's job; this module
//! gives you the serialized bytes ([`Manifest::to_bytes`]) to hand to whatever
//! does the `put` (e.g. `slatedb::Db::put` directly, or a future write trait) —
//! it does not assume any write method on `BlobStore`.

use serde::{Deserialize, Serialize};

use bluedb_storage::BlobStore;

/// Metadata for a single split within an index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitMeta {
    /// Opaque, unique-within-index identifier. Also the blob key suffix by
    /// convention (see [`SplitMeta::blob_key`]).
    pub split_id: String,
    /// Number of documents indexed in this split.
    pub num_docs: u64,
    /// Size of the packed split blob, in bytes.
    pub num_bytes: u64,
    /// Monotonic generation counter — bumped each time the split set is
    /// rewritten (merge/compaction). Lets readers detect staleness.
    pub generation: u64,
    /// Optional `[min, max]` event-time range covered by the split, as
    /// millisecond epoch timestamps. Enables time-range split pruning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_range: Option<(i64, i64)>,
}

impl SplitMeta {
    /// A new split-meta with no time range.
    pub fn new(
        split_id: impl Into<String>,
        num_docs: u64,
        num_bytes: u64,
        generation: u64,
    ) -> Self {
        Self {
            split_id: split_id.into(),
            num_docs,
            num_bytes,
            generation,
            time_range: None,
        }
    }

    /// Attach a `[min, max]` epoch-millis time range.
    pub fn with_time_range(mut self, min: i64, max: i64) -> Self {
        self.time_range = Some((min, max));
        self
    }

    /// Conventional blob key for this split within `index_id`:
    /// `indexes/<index_id>/splits/<split_id>.split`.
    pub fn blob_key(&self, index_id: &str) -> String {
        format!("indexes/{index_id}/splits/{}.split", self.split_id)
    }
}

/// The catalog of all splits for one logical index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Logical index identifier.
    pub index_id: String,
    /// All splits currently part of the index.
    #[serde(default)]
    pub splits: Vec<SplitMeta>,
}

impl Manifest {
    /// A new, empty manifest for `index_id`.
    pub fn new(index_id: impl Into<String>) -> Self {
        Self {
            index_id: index_id.into(),
            splits: Vec::new(),
        }
    }

    /// Append a split to the catalog.
    pub fn push(&mut self, split: SplitMeta) -> &mut Self {
        self.splits.push(split);
        self
    }

    /// Total document count across all splits.
    pub fn num_docs(&self) -> u64 {
        self.splits.iter().map(|s| s.num_docs).sum()
    }

    /// The highest split `generation` currently in the catalog, or `0` if empty.
    ///
    /// Appends/compactions mint the next split at `max_generation() + 1`, so
    /// this is the boundary used by generation-scoped deletes
    /// ([`crate::tombstones`]): a delete recorded at `max_generation()` hides
    /// every existing split's copy of an id while letting a fresh re-append (at
    /// `max_generation() + 1`) survive.
    pub fn max_generation(&self) -> u64 {
        self.splits.iter().map(|s| s.generation).max().unwrap_or(0)
    }

    /// Conventional blob key for this index's manifest:
    /// `indexes/<index_id>/manifest.json`.
    pub fn blob_key(&self) -> String {
        Self::blob_key_for(&self.index_id)
    }

    /// Conventional manifest blob key for an index id.
    pub fn blob_key_for(index_id: &str) -> String {
        format!("indexes/{index_id}/manifest.json")
    }

    /// Serialize to pretty JSON bytes (human-inspectable in the substrate).
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(self)?)
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// Load and parse a manifest from `blob` at the given `key`.
    ///
    /// Reads through the frozen [`BlobStore::get_all`] seam.
    pub async fn load<B: BlobStore + ?Sized>(blob: &B, key: &str) -> anyhow::Result<Self> {
        let bytes = blob.get_all(key).await?;
        Self::from_bytes(&bytes)
    }

    /// Load a manifest for `index_id` using the conventional blob key.
    pub async fn load_for<B: BlobStore + ?Sized>(
        blob: &B,
        index_id: &str,
    ) -> anyhow::Result<Self> {
        Self::load(blob, &Self::blob_key_for(index_id)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_serialize_deserialize() {
        let mut m = Manifest::new("recon-2026");
        m.push(SplitMeta::new("split-001", 10, 4096, 1))
            .push(SplitMeta::new("split-002", 5, 2048, 1).with_time_range(1_700_000_000, 1_700_100_000));

        let bytes = m.to_bytes().expect("serialize");
        let back = Manifest::from_bytes(&bytes).expect("deserialize");

        assert_eq!(back, m);
        assert_eq!(back.num_docs(), 15);
        assert_eq!(back.splits[1].time_range, Some((1_700_000_000, 1_700_100_000)));
        assert_eq!(back.splits[0].blob_key("recon-2026"), "indexes/recon-2026/splits/split-001.split");
        assert_eq!(m.blob_key(), "indexes/recon-2026/manifest.json");
    }
}
