//! Logical deletes — **tombstones** over immutable splits.
//!
//! Splits are immutable bundles in object storage: once packed, a split's
//! documents never change. So a "delete" can't physically remove a document
//! without rewriting the split. Instead we record deletes *logically* — a set
//! of deleted **doc-ids** — and filter them out at query time. The physical
//! removal happens later, lazily, during merge/compaction (see
//! [`crate::merge`]).
//!
//! ## Doc-id field
//! Tombstoning requires a way to name a document independently of its physical
//! `DocAddress` (which is per-split and changes on rebuild). The engine asks
//! the caller to designate one **STORED** field as the document id (a string
//! primary key, or a u64 rendered as a decimal string). At query time the
//! filter fetches each candidate hit's stored id and drops it if tombstoned.
//!
//! ## Where tombstones live
//! Tombstones are a small serde-serializable set persisted as a sibling blob
//! at `indexes/<index_id>/deletes.json` (see [`Tombstones::blob_key_for`]).
//! Keeping them *out* of the manifest means a delete is a cheap independent
//! write that doesn't churn the (larger) split catalog, and old manifests stay
//! byte-for-byte valid. Like the manifest, this module only produces/parses
//! bytes — persisting them is the coordinator's job (no write method is added
//! to the frozen [`bluedb_storage::BlobStore`]).
//!
//! ## Generation-scoped deletes (why a plain id-set is wrong)
//! Splits carry a monotonic `generation` (see [`crate::manifest::SplitMeta`]):
//! every append/compaction mints a split with a strictly higher generation than
//! any existing split. An update is modeled as *tombstone the old id, then
//! append a new doc carrying the same id* in a fresh, higher-generation split.
//!
//! A naïve "set of deleted ids" cannot distinguish the old version from the
//! re-inserted new one — both share the id — so it would hide BOTH and the row
//! would vanish entirely. We instead record, per id, the **generation at which
//! the delete was issued**: a document is hidden only if **its split's
//! generation `<=` the recorded deletion generation**. A re-append lands in a
//! split whose generation is strictly greater, so it stays live. This makes
//! a same-id update behave as a true in-place replace.
//!
//! ## Legacy on-disk format
//! Old `deletes.json` blobs carry a bare `deleted_ids` array (no generations).
//! Such a delete predates the current write and was meant to hide every then
//! existing copy of that id, so legacy entries deserialize as **deleted at
//! `u64::MAX`** — "always deleted", hiding the id in any split (no future
//! re-append can outrank `u64::MAX` short of saturation, which never happens in
//! practice). New writes always record a concrete generation, so the format
//! self-migrates on the next persisted delete.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use bluedb_storage::BlobStore;

/// Sentinel deletion generation for legacy (generation-less) tombstones: an id
/// deleted "at `u64::MAX`" is hidden in every split, matching the old
/// always-delete semantics. See the module docs.
pub const LEGACY_DELETED_AT: u64 = u64::MAX;

/// The set of logically deleted doc-ids for one logical index, each scoped to
/// the **generation at which it was deleted**.
///
/// A `BTreeMap<id, deleted_at_generation>` keeps the on-disk JSON stable
/// (sorted by id) and membership tests `O(log n)`. Doc-ids are strings; a u64
/// primary key is stored as its decimal rendering (see [`crate::id_of_u64`]).
///
/// A document in a split of generation `g` is hidden iff `g <= deleted_at` for
/// its id. A re-append at a higher generation therefore survives the delete —
/// the model that makes a same-id update a true in-place replace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstones {
    /// Logical index identifier these tombstones belong to.
    pub index_id: String,
    /// Map of deleted doc-id → the split generation at which it was deleted.
    /// Parsed via [`deser_deleted`] so legacy bare-array `deleted_ids` blobs
    /// still load (each legacy id mapped to [`LEGACY_DELETED_AT`]).
    #[serde(default, alias = "deleted_ids", deserialize_with = "deser_deleted")]
    pub deleted: BTreeMap<String, u64>,
}

/// Deserialize the `deleted` field, accepting BOTH the current shape
/// (`{"id": generation}`) and the legacy shape (`["id", ...]`). Legacy ids map
/// to [`LEGACY_DELETED_AT`].
fn deser_deleted<'de, D>(deserializer: D) -> Result<BTreeMap<String, u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    use std::collections::BTreeSet;

    /// Either the new map form or the legacy array form.
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum DeletedRepr {
        Map(BTreeMap<String, u64>),
        Legacy(BTreeSet<String>),
    }

    Ok(match DeletedRepr::deserialize(deserializer)? {
        DeletedRepr::Map(m) => m,
        DeletedRepr::Legacy(ids) => ids.into_iter().map(|id| (id, LEGACY_DELETED_AT)).collect(),
    })
}

impl Tombstones {
    /// An empty tombstone set for `index_id`.
    pub fn new(index_id: impl Into<String>) -> Self {
        Self {
            index_id: index_id.into(),
            deleted: BTreeMap::new(),
        }
    }

    /// Mark a single doc-id deleted at `generation`: every copy of `id` in a
    /// split of generation `<= generation` is hidden; a re-append at a higher
    /// generation survives. If the id is already tombstoned, the recorded
    /// generation is raised to the max of the two (a later delete supersedes an
    /// earlier one). Returns `true` if it was newly inserted.
    pub fn delete_doc_at(&mut self, id: impl Into<String>, generation: u64) -> bool {
        use std::collections::btree_map::Entry;
        match self.deleted.entry(id.into()) {
            Entry::Vacant(slot) => {
                slot.insert(generation);
                true
            }
            Entry::Occupied(mut slot) => {
                let cur = slot.get_mut();
                *cur = (*cur).max(generation);
                false
            }
        }
    }

    /// Mark a single doc-id deleted "always" (at [`LEGACY_DELETED_AT`]) — hidden
    /// regardless of split generation. Prefer [`Tombstones::delete_doc_at`] with
    /// the index's current max generation so a same-id re-append can supersede
    /// the delete; this method is the ergonomic choice when there is no notion
    /// of a future re-append at a higher generation. Returns `true` if newly
    /// inserted.
    pub fn delete_doc(&mut self, id: impl Into<String>) -> bool {
        self.delete_doc_at(id, LEGACY_DELETED_AT)
    }

    /// Mark many doc-ids deleted at `generation`. Returns the count newly
    /// inserted.
    pub fn delete_docs_at<I, S>(&mut self, ids: I, generation: u64) -> usize
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut added = 0;
        for id in ids {
            if self.delete_doc_at(id, generation) {
                added += 1;
            }
        }
        added
    }

    /// Mark many doc-ids deleted "always" (at [`LEGACY_DELETED_AT`]). See
    /// [`Tombstones::delete_doc`]. Returns the count newly inserted.
    pub fn delete_docs<I, S>(&mut self, ids: I) -> usize
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.delete_docs_at(ids, LEGACY_DELETED_AT)
    }

    /// Remove a doc-id from the tombstone set (e.g. it was physically dropped
    /// during compaction). Returns `true` if it had been tombstoned.
    pub fn clear_id(&mut self, id: &str) -> bool {
        self.deleted.remove(id).is_some()
    }

    /// Is this doc-id tombstoned at ALL (at any generation)? Coarse test, kept
    /// for callers (and tests) that don't care about generation scoping. For the
    /// generation-aware liveness test used by search/compaction, use
    /// [`Tombstones::is_deleted_at`].
    pub fn is_deleted(&self, id: &str) -> bool {
        self.deleted.contains_key(id)
    }

    /// Is a document with `id` living in a split of `split_generation` hidden by
    /// a tombstone? True iff the id was deleted at a generation `>=
    /// split_generation` (i.e. the delete is as new as, or newer than, the
    /// split). A re-append in a strictly newer split survives.
    pub fn is_deleted_at(&self, id: &str, split_generation: u64) -> bool {
        match self.deleted.get(id) {
            Some(&deleted_at) => split_generation <= deleted_at,
            None => false,
        }
    }

    /// The generation at which `id` was deleted, if tombstoned.
    pub fn deleted_at(&self, id: &str) -> Option<u64> {
        self.deleted.get(id).copied()
    }

    /// Number of tombstoned doc-ids.
    pub fn len(&self) -> usize {
        self.deleted.len()
    }

    /// Are there no tombstones?
    pub fn is_empty(&self) -> bool {
        self.deleted.is_empty()
    }

    /// Conventional blob key for this index's tombstones:
    /// `indexes/<index_id>/deletes.json`.
    pub fn blob_key(&self) -> String {
        Self::blob_key_for(&self.index_id)
    }

    /// Conventional tombstones blob key for an index id.
    pub fn blob_key_for(index_id: &str) -> String {
        format!("indexes/{index_id}/deletes.json")
    }

    /// Serialize to pretty JSON bytes (human-inspectable in the substrate).
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(self)?)
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// Load and parse tombstones from `blob` at `key`. Reads through the frozen
    /// [`BlobStore::get_all`] seam.
    pub async fn load<B: BlobStore + ?Sized>(blob: &B, key: &str) -> anyhow::Result<Self> {
        let bytes = blob.get_all(key).await?;
        Self::from_bytes(&bytes)
    }

    /// Load tombstones for `index_id` using the conventional blob key.
    pub async fn load_for<B: BlobStore + ?Sized>(blob: &B, index_id: &str) -> anyhow::Result<Self> {
        Self::load(blob, &Self::blob_key_for(index_id)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_and_membership() {
        let mut t = Tombstones::new("recon-2026");
        assert!(t.is_empty());
        assert!(t.delete_doc("inv-1"));
        assert!(!t.delete_doc("inv-1"), "re-delete is a no-op");
        assert_eq!(t.delete_docs(["inv-2", "inv-3", "inv-1"]), 2);
        assert!(t.is_deleted("inv-2"));
        assert!(!t.is_deleted("inv-99"));
        assert_eq!(t.len(), 3);
        assert!(t.clear_id("inv-2"));
        assert!(!t.is_deleted("inv-2"));
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn generation_scoped_liveness() {
        let mut t = Tombstones::new("recon-2026");
        // Deleted at generation 1: hides any copy in a split of gen <= 1.
        t.delete_doc_at("X", 1);
        assert!(t.is_deleted("X"), "coarse membership: tombstoned at all");
        assert!(t.is_deleted_at("X", 1), "same-gen split is hidden");
        assert!(t.is_deleted_at("X", 0), "older split is hidden");
        assert!(
            !t.is_deleted_at("X", 2),
            "a re-append at a NEWER generation survives the delete"
        );
        // A later delete supersedes (raises the recorded generation).
        assert!(!t.delete_doc_at("X", 3), "id already present");
        assert_eq!(t.deleted_at("X"), Some(3));
        assert!(t.is_deleted_at("X", 2), "now hidden up to gen 3");
        assert!(!t.is_deleted_at("X", 4));
        // A lower-gen re-delete does not lower the recorded generation.
        assert!(!t.delete_doc_at("X", 1));
        assert_eq!(t.deleted_at("X"), Some(3));
    }

    #[test]
    fn round_trip_serialize_deserialize() {
        let mut t = Tombstones::new("recon-2026");
        t.delete_docs_at(["b", "a", "c"], 2);
        let bytes = t.to_bytes().expect("serialize");
        let back = Tombstones::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, t);
        // BTreeMap keeps the JSON sorted+stable by id.
        let json = String::from_utf8(bytes).unwrap();
        assert!(json.find("\"a\"").unwrap() < json.find("\"b\"").unwrap());
        assert_eq!(t.deleted_at("a"), Some(2));
        assert_eq!(t.blob_key(), "indexes/recon-2026/deletes.json");
    }

    #[test]
    fn empty_deletes_json_parses() {
        // Back-compat: a tombstones blob with no `deleted`/`deleted_ids` key
        // parses to empty.
        let json = br#"{"index_id":"x"}"#;
        let t = Tombstones::from_bytes(json).expect("parse");
        assert!(t.is_empty());
        assert_eq!(t.index_id, "x");
    }

    #[test]
    fn legacy_bare_array_parses_as_always_deleted() {
        // Back-compat: an OLD blob carried a bare `deleted_ids` array (no
        // generations). Each legacy id deserializes as deleted at u64::MAX, so
        // it is hidden in EVERY split (the old always-delete semantics).
        let json = br#"{"index_id":"x","deleted_ids":["inv-1","inv-2"]}"#;
        let t = Tombstones::from_bytes(json).expect("parse legacy");
        assert_eq!(t.len(), 2);
        assert_eq!(t.deleted_at("inv-1"), Some(LEGACY_DELETED_AT));
        assert!(t.is_deleted_at("inv-1", u64::MAX), "always hidden");
        assert!(t.is_deleted_at("inv-1", 0));
        assert!(t.is_deleted_at("inv-2", 1_000_000));
        assert!(!t.is_deleted_at("inv-99", 0));
    }

    #[test]
    fn current_map_form_parses() {
        // The current on-disk form is a {id: generation} map.
        let json = br#"{"index_id":"x","deleted":{"inv-1":3,"inv-2":7}}"#;
        let t = Tombstones::from_bytes(json).expect("parse map form");
        assert_eq!(t.deleted_at("inv-1"), Some(3));
        assert_eq!(t.deleted_at("inv-2"), Some(7));
        assert!(t.is_deleted_at("inv-1", 3));
        assert!(!t.is_deleted_at("inv-1", 4));
    }
}
