//! Garbage collection — physically delete split blobs that the manifest no
//! longer references.
//!
//! Compaction ([`crate::merge`]) rewrites N input splits into one and returns
//! [`CompactionResult::superseded_keys`](crate::merge::CompactionResult::superseded_keys),
//! but it deletes nothing: the engine's frozen read seam ([`bluedb_storage::BlobStore`])
//! has no `delete`. Reclaiming the storage is therefore the caller's job, done
//! through the additive write seam ([`bluedb_storage::BlobStoreMut`]). This
//! module is that executor.
//!
//! Three flavors, ordered by how the keys are obtained:
//! - [`gc_keys`] — delete an explicit list (e.g. the compactor's
//!   `superseded_keys`). The primitive the other two build on.
//! - [`gc_orphaned_splits`] — list what physically exists under the index's
//!   `splits/` prefix, diff against what the *current* manifest references, and
//!   delete the unreferenced remainder. This catches splits left behind by a
//!   compaction whose manifest was already advanced (or by a crashed GC).
//! - retention policies ([`splits_below_generation`], [`expired_by_time`]) —
//!   *pure* functions that pick blob keys eligible for GC from a manifest; the
//!   caller feeds the result to [`gc_keys`]. Keeping policy pure (no I/O) keeps
//!   the dangerous part (the actual `delete`) in exactly one place and trivially
//!   testable.
//!
//! ## Safety / ordering
//! GC is a tail operation. The caller MUST persist the advanced manifest
//! *before* GC'ing the superseded splits — otherwise a reader could load the new
//! manifest, fail to find a split GC already deleted, or load the old manifest
//! and read a split GC is about to delete. Delete of a missing key is a no-op
//! (idempotent), so a GC pass can be safely retried after a crash.

use std::collections::BTreeSet;

use anyhow::Result;

use bluedb_storage::BlobStoreMut;

use crate::manifest::Manifest;

/// Delete each key in `keys`, returning the number deleted.
///
/// A delete of a missing key is a no-op (it does NOT fail the batch), so this is
/// idempotent and safe to retry. Feed it the compactor's
/// [`CompactionResult::superseded_keys`](crate::merge::CompactionResult::superseded_keys),
/// or the output of a retention policy ([`splits_below_generation`],
/// [`expired_by_time`]).
pub async fn gc_keys<B: BlobStoreMut + ?Sized>(blob: &B, keys: &[String]) -> Result<usize> {
    let mut deleted = 0usize;
    for key in keys {
        blob.delete(key).await?;
        deleted += 1;
    }
    Ok(deleted)
}

/// The blob-key prefix under which an index's splits live:
/// `indexes/<index_id>/splits/`. Matches [`SplitMeta::blob_key`](crate::manifest::SplitMeta::blob_key).
pub fn splits_prefix(index_id: &str) -> String {
    format!("indexes/{index_id}/splits/")
}

/// List every split blob physically present under the index's `splits/` prefix,
/// diff against the splits the **current** `manifest` references, and delete the
/// unreferenced ("orphaned") ones. Returns the keys deleted (ascending order).
///
/// An orphan is a split left behind by an advance the manifest already recorded:
/// e.g. a compaction persisted the new manifest (dropping the inputs) but its
/// [`gc_keys`] call never ran, or a previous GC pass was interrupted. Because
/// the manifest is the source of truth, anything under `splits/` that the
/// manifest does not list is safe to reclaim.
///
/// Reads the listing through [`BlobStoreMut::scan_prefix`] (ordered), so the
/// returned keys are sorted.
pub async fn gc_orphaned_splits<B: BlobStoreMut + ?Sized>(
    blob: &B,
    manifest: &Manifest,
) -> Result<Vec<String>> {
    let referenced: BTreeSet<String> = manifest
        .splits
        .iter()
        .map(|sm| sm.blob_key(&manifest.index_id))
        .collect();

    let prefix = splits_prefix(&manifest.index_id);
    let present = blob.scan_prefix(&prefix).await?;

    let mut orphaned: Vec<String> = present
        .into_iter()
        .map(|(key, _bytes)| key)
        .filter(|key| !referenced.contains(key))
        .collect();
    orphaned.sort();

    for key in &orphaned {
        blob.delete(key).await?;
    }
    Ok(orphaned)
}

/// Pure retention policy: blob keys of every split whose `generation` is `<`
/// `watermark`. Pair with [`gc_keys`] to drop old generations once you are sure
/// no reader will open them (e.g. after a full compaction advanced everything to
/// a higher generation).
///
/// Strictly-less-than is deliberate: a split at exactly `watermark` is kept, so
/// passing the manifest's current [`max_generation`](Manifest::max_generation)
/// retains the live generation and reclaims only strictly older ones.
pub fn splits_below_generation(manifest: &Manifest, watermark: u64) -> Vec<String> {
    manifest
        .splits
        .iter()
        .filter(|sm| sm.generation < watermark)
        .map(|sm| sm.blob_key(&manifest.index_id))
        .collect()
}

/// Pure retention policy: blob keys of every split whose recorded
/// [`time_range`](crate::manifest::SplitMeta::time_range) ends strictly before
/// `cutoff_millis` (epoch milliseconds) — i.e. the entire split is older than the
/// cutoff. Splits with no `time_range` are conservatively **kept** (we can't
/// prove they are expired). Pair with [`gc_keys`] for time-based retention.
pub fn expired_by_time(manifest: &Manifest, cutoff_millis: i64) -> Vec<String> {
    manifest
        .splits
        .iter()
        .filter(|sm| match sm.time_range {
            Some((_min, max)) => max < cutoff_millis,
            None => false,
        })
        .map(|sm| sm.blob_key(&manifest.index_id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::SplitMeta;

    /// `(split_id, generation, optional [min,max] time range)`.
    type SplitSpec<'a> = (&'a str, u64, Option<(i64, i64)>);

    fn manifest_with(specs: &[SplitSpec<'_>]) -> Manifest {
        let mut m = Manifest::new("recon-2026");
        for (id, gen, tr) in specs {
            let mut sm = SplitMeta::new(*id, 1, 100, *gen);
            if let Some((lo, hi)) = tr {
                sm = sm.with_time_range(*lo, *hi);
            }
            m.push(sm);
        }
        m
    }

    #[test]
    fn splits_below_generation_is_strict() {
        let m = manifest_with(&[("a", 1, None), ("b", 2, None), ("c", 3, None)]);
        let keys = splits_below_generation(&m, 3);
        assert_eq!(
            keys,
            vec![
                "indexes/recon-2026/splits/a.split".to_string(),
                "indexes/recon-2026/splits/b.split".to_string(),
            ],
            "gen 3 (== watermark) is kept; only strictly older are eligible"
        );
        assert!(splits_below_generation(&m, 1).is_empty());
        assert_eq!(splits_below_generation(&m, u64::MAX).len(), 3);
    }

    #[test]
    fn expired_by_time_keeps_unbounded_splits() {
        let m = manifest_with(&[
            ("old", 1, Some((0, 100))),
            ("mid", 1, Some((150, 250))),
            ("notime", 1, None),
        ]);
        // cutoff = 200: "old" (max 100 < 200) is expired; "mid" (max 250) and the
        // time-less "notime" are kept.
        let keys = expired_by_time(&m, 200);
        assert_eq!(keys, vec!["indexes/recon-2026/splits/old.split".to_string()]);
        // A cutoff past everything expires both time-ranged splits but never the
        // time-less one.
        assert_eq!(expired_by_time(&m, 10_000).len(), 2);
    }
}
