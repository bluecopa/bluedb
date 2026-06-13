//! Compaction policy — a pure decision layer over a manifest + tombstones.
//!
//! Compaction ([`crate::merge`]) is the mechanism; this is the *trigger*. It
//! answers two questions with **no I/O** (so it is trivially testable and can be
//! evaluated cheaply by any coordinator on every write):
//!
//! 1. [`CompactionPolicy::should_compact`] — is the index unhealthy enough to
//!    pay for a rewrite?
//! 2. [`CompactionPolicy::plan_compaction`] — if so, which split-ids should the
//!    caller hand to [`Compactor::compact`](crate::merge::Compactor::compact)?
//!
//! Two health signals drive it:
//! - **Split count.** Every append mints a new split ([`crate::writer`]); query
//!   fan-out is one `Index::open` + search per split, so an unbounded split
//!   count linearly degrades search. Past `max_splits` we compact.
//! - **Tombstone ratio.** Logical deletes ([`crate::tombstones`]) leave dead
//!   documents physically present and scored until a compaction drops them. When
//!   tombstones / total-docs exceeds `max_tombstone_ratio`, the index is mostly
//!   dead weight and we compact to reclaim it.
//!
//! In both cases we only fire if there are at least `min_splits_to_merge` splits
//! — merging a single split is pointless churn (it would just rewrite it).

use crate::manifest::Manifest;
use crate::tombstones::Tombstones;

/// Thresholds that decide when an index should be compacted.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactionPolicy {
    /// Compact once the split count exceeds this (query fan-out bound).
    pub max_splits: usize,
    /// Compact once `tombstones.len() / total_docs` exceeds this (dead-weight
    /// bound). A ratio in `[0.0, 1.0]`.
    pub max_tombstone_ratio: f64,
    /// Never compact fewer than this many splits — merging one split is pointless
    /// churn. Acts as a floor on both triggers.
    pub min_splits_to_merge: usize,
}

impl Default for CompactionPolicy {
    /// Sensible defaults: compact past 8 splits or a 30% tombstone ratio, and
    /// never merge fewer than 2 splits.
    fn default() -> Self {
        Self {
            max_splits: 8,
            max_tombstone_ratio: 0.30,
            min_splits_to_merge: 2,
        }
    }
}

impl CompactionPolicy {
    /// Should the index be compacted now?
    ///
    /// True when there are at least `min_splits_to_merge` splits AND either:
    /// - the split count exceeds `max_splits`, OR
    /// - the tombstone ratio (`tombstones.len() / manifest.num_docs()`) exceeds
    ///   `max_tombstone_ratio`.
    ///
    /// With zero total docs the ratio is treated as `0.0` (nothing to reclaim),
    /// so only the split-count trigger can fire.
    pub fn should_compact(&self, manifest: &Manifest, tombstones: &Tombstones) -> bool {
        let n_splits = manifest.splits.len();
        if n_splits < self.min_splits_to_merge {
            return false;
        }
        if n_splits > self.max_splits {
            return true;
        }
        self.tombstone_ratio(manifest, tombstones) > self.max_tombstone_ratio
    }

    /// Tombstone ratio: tombstoned ids / total docs, or `0.0` when the index is
    /// empty. Exposed so callers can log/observe the signal that drove a decision.
    pub fn tombstone_ratio(&self, manifest: &Manifest, tombstones: &Tombstones) -> f64 {
        let total = manifest.num_docs();
        if total == 0 {
            return 0.0;
        }
        tombstones.len() as f64 / total as f64
    }

    /// Plan which split-ids to merge, in manifest order.
    ///
    /// **v1 policy: full compaction.** When [`should_compact`](Self::should_compact)
    /// fires we return *every* split id, folding the whole index into one split.
    /// This is the simplest correct plan and is what bounds both signals in one
    /// shot: split count drops to 1 and every tombstone whose copies are all
    /// covered gets physically dropped (and its tombstone cleared — the compactor
    /// only clears tombstones when the merge covered every split, see
    /// [`crate::merge`]). A partial/tiered plan (e.g. merge the smallest-N by
    /// `num_bytes`) is a future refinement; it trades a cheaper rewrite for
    /// leaving some tombstones uncleared, so v1 prefers the always-correct full
    /// merge. Returns an empty vec when the policy does not fire.
    pub fn plan_compaction(&self, manifest: &Manifest, tombstones: &Tombstones) -> Vec<String> {
        if !self.should_compact(manifest, tombstones) {
            return Vec::new();
        }
        manifest
            .splits
            .iter()
            .map(|sm| sm.split_id.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::SplitMeta;

    fn manifest(n_splits: usize, docs_per_split: u64) -> Manifest {
        let mut m = Manifest::new("recon-2026");
        for i in 0..n_splits {
            m.push(SplitMeta::new(format!("split-{i:03}"), docs_per_split, 100, 1));
        }
        m
    }

    #[test]
    fn fires_on_split_count() {
        let p = CompactionPolicy {
            max_splits: 3,
            max_tombstone_ratio: 0.9,
            min_splits_to_merge: 2,
        };
        let empty = Tombstones::new("recon-2026");
        assert!(!p.should_compact(&manifest(3, 10), &empty), "at the cap, not over");
        assert!(p.should_compact(&manifest(4, 10), &empty), "over the cap fires");
    }

    #[test]
    fn fires_on_tombstone_ratio() {
        let p = CompactionPolicy {
            max_splits: 100,
            max_tombstone_ratio: 0.30,
            min_splits_to_merge: 2,
        };
        // 2 splits * 10 docs = 20 docs total.
        let m = manifest(2, 10);
        let mut tombs = Tombstones::new("recon-2026");
        for i in 0..6 {
            tombs.delete_doc(format!("id-{i}")); // 6/20 = 0.30, NOT > 0.30
        }
        assert!(!p.should_compact(&m, &tombs), "ratio at threshold does not fire");
        tombs.delete_doc("id-extra"); // 7/20 = 0.35 > 0.30
        assert!(p.should_compact(&m, &tombs), "ratio over threshold fires");
    }

    #[test]
    fn respects_min_splits_to_merge() {
        let p = CompactionPolicy {
            max_splits: 0, // every non-trivial index is "over" the split cap...
            max_tombstone_ratio: 0.0,
            min_splits_to_merge: 2,
        };
        let empty = Tombstones::new("recon-2026");
        assert!(!p.should_compact(&manifest(1, 10), &empty), "one split never merges");
        assert!(!p.should_compact(&manifest(0, 0), &empty), "empty index never merges");
        assert!(p.should_compact(&manifest(2, 10), &empty), "two splits can merge");
    }

    #[test]
    fn empty_index_has_zero_ratio() {
        let p = CompactionPolicy::default();
        let m = manifest(2, 0); // 0 docs
        let mut tombs = Tombstones::new("recon-2026");
        tombs.delete_doc("ghost");
        // No divide-by-zero blowup; ratio is 0.0, and split count (2) is under the
        // default cap (8), so it does not fire.
        assert_eq!(p.tombstone_ratio(&m, &tombs), 0.0);
        assert!(!p.should_compact(&m, &tombs));
    }

    #[test]
    fn plan_returns_all_ids_when_fired_else_empty() {
        let p = CompactionPolicy {
            max_splits: 2,
            max_tombstone_ratio: 0.9,
            min_splits_to_merge: 2,
        };
        let empty = Tombstones::new("recon-2026");

        // Not fired (3 == ...; actually 3 > 2 fires). Use 2 splits to stay under.
        assert!(p.plan_compaction(&manifest(2, 10), &empty).is_empty());

        // Fired: 3 splits > max_splits=2 -> full compaction returns all 3 ids.
        let plan = p.plan_compaction(&manifest(3, 10), &empty);
        assert_eq!(
            plan,
            vec![
                "split-000".to_string(),
                "split-001".to_string(),
                "split-002".to_string(),
            ]
        );
    }
}
