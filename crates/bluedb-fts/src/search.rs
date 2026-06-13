//! Multi-split search — run one BM25 query across N splits and merge the
//! top-K ranked results into a single sorted result set.
//!
//! A logical index is a set of splits (see [`crate::manifest::Manifest`]).
//! Searching it means querying each split's tantivy [`Index`] independently and
//! merging the per-split ranked hits. BM25 scores are per-segment but
//! comparable enough across splits of the same corpus for top-K ranking; we
//! merge by descending score, breaking ties deterministically by split index
//! then `DocAddress` so results are stable.

use std::cmp::Ordering;

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Value};
use tantivy::{DocAddress, Index, TantivyDocument};

use crate::tombstones::Tombstones;
use crate::{doc_id_string, IdField};

/// One hit from a multi-split search.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MultiSplitHit {
    /// BM25 score.
    pub score: f32,
    /// Index of the split (into the `splits` slice passed to the search) that
    /// produced this hit. Use it to fetch the document from the right searcher.
    pub split_ord: usize,
    /// Address of the matching document *within that split*.
    pub doc_address: DocAddress,
}

/// A split to search: an identifier, its [`SplitMeta`]-recorded `generation`,
/// and its opened tantivy index.
///
/// The `generation` is what makes generation-scoped deletes work
/// ([`crate::tombstones`]): the filtered search compares each split's generation
/// against the tombstone's recorded deletion generation, so a same-id re-append
/// in a newer split survives a delete of the old version. It also orders
/// last-write-wins dedup: when one id appears live in multiple splits, the
/// highest-generation occurrence wins.
///
/// [`SplitHandle::new`] defaults the generation to `0` for back-compat with
/// callers that don't track it (e.g. the plain non-filtered search, which
/// ignores generation entirely); use [`SplitHandle::with_generation`] or set
/// the field to carry the manifest's recorded value.
///
/// [`SplitMeta`]: crate::manifest::SplitMeta
pub struct SplitHandle<'a> {
    /// Split identifier (for diagnostics / mapping back to the manifest).
    pub split_id: String,
    /// The split's monotonic generation, from its
    /// [`SplitMeta`](crate::manifest::SplitMeta). Higher = written later.
    pub generation: u64,
    /// The opened index (whole-split or lazily opened — either works).
    pub index: &'a Index,
}

impl<'a> SplitHandle<'a> {
    /// Convenience constructor. Generation defaults to `0`; use
    /// [`SplitHandle::with_generation`] (or set the `generation` field) to carry
    /// the manifest's recorded value, which the generation-aware filtered search
    /// needs.
    pub fn new(split_id: impl Into<String>, index: &'a Index) -> Self {
        Self {
            split_id: split_id.into(),
            generation: 0,
            index,
        }
    }

    /// Constructor that records the split's `generation` (from its
    /// [`SplitMeta`](crate::manifest::SplitMeta)). Use this for
    /// [`multi_split_search_filtered`] so generation-scoped deletes and
    /// last-write-wins dedup work correctly.
    pub fn with_generation(
        split_id: impl Into<String>,
        generation: u64,
        index: &'a Index,
    ) -> Self {
        Self {
            split_id: split_id.into(),
            generation,
            index,
        }
    }
}

/// Run `query_str` over `fields` across every split in `splits`, returning the
/// merged top-`limit` hits sorted by descending score.
///
/// Each split is queried independently with a fresh [`QueryParser`] built from
/// that split's schema (splits of one logical index share a schema, but parsing
/// per-index keeps field ids correct). Per-split top-`limit` hits are collected,
/// then globally merged and truncated to `limit`.
pub fn multi_split_search(
    splits: &[SplitHandle<'_>],
    query_str: &str,
    fields: &[Field],
    limit: usize,
) -> anyhow::Result<Vec<MultiSplitHit>> {
    let mut all: Vec<MultiSplitHit> = Vec::new();

    for (split_ord, handle) in splits.iter().enumerate() {
        let index = handle.index;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let parser = QueryParser::for_index(index, fields.to_vec());
        let query = parser
            .parse_query(query_str)
            .map_err(|err| anyhow::anyhow!("parse query for split {}: {err}", handle.split_id))?;

        // In this tantivy fork `TopDocs` becomes a `Collector` only after
        // `.order_by_score()`.
        let hits = searcher.search(&query, &TopDocs::with_limit(limit).order_by_score())?;
        for (score, doc_address) in hits {
            all.push(MultiSplitHit {
                score,
                split_ord,
                doc_address,
            });
        }
    }

    // Merge: descending score, deterministic tie-break by (split_ord, doc).
    all.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.split_ord.cmp(&b.split_ord))
            .then_with(|| a.doc_address.segment_ord.cmp(&b.doc_address.segment_ord))
            .then_with(|| a.doc_address.doc_id.cmp(&b.doc_address.doc_id))
    });
    all.truncate(limit);
    Ok(all)
}

/// Count total matches for `query_str` across all `splits` (sum of per-split
/// hit counts, capped at each split's `limit`-less count via [`tantivy::collector::Count`]).
pub fn multi_split_count(
    splits: &[SplitHandle<'_>],
    query_str: &str,
    fields: &[Field],
) -> anyhow::Result<usize> {
    use tantivy::collector::Count;

    let mut total = 0usize;
    for handle in splits {
        let index = handle.index;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let parser = QueryParser::for_index(index, fields.to_vec());
        let query = parser
            .parse_query(query_str)
            .map_err(|err| anyhow::anyhow!("parse query for split {}: {err}", handle.split_id))?;
        total += searcher.search(&query, &Count)?;
    }
    Ok(total)
}

/// Tombstone-aware, generation-scoped search: like [`multi_split_search`], but
/// every candidate hit's stored doc-id is fetched and the hit is dropped if it
/// is hidden by `tombstones` **for that split's generation**, then
/// last-write-wins dedup keeps exactly one live copy of each id.
///
/// Two pieces of correctness beyond a plain id-set filter:
///
/// 1. **Generation-scoped deletes.** A hit is dropped iff its split's
///    `generation` (from [`SplitHandle::generation`]) is `<=` the generation at
///    which its id was tombstoned (see [`Tombstones::is_deleted_at`]). So a
///    same-id update — tombstone old `X` at the current generation, then
///    re-append `X` in a strictly newer split — keeps the new `X` live while
///    hiding the old one. (For this to work the handles MUST carry their
///    manifest generation; see [`SplitHandle::with_generation`].)
/// 2. **Last-write-wins dedup by id.** When the same id is live in more than one
///    split (a same-id append without a tombstone, or a duplicate), only the
///    **highest-generation** occurrence is kept (ties broken by higher
///    `split_ord`, then higher score), so an update yields exactly one hit.
///
/// Because deletes are logical and a doc may appear in several splits, we
/// over-fetch per split — collecting `limit + tombstones.len()` raw hits per
/// split before filtering — so the merged-and-truncated top-`limit` can still be
/// filled after dead/superseded copies are removed.
///
/// `id_field` names the **STORED** field that uniquely identifies a document
/// (see [`IdField`]); it must be the same field, by schema position, in every
/// split. The existing [`multi_split_search`] signature is left untouched for
/// back-compat — this is purely additive.
pub fn multi_split_search_filtered(
    splits: &[SplitHandle<'_>],
    query_str: &str,
    fields: &[Field],
    limit: usize,
    id_field: IdField,
    tombstones: &Tombstones,
) -> anyhow::Result<Vec<MultiSplitHit>> {
    // Over-fetch so dropped (tombstoned/superseded) hits don't starve the
    // top-`limit`. (We can't take the plain-search fast path even with no
    // tombstones, because last-write-wins dedup must still collapse a same-id
    // doc that appears live in multiple splits.)
    let per_split_limit = limit.saturating_add(tombstones.len()).max(limit);

    // Carry the id + generation alongside each surviving hit so we can dedup.
    struct Candidate {
        hit: MultiSplitHit,
        id: String,
        generation: u64,
    }

    let mut all: Vec<Candidate> = Vec::new();
    for (split_ord, handle) in splits.iter().enumerate() {
        let index = handle.index;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let parser = QueryParser::for_index(index, fields.to_vec());
        let query = parser
            .parse_query(query_str)
            .map_err(|err| anyhow::anyhow!("parse query for split {}: {err}", handle.split_id))?;

        let hits = searcher.search(
            &query,
            &TopDocs::with_limit(per_split_limit).order_by_score(),
        )?;
        for (score, doc_address) in hits {
            let stored: TantivyDocument = searcher.doc(doc_address)?;
            let id = id_field.extract(&stored).ok_or_else(|| {
                anyhow::anyhow!(
                    "split {} hit at {doc_address:?} has no stored value for the id field; \
                     the id field must be STORED and present on every document",
                    handle.split_id
                )
            })?;
            // Generation-scoped delete: hide this copy only if the delete is as
            // new as, or newer than, this split.
            if tombstones.is_deleted_at(&id, handle.generation) {
                continue;
            }
            all.push(Candidate {
                hit: MultiSplitHit {
                    score,
                    split_ord,
                    doc_address,
                },
                id,
                generation: handle.generation,
            });
        }
    }

    // Last-write-wins dedup by id: for each id keep the highest-generation
    // occurrence (tie-break: higher split_ord, then higher score). This
    // collapses an un-tombstoned same-id re-append to a single live hit.
    use std::collections::HashMap;
    let mut best_by_id: HashMap<&str, usize> = HashMap::with_capacity(all.len());
    for (i, cand) in all.iter().enumerate() {
        match best_by_id.get(cand.id.as_str()) {
            Some(&j) => {
                let cur = &all[j];
                let wins = cand.generation > cur.generation
                    || (cand.generation == cur.generation
                        && (cand.hit.split_ord > cur.hit.split_ord
                            || (cand.hit.split_ord == cur.hit.split_ord
                                && cand.hit.score > cur.hit.score)));
                if wins {
                    best_by_id.insert(cand.id.as_str(), i);
                }
            }
            None => {
                best_by_id.insert(cand.id.as_str(), i);
            }
        }
    }

    let keep: std::collections::BTreeSet<usize> = best_by_id.values().copied().collect();
    let mut merged: Vec<MultiSplitHit> = all
        .iter()
        .enumerate()
        .filter(|(i, _)| keep.contains(i))
        .map(|(_, c)| c.hit)
        .collect();

    merged.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.split_ord.cmp(&b.split_ord))
            .then_with(|| a.doc_address.segment_ord.cmp(&b.doc_address.segment_ord))
            .then_with(|| a.doc_address.doc_id.cmp(&b.doc_address.doc_id))
    });
    merged.truncate(limit);
    Ok(merged)
}

/// Tombstone-aware count: total live matches for `query_str` across all
/// `splits`, excluding documents whose stored id is in `tombstones`.
///
/// Unlike [`multi_split_count`] (a cheap per-split `Count`), this must inspect
/// each match's stored id to decide liveness, so it collects matching addresses
/// and filters. With no tombstones it falls back to the cheap path.
pub fn multi_split_count_filtered(
    splits: &[SplitHandle<'_>],
    query_str: &str,
    fields: &[Field],
    id_field: IdField,
    tombstones: &Tombstones,
) -> anyhow::Result<usize> {
    if tombstones.is_empty() {
        return multi_split_count(splits, query_str, fields);
    }

    let mut total = 0usize;
    for handle in splits {
        let index = handle.index;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let parser = QueryParser::for_index(index, fields.to_vec());
        let query = parser
            .parse_query(query_str)
            .map_err(|err| anyhow::anyhow!("parse query for split {}: {err}", handle.split_id))?;
        // Collect all matching addresses, then drop tombstoned ids. `usize::MAX`
        // as the TopDocs limit gathers every match for the (typically small)
        // per-split result set.
        let hits = searcher.search(&query, &TopDocs::with_limit(usize::MAX).order_by_score())?;
        for (_score, doc_address) in hits {
            let stored: TantivyDocument = searcher.doc(doc_address)?;
            if let Some(id) = id_field.extract(&stored) {
                if tombstones.is_deleted(&id) {
                    continue;
                }
            }
            total += 1;
        }
    }
    Ok(total)
}

/// Extract the stored id value of `field` from `doc` as a string, matching the
/// convention used by [`IdField`]. Returns `None` if the field has no stored
/// string/u64 value. Exposed for callers that read ids directly.
pub fn stored_id(doc: &TantivyDocument, field: Field) -> Option<String> {
    let v = doc.get_first(field)?;
    doc_id_string(v.as_str(), v.as_u64())
}
