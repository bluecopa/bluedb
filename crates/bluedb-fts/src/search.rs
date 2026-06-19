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
use tantivy::query::{BooleanQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Term, Value};
use tantivy::{DocAddress, Index, TantivyDocument};

use crate::tombstones::Tombstones;
use crate::{doc_id_string, IdField};

/// Deterministic merged ordering for multi-split hits: descending score, then
/// ascending `(split_ord, segment_ord, doc_id)` so results are stable across
/// runs. Shared by every search variant.
fn cmp_hits(a: &MultiSplitHit, b: &MultiSplitHit) -> Ordering {
    b.score
        .partial_cmp(&a.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| a.split_ord.cmp(&b.split_ord))
        .then_with(|| a.doc_address.segment_ord.cmp(&b.doc_address.segment_ord))
        .then_with(|| a.doc_address.doc_id.cmp(&b.doc_address.doc_id))
}

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
    all.sort_by(cmp_hits);
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

    merged.sort_by(cmp_hits);
    merged.truncate(limit);
    Ok(merged)
}

/// Like [`multi_split_search_filtered`], but returns the surviving hits as
/// `(id, score)` pairs instead of `(score, DocAddress)`.
///
/// The union searcher (the live ∪ durable-split merge in `bluedb-engine`) needs
/// each durable hit's **id** (the pk) to mask any pk the live segment already
/// covers — a `DocAddress` is per-split and meaningless to the live tier. The
/// filtered search already computes each candidate's stored id internally; this
/// variant simply keeps it. Identical over-fetch, generation-scoped tombstone
/// filtering, and last-write-wins dedup as [`multi_split_search_filtered`];
/// results sorted by descending score (with the same deterministic tie-break)
/// and truncated to `limit`.
pub fn multi_split_search_filtered_ids(
    splits: &[SplitHandle<'_>],
    query_str: &str,
    fields: &[Field],
    limit: usize,
    id_field: IdField,
    tombstones: &Tombstones,
) -> anyhow::Result<Vec<(String, f32)>> {
    // Over-fetch so dropped (tombstoned/superseded) hits don't starve the
    // top-`limit` (see [`multi_split_search_filtered`]).
    let per_split_limit = limit.saturating_add(tombstones.len()).max(limit);

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

    // Last-write-wins dedup by id: keep the highest-generation occurrence
    // (tie-break: higher split_ord, then higher score).
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
    // Sort the kept candidates by the same deterministic ordering as
    // [`multi_split_search_filtered`] (descending score, then split/doc), then
    // project to `(id, score)` and truncate.
    let mut kept: Vec<&Candidate> = all
        .iter()
        .enumerate()
        .filter(|(i, _)| keep.contains(i))
        .map(|(_, c)| c)
        .collect();
    kept.sort_by(|a, b| cmp_hits(&a.hit, &b.hit));
    let mut out: Vec<(String, f32)> = kept
        .into_iter()
        .map(|c| (c.id.clone(), c.hit.score))
        .collect();
    out.truncate(limit);
    Ok(out)
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

// ============================================================================
// Query-object variants: same logic as the string-based functions above, but
// accept a pre-built `&dyn Query` instead of a query string + parser.
// ============================================================================

/// Like [`multi_split_search_filtered_ids`] but takes a pre-built tantivy
/// [`Query`] (no `QueryParser`). Returns `(id, bm25_score)` newest-wins-deduped,
/// tombstone-filtered, BM25-ordered, truncated to `limit`.
pub fn multi_split_search_query_filtered_ids(
    splits: &[SplitHandle<'_>],
    query: &dyn Query,
    limit: usize,
    id_field: IdField,
    tombstones: &Tombstones,
) -> anyhow::Result<Vec<(String, f32)>> {
    let per_split_limit = limit.saturating_add(tombstones.len()).max(limit);

    struct Candidate {
        hit: MultiSplitHit,
        id: String,
        generation: u64,
    }

    let mut all: Vec<Candidate> = Vec::new();
    for (split_ord, handle) in splits.iter().enumerate() {
        let reader = handle.index.reader()?;
        let searcher = reader.searcher();
        let hits = searcher.search(query, &TopDocs::with_limit(per_split_limit).order_by_score())?;
        for (score, doc_address) in hits {
            let stored: TantivyDocument = searcher.doc(doc_address)?;
            let id = id_field.extract(&stored).ok_or_else(|| {
                anyhow::anyhow!(
                    "split {} hit at {doc_address:?} has no stored id-field value",
                    handle.split_id
                )
            })?;
            if tombstones.is_deleted_at(&id, handle.generation) {
                continue;
            }
            all.push(Candidate {
                hit: MultiSplitHit { score, split_ord, doc_address },
                id,
                generation: handle.generation,
            });
        }
    }

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
    let mut kept: Vec<&Candidate> = all
        .iter()
        .enumerate()
        .filter(|(i, _)| keep.contains(i))
        .map(|(_, c)| c)
        .collect();
    kept.sort_by(|a, b| cmp_hits(&a.hit, &b.hit));
    let mut out: Vec<(String, f32)> = kept.into_iter().map(|c| (c.id.clone(), c.hit.score)).collect();
    out.truncate(limit);
    Ok(out)
}

/// Total live matches for a pre-built [`Query`] (tombstone-filtered, deduped).
///
/// Like [`multi_split_count_filtered`] but accepts a pre-built `&dyn Query`
/// instead of a query string. Counts distinct live document ids across all
/// splits, applying generation-scoped tombstone filtering and last-write-wins
/// dedup so each logical document is counted at most once.
pub fn multi_split_count_query_filtered(
    splits: &[SplitHandle<'_>],
    query: &dyn Query,
    id_field: IdField,
    tombstones: &Tombstones,
) -> anyhow::Result<usize> {
    let mut live: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for handle in splits.iter() {
        let reader = handle.index.reader()?;
        let searcher = reader.searcher();
        let hits = searcher.search(query, &TopDocs::with_limit(100_000).order_by_score())?;
        for (_score, doc_address) in hits {
            let stored: TantivyDocument = searcher.doc(doc_address)?;
            if let Some(id) = id_field.extract(&stored) {
                if tombstones.is_deleted_at(&id, handle.generation) {
                    continue;
                }
                let e = live.entry(id).or_insert(handle.generation);
                if handle.generation > *e {
                    *e = handle.generation;
                }
            }
        }
    }
    Ok(live.len())
}

/// Like [`multi_split_search_query_filtered_ids`] but ordered by an `i64`
/// fast field instead of BM25 score. Returns `(id, 0.0)` pairs in the
/// requested order; docs missing the field sort last.
///
/// `sort_field_name` must be registered as a fast `i64` field in every split's
/// schema. Results are tombstone-filtered and last-write-wins deduped before
/// sorting and truncation.
pub fn multi_split_search_query_sorted_ids(
    splits: &[SplitHandle<'_>],
    query: &dyn Query,
    sort_field_name: &str,
    descending: bool,
    limit: usize,
    id_field: IdField,
    tombstones: &Tombstones,
) -> anyhow::Result<Vec<(String, f32)>> {
    use tantivy::Order;

    let per_split_limit = limit.saturating_add(tombstones.len()).max(limit);

    struct Cand {
        id: String,
        sort: Option<i64>,
        generation: u64,
    }

    let mut all: Vec<Cand> = Vec::new();
    for handle in splits.iter() {
        let reader = handle.index.reader()?;
        let searcher = reader.searcher();
        // `Order` is `Copy`, so we can construct it inside the loop without
        // needing to clone.
        let order = if descending { Order::Desc } else { Order::Asc };
        let collector = TopDocs::with_limit(per_split_limit)
            .order_by_fast_field::<i64>(sort_field_name, order);
        // `order_by_fast_field::<i64>` returns `Vec<(Option<i64>, DocAddress)>`
        // in this tantivy fork (rev 6270552).
        let hits = searcher.search(query, &collector)?;
        for (sort_val, doc_address) in hits {
            let stored: TantivyDocument = searcher.doc(doc_address)?;
            if let Some(id) = id_field.extract(&stored) {
                if tombstones.is_deleted_at(&id, handle.generation) {
                    continue;
                }
                all.push(Cand { id, sort: sort_val, generation: handle.generation });
            }
        }
    }

    use std::collections::HashMap;
    let mut best: HashMap<String, usize> = HashMap::new();
    for (i, c) in all.iter().enumerate() {
        match best.get(&c.id) {
            Some(&j) if all[j].generation >= c.generation => {}
            _ => {
                best.insert(c.id.clone(), i);
            }
        }
    }
    let keep: std::collections::BTreeSet<usize> = best.values().copied().collect();
    let mut kept: Vec<&Cand> = all
        .iter()
        .enumerate()
        .filter(|(i, _)| keep.contains(i))
        .map(|(_, c)| c)
        .collect();
    kept.sort_by(|a, b| {
        let cmp = match (a.sort, b.sort) {
            (Some(x), Some(y)) => x.cmp(&y),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };
        if descending { cmp.reverse() } else { cmp }
    });
    let mut out: Vec<(String, f32)> = kept.into_iter().map(|c| (c.id.clone(), 0.0f32)).collect();
    out.truncate(limit);
    Ok(out)
}

/// Extract the stored id value of `field` from `doc` as a string, matching the
/// convention used by [`IdField`]. Returns `None` if the field has no stored
/// string/u64 value. Exposed for callers that read ids directly.
pub fn stored_id(doc: &TantivyDocument, field: Field) -> Option<String> {
    let v = doc.get_first(field)?;
    doc_id_string(v.as_str(), v.as_u64())
}

// ----------------------------------------------------------------------------
// Query niceties: pagination, highlighting, structured filters.
// ----------------------------------------------------------------------------

/// Paginated multi-split search: the merged hits for `query_str`, skipping the
/// first `offset` and returning the next `limit`.
///
/// Correctness across merged splits is the whole point: scores are per-split, so
/// we cannot ask each split for "page N" independently. Instead each split is
/// over-fetched to `offset + limit` candidates, all candidates are merged into
/// one globally-sorted list (same ordering as [`multi_split_search`]), and only
/// then do we `skip(offset).take(limit)`. Over-fetching `offset + limit` per
/// split guarantees the merged window is complete: the global top `offset +
/// limit` can contain at most that many hits from any single split.
pub fn multi_split_search_paginated(
    splits: &[SplitHandle<'_>],
    query_str: &str,
    fields: &[Field],
    offset: usize,
    limit: usize,
) -> anyhow::Result<Vec<MultiSplitHit>> {
    let per_split = offset.saturating_add(limit);
    if per_split == 0 {
        return Ok(Vec::new());
    }

    let mut all: Vec<MultiSplitHit> = Vec::new();
    for (split_ord, handle) in splits.iter().enumerate() {
        let index = handle.index;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let parser = QueryParser::for_index(index, fields.to_vec());
        let query = parser
            .parse_query(query_str)
            .map_err(|err| anyhow::anyhow!("parse query for split {}: {err}", handle.split_id))?;

        let hits = searcher.search(&query, &TopDocs::with_limit(per_split).order_by_score())?;
        for (score, doc_address) in hits {
            all.push(MultiSplitHit {
                score,
                split_ord,
                doc_address,
            });
        }
    }

    all.sort_by(cmp_hits);
    Ok(all.into_iter().skip(offset).take(limit).collect())
}

/// A structured filter to AND with a parsed full-text query.
///
/// Each variant becomes a tantivy sub-query that the document MUST match (an
/// `Occur::Must` clause), so the result is the text-query hits *narrowed* to
/// those also satisfying the filter. Use with [`multi_split_search_with_filter`].
#[derive(Debug, Clone)]
pub enum Filter {
    /// Exact term match on a `STRING`/keyword `field` (a [`TermQuery`]).
    Term { field: Field, value: String },
    /// Inclusive `u64` range `[lo, hi]` on an indexed `field` (a
    /// [`tantivy::query::RangeQuery`]). Bounds are inclusive on both ends.
    U64Range { field: Field, lo: u64, hi: u64 },
    /// Inclusive `i64` range `[lo, hi]` on an indexed `field` (e.g. an
    /// epoch-millis timestamp).
    I64Range { field: Field, lo: i64, hi: i64 },
}

impl Filter {
    /// Build the tantivy [`Query`] for this filter.
    fn to_query(&self) -> Box<dyn Query> {
        use std::ops::Bound;
        use tantivy::query::RangeQuery;
        match self {
            Filter::Term { field, value } => Box::new(TermQuery::new(
                Term::from_field_text(*field, value),
                IndexRecordOption::Basic,
            )),
            Filter::U64Range { field, lo, hi } => Box::new(RangeQuery::new(
                Bound::Included(Term::from_field_u64(*field, *lo)),
                Bound::Included(Term::from_field_u64(*field, *hi)),
            )),
            Filter::I64Range { field, lo, hi } => Box::new(RangeQuery::new(
                Bound::Included(Term::from_field_i64(*field, *lo)),
                Bound::Included(Term::from_field_i64(*field, *hi)),
            )),
        }
    }
}

/// Full-text search ANDed with a structured `filter`.
///
/// Parses `query_str` over `fields` (as [`multi_split_search`] does), then wraps
/// it and `filter` in a [`BooleanQuery`] where BOTH are `Occur::Must`, so only
/// documents matching the text query *and* the filter are returned. Merged and
/// truncated to `limit` with the standard ordering. The filter's fields must
/// exist (and be indexed appropriately) in every split's schema.
pub fn multi_split_search_with_filter(
    splits: &[SplitHandle<'_>],
    query_str: &str,
    fields: &[Field],
    filter: &Filter,
    limit: usize,
) -> anyhow::Result<Vec<MultiSplitHit>> {
    let mut all: Vec<MultiSplitHit> = Vec::new();
    for (split_ord, handle) in splits.iter().enumerate() {
        let index = handle.index;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let parser = QueryParser::for_index(index, fields.to_vec());
        let text_query = parser
            .parse_query(query_str)
            .map_err(|err| anyhow::anyhow!("parse query for split {}: {err}", handle.split_id))?;

        let combined: BooleanQuery = BooleanQuery::new(vec![
            (Occur::Must, text_query),
            (Occur::Must, filter.to_query()),
        ]);

        let hits = searcher.search(&combined, &TopDocs::with_limit(limit).order_by_score())?;
        for (score, doc_address) in hits {
            all.push(MultiSplitHit {
                score,
                split_ord,
                doc_address,
            });
        }
    }

    all.sort_by(cmp_hits);
    all.truncate(limit);
    Ok(all)
}

/// Build a highlighted HTML snippet for one hit's `field`, with the query terms
/// wrapped in `<b>...</b>` (tantivy's default snippet markup).
///
/// `index` is the split that produced the hit; `query_str` + `fields` are parsed
/// exactly as the search did, so the snippet highlights the same terms that
/// matched. `field` must be a `STORED` text field (the snippet generator reads
/// its stored value). Returns the highlighted fragment; empty string if the
/// field has no stored text on that document.
pub fn highlight(
    index: &Index,
    query_str: &str,
    fields: &[Field],
    field: Field,
    doc_address: DocAddress,
) -> anyhow::Result<String> {
    use tantivy::snippet::SnippetGenerator;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    let parser = QueryParser::for_index(index, fields.to_vec());
    let query = parser
        .parse_query(query_str)
        .map_err(|err| anyhow::anyhow!("parse query for highlight: {err}"))?;

    let generator = SnippetGenerator::create(&searcher, query.as_ref(), field)?;
    let doc: TantivyDocument = searcher.doc(doc_address)?;
    let snippet = generator.snippet_from_doc(&doc);
    Ok(snippet.to_html())
}
