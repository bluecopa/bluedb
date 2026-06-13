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
use tantivy::schema::Field;
use tantivy::{DocAddress, Index};

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

/// A split to search: an identifier and its opened tantivy index.
pub struct SplitHandle<'a> {
    /// Split identifier (for diagnostics / mapping back to the manifest).
    pub split_id: String,
    /// The opened index (whole-split or lazily opened — either works).
    pub index: &'a Index,
}

impl<'a> SplitHandle<'a> {
    /// Convenience constructor.
    pub fn new(split_id: impl Into<String>, index: &'a Index) -> Self {
        Self {
            split_id: split_id.into(),
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
