//! Query niceties: pagination correct across merged splits, snippet
//! highlighting, and FTS ANDed with a structured filter.

use std::sync::Arc;

use bluedb_fts::indexer::Indexer;
use bluedb_fts::search::{
    highlight, multi_split_search, multi_split_search_paginated, multi_split_search_with_filter,
    Filter, SplitHandle,
};
use bluedb_fts::vendor::BundleDirectory;
use tantivy::directory::{FileSlice, OwnedBytes};
use tantivy::schema::{Field, Schema, FAST, INDEXED, STORED, STRING, TEXT};
use tantivy::{Index, TantivyDocument};

/// Open a packed split (empty hotcache) whole, as the indexer test does.
fn open_whole(split_bytes: Vec<u8>) -> Index {
    let file_slice = FileSlice::new(Arc::new(OwnedBytes::new(split_bytes)));
    let bundle = BundleDirectory::open_split(file_slice).expect("open split");
    Index::open(bundle).expect("open index")
}

/// Schema: stored `body` (TEXT), keyword `cat` (STRING), indexed/fast `year` (u64).
fn schema() -> (Schema, Field, Field, Field) {
    let mut sb = Schema::builder();
    let body = sb.add_text_field("body", TEXT | STORED);
    let cat = sb.add_text_field("cat", STRING | STORED);
    let year = sb.add_u64_field("year", INDEXED | STORED | FAST);
    (sb.build(), body, cat, year)
}

fn doc(
    body_f: Field,
    body: &str,
    cat_f: Field,
    cat: &str,
    year_f: Field,
    year: u64,
) -> TantivyDocument {
    let mut d = TantivyDocument::default();
    d.add_text(body_f, body);
    d.add_text(cat_f, cat);
    d.add_u64(year_f, year);
    d
}

/// Build two in-memory indexes (whole-fetch open) sharing the schema.
fn build_two_splits() -> (Schema, Field, Field, Field, tantivy::Index, tantivy::Index) {
    let (schema, body, cat, year) = schema();

    // Split A: 3 docs all mentioning "ledger".
    let a_split = Indexer::new()
        .build(
            schema.clone(),
            vec![
                doc(body, "ledger reconciliation alpha", cat, "fin", year, 2024),
                doc(body, "ledger entry beta", cat, "ops", year, 2025),
                doc(body, "ledger close gamma", cat, "fin", year, 2026),
            ],
        )
        .expect("build A");
    // Split B: 2 docs mentioning "ledger".
    let b_split = Indexer::new()
        .build(
            schema.clone(),
            vec![
                doc(body, "ledger audit delta", cat, "fin", year, 2023),
                doc(body, "ledger summary epsilon", cat, "ops", year, 2026),
            ],
        )
        .expect("build B");

    let a = open_whole(a_split);
    let b = open_whole(b_split);

    (schema, body, cat, year, a, b)
}

#[test]
fn pagination_returns_consecutive_non_overlapping_pages() {
    let (_schema, body, _cat, _year, a, b) = build_two_splits();
    let handles = vec![SplitHandle::new("a", &a), SplitHandle::new("b", &b)];

    // 5 docs total all match "ledger".
    let all = multi_split_search(&handles, "ledger", &[body], 100).expect("all");
    assert_eq!(all.len(), 5, "five matching docs across the two splits");

    let page1 = multi_split_search_paginated(&handles, "ledger", &[body], 0, 2).expect("p1");
    let page2 = multi_split_search_paginated(&handles, "ledger", &[body], 2, 2).expect("p2");
    let page3 = multi_split_search_paginated(&handles, "ledger", &[body], 4, 2).expect("p3");

    assert_eq!(page1.len(), 2);
    assert_eq!(page2.len(), 2);
    assert_eq!(page3.len(), 1, "last page has the remaining single hit");

    // The three pages, concatenated, reproduce the full ordered result exactly.
    let paged: Vec<_> = page1.iter().chain(&page2).chain(&page3).cloned().collect();
    assert_eq!(
        paged, all,
        "offset+limit paging across merged splits matches the full ordered set"
    );

    // Offset past the end yields nothing.
    assert!(
        multi_split_search_paginated(&handles, "ledger", &[body], 10, 5)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn highlight_wraps_the_matched_term() {
    let (_schema, body, _cat, _year, a, b) = build_two_splits();
    let handles = vec![SplitHandle::new("a", &a), SplitHandle::new("b", &b)];

    let hits = multi_split_search(&handles, "reconciliation", &[body], 1).expect("search");
    assert_eq!(hits.len(), 1, "only one doc mentions 'reconciliation'");
    let hit = hits[0];
    let index = if hit.split_ord == 0 { &a } else { &b };

    let html = highlight(index, "reconciliation", &[body], body, hit.doc_address).expect("snippet");
    assert!(
        html.contains("<b>reconciliation</b>"),
        "snippet highlights the matched term with <b> markup, got: {html}"
    );
}

#[test]
fn filter_narrows_by_term_and_by_range() {
    let (_schema, body, cat, year, a, b) = build_two_splits();
    let handles = vec![SplitHandle::new("a", &a), SplitHandle::new("b", &b)];

    // All five docs match "ledger".
    let all = multi_split_search(&handles, "ledger", &[body], 100).unwrap();
    assert_eq!(all.len(), 5);

    // AND with an exact term filter cat == "fin": docs at years 2024, 2026 (A) +
    // 2023 (B) = 3.
    let fin = multi_split_search_with_filter(
        &handles,
        "ledger",
        &[body],
        &Filter::Term {
            field: cat,
            value: "fin".to_string(),
        },
        100,
    )
    .expect("term filter");
    assert_eq!(fin.len(), 3, "cat=fin narrows 5 -> 3");

    // AND with a u64 range filter year in [2025, 2026]: A(2025,2026) + B(2026) = 3.
    let recent = multi_split_search_with_filter(
        &handles,
        "ledger",
        &[body],
        &Filter::U64Range {
            field: year,
            lo: 2025,
            hi: 2026,
        },
        100,
    )
    .expect("range filter");
    assert_eq!(recent.len(), 3, "year in [2025,2026] narrows 5 -> 3");

    // A filter that matches nothing yields nothing.
    let none = multi_split_search_with_filter(
        &handles,
        "ledger",
        &[body],
        &Filter::Term {
            field: cat,
            value: "nope".to_string(),
        },
        100,
    )
    .expect("empty filter");
    assert!(none.is_empty());
}
