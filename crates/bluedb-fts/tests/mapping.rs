//! Per-field analyzers through the full lifecycle: build a split from an
//! `IndexMapping` (stemmed body + keyword id), store it in SlateDB, reopen it
//! lazily, register the mapping's tokenizers on the opened index, and confirm
//! stemming matches (`running` -> `runs`) while a keyword field only matches
//! exactly.

use std::sync::Arc;

use bluedb_fts::indexer::Indexer;
use bluedb_fts::mapping::{Analyzer, IndexMapping};
use bluedb_fts::open::open_split_lazy;
use bluedb_fts::search::{multi_split_search, SplitHandle};
use bluedb_storage::SlateDbBlobStore;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::Db;
use tantivy::TantivyDocument;

const INDEX_ID: &str = "books-2026";
const SPLIT_KEY: &str = "indexes/books-2026/splits/s1.split";

#[tokio::test]
async fn stemming_field_matches_inflections_keyword_is_exact() {
    // body: stemmed English; code: exact keyword.
    let mapping = IndexMapping::new()
        .text("body", Analyzer::EnStem)
        .keyword("code");
    let schema = mapping.build_schema();
    let body = schema.get_field("body").unwrap();
    let code = schema.get_field("code").unwrap();

    let mut d1 = TantivyDocument::default();
    d1.add_text(body, "the system runs nightly");
    d1.add_text(code, "ACME-1");

    let mut d2 = TantivyDocument::default();
    d2.add_text(body, "we run the batch");
    d2.add_text(code, "ACME-2");

    let split = Indexer::new()
        .build_with_hotcache(schema.clone(), vec![d1, d2])
        .expect("build split");

    // Store + reopen lazily.
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("fts-mapping-test", object_store)
            .await
            .expect("open db"),
    );
    db.put(SPLIT_KEY.as_bytes(), &split[..])
        .await
        .expect("put split");
    let blob = Arc::new(SlateDbBlobStore::new(db));
    let index = open_split_lazy(blob.clone(), SPLIT_KEY)
        .await
        .expect("open split");

    // CRUCIAL: the lazily-opened index has only the default tokenizer manager;
    // register the mapping's analyzers before querying.
    mapping.register_tokenizers(&index);

    let handles = vec![SplitHandle::new("s1", &index)];

    // Stemming: a query for "running" matches both "runs" (d1) and "run" (d2) —
    // all three regular inflections share the stem `run`.
    let running = multi_split_search(&handles, "running", &[body], 10).expect("search running");
    assert_eq!(
        running.len(),
        2,
        "stemmed body matches both 'runs' and 'run'"
    );

    // The bare stem matches both as well (confirming the body terms were stored
    // stemmed, not raw).
    let run = multi_split_search(&handles, "run", &[body], 10).expect("search run");
    assert_eq!(run.len(), 2, "the stem 'run' matches both inflected docs");

    // Keyword field: exact match hits...
    let exact = multi_split_search(&handles, "ACME-1", &[code], 10).expect("search exact");
    assert_eq!(exact.len(), 1, "keyword field matches the exact value");

    // ...but a sub-token does NOT (the whole value is one term, untokenized).
    let partial = multi_split_search(&handles, "ACME", &[code], 10).expect("search partial");
    assert_eq!(partial.len(), 0, "keyword field does not match a sub-token");

    let _ = INDEX_ID;
}
