//! The indexing pipeline end-to-end: `build_split` from a schema + docs, store
//! the split in SlateDB, fetch it back, open it with `BundleDirectory`, and run
//! BM25 queries. Mirrors `tests/query.rs` but uses `indexer::build_split`
//! instead of hand-rolling the on-disk index + file collection.

use std::sync::Arc;

use bluedb_fts::indexer::{build_split, Indexer};
use bluedb_fts::vendor::BundleDirectory;
use bluedb_storage::{BlobStore, SlateDbBlobStore};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::Db;
use tantivy::collector::TopDocs;
use tantivy::directory::{FileSlice, OwnedBytes};
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, STORED, TEXT};
use tantivy::{Index, TantivyDocument};

fn sample_schema() -> (Schema, tantivy::schema::Field, tantivy::schema::Field) {
    let mut sb = Schema::builder();
    let title = sb.add_text_field("title", TEXT | STORED);
    let body = sb.add_text_field("body", TEXT);
    (sb.build(), title, body)
}

fn sample_docs(
    title: tantivy::schema::Field,
    body: tantivy::schema::Field,
) -> Vec<TantivyDocument> {
    let mut d1 = TantivyDocument::default();
    d1.add_text(title, "ledger reconciliation");
    d1.add_text(body, "match invoices against bank statements financial");

    let mut d2 = TantivyDocument::default();
    d2.add_text(title, "quarterly revenue report");
    d2.add_text(body, "revenue recognition deferred income financial");

    vec![d1, d2]
}

#[tokio::test]
async fn indexer_build_split_queries_in_slatedb() {
    let (schema, title, body) = sample_schema();
    let docs = sample_docs(title, body);

    // ---- build a split straight from schema + docs ----
    let split_bytes = build_split(schema, docs).expect("build split");

    // ---- store it in SlateDB ----
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("bluedb-fts-indexer-test", object_store)
            .await
            .expect("open slatedb"),
    );
    db.put(b"splits/idx-001.split", &split_bytes[..])
        .await
        .expect("put split");

    // ---- fetch + open + query ----
    let blob = SlateDbBlobStore::new(db);
    let fetched = blob.get_all("splits/idx-001.split").await.expect("get");
    let file_slice = FileSlice::new(Arc::new(OwnedBytes::new(fetched.to_vec())));
    let bundle = BundleDirectory::open_split(file_slice).expect("open split");
    let index = Index::open(bundle).expect("open index");
    let reader = index.reader().expect("reader");
    let searcher = reader.searcher();
    let parser = QueryParser::for_index(&index, vec![title, body]);

    let hits = |q: &str| -> usize {
        let query = parser.parse_query(q).expect("parse");
        searcher
            .search(&query, &TopDocs::with_limit(10).order_by_score())
            .expect("search")
            .len()
    };

    assert_eq!(hits("revenue"), 1, "'revenue' only in doc 2");
    assert_eq!(hits("ledger"), 1, "'ledger' only in doc 1");
    assert_eq!(hits("financial"), 2, "'financial' in both docs");
    assert_eq!(hits("nonexistentterm"), 0, "no doc matches");
}

#[tokio::test]
async fn indexer_struct_with_custom_heap_builds_queryable_split() {
    let (schema, title, body) = sample_schema();
    let docs = sample_docs(title, body);

    let split_bytes = Indexer::new()
        .with_writer_heap_bytes(20_000_000)
        .build(schema, docs)
        .expect("build split");

    let file_slice = FileSlice::new(Arc::new(OwnedBytes::new(split_bytes)));
    let bundle = BundleDirectory::open_split(file_slice).expect("open split");
    let index = Index::open(bundle).expect("open index");
    let reader = index.reader().expect("reader");
    let searcher = reader.searcher();
    let parser = QueryParser::for_index(&index, vec![title, body]);
    let query = parser.parse_query("financial").expect("parse");
    let hits = searcher
        .search(&query, &TopDocs::with_limit(10).order_by_score())
        .expect("search");
    assert_eq!(hits.len(), 2);
}
