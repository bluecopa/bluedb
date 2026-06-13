//! Lazy split open over the substrate: build a split **with a real hotcache**,
//! store it in SlateDB, then `Index::open` + run BM25 queries by range-fetching
//! only the footer + hotcache — never calling `get_all`.
//!
//! We prove "no `get_all`" with a counting `BlobStore` decorator that increments
//! a counter on every call and panics if `get_all` is ever invoked. (Note: the
//! SlateDB backend itself stores whole values and slices them locally on a
//! `get_range`, so the *physical* read is still whole-value — the architectural
//! property we prove is that the FTS read path issues only scoped `get_range`
//! calls and never `get_all`. On a true range-capable object store this is the
//! footer + hotcache only.)

use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bluedb_fts::indexer::Indexer;
use bluedb_fts::open::open_split_lazy;
use bluedb_storage::{BlobStore, SlateDbBlobStore};
use bytes::Bytes;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::Db;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, STORED, TEXT};
use tantivy::TantivyDocument;

/// Wraps a `BlobStore`, counting `get_range` calls and forbidding `get_all`.
struct CountingBlobStore<B: BlobStore> {
    inner: B,
    get_range_calls: AtomicUsize,
    get_all_calls: AtomicUsize,
}

impl<B: BlobStore> CountingBlobStore<B> {
    fn new(inner: B) -> Self {
        Self {
            inner,
            get_range_calls: AtomicUsize::new(0),
            get_all_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl<B: BlobStore> BlobStore for CountingBlobStore<B> {
    async fn get_range(&self, path: &str, range: Range<usize>) -> Result<Bytes> {
        self.get_range_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.get_range(path, range).await
    }

    async fn get_all(&self, path: &str) -> Result<Bytes> {
        self.get_all_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.get_all(path).await
    }

    async fn len(&self, path: &str) -> Result<usize> {
        self.inner.len(path).await
    }
}

#[tokio::test]
async fn lazy_open_queries_without_get_all() {
    // ---- build a split WITH a real hotcache ----
    let mut sb = Schema::builder();
    let title = sb.add_text_field("title", TEXT | STORED);
    let body = sb.add_text_field("body", TEXT);
    let schema = sb.build();

    let mut d1 = TantivyDocument::default();
    d1.add_text(title, "ledger reconciliation");
    d1.add_text(body, "match invoices against bank statements financial");
    let mut d2 = TantivyDocument::default();
    d2.add_text(title, "quarterly revenue report");
    d2.add_text(body, "revenue recognition deferred income financial");

    let split_bytes = Indexer::new()
        .build_with_hotcache(schema, vec![d1, d2])
        .expect("build split with hotcache");

    // ---- store the split in SlateDB ----
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("bluedb-fts-lazy-test", object_store)
            .await
            .expect("open slatedb"),
    );
    let key = "splits/lazy-001.split";
    db.put(key.as_bytes(), &split_bytes[..])
        .await
        .expect("put split");

    // ---- open lazily through a counting store that forbids get_all ----
    let counting = Arc::new(CountingBlobStore::new(SlateDbBlobStore::new(db)));
    let index = open_split_lazy(counting.clone(), key)
        .await
        .expect("open split lazily");

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

    assert_eq!(hits("revenue"), 1);
    assert_eq!(hits("ledger"), 1);
    assert_eq!(hits("financial"), 2);
    assert_eq!(hits("nonexistentterm"), 0);

    // The core proof: the FTS read path never issued a whole-object fetch.
    assert_eq!(
        counting.get_all_calls.load(Ordering::SeqCst),
        0,
        "lazy open must never call get_all"
    );
    assert!(
        counting.get_range_calls.load(Ordering::SeqCst) > 0,
        "lazy open must fetch via get_range"
    );
}

#[tokio::test]
async fn lazy_open_rejects_empty_hotcache_split() {
    // A split packed WITHOUT a hotcache cannot be opened lazily.
    let mut sb = Schema::builder();
    let body = sb.add_text_field("body", TEXT);
    let schema = sb.build();
    let mut d = TantivyDocument::default();
    d.add_text(body, "hello world");
    let split_bytes = Indexer::new().build(schema, vec![d]).expect("build split");

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("bluedb-fts-lazy-empty-test", object_store)
            .await
            .expect("open slatedb"),
    );
    let key = "splits/empty-hotcache.split";
    db.put(key.as_bytes(), &split_bytes[..])
        .await
        .expect("put split");

    let blob = Arc::new(SlateDbBlobStore::new(db));
    let err = open_split_lazy(blob, key)
        .await
        .expect_err("empty-hotcache split must be rejected by lazy open");
    assert!(
        err.to_string().contains("empty hotcache"),
        "unexpected error: {err}"
    );
}
