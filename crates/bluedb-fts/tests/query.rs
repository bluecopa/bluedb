//! Full end-to-end: build a tantivy index, pack it into a split, store the split
//! in **SlateDB**, fetch it back through `SlateDbBlobStore`, open it with the
//! vendored `BundleDirectory`, and run real BM25 queries.
//!
//! The "search actually works over a split in the substrate" proof — read path
//! (tests/read_path.rs) plus the split *writer* (the indexer's packing step).

use std::path::PathBuf;
use std::sync::Arc;

use bluedb_fts::split::pack_split;
use bluedb_fts::vendor::BundleDirectory;
use bluedb_storage::{BlobStore, SlateDbBlobStore};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::Db;
use tantivy::collector::TopDocs;
use tantivy::directory::{FileSlice, OwnedBytes};
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, STORED, TEXT};
use tantivy::{Index, TantivyDocument};
use tempfile::tempdir;

#[tokio::test]
async fn bm25_query_over_split_in_slatedb() {
    // ---- 1. build a tiny tantivy index on disk ----
    let mut schema_builder = Schema::builder();
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();

    let index_dir = tempdir().expect("tempdir");
    let index = Index::create_in_dir(index_dir.path(), schema).expect("create index");
    {
        let mut writer = index.writer(15_000_000).expect("writer");

        let mut d1 = TantivyDocument::default();
        d1.add_text(title, "ledger reconciliation");
        d1.add_text(body, "match invoices against bank statements financial");
        writer.add_document(d1).expect("add d1");

        let mut d2 = TantivyDocument::default();
        d2.add_text(title, "quarterly revenue report");
        d2.add_text(body, "revenue recognition deferred income financial");
        writer.add_document(d2).expect("add d2");

        writer.commit().expect("commit");
    } // writer dropped -> lock released, files flushed

    // ---- 2. pack the on-disk index files into a split ----
    let mut files: Vec<(PathBuf, Vec<u8>)> = Vec::new();
    for entry in std::fs::read_dir(index_dir.path()).expect("read_dir") {
        let path = entry.expect("entry").path();
        if !path.is_file() {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if name.ends_with(".lock") {
            continue; // tantivy lock files are not part of the split
        }
        files.push((
            PathBuf::from(name),
            std::fs::read(&path).expect("read file"),
        ));
    }
    let split_bytes = pack_split(&files);

    // ---- 3. store the split in SlateDB ----
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("bluedb-fts-test", object_store)
            .await
            .expect("open slatedb"),
    );
    db.put(b"splits/split-001.split", &split_bytes[..])
        .await
        .expect("put split");

    // ---- 4. fetch the split back via BlobStore, open with BundleDirectory ----
    let blob = SlateDbBlobStore::new(db);
    let fetched = blob
        .get_all("splits/split-001.split")
        .await
        .expect("get split");
    let file_slice = FileSlice::new(Arc::new(OwnedBytes::new(fetched.to_vec())));
    let bundle = BundleDirectory::open_split(file_slice).expect("open split");

    // ---- 5. open the tantivy index from the split and run BM25 queries ----
    let index = Index::open(bundle).expect("open index from split");
    let reader = index.reader().expect("reader");
    let searcher = reader.searcher();
    let parser = QueryParser::for_index(&index, vec![title, body]);

    let hits = |q: &str| -> usize {
        let query = parser.parse_query(q).expect("parse query");
        searcher
            .search(&query, &TopDocs::with_limit(10).order_by_score())
            .expect("search")
            .len()
    };

    assert_eq!(hits("revenue"), 1, "'revenue' is only in doc 2");
    assert_eq!(hits("ledger"), 1, "'ledger' is only in doc 1");
    assert_eq!(hits("financial"), 2, "'financial' is in both docs");
    assert_eq!(hits("nonexistentterm"), 0, "no doc matches");
}
