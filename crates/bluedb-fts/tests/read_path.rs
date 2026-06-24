//! End-to-end read path over the real substrate: a "split" stored in SlateDB,
//! read back through `SlateDbBlobStore` → (blanket) `Storage` →
//! `StorageDirectory` → tantivy async `FileHandle`, byte-exact, plus a
//! `CachingDirectory` round-trip (exercising `ByteRangeCache` + `CacheMetrics`).
//!
//! NOTE: a full BM25 *query* lives in tests/query.rs.

use std::path::Path;
use std::sync::Arc;

use bluedb_fts::vendor::{CachingDirectory, StorageDirectory};
use bluedb_storage::SlateDbBlobStore;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::Db;
use tantivy::directory::FileHandle;
use tantivy::Directory;

fn sample_bytes(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

/// Open a fresh in-memory SlateDB and store `content` under `key`.
async fn slatedb_with(key: &str, content: &[u8]) -> Arc<Db> {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::open("bluedb-readpath-test", object_store)
        .await
        .expect("open slatedb");
    db.put(key.as_bytes(), content).await.expect("put");
    Arc::new(db)
}

#[tokio::test]
async fn storage_directory_reads_substrate_byte_exact() {
    let content = sample_bytes(10_000);
    let db = slatedb_with("idx/split-001.data", &content).await;
    let dir = StorageDirectory::new(Arc::new(SlateDbBlobStore::new(db)));

    let handle: Arc<dyn FileHandle> = dir
        .get_file_handle(Path::new("idx/split-001.data"))
        .expect("file handle");
    let slice = handle
        .read_bytes_async(100..200)
        .await
        .expect("async slice");
    assert_eq!(&slice[..], &content[100..200], "byte-range read must match");

    let all = dir
        .get_all(Path::new("idx/split-001.data"))
        .await
        .expect("get_all");
    assert_eq!(&all[..], &content[..], "full read must match");
}

#[tokio::test]
async fn caching_directory_serves_repeated_reads() {
    let content = sample_bytes(4096);
    let db = slatedb_with("idx/split-002.data", &content).await;
    let storage_dir = StorageDirectory::new(Arc::new(SlateDbBlobStore::new(db)));
    let caching = CachingDirectory::new_unbounded(Arc::new(storage_dir));

    let handle = caching
        .get_file_handle(Path::new("idx/split-002.data"))
        .expect("file handle");
    let first = handle.read_bytes_async(0..512).await.expect("first read");
    let second = handle.read_bytes_async(0..512).await.expect("second read");
    assert_eq!(&first[..], &content[0..512], "first read must match source");
    assert_eq!(&second[..], &first[..], "cached read must match first");
}
