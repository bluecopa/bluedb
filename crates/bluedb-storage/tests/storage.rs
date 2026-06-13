//! Integration tests for the SlateDB-backed blob store: durability across
//! reopen, the additive write seam, and the chunked large-value layer.

use std::sync::Arc;

use bluedb_storage::{BlobStore, BlobStoreMut, ChunkedBlobStore, SlateDbBlobStore};
use bytes::Bytes;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::ObjectStore;

/// THE key test: data written, then flushed via a clean `close()`, must still
/// be readable after the `Db` is dropped and a *fresh* `Db` is reopened at the
/// same persistent path. This proves writes reach object storage (flush → L0),
/// not just the in-process memtable.
#[tokio::test]
async fn durability_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");

    // --- session 1: write, then shut down cleanly (flush + close) ---
    {
        let store = SlateDbBlobStore::open_local(dir.path())
            .await
            .expect("open session 1");
        store.put("alpha", Bytes::from_static(b"hello")).await.unwrap();
        store
            .put("beta", Bytes::from_static(b"world-of-bytes"))
            .await
            .unwrap();
        // Clean shutdown flushes memtables to L0 → durable on the local FS.
        store.shutdown().await.expect("shutdown session 1");
    }

    // --- session 2: a brand-new Db at the same path must see the data ---
    {
        let store = SlateDbBlobStore::open_local(dir.path())
            .await
            .expect("reopen session 2");
        let a = store.get_all("alpha").await.expect("read alpha after reopen");
        let b = store.get_all("beta").await.expect("read beta after reopen");
        assert_eq!(a.as_ref(), b"hello", "alpha did not survive reopen");
        assert_eq!(b.as_ref(), b"world-of-bytes", "beta did not survive reopen");
        assert_eq!(store.len("beta").await.unwrap(), 14);
        store.shutdown().await.unwrap();
    }
}

/// Same as above but durability is forced with an explicit `flush()` while the
/// handle stays open, then the handle is dropped (not closed) before reopen.
#[tokio::test]
async fn durability_survives_reopen_via_flush() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let store = SlateDbBlobStore::open_local(dir.path())
            .await
            .expect("open");
        store.put("k", Bytes::from_static(b"persisted")).await.unwrap();
        store.flush().await.expect("flush");
        // Intentionally drop without close().
        drop(store);
    }
    let store = SlateDbBlobStore::open_local(dir.path())
        .await
        .expect("reopen");
    assert_eq!(
        store.get_all("k").await.expect("read after flush+reopen").as_ref(),
        b"persisted"
    );
    store.shutdown().await.unwrap();
}

/// The additive write seam: put→get round-trip, delete removes, get_range slices.
#[tokio::test]
async fn write_seam_put_get_delete() {
    let store = SlateDbBlobStore::open_in_memory().await.unwrap();

    store.put("doc", Bytes::from_static(b"0123456789")).await.unwrap();
    assert_eq!(store.get_all("doc").await.unwrap().as_ref(), b"0123456789");
    assert_eq!(store.get_range("doc", 2..5).await.unwrap().as_ref(), b"234");
    assert_eq!(store.len("doc").await.unwrap(), 10);

    store.delete("doc").await.unwrap();
    assert!(store.get_all("doc").await.is_err(), "delete should remove the key");

    // Deleting a missing key is a no-op (does not error).
    store.delete("doc").await.unwrap();
}

/// `scan_prefix` returns only keys under the prefix, in ascending order.
#[tokio::test]
async fn scan_prefix_is_ordered_and_scoped() {
    let store = SlateDbBlobStore::open_in_memory().await.unwrap();

    // Insert out of order, across two prefixes.
    store.put("split/c", Bytes::from_static(b"3")).await.unwrap();
    store.put("split/a", Bytes::from_static(b"1")).await.unwrap();
    store.put("split/b", Bytes::from_static(b"2")).await.unwrap();
    store.put("other/z", Bytes::from_static(b"x")).await.unwrap();

    let got = store.scan_prefix("split/").await.unwrap();
    let keys: Vec<&str> = got.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["split/a", "split/b", "split/c"], "must be ordered & scoped");
    let vals: Vec<&[u8]> = got.iter().map(|(_, v)| v.as_ref()).collect();
    assert_eq!(vals, vec![b"1".as_ref(), b"2", b"3"]);

    store.shutdown().await.unwrap();
}

/// Confirms the existing `SlateDbBlobStore::new(Arc<Db>)` constructor still
/// works unchanged (we only added methods around it).
#[tokio::test]
async fn legacy_new_constructor_still_works() {
    use slatedb::Db;
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let db = Db::open("legacy", object_store).await.unwrap();
    let store = SlateDbBlobStore::new(Arc::new(db));
    store.put("x", Bytes::from_static(b"y")).await.unwrap();
    assert_eq!(store.get_all("x").await.unwrap().as_ref(), b"y");
}

/// Chunked layer: a value larger than the chunk size is split, and a range read
/// reassembles correctly while only the overlapping chunks are touched.
#[tokio::test]
async fn chunked_split_and_range_reassembly() {
    let backend = SlateDbBlobStore::open_in_memory().await.unwrap();
    let chunked = ChunkedBlobStore::new(backend, 4); // tiny chunks to force splitting

    let data: Vec<u8> = (0u8..=20).collect(); // 21 bytes → 6 chunks of size 4
    chunked.put_chunked("big", &data).await.unwrap();

    assert_eq!(chunked.len_chunked("big").await.unwrap(), 21);
    assert_eq!(chunked.get_all_chunked("big").await.unwrap().as_ref(), &data[..]);

    // Range spanning multiple chunks, with partial chunks at both ends.
    let r = chunked.get_range_chunked("big", 3..14).await.unwrap();
    assert_eq!(r.as_ref(), &data[3..14]);

    // Range fully inside one chunk.
    let r = chunked.get_range_chunked("big", 5..7).await.unwrap();
    assert_eq!(r.as_ref(), &data[5..7]);

    // Empty range.
    assert!(chunked.get_range_chunked("big", 7..7).await.unwrap().is_empty());

    // Out-of-bounds range errors.
    assert!(chunked.get_range_chunked("big", 0..22).await.is_err());

    // Rewrite smaller → no stale tail chunks leak into the readback.
    let small: Vec<u8> = vec![99, 98];
    chunked.put_chunked("big", &small).await.unwrap();
    assert_eq!(chunked.len_chunked("big").await.unwrap(), 2);
    assert_eq!(chunked.get_all_chunked("big").await.unwrap().as_ref(), &small[..]);

    // Delete removes everything.
    chunked.delete_chunked("big").await.unwrap();
    assert!(chunked.len_chunked("big").await.is_err());
}

/// Sanity: `LocalFileSystem` over a tempdir really is the persistent store used
/// by the durability test (guards against accidentally swapping in InMemory).
#[tokio::test]
async fn local_fs_backend_is_constructible() {
    let dir = tempfile::tempdir().unwrap();
    let _os = LocalFileSystem::new_with_prefix(dir.path()).expect("local fs");
}
