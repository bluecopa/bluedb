//! Read-replica path: a writer `Db` and a read-only `DbReader` over the SAME
//! object store, in one process — the standby-reads-the-writer's-data model,
//! verifiable without a cluster.

use std::sync::Arc;

use bluedb_storage::{BlobStore, BlobStoreMut, SlateDbBlobStore};
use bytes::Bytes;
use slatedb::object_store::{memory::InMemory, ObjectStore};

#[tokio::test]
async fn reader_sees_writer_data_and_cannot_write() {
    // One shared object store stands in for the shared bucket both roles use.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Active writer creates the database and writes a blob, then flushes so the
    // write is durable in the manifest the reader will follow.
    let writer = SlateDbBlobStore::open("bluedb", store.clone())
        .await
        .expect("open writer");
    assert!(writer.is_writer());
    writer
        .put("splits/s1", Bytes::from_static(b"hello from the writer"))
        .await
        .expect("put");
    writer
        .put("splits/s2", Bytes::from_static(b"second blob"))
        .await
        .expect("put 2");
    writer.flush().await.expect("flush");

    // A read replica over the SAME store follows the manifest and serves reads.
    let reader = SlateDbBlobStore::open_reader("bluedb", store.clone())
        .await
        .expect("open reader");
    assert!(!reader.is_writer());

    assert_eq!(
        &reader.get_all("splits/s1").await.expect("read s1")[..],
        b"hello from the writer"
    );
    assert_eq!(
        &reader.get_all("splits/s2").await.expect("read s2")[..],
        b"second blob"
    );
    // Ranged read works through the replica too.
    assert_eq!(
        &reader.get_range("splits/s1", 0..5).await.expect("range")[..],
        b"hello"
    );

    // A replica must refuse writes (it is read-only).
    assert!(
        reader
            .put("splits/s3", Bytes::from_static(b"nope"))
            .await
            .is_err(),
        "a read replica must not accept writes"
    );
    assert!(
        reader.delete("splits/s1").await.is_err(),
        "a read replica must not delete"
    );

    let _ = writer.shutdown().await;
}
