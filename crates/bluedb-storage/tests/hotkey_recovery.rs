//! Isolation reproducer for the Jepsen counter-workload finding: under writer
//! crash + failover, a repeatedly-overwritten ("hot") key loses acknowledged
//! durable writes (~45% of kill runs), while distinct-key inserts never do.
//!
//! These tests strip away ALL of bluedb/HA/Jepsen and exercise SlateDB 0.13.1
//! directly to answer one question: does SlateDB itself lose durable
//! (`await_durable=true`) writes to a hot key across an abrupt reopen (the
//! crash-then-promote shape), and is a hot key treated differently from distinct
//! keys? The object store is a SHARED `Arc<InMemory>` so the "reopened" Db sees
//! everything the first Db flushed; dropping the first Db without `close()`
//! simulates the SIGKILL (no graceful flush) — `await_durable=true` is supposed
//! to have already made each write durable before it returned.

use std::sync::Arc;

use slatedb::config::{PutOptions, WriteOptions};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;

const N: u64 = 500;

/// Overwrite ONE key N times with `await_durable=true`, drop the Db abruptly
/// (no close), reopen over the same store, and check the latest value survived.
/// This is the counter (`UPDATE n = n + 1`) shape.
#[tokio::test]
async fn hot_key_survives_abrupt_reopen() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    {
        let db = Db::open("t", store.clone()).await.unwrap();
        for i in 1..=N {
            // Db::put defaults to await_durable=true: returns only after the WAL
            // SST is flushed to the (shared) object store.
            db.put(b"k".as_slice(), &i.to_be_bytes()).await.unwrap();
        }
        // Abrupt "crash": drop without close()/flush. Every put above already
        // returned durable, so all N must survive recovery.
    }
    let db2 = Db::open("t", store.clone()).await.unwrap();
    let got = db2
        .get(b"k".as_slice())
        .await
        .unwrap()
        .map(|v| u64::from_be_bytes(v[..8].try_into().unwrap()));
    db2.close().await.unwrap();
    assert_eq!(
        got,
        Some(N),
        "hot key lost durable overwrites on abrupt reopen: latest={got:?}, expected {N}"
    );
}

/// Control: N DISTINCT keys (the `set`/`dur` insert shape). If this survives but
/// the hot-key test does not, the loss is specific to overwriting one key.
#[tokio::test]
async fn distinct_keys_survive_abrupt_reopen() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    {
        let db = Db::open("t", store.clone()).await.unwrap();
        for i in 1..=N {
            db.put(&i.to_be_bytes(), b"x".as_slice()).await.unwrap();
        }
    }
    let db2 = Db::open("t", store.clone()).await.unwrap();
    let mut present = 0u64;
    for i in 1..=N {
        if db2.get(&i.to_be_bytes()).await.unwrap().is_some() {
            present += 1;
        }
    }
    db2.close().await.unwrap();
    assert_eq!(present, N, "distinct-key inserts lost {} of {N} on abrupt reopen", N - present);
}

/// Sanity: with `await_durable=false` the writes are NOT promised durable, so an
/// abrupt drop is *allowed* to lose them. Confirms the harness can observe loss
/// at all (so a pass in `hot_key_survives_abrupt_reopen` is meaningful).
#[tokio::test]
async fn non_durable_writes_may_be_lost() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    {
        let db = Db::open("t", store.clone()).await.unwrap();
        let opts = WriteOptions { await_durable: false, ..Default::default() };
        for i in 1..=N {
            db.put_with_options(b"k".as_slice(), &i.to_be_bytes(), &PutOptions::default(), &opts)
                .await
                .unwrap();
        }
    }
    let db2 = Db::open("t", store.clone()).await.unwrap();
    let got = db2
        .get(b"k".as_slice())
        .await
        .unwrap()
        .map(|v| u64::from_be_bytes(v[..8].try_into().unwrap()));
    db2.close().await.unwrap();
    // Not an assertion on a specific value — just report what survived.
    eprintln!("non-durable hot key after abrupt reopen: {got:?} (expected to possibly be < {N})");
}
