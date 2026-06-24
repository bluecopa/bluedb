//! Spike step 1: the PK fast-path pushdown — the OLTP-latency gate of the
//! DataFusion front-door flip.
//!
//! Proves that a `WHERE pk = <literal>` read is served by a point read from the
//! row store (SlateDB, always fresh) and **skips Iceberg entirely**, while a
//! non-PK filter takes the Iceberg ∪ unsealed-tail merge path. A per-provider
//! counter (`ProviderStats`) records which path each scan took.

use std::sync::Arc;

use arrow_array::Int64Array;
use bluedb_lakehouse::{object_store_file_io, LakehouseEngine};
use bluedb_query::BluedbTableProvider;
use bluedb_sql::{CdcConfig, Database};
use datafusion::prelude::SessionContext;
use gluesql_core::prelude::Glue;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;

async fn make_engine() -> (Database, CdcConfig, Arc<LakehouseEngine>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Database::new(Arc::new(Db::open("bluedb", store.clone()).await.unwrap()));
    let cdc = CdcConfig::default();
    let file_io = object_store_file_io(store.clone(), "");
    let eng = LakehouseEngine::reopen(file_io, "lakehouse", "_", db.clone(), cdc.clone())
        .await
        .unwrap();
    (db, cdc, Arc::new(eng))
}

async fn ddl(db: &Database, sql: &str) {
    let mut g = Glue::new(db.connection_serialized());
    g.execute(sql).await.unwrap();
}

async fn dml(db: &Database, cdc: &CdcConfig, sql: &str) {
    let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
    g.execute(sql).await.unwrap();
}

fn ids_vs(batches: &[arrow_array::RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let vs = b
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((ids.value(i), vs.value(i)));
        }
    }
    out
}

/// A `WHERE pk = <post-seal id>` read returns the fresh row from the row store
/// and NEVER opens Iceberg (fast_path=1, merge_path=0) — even though that id was
/// inserted after the seal and never made it into a snapshot.
#[tokio::test]
async fn pk_equality_takes_fast_path_and_skips_iceberg() {
    let (db, cdc, eng) = make_engine().await;
    eng.enable_table("t").await.unwrap();
    ddl(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);").await;
    dml(&db, &cdc, "INSERT INTO t VALUES (1, 10), (2, 20);").await;
    eng.seal().await.unwrap();
    // id=3 lives only in the row store + CDC tail, NOT in Iceberg.
    dml(&db, &cdc, "INSERT INTO t VALUES (3, 30);").await;

    let provider = BluedbTableProvider::try_new(eng.clone(), "t")
        .await
        .unwrap();
    let stats = provider.stats_handle();
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let batches = ctx
        .sql("SELECT id, v FROM t WHERE id = 3")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        ids_vs(&batches),
        vec![(3, 30)],
        "fresh post-seal row served via the row-store point read"
    );
    assert_eq!(stats.fast_path(), 1, "PK equality must take the fast path");
    assert_eq!(
        stats.merge_path(),
        0,
        "the PK fast path must NOT open Iceberg / run the merge"
    );
}

/// A non-PK filter takes the merge path (Iceberg ∪ tail) and sees both sealed
/// and unsealed rows.
#[tokio::test]
async fn non_pk_filter_takes_merge_path() {
    let (db, cdc, eng) = make_engine().await;
    eng.enable_table("t").await.unwrap();
    ddl(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);").await;
    dml(&db, &cdc, "INSERT INTO t VALUES (1, 10), (2, 20);").await;
    eng.seal().await.unwrap();
    dml(&db, &cdc, "INSERT INTO t VALUES (3, 30);").await; // tail-only

    let provider = BluedbTableProvider::try_new(eng.clone(), "t")
        .await
        .unwrap();
    let stats = provider.stats_handle();
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let batches = ctx
        .sql("SELECT id, v FROM t WHERE v >= 10 ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        ids_vs(&batches),
        vec![(1, 10), (2, 20), (3, 30)],
        "merge path sees sealed (1,2) and unsealed tail (3)"
    );
    assert_eq!(stats.merge_path(), 1, "non-PK filter takes the merge path");
    assert_eq!(stats.fast_path(), 0, "non-PK filter is not the fast path");
}

/// PK-less tables are rejected at provider construction (the pushdown/merge keys
/// on the PK) — the front-door's "PK-less errors" decision.
#[tokio::test]
async fn provider_rejects_pk_less_table() {
    let (db, _cdc, eng) = make_engine().await;
    ddl(&db, "CREATE TABLE nopk (a INTEGER, b INTEGER);").await;
    let result = BluedbTableProvider::try_new(eng.clone(), "nopk").await;
    assert!(
        result.is_err(),
        "PK-less table must be rejected: {result:?}"
    );
}
