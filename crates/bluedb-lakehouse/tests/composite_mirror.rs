//! Composite-primary-key mirroring (variant A): after the bluedb-sql rewrite a
//! composite-PK table is an ordinary single-PK table whose key is the hidden
//! `__bluedb_pk BYTEA`. The mirror therefore keys merge-on-read equality deletes
//! on `__bluedb_pk` and carries the component columns (`a`, `b`, …) as ordinary,
//! warehouse-visible columns — no lakehouse changes needed beyond what already
//! exists. This test proves full CRUD survives a seal, read back by iceberg-rust.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use bluedb_lakehouse::LakehouseEngine;
use bluedb_sql::{CdcConfig, Database};
use futures::TryStreamExt;
use gluesql_core::prelude::Glue;
use iceberg::io::FileIO;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn make_db(name: &str) -> Database {
    Database::new(Arc::new(
        Db::open(name, Arc::new(InMemory::new())).await.unwrap(),
    ))
}

async fn engine(root: &str, db: Database, cdc: CdcConfig) -> LakehouseEngine {
    LakehouseEngine::reopen(FileIO::new_with_fs(), root, "_", db, cdc)
        .await
        .unwrap()
}

/// Apply the composite-PK rewrite, then execute, on a CDC-tapped connection.
async fn exec(glue: &mut Glue<bluedb_sql::SlateDbStorage>, sql: &str) {
    let prepared = bluedb_sql::prepare_composite_pk(&mut glue.storage, sql, &[])
        .await
        .unwrap();
    glue.execute(&prepared).await.unwrap();
}

/// Read the mirror back as (a, b) -> payload over the user columns.
async fn read_back(engine: &LakehouseEngine, table: &str) -> BTreeMap<(i64, String), String> {
    let schema = engine.fetch_schema(table).await.unwrap();
    let writer = engine.writer_for(table, &schema, &[]).await.unwrap();
    let batches: Vec<RecordBatch> = writer
        .to_table()
        .unwrap()
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    let mut out = BTreeMap::new();
    for batch in batches {
        let a = batch
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let b = batch
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let p = batch
            .column_by_name("payload")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            out.insert((a.value(i), b.value(i).to_string()), p.value(i).to_string());
        }
    }
    out
}

#[tokio::test]
async fn composite_table_mirrors_with_full_crud() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db = make_db("composite-mirror").await;
    let cdc = CdcConfig::default();
    let eng = engine(root, db.clone(), cdc.clone()).await;
    eng.enable_table("t").await.unwrap();

    // CREATE persists the Pk catalog (on a plain connection).
    {
        let mut g = Glue::new(db.connection_serialized());
        exec(
            &mut g,
            "CREATE TABLE t (a INTEGER, b TEXT, payload TEXT, PRIMARY KEY (a, b))",
        )
        .await;
    }
    // Writes flow through a CDC-tapped connection so the mirror captures them.
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        exec(
            &mut g,
            "INSERT INTO t (a, b, payload) VALUES (1, 'x', 'p1')",
        )
        .await;
        exec(
            &mut g,
            "INSERT INTO t (a, b, payload) VALUES (1, 'y', 'p2')",
        )
        .await;
        exec(
            &mut g,
            "INSERT INTO t (a, b, payload) VALUES (2, 'x', 'p3')",
        )
        .await;
        exec(
            &mut g,
            "UPDATE t SET payload = 'p1b' WHERE a = 1 AND b = 'x'",
        )
        .await;
        exec(&mut g, "DELETE FROM t WHERE a = 1 AND b = 'y'").await;
    }

    eng.seal().await.unwrap();

    let rows = read_back(&eng, "t").await;
    assert_eq!(rows.len(), 2, "one row deleted, one updated, one untouched");
    assert_eq!(
        rows.get(&(1, "x".into())).map(String::as_str),
        Some("p1b"),
        "updated"
    );
    assert_eq!(rows.get(&(2, "x".into())).map(String::as_str), Some("p3"));
    assert_eq!(rows.get(&(1, "y".into())), None, "deleted");

    // The mirror declares an Iceberg sort order on the component columns
    // (a = field-id 1, b = field-id 2) — so warehouses know files cluster by (a,b).
    let (_, meta) = eng.table_metadata_json("t").await.unwrap().unwrap();
    let default_id = meta["default-sort-order-id"].as_i64().unwrap();
    let order = meta["sort-orders"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["order-id"].as_i64() == Some(default_id))
        .unwrap();
    let source_ids: Vec<i64> = order["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["source-id"].as_i64().unwrap())
        .collect();
    assert_eq!(
        source_ids,
        vec![1, 2],
        "sort order should be on the (a, b) components"
    );
}
