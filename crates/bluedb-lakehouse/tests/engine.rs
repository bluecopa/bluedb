//! Engine tests: the durable mirror registry survives reopen, and `seal()`
//! drains the CDC log into Iceberg (final state, then GC).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use bluedb_lakehouse::LakehouseEngine;
use bluedb_sql::{CdcConfig, Database, LhPragma};
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
    LakehouseEngine::reopen(FileIO::new_with_fs(), root, "main", db, cdc)
        .await
        .unwrap()
}

/// Read a `(id BIGINT, body TEXT)` mirror back as id -> body (columns by index).
async fn read_table(engine: &LakehouseEngine, table: &str) -> BTreeMap<i64, String> {
    let schema = engine.fetch_schema(table).await.unwrap();
    let writer = engine.writer_for(table, &schema, &[]).await.unwrap();
    let table = writer.to_table().unwrap();
    let batches: Vec<RecordBatch> = table
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
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let bodies = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            out.insert(ids.value(i), bodies.value(i).to_string());
        }
    }
    out
}

#[tokio::test]
async fn registry_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db = make_db("reg").await;

    let cdc = CdcConfig::default();
    let eng = engine(root, db.clone(), cdc.clone()).await;
    eng.enable_table("docs").await.unwrap();
    assert!(eng.is_mirrored("docs"));

    // A fresh engine + fresh CDC control over the same root re-derives the set.
    let cdc2 = CdcConfig::default();
    let eng2 = engine(root, db.clone(), cdc2.clone()).await;
    assert_eq!(eng2.mirrored_tables(), vec!["docs".to_string()]);
    assert!(cdc2.is_enabled("docs"), "registry applied to the fresh CDC control");
}

#[tokio::test]
async fn seal_publishes_final_state_then_gcs() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db = make_db("seal").await;
    let cdc = CdcConfig::default();
    let eng = engine(root, db.clone(), cdc.clone()).await;
    eng.enable_table("docs").await.unwrap();

    {
        let mut g = Glue::new(db.connection_serialized());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
    }
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        g.execute("INSERT INTO docs VALUES (1,'a'),(2,'b');")
            .await
            .unwrap();
        g.execute("UPDATE docs SET body='c' WHERE id=1;")
            .await
            .unwrap();
        g.execute("DELETE FROM docs WHERE id=2;").await.unwrap();
    }

    eng.seal().await.unwrap();

    let rows = read_table(&eng, "docs").await;
    assert_eq!(rows.len(), 1, "id=2 deleted");
    assert_eq!(rows.get(&1).map(String::as_str), Some("c"), "id=1 updated");

    // The log is GC'd through the sealed watermark; sealing again is a no-op.
    assert!(db.scan_cdc(0).await.unwrap().is_empty());
    eng.seal().await.unwrap();
    assert_eq!(read_table(&eng, "docs").await.len(), 1, "no-op seal changes nothing");
}

/// Does the Iceberg table exist yet (version-hint present)?
async fn table_exists(root: &str) -> bool {
    FileIO::new_with_fs()
        .exists(&format!("{root}/main/docs/metadata/version-hint.text"))
        .await
        .unwrap()
}

#[tokio::test]
async fn seal_loop_fires_on_commit_and_skips_idle() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let db = make_db("loop").await;
    let cdc = CdcConfig::default();
    let eng = Arc::new(engine(&root, db.clone(), cdc.clone()).await);
    eng.enable_table("docs").await.unwrap();
    {
        let mut g = Glue::new(db.connection_serialized());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
    }

    let handle = eng.clone().spawn_seal_loop(Duration::from_millis(50), Duration::from_millis(500));

    // Idle (no mirror-enabled commits): the loop blocks, no snapshot appears.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!table_exists(&root).await, "idle table must not be sealed");

    // A commit wakes the loop; the row appears within a few debounce windows.
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        g.execute("INSERT INTO docs VALUES (1,'a');").await.unwrap();
    }
    let mut sealed = false;
    for _ in 0..60 {
        if table_exists(&root).await && read_table(&eng, "docs").await.get(&1).map(String::as_str) == Some("a")
        {
            sealed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    handle.abort();
    assert!(sealed, "seal loop should publish the row shortly after commit");
}

#[tokio::test]
async fn enable_backfills_preexisting_rows() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db = make_db("backfill").await;
    let cdc = CdcConfig::default();
    let eng = engine(root, db.clone(), cdc.clone()).await;

    // Rows written with CDC OFF (plain connection): no CDC entries recorded.
    {
        let mut g = Glue::new(db.connection_serialized());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
        g.execute("INSERT INTO docs VALUES (1,'a'),(2,'b'),(3,'c');")
            .await
            .unwrap();
    }
    assert!(db.scan_cdc(0).await.unwrap().is_empty(), "no CDC yet");

    // Enabling mirrors the existing rows via a backfill scan (not CDC).
    eng.enable_table("docs").await.unwrap();
    let rows = read_table(&eng, "docs").await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows.get(&2).map(String::as_str), Some("b"));
}

#[tokio::test]
async fn compaction_reduces_files_and_preserves_state() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db = make_db("compact").await;
    let cdc = CdcConfig::default();
    let eng = engine(root, db.clone(), cdc.clone()).await;
    eng.enable_table("docs").await.unwrap();
    {
        let mut g = Glue::new(db.connection_serialized());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
    }

    // Many separate insert+seal cycles → many tiny data files, plus an update
    // and a delete (→ equality-delete files).
    for i in 1..=12 {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        g.execute(&format!("INSERT INTO docs VALUES ({i}, 'v{i}');"))
            .await
            .unwrap();
        drop(g);
        eng.seal().await.unwrap();
    }
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        g.execute("UPDATE docs SET body='updated' WHERE id=1;")
            .await
            .unwrap();
        g.execute("DELETE FROM docs WHERE id=2;").await.unwrap();
    }
    eng.seal().await.unwrap();

    let before = eng.data_file_count("docs").await.unwrap();
    assert!(before > 5, "expected many small files, got {before}");

    eng.compact("docs").await.unwrap();

    let after = eng.data_file_count("docs").await.unwrap();
    assert!(after < before, "compaction should reduce file count ({after} < {before})");

    // Correctness vs the expected final state: id=2 deleted, id=1 updated.
    let rows = read_table(&eng, "docs").await;
    assert_eq!(rows.len(), 11, "12 inserted, 1 deleted");
    assert_eq!(rows.get(&1).map(String::as_str), Some("updated"));
    assert_eq!(rows.get(&2), None);
    assert_eq!(rows.get(&7).map(String::as_str), Some("v7"));
}

#[tokio::test]
async fn pragma_controls_default_and_per_table_and_persists() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db = make_db("pragma").await;
    let cdc = CdcConfig::default();
    let eng = engine(root, db.clone(), cdc.clone()).await;

    // Opt-in default (off): an arbitrary table is not mirrored.
    eng.apply_pragma(LhPragma::GlobalDefault(false)).await.unwrap();
    assert!(!eng.is_mirrored("docs"));

    // Per-table override on.
    eng.apply_pragma(LhPragma::Table("docs".into(), true)).await.unwrap();
    assert!(eng.is_mirrored("docs"));

    // Opt-out default (on): a different, un-overridden table is mirrored.
    eng.apply_pragma(LhPragma::GlobalDefault(true)).await.unwrap();
    assert!(eng.is_mirrored("anything_else"));
    assert!(eng.is_mirrored("docs")); // still on

    // Per-table override off under opt-out default.
    eng.apply_pragma(LhPragma::Table("secret".into(), false)).await.unwrap();
    assert!(!eng.is_mirrored("secret"));

    // All of it survives a reopen with a fresh CDC control.
    let cdc2 = CdcConfig::default();
    let eng2 = engine(root, db.clone(), cdc2.clone()).await;
    assert!(eng2.is_mirrored("anything_else"));
    assert!(!eng2.is_mirrored("secret"));
}
