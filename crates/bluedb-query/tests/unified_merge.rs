//! Spike: the DataFusion front-door's exact read-your-writes **union**.
//!
//! Proves the keystone claims of the front-door design:
//!  1. `merged_record_batch` returns the columnar Iceberg bulk merged with the
//!     unsealed CDC tail (anti-join on PK + last-writer-wins), so a query sees
//!     post-seal updates / deletes / inserts WITHOUT a re-seal (read-your-writes).
//!  2. Multi-table JOINs and window functions — neither of which GlueSQL handles
//!     correctly — "fall out" once each table is a DataFusion provider.
//!  3. PK-less tables are rejected (the merge keys on the PK).

use std::sync::Arc;

use arrow_array::Int64Array;
use bluedb_lakehouse::{object_store_file_io, LakehouseEngine};
use bluedb_query::{query_sql_unified, query_sql_unified_multi};
use bluedb_sql::{CdcConfig, Database};
use gluesql_core::prelude::Glue;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;

/// In-memory `Database` + lakehouse engine over a fresh `InMemory` object store.
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

/// Run DDL (no CDC needed).
async fn ddl(db: &Database, sql: &str) {
    let mut g = Glue::new(db.connection_serialized());
    g.execute(sql).await.unwrap();
}

/// Run DML through a CDC-enabled connection so the change is logged for the seal
/// loop / unsealed-tail replay.
async fn dml(db: &Database, cdc: &CdcConfig, sql: &str) {
    let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
    g.execute(sql).await.unwrap();
}

/// Collect `(id, v)` from result batches, looking columns up by name.
fn collect_id_v(batches: &[arrow_array::RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let vs = b
            .column_by_name("v")
            .expect("v column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v is Int64");
        for i in 0..b.num_rows() {
            out.push((ids.value(i), vs.value(i)));
        }
    }
    out
}

/// Claim 1: the unified read reflects post-seal update + delete + insert without
/// a re-seal — the exact read-your-writes union (Iceberg bulk ∪ CDC tail).
#[tokio::test]
async fn ryw_union_reflects_post_seal_mutations() {
    let (db, cdc, eng) = make_engine().await;
    eng.enable_table("t").await.unwrap();
    ddl(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, label TEXT);").await;

    // Seal three rows into Iceberg (the columnar bulk).
    dml(
        &db,
        &cdc,
        "INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c');",
    )
    .await;
    eng.seal().await.unwrap();

    // Mutate AFTER the seal — these live only in the unsealed CDC tail.
    dml(&db, &cdc, "UPDATE t SET v = 99 WHERE id = 2;").await; // update
    dml(&db, &cdc, "DELETE FROM t WHERE id = 3;").await; // delete
    dml(&db, &cdc, "INSERT INTO t VALUES (4, 40, 'd');").await; // insert

    let batches = query_sql_unified(eng.clone(), "t", "SELECT id, v FROM t ORDER BY id")
        .await
        .expect("unified query should succeed");

    let rows = collect_id_v(&batches);
    assert_eq!(
        rows,
        vec![(1, 10), (2, 99), (4, 40)],
        "id=1 from Iceberg bulk, id=2 updated by the tail (anti-join + LWW), \
         id=3 deleted by the tail, id=4 inserted by the tail: {rows:?}"
    );
}

/// Claim 1b: with no post-seal writes, the union equals the sealed snapshot
/// (the tail is empty, so every row comes from the Iceberg bulk).
#[tokio::test]
async fn union_equals_sealed_when_tail_empty() {
    let (db, cdc, eng) = make_engine().await;
    eng.enable_table("t").await.unwrap();
    ddl(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, label TEXT);").await;
    dml(&db, &cdc, "INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b');").await;
    eng.seal().await.unwrap();

    let batches = query_sql_unified(eng.clone(), "t", "SELECT id, v FROM t ORDER BY id")
        .await
        .expect("unified query should succeed");
    assert_eq!(collect_id_v(&batches), vec![(1, 10), (2, 20)]);
}

/// Claim 2a: a multi-table JOIN — GlueSQL's weak spot — plans over two merged
/// providers, and the tail's freshness flows through the join.
#[tokio::test]
async fn unified_join_across_two_tables() {
    let (db, cdc, eng) = make_engine().await;
    eng.enable_table("orders").await.unwrap();
    eng.enable_table("customers").await.unwrap();
    ddl(&db, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);").await;
    ddl(&db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER);").await;
    dml(&db, &cdc, "INSERT INTO customers VALUES (1, 'alice'), (2, 'bob');").await;
    dml(&db, &cdc, "INSERT INTO orders VALUES (1, 100), (2, 200);").await;
    eng.seal().await.unwrap();

    // Post-seal: bob's order amount changes (tail-only) — the join must see it.
    dml(&db, &cdc, "UPDATE orders SET amount = 250 WHERE id = 2;").await;

    let batches = query_sql_unified_multi(
        eng.clone(),
        &["orders", "customers"],
        "SELECT c.name, o.amount \
         FROM orders o JOIN customers c ON o.id = c.id \
         ORDER BY o.amount",
    )
    .await
    .expect("unified join should succeed");

    let mut out = Vec::new();
    for b in &batches {
        let names = b
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let amts = b
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((names.value(i).to_string(), amts.value(i)));
        }
    }
    assert_eq!(
        out,
        vec![("alice".to_string(), 100), ("bob".to_string(), 250)],
        "join sees the post-seal amount update on bob's order: {out:?}"
    );
}

/// Claim 2b: a window function — silently wrong on GlueSQL — is correct via
/// DataFusion over the union, including a tail-inserted row.
#[tokio::test]
async fn window_function_over_union() {
    let (db, cdc, eng) = make_engine().await;
    eng.enable_table("t").await.unwrap();
    ddl(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);").await;
    dml(&db, &cdc, "INSERT INTO t VALUES (1, 30), (2, 10);").await;
    eng.seal().await.unwrap();
    dml(&db, &cdc, "INSERT INTO t VALUES (3, 20);").await; // tail-only

    // ROW_NUMBER() ordered by v ascending: v=10 → 1, v=20 → 2, v=30 → 3.
    let batches = query_sql_unified(
        eng.clone(),
        "t",
        "SELECT id, CAST(ROW_NUMBER() OVER (ORDER BY v) AS BIGINT) AS rn FROM t",
    )
    .await
    .expect("window query should succeed");

    let mut out = Vec::new();
    for b in &batches {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let rn = b
            .column_by_name("rn")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((ids.value(i), rn.value(i)));
        }
    }
    out.sort_by_key(|(_, rn)| *rn);
    assert_eq!(
        out,
        vec![(2, 1), (3, 2), (1, 3)],
        "row-number by ascending v, including the tail-inserted id=3: {out:?}"
    );
}

/// Apply the composite-PK rewrite (surrogate `__bluedb_pk`) then execute — the
/// same path the server uses (see lakehouse `composite_mirror` test).
async fn exec_cpk(g: &mut Glue<bluedb_sql::SlateDbStorage>, sql: &str) {
    let prepared = bluedb_sql::prepare_composite_pk(&mut g.storage, sql, &[])
        .await
        .unwrap();
    g.execute(&prepared).await.unwrap();
}

fn collect_abv(batches: &[arrow_array::RecordBatch]) -> Vec<(i64, String, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let a = b
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let bb = b
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let v = b
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((a.value(i), bb.value(i).to_string(), v.value(i)));
        }
    }
    out
}

/// Claim 3 (composite PK): the RYW union works for a composite-key table — whose
/// merge/anti-join key is the hidden `__bluedb_pk BYTEA` surrogate — reflecting a
/// post-seal update + delete + insert addressed by the user components (a, b).
#[tokio::test]
async fn ryw_union_on_composite_pk_table() {
    let (db, cdc, eng) = make_engine().await;
    eng.enable_table("t").await.unwrap();
    {
        let mut g = Glue::new(db.connection_serialized());
        exec_cpk(
            &mut g,
            "CREATE TABLE t (a INTEGER, b TEXT, v INTEGER, PRIMARY KEY (a, b))",
        )
        .await;
    }
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        exec_cpk(
            &mut g,
            "INSERT INTO t (a, b, v) VALUES (1, 'x', 10), (1, 'y', 20), (2, 'x', 30)",
        )
        .await;
    }
    eng.seal().await.unwrap();
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        exec_cpk(&mut g, "UPDATE t SET v = 99 WHERE a = 1 AND b = 'x'").await;
        exec_cpk(&mut g, "DELETE FROM t WHERE a = 1 AND b = 'y'").await;
        exec_cpk(&mut g, "INSERT INTO t (a, b, v) VALUES (3, 'z', 40)").await;
    }

    let batches = query_sql_unified(eng.clone(), "t", "SELECT a, b, v FROM t ORDER BY a, b")
        .await
        .expect("composite-PK unified query should succeed");

    assert_eq!(
        collect_abv(&batches),
        vec![
            (1, "x".to_string(), 99), // updated in the tail
            (2, "x".to_string(), 30), // sealed, untouched
            (3, "z".to_string(), 40), // inserted in the tail
        ],
        "(1,'y') deleted; (1,'x') updated; (3,'z') inserted — anti-joined on the \
         __bluedb_pk surrogate"
    );
}

/// Claim 3b: `SELECT *` on a composite-PK table hides the internal
/// `__bluedb_pk` surrogate column — the user sees only their declared columns.
#[tokio::test]
async fn composite_select_star_hides_surrogate() {
    let (db, cdc, eng) = make_engine().await;
    eng.enable_table("t").await.unwrap();
    {
        let mut g = Glue::new(db.connection_serialized());
        exec_cpk(
            &mut g,
            "CREATE TABLE t (a INTEGER, b TEXT, v INTEGER, PRIMARY KEY (a, b))",
        )
        .await;
    }
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        exec_cpk(&mut g, "INSERT INTO t (a, b, v) VALUES (1, 'x', 10)").await;
    }
    eng.seal().await.unwrap();

    let batches = query_sql_unified(eng.clone(), "t", "SELECT * FROM t")
        .await
        .expect("composite SELECT * should succeed");
    let schema = batches[0].schema();
    let cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    assert_eq!(
        cols,
        vec!["a".to_string(), "b".to_string(), "v".to_string()],
        "SELECT * must hide the __bluedb_pk surrogate: {cols:?}"
    );
}

/// Claim 4: a PK-less table is rejected — the merge has no key to anti-join on.
#[tokio::test]
async fn merged_record_batch_errors_on_pk_less_table() {
    let (db, _cdc, eng) = make_engine().await;
    ddl(&db, "CREATE TABLE nopk (a INTEGER, b INTEGER);").await;

    let result = eng.merged_record_batch("nopk").await;
    assert!(result.is_err(), "PK-less table must error, got {result:?}");
    let msg = format!("{:?}", result.unwrap_err());
    assert!(
        msg.to_lowercase().contains("primary key"),
        "error should mention the missing primary key: {msg}"
    );
}
