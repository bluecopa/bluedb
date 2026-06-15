//! End-to-end composite-primary-key tests (Phase 2: CREATE + INSERT).
//!
//! Drives raw SQL through `bluedb_sql::prepare_composite_pk` (the rewrite the
//! production execution chokepoint applies) and then gluesql, against a real
//! SlateDB-backed connection — the same path the server uses.

use std::sync::Arc;

use bluedb_sql::{Database, SlateDbStorage};
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn new_glue() -> Glue<SlateDbStorage> {
    let db = Arc::new(Db::open("composite-pk-test", Arc::new(InMemory::new())).await.unwrap());
    Glue::new(Database::new(db).connection_serialized())
}

/// Apply the composite-PK rewrite, then execute — returning the single payload.
async fn exec(glue: &mut Glue<SlateDbStorage>, sql: &str) -> Result<Payload, String> {
    let prepared = bluedb_sql::prepare_composite_pk(&mut glue.storage, sql)
        .await
        .map_err(|e| e.to_string())?;
    let mut payloads = glue.execute(&prepared).await.map_err(|e| e.to_string())?;
    Ok(payloads.pop().unwrap())
}

fn rows(payload: Payload) -> Vec<Vec<Value>> {
    match payload {
        Payload::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    }
}

#[tokio::test]
async fn create_insert_and_readback() {
    let mut glue = new_glue().await;
    exec(
        &mut glue,
        "CREATE TABLE t (a INTEGER, b TEXT, payload TEXT, PRIMARY KEY (a, b))",
    )
    .await
    .unwrap();

    exec(&mut glue, "INSERT INTO t (a, b, payload) VALUES (1, 'x', 'p1')").await.unwrap();
    exec(&mut glue, "INSERT INTO t VALUES (1, 'y', 'p2')").await.unwrap(); // positional
    exec(&mut glue, "INSERT INTO t (a, b, payload) VALUES (2, 'x', 'p3')").await.unwrap();

    // Full-scan readback of the user columns (no PK predicate needed).
    let got = rows(exec(&mut glue, "SELECT a, b, payload FROM t ORDER BY a, b").await.unwrap());
    assert_eq!(got.len(), 3);
    assert_eq!(got[0], vec![Value::I64(1), Value::Str("x".into()), Value::Str("p1".into())]);
    assert_eq!(got[1], vec![Value::I64(1), Value::Str("y".into()), Value::Str("p2".into())]);
    assert_eq!(got[2], vec![Value::I64(2), Value::Str("x".into()), Value::Str("p3".into())]);
}

#[tokio::test]
async fn composite_key_uniqueness_is_enforced() {
    let mut glue = new_glue().await;
    exec(&mut glue, "CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a, b))").await.unwrap();
    exec(&mut glue, "INSERT INTO t (a, b) VALUES (1, 'x')").await.unwrap();

    // Same (a,b) → same __bluedb_pk → rejected.
    let dup = exec(&mut glue, "INSERT INTO t (a, b) VALUES (1, 'x')").await;
    assert!(dup.is_err(), "duplicate composite key should be rejected");

    // Differing in either component → distinct key → accepted.
    exec(&mut glue, "INSERT INTO t (a, b) VALUES (1, 'y')").await.unwrap();
    exec(&mut glue, "INSERT INTO t (a, b) VALUES (2, 'x')").await.unwrap();

    let got = rows(exec(&mut glue, "SELECT a, b FROM t ORDER BY a, b").await.unwrap());
    assert_eq!(got.len(), 3);
}

#[tokio::test]
async fn insert_missing_pk_component_is_rejected() {
    let mut glue = new_glue().await;
    exec(&mut glue, "CREATE TABLE t (a INTEGER, b TEXT, payload TEXT, PRIMARY KEY (a, b))")
        .await
        .unwrap();
    let err = exec(&mut glue, "INSERT INTO t (a, payload) VALUES (1, 'p')").await;
    assert!(err.is_err(), "omitting a PK component must be rejected");
}

#[tokio::test]
async fn single_column_pk_table_is_unaffected() {
    let mut glue = new_glue().await;
    // No composite PK → prepare is a passthrough; ordinary single-col PK works.
    exec(&mut glue, "CREATE TABLE s (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();
    exec(&mut glue, "INSERT INTO s VALUES (1, 'a')").await.unwrap();
    let got = rows(exec(&mut glue, "SELECT name FROM s WHERE id = 1").await.unwrap());
    assert_eq!(got, vec![vec![Value::Str("a".into())]]);
}
