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
    let db = Arc::new(
        Db::open("composite-pk-test", Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    Glue::new(Database::new(db).connection_serialized())
}

/// A guarded (user-facing) connection: full-scan / non-indexed filters are
/// rejected. Used to prove the composite-key rewrite is index-served.
async fn new_guarded_glue() -> Glue<SlateDbStorage> {
    let db = Arc::new(
        Db::open("composite-pk-guarded", Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    Glue::new(Database::new(db).connection_serialized_guarded())
}

/// Apply the composite-PK rewrite, then execute — returning the single payload.
async fn exec(glue: &mut Glue<SlateDbStorage>, sql: &str) -> Result<Payload, String> {
    let prepared = bluedb_sql::prepare_composite_pk(&mut glue.storage, sql, &[])
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

    exec(
        &mut glue,
        "INSERT INTO t (a, b, payload) VALUES (1, 'x', 'p1')",
    )
    .await
    .unwrap();
    exec(&mut glue, "INSERT INTO t VALUES (1, 'y', 'p2')")
        .await
        .unwrap(); // positional
    exec(
        &mut glue,
        "INSERT INTO t (a, b, payload) VALUES (2, 'x', 'p3')",
    )
    .await
    .unwrap();

    // Full-scan readback of the user columns (no PK predicate needed).
    let got = rows(
        exec(&mut glue, "SELECT a, b, payload FROM t ORDER BY a, b")
            .await
            .unwrap(),
    );
    assert_eq!(got.len(), 3);
    assert_eq!(
        got[0],
        vec![
            Value::I64(1),
            Value::Str("x".into()),
            Value::Str("p1".into())
        ]
    );
    assert_eq!(
        got[1],
        vec![
            Value::I64(1),
            Value::Str("y".into()),
            Value::Str("p2".into())
        ]
    );
    assert_eq!(
        got[2],
        vec![
            Value::I64(2),
            Value::Str("x".into()),
            Value::Str("p3".into())
        ]
    );
}

#[tokio::test]
async fn composite_key_uniqueness_is_enforced() {
    let mut glue = new_glue().await;
    exec(
        &mut glue,
        "CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a, b))",
    )
    .await
    .unwrap();
    exec(&mut glue, "INSERT INTO t (a, b) VALUES (1, 'x')")
        .await
        .unwrap();

    // Same (a,b) → same __bluedb_pk → rejected.
    let dup = exec(&mut glue, "INSERT INTO t (a, b) VALUES (1, 'x')").await;
    assert!(dup.is_err(), "duplicate composite key should be rejected");

    // Differing in either component → distinct key → accepted.
    exec(&mut glue, "INSERT INTO t (a, b) VALUES (1, 'y')")
        .await
        .unwrap();
    exec(&mut glue, "INSERT INTO t (a, b) VALUES (2, 'x')")
        .await
        .unwrap();

    let got = rows(
        exec(&mut glue, "SELECT a, b FROM t ORDER BY a, b")
            .await
            .unwrap(),
    );
    assert_eq!(got.len(), 3);
}

#[tokio::test]
async fn insert_missing_pk_component_is_rejected() {
    let mut glue = new_glue().await;
    exec(
        &mut glue,
        "CREATE TABLE t (a INTEGER, b TEXT, payload TEXT, PRIMARY KEY (a, b))",
    )
    .await
    .unwrap();
    let err = exec(&mut glue, "INSERT INTO t (a, payload) VALUES (1, 'p')").await;
    assert!(err.is_err(), "omitting a PK component must be rejected");
}

/// Seed a `t (a INTEGER, b TEXT, payload TEXT, PRIMARY KEY (a,b))` with rows.
async fn seed(glue: &mut Glue<SlateDbStorage>) {
    exec(
        glue,
        "CREATE TABLE t (a INTEGER, b TEXT, payload TEXT, PRIMARY KEY (a, b))",
    )
    .await
    .unwrap();
    for (a, b, p) in [
        (1, "m", "p1"),
        (1, "x", "p2"),
        (1, "z", "p3"),
        (2, "a", "p4"),
    ] {
        exec(
            glue,
            &format!("INSERT INTO t (a, b, payload) VALUES ({a}, '{b}', '{p}')"),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn point_prefix_and_range_lookups() {
    let mut glue = new_glue().await;
    seed(&mut glue).await;

    // Full-key point lookup.
    let got = rows(
        exec(&mut glue, "SELECT payload FROM t WHERE a = 1 AND b = 'x'")
            .await
            .unwrap(),
    );
    assert_eq!(got, vec![vec![Value::Str("p2".into())]]);

    // Leading-prefix lookup (all rows with a = 1), in key order.
    let got = rows(
        exec(&mut glue, "SELECT b FROM t WHERE a = 1 ORDER BY a, b")
            .await
            .unwrap(),
    );
    assert_eq!(
        got,
        vec![
            vec![Value::Str("m".into())],
            vec![Value::Str("x".into())],
            vec![Value::Str("z".into())],
        ]
    );

    // Prefix + trailing range.
    let got = rows(
        exec(
            &mut glue,
            "SELECT b FROM t WHERE a = 1 AND b > 'm' ORDER BY a, b",
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        got,
        vec![vec![Value::Str("x".into())], vec![Value::Str("z".into())]]
    );
}

#[tokio::test]
async fn row_value_keyset_pagination() {
    // (a,b) > (1,'m') — cross-partition keyset, the canonical pagination idiom.
    let mut glue = new_guarded_glue().await;
    seed(&mut glue).await;
    let got = rows(
        exec(
            &mut glue,
            "SELECT a, b FROM t WHERE (a, b) > (1, 'm') ORDER BY a, b",
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        got,
        vec![
            vec![Value::I64(1), Value::Str("x".into())],
            vec![Value::I64(1), Value::Str("z".into())],
            vec![Value::I64(2), Value::Str("a".into())],
        ]
    );
}

#[tokio::test]
async fn select_star_hides_the_surrogate() {
    let mut glue = new_glue().await;
    seed(&mut glue).await;
    let got = rows(
        exec(&mut glue, "SELECT * FROM t WHERE a = 1 AND b = 'x'")
            .await
            .unwrap(),
    );
    // Three user columns (a, b, payload) — NOT the hidden __bluedb_pk.
    assert_eq!(
        got,
        vec![vec![
            Value::I64(1),
            Value::Str("x".into()),
            Value::Str("p2".into())
        ]]
    );
}

#[tokio::test]
async fn update_and_delete_by_composite_key() {
    let mut glue = new_glue().await;
    seed(&mut glue).await;

    exec(
        &mut glue,
        "UPDATE t SET payload = 'updated' WHERE a = 1 AND b = 'x'",
    )
    .await
    .unwrap();
    let got = rows(
        exec(&mut glue, "SELECT payload FROM t WHERE a = 1 AND b = 'x'")
            .await
            .unwrap(),
    );
    assert_eq!(got, vec![vec![Value::Str("updated".into())]]);

    exec(&mut glue, "DELETE FROM t WHERE a = 1 AND b = 'm'")
        .await
        .unwrap();
    let got = rows(
        exec(&mut glue, "SELECT b FROM t WHERE a = 1 ORDER BY a, b")
            .await
            .unwrap(),
    );
    assert_eq!(
        got,
        vec![vec![Value::Str("x".into())], vec![Value::Str("z".into())]]
    );
}

#[tokio::test]
async fn update_of_pk_component_is_rejected() {
    let mut glue = new_glue().await;
    seed(&mut glue).await;
    let err = exec(&mut glue, "UPDATE t SET a = 9 WHERE a = 1 AND b = 'x'").await;
    assert!(err.is_err(), "changing a PK component must be rejected");
}

#[tokio::test]
async fn composite_lookups_are_index_served_on_a_guarded_connection() {
    // The whole point: without the rewrite, `WHERE a=.. AND b=..` on non-indexed
    // columns is rejected by the guardrail. With it, the predicate becomes a
    // __bluedb_pk point/range lookup the guardrail allows.
    let mut glue = new_guarded_glue().await;
    seed(&mut glue).await;

    // Point + prefix + ordered prefix all succeed (index-served).
    assert!(
        exec(&mut glue, "SELECT payload FROM t WHERE a = 1 AND b = 'x'")
            .await
            .is_ok()
    );
    assert!(exec(&mut glue, "SELECT b FROM t WHERE a = 1 ORDER BY a, b")
        .await
        .is_ok());
    assert!(exec(
        &mut glue,
        "SELECT b FROM t WHERE a = 1 AND b >= 'x' ORDER BY a, b"
    )
    .await
    .is_ok());

    // Control: a genuinely non-indexed filter is still rejected (guardrail live).
    assert!(
        exec(&mut glue, "SELECT a FROM t WHERE payload = 'p1'")
            .await
            .is_err(),
        "non-indexed filter should be rejected — proves the guardrail is active"
    );
}

#[tokio::test]
async fn single_column_pk_table_is_unaffected() {
    let mut glue = new_glue().await;
    // No composite PK → prepare is a passthrough; ordinary single-col PK works.
    exec(
        &mut glue,
        "CREATE TABLE s (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await
    .unwrap();
    exec(&mut glue, "INSERT INTO s VALUES (1, 'a')")
        .await
        .unwrap();
    let got = rows(
        exec(&mut glue, "SELECT name FROM s WHERE id = 1")
            .await
            .unwrap(),
    );
    assert_eq!(got, vec![vec![Value::Str("a".into())]]);
}
