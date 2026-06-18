//! End-to-end tests for [`bluedb_sql::SlateDbStorage`].
//!
//! Each test opens an in-memory SlateDB (`InMemory` object store), wraps it in
//! the storage, builds a `Glue`, and drives everything through SQL strings via
//! `Glue::execute`, asserting on the returned `Payload`s.

use std::sync::Arc;

use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

/// Open a fresh in-memory-backed SlateDB store wrapped for GlueSQL.
async fn new_glue() -> Glue<SlateDbStorage> {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("bluedb-sql-test", object_store)
        .await
        .expect("open slatedb");
    Glue::new(SlateDbStorage::new(Arc::new(db)))
}

/// Run one SQL statement and return its single payload.
async fn exec_one(glue: &mut Glue<SlateDbStorage>, sql: &str) -> Payload {
    let mut payloads = glue.execute(sql).await.expect("execute sql");
    assert_eq!(payloads.len(), 1, "expected one payload for: {sql}");
    payloads.pop().unwrap()
}

/// Pull the `rows` out of a `Payload::Select`.
fn select_rows(payload: Payload) -> Vec<Vec<Value>> {
    match payload {
        Payload::Select { rows, .. } => rows,
        other => panic!("expected Select payload, got {other:?}"),
    }
}

#[tokio::test]
async fn create_insert_select_where() {
    let mut glue = new_glue().await;

    exec_one(
        &mut glue,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);",
    )
    .await;

    let inserted = exec_one(
        &mut glue,
        "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 40);",
    )
    .await;
    assert!(matches!(inserted, Payload::Insert(3)));

    // WHERE filters to the matching rows.
    let rows = select_rows(
        exec_one(
            &mut glue,
            "SELECT id, name FROM users WHERE age >= 30 ORDER BY id;",
        )
        .await,
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::I64(1), Value::Str("alice".to_owned())],
            vec![Value::I64(3), Value::Str("carol".to_owned())],
        ]
    );

    // Single-row WHERE on the primary key.
    let rows = select_rows(exec_one(&mut glue, "SELECT name FROM users WHERE id = 2;").await);
    assert_eq!(rows, vec![vec![Value::Str("bob".to_owned())]]);
}

/// Ask #3: a bound integer param must land in a DECIMAL column the way an inline
/// literal already does. Without the `coerce_writes` planner pass this is the
/// reported failure: `incompatible data type, data type: Decimal, value: I64(1)`.
#[tokio::test]
async fn bound_int_param_widens_into_decimal_column() {
    let mut glue = new_glue().await;
    exec_one(
        &mut glue,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, amount DECIMAL);",
    )
    .await;

    // INSERT with both columns bound as I64 params (the data plane's form).
    let payloads = glue
        .execute_with_params(
            "INSERT INTO t (id, amount) VALUES ($1, $2);",
            gluesql_core::params!(1_i64, 20_i64),
        )
        .await
        .expect("parameterised insert into a DECIMAL column should succeed");
    assert!(matches!(payloads[0], Payload::Insert(1)), "got {:?}", payloads[0]);

    // It is stored as a real DECIMAL (the cast widened I64 → Decimal).
    let rows = select_rows(exec_one(&mut glue, "SELECT amount FROM t WHERE id = 1;").await);
    assert!(
        matches!(rows[0][0], Value::Decimal(_)),
        "expected a Decimal, got {:?}",
        rows[0][0]
    );

    // UPDATE with a bound int param into the same DECIMAL column also widens.
    let updated = glue
        .execute_with_params(
            "UPDATE t SET amount = $1 WHERE id = $2;",
            gluesql_core::params!(33_i64, 1_i64),
        )
        .await
        .expect("parameterised update of a DECIMAL column should succeed");
    assert!(matches!(updated[0], Payload::Update(1)), "got {:?}", updated[0]);
}

#[tokio::test]
async fn ordered_scan_returns_sorted_rows() {
    let mut glue = new_glue().await;

    exec_one(
        &mut glue,
        "CREATE TABLE nums (k INTEGER PRIMARY KEY, label TEXT);",
    )
    .await;

    // Insert deliberately out of order, including a negative key to exercise
    // the sign boundary in the comparable-bytes encoding.
    exec_one(
        &mut glue,
        "INSERT INTO nums VALUES (50, 'fifty'), (-5, 'neg'), (0, 'zero'), (7, 'seven'), (100, 'hundred');",
    )
    .await;

    let rows = select_rows(exec_one(&mut glue, "SELECT k FROM nums ORDER BY k;").await);
    let got: Vec<i64> = rows
        .into_iter()
        .map(|r| match r.into_iter().next().unwrap() {
            Value::I64(n) => n,
            other => panic!("expected I64, got {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![-5, 0, 7, 50, 100]);

    // The physical scan order (no ORDER BY clause) is already sorted because
    // storage keys sort by the encoded primary key.
    let rows_no_orderby = select_rows(exec_one(&mut glue, "SELECT k FROM nums;").await);
    let physical: Vec<i64> = rows_no_orderby
        .into_iter()
        .map(|r| match r.into_iter().next().unwrap() {
            Value::I64(n) => n,
            other => panic!("expected I64, got {other:?}"),
        })
        .collect();
    assert_eq!(physical, vec![-5, 0, 7, 50, 100]);
}

#[tokio::test]
async fn update_and_delete_mutate_correctly() {
    let mut glue = new_glue().await;

    exec_one(
        &mut glue,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER);",
    )
    .await;
    exec_one(
        &mut glue,
        "INSERT INTO items VALUES (1, 10), (2, 20), (3, 30);",
    )
    .await;

    // UPDATE one row.
    let updated = exec_one(&mut glue, "UPDATE items SET qty = 99 WHERE id = 2;").await;
    assert!(matches!(updated, Payload::Update(1)));

    let rows = select_rows(exec_one(&mut glue, "SELECT qty FROM items WHERE id = 2;").await);
    assert_eq!(rows, vec![vec![Value::I64(99)]]);

    // DELETE one row, then confirm the remaining set.
    let deleted = exec_one(&mut glue, "DELETE FROM items WHERE id = 1;").await;
    assert!(matches!(deleted, Payload::Delete(1)));

    let rows = select_rows(exec_one(&mut glue, "SELECT id, qty FROM items ORDER BY id;").await);
    assert_eq!(
        rows,
        vec![
            vec![Value::I64(2), Value::I64(99)],
            vec![Value::I64(3), Value::I64(30)],
        ]
    );
}

#[tokio::test]
async fn schemaless_table_insert_and_select() {
    let mut glue = new_glue().await;

    // A table with no column defs is schemaless: rows are maps.
    exec_one(&mut glue, "CREATE TABLE docs;").await;

    let inserted = exec_one(
        &mut glue,
        r#"INSERT INTO docs VALUES ('{"name": "alice", "score": 1}'), ('{"name": "bob", "score": 2}');"#,
    )
    .await;
    assert!(matches!(inserted, Payload::Insert(2)));

    // Schemaless selects come back as SelectMap (one BTreeMap per row).
    let payload = exec_one(&mut glue, "SELECT name, score FROM docs ORDER BY score;").await;
    let maps = match payload {
        Payload::Select { rows, .. } => rows
            .into_iter()
            .map(|row| row.into_iter().collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        Payload::SelectMap(maps) => maps
            .into_iter()
            .map(|m| m.into_values().collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        other => panic!("expected Select/SelectMap, got {other:?}"),
    };

    // Two rows, with the expected names present.
    assert_eq!(maps.len(), 2);
    let names: Vec<String> = maps
        .iter()
        .flatten()
        .filter_map(|v| match v {
            Value::Str(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"alice".to_owned()));
    assert!(names.contains(&"bob".to_owned()));
}
