//! Integration tests for the real [`Transaction`] implementation:
//! `BEGIN`/`COMMIT`/`ROLLBACK` with a write-buffer overlay over a SlateDB
//! snapshot. Everything is driven through SQL via `Glue::execute`.
//!
//! [`Transaction`]: gluesql_core::store::Transaction

use std::sync::Arc;

use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

/// Open a fresh in-memory-backed SlateDB store wrapped for GlueSQL.
async fn new_glue() -> Glue<SlateDbStorage> {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("bluedb-sql-txn-test", object_store)
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

/// Return the single integer column of every selected row, in order.
async fn select_ids(glue: &mut Glue<SlateDbStorage>, sql: &str) -> Vec<i64> {
    select_rows(exec_one(glue, sql).await)
        .into_iter()
        .map(|r| match r.into_iter().next().unwrap() {
            Value::I64(n) => n,
            other => panic!("expected I64, got {other:?}"),
        })
        .collect()
}

async fn seed_users(glue: &mut Glue<SlateDbStorage>) {
    exec_one(
        glue,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
    )
    .await;
}

#[tokio::test]
async fn rollback_discards_inserts() {
    let mut glue = new_glue().await;
    seed_users(&mut glue).await;

    exec_one(&mut glue, "BEGIN;").await;
    exec_one(&mut glue, "INSERT INTO users VALUES (1, 'alice');").await;
    // Read-your-own-writes: the buffered insert is visible inside the txn.
    assert_eq!(select_ids(&mut glue, "SELECT id FROM users;").await, vec![1]);
    exec_one(&mut glue, "ROLLBACK;").await;

    // After rollback the row never reached storage.
    assert_eq!(
        select_ids(&mut glue, "SELECT id FROM users;").await,
        Vec::<i64>::new()
    );
}

#[tokio::test]
async fn commit_persists_inserts() {
    let mut glue = new_glue().await;
    seed_users(&mut glue).await;

    exec_one(&mut glue, "BEGIN;").await;
    exec_one(&mut glue, "INSERT INTO users VALUES (1, 'alice');").await;
    exec_one(&mut glue, "COMMIT;").await;

    assert_eq!(select_ids(&mut glue, "SELECT id FROM users;").await, vec![1]);
}

#[tokio::test]
async fn read_your_own_writes_inside_txn() {
    let mut glue = new_glue().await;
    seed_users(&mut glue).await;
    exec_one(&mut glue, "INSERT INTO users VALUES (1, 'alice');").await;

    exec_one(&mut glue, "BEGIN;").await;
    exec_one(&mut glue, "INSERT INTO users VALUES (2, 'bob');").await;
    exec_one(&mut glue, "UPDATE users SET name = 'ALICE' WHERE id = 1;").await;
    // The txn sees its own insert AND its own update, layered over the base.
    let rows = select_rows(exec_one(&mut glue, "SELECT id, name FROM users ORDER BY id;").await);
    assert_eq!(
        rows,
        vec![
            vec![Value::I64(1), Value::Str("ALICE".to_owned())],
            vec![Value::I64(2), Value::Str("bob".to_owned())],
        ]
    );
    exec_one(&mut glue, "ROLLBACK;").await;
}

#[tokio::test]
async fn rollback_after_update_restores_prior_state() {
    let mut glue = new_glue().await;
    seed_users(&mut glue).await;
    exec_one(&mut glue, "INSERT INTO users VALUES (1, 'alice');").await;

    exec_one(&mut glue, "BEGIN;").await;
    exec_one(&mut glue, "UPDATE users SET name = 'mutated' WHERE id = 1;").await;
    let rows = select_rows(exec_one(&mut glue, "SELECT name FROM users WHERE id = 1;").await);
    assert_eq!(rows, vec![vec![Value::Str("mutated".to_owned())]]);
    exec_one(&mut glue, "ROLLBACK;").await;

    // Original value is back.
    let rows = select_rows(exec_one(&mut glue, "SELECT name FROM users WHERE id = 1;").await);
    assert_eq!(rows, vec![vec![Value::Str("alice".to_owned())]]);
}

#[tokio::test]
async fn rollback_after_delete_restores_prior_state() {
    let mut glue = new_glue().await;
    seed_users(&mut glue).await;
    exec_one(&mut glue, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob');").await;

    exec_one(&mut glue, "BEGIN;").await;
    exec_one(&mut glue, "DELETE FROM users WHERE id = 1;").await;
    assert_eq!(select_ids(&mut glue, "SELECT id FROM users;").await, vec![2]);
    exec_one(&mut glue, "ROLLBACK;").await;

    // The deleted row is restored.
    assert_eq!(
        select_ids(&mut glue, "SELECT id FROM users ORDER BY id;").await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn multi_statement_txn_commits_atomically() {
    let mut glue = new_glue().await;
    seed_users(&mut glue).await;
    exec_one(&mut glue, "INSERT INTO users VALUES (1, 'alice');").await;

    // A mix of insert/update/delete across several statements commits as one.
    exec_one(&mut glue, "BEGIN;").await;
    exec_one(&mut glue, "INSERT INTO users VALUES (2, 'bob'), (3, 'carol');").await;
    exec_one(&mut glue, "UPDATE users SET name = 'ALICE' WHERE id = 1;").await;
    exec_one(&mut glue, "DELETE FROM users WHERE id = 3;").await;
    exec_one(&mut glue, "COMMIT;").await;

    let rows = select_rows(exec_one(&mut glue, "SELECT id, name FROM users ORDER BY id;").await);
    assert_eq!(
        rows,
        vec![
            vec![Value::I64(1), Value::Str("ALICE".to_owned())],
            vec![Value::I64(2), Value::Str("bob".to_owned())],
        ]
    );
}
