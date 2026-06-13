//! Uniqueness-constraint tests for [`bluedb_sql::SlateDbStorage`].
//!
//! GlueSQL enforces uniqueness in its **executor**, on top of our `Store`:
//! * a PRIMARY KEY column is checked with a point `fetch_data` (O(1));
//! * any other `UNIQUE` column is checked by scanning the table via `scan_data`
//!   (O(n) per insert — gluesql does NOT route uniqueness through a secondary
//!   index; secondary indexes only accelerate query predicates).
//!
//! Both paths run through our overlay-aware `Store`, so these tests also confirm
//! uniqueness is correct *inside* a transaction (a buffered insert is visible to
//! the next statement's check, and a rolled-back insert frees the value again).

use std::sync::Arc;

use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::Glue;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn new_glue() -> Glue<SlateDbStorage> {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("bluedb-sql-unique-test", object_store)
        .await
        .expect("open slatedb");
    Glue::new(SlateDbStorage::new(Arc::new(db)))
}

#[tokio::test]
async fn primary_key_uniqueness_is_enforced() {
    let mut glue = new_glue().await;
    glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);").await.unwrap();
    glue.execute("INSERT INTO t VALUES (1, 'a');").await.unwrap();

    // Duplicate primary key is rejected.
    assert!(
        glue.execute("INSERT INTO t VALUES (1, 'b');").await.is_err(),
        "duplicate primary key must be rejected"
    );

    // A different key is fine, and the original row is untouched.
    glue.execute("INSERT INTO t VALUES (2, 'b');").await.unwrap();
    let rows = match glue.execute("SELECT id FROM t ORDER BY id;").await.unwrap().pop().unwrap() {
        gluesql_core::prelude::Payload::Select { rows, .. } => rows.len(),
        p => panic!("{p:?}"),
    };
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn unique_column_constraint_is_enforced_on_insert_and_update() {
    let mut glue = new_glue().await;
    glue.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT UNIQUE);").await.unwrap();
    glue.execute("INSERT INTO u VALUES (1, 'x@y');").await.unwrap();

    // Duplicate value in the UNIQUE column is rejected on INSERT...
    assert!(
        glue.execute("INSERT INTO u VALUES (2, 'x@y');").await.is_err(),
        "duplicate UNIQUE column value must be rejected on insert"
    );
    // ...a distinct value is accepted...
    glue.execute("INSERT INTO u VALUES (2, 'z@y');").await.unwrap();
    // ...and an UPDATE that would collide is also rejected.
    assert!(
        glue.execute("UPDATE u SET email = 'x@y' WHERE id = 2;").await.is_err(),
        "UPDATE into an existing UNIQUE value must be rejected"
    );
}

#[tokio::test]
async fn uniqueness_is_consistent_within_a_transaction() {
    let mut glue = new_glue().await;
    glue.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT UNIQUE);").await.unwrap();
    glue.execute("INSERT INTO u VALUES (1, 'x@y');").await.unwrap();

    // A buffered (uncommitted) insert is visible to the next uniqueness check.
    glue.execute("BEGIN;").await.unwrap();
    glue.execute("INSERT INTO u VALUES (2, 'new@y');").await.unwrap();
    assert!(
        glue.execute("INSERT INTO u VALUES (3, 'new@y');").await.is_err(),
        "uniqueness check sees the txn's own uncommitted insert"
    );
    // Colliding with an already-committed value is likewise rejected mid-txn.
    assert!(
        glue.execute("INSERT INTO u VALUES (4, 'x@y');").await.is_err(),
        "uniqueness check sees committed rows from inside the txn"
    );
    glue.execute("ROLLBACK;").await.unwrap();

    // After rollback the buffered 'new@y' is gone, so the value is free again.
    glue.execute("INSERT INTO u VALUES (2, 'new@y');").await.unwrap();
    let n = match glue.execute("SELECT id FROM u;").await.unwrap().pop().unwrap() {
        gluesql_core::prelude::Payload::Select { rows, .. } => rows.len(),
        p => panic!("{p:?}"),
    };
    assert_eq!(n, 2, "only the committed rows (1 and the post-rollback 2) remain");
}
