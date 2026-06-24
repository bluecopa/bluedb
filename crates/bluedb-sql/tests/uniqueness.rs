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
    glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
        .await
        .unwrap();
    glue.execute("INSERT INTO t VALUES (1, 'a');")
        .await
        .unwrap();

    // Duplicate primary key is rejected.
    assert!(
        glue.execute("INSERT INTO t VALUES (1, 'b');")
            .await
            .is_err(),
        "duplicate primary key must be rejected"
    );

    // A different key is fine, and the original row is untouched.
    glue.execute("INSERT INTO t VALUES (2, 'b');")
        .await
        .unwrap();
    let rows = match glue
        .execute("SELECT id FROM t ORDER BY id;")
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        gluesql_core::prelude::Payload::Select { rows, .. } => rows.len(),
        p => panic!("{p:?}"),
    };
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn unique_column_constraint_is_enforced_on_insert_and_update() {
    let mut glue = new_glue().await;
    glue.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT UNIQUE);")
        .await
        .unwrap();
    glue.execute("INSERT INTO u VALUES (1, 'x@y');")
        .await
        .unwrap();

    // Duplicate value in the UNIQUE column is rejected on INSERT...
    assert!(
        glue.execute("INSERT INTO u VALUES (2, 'x@y');")
            .await
            .is_err(),
        "duplicate UNIQUE column value must be rejected on insert"
    );
    // ...a distinct value is accepted...
    glue.execute("INSERT INTO u VALUES (2, 'z@y');")
        .await
        .unwrap();
    // ...and an UPDATE that would collide is also rejected.
    assert!(
        glue.execute("UPDATE u SET email = 'x@y' WHERE id = 2;")
            .await
            .is_err(),
        "UPDATE into an existing UNIQUE value must be rejected"
    );
}

#[tokio::test]
async fn uniqueness_is_consistent_within_a_transaction() {
    let mut glue = new_glue().await;
    glue.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT UNIQUE);")
        .await
        .unwrap();
    glue.execute("INSERT INTO u VALUES (1, 'x@y');")
        .await
        .unwrap();

    // A buffered (uncommitted) insert is visible to the next uniqueness check.
    glue.execute("BEGIN;").await.unwrap();
    glue.execute("INSERT INTO u VALUES (2, 'new@y');")
        .await
        .unwrap();
    assert!(
        glue.execute("INSERT INTO u VALUES (3, 'new@y');")
            .await
            .is_err(),
        "uniqueness check sees the txn's own uncommitted insert"
    );
    // Colliding with an already-committed value is likewise rejected mid-txn.
    assert!(
        glue.execute("INSERT INTO u VALUES (4, 'x@y');")
            .await
            .is_err(),
        "uniqueness check sees committed rows from inside the txn"
    );
    glue.execute("ROLLBACK;").await.unwrap();

    // After rollback the buffered 'new@y' is gone, so the value is free again.
    glue.execute("INSERT INTO u VALUES (2, 'new@y');")
        .await
        .unwrap();
    let n = match glue
        .execute("SELECT id FROM u;")
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        gluesql_core::prelude::Payload::Select { rows, .. } => rows.len(),
        p => panic!("{p:?}"),
    };
    assert_eq!(
        n, 2,
        "only the committed rows (1 and the post-rollback 2) remain"
    );
}

/// Run `sql` through the composite-PK pre-parse rewrite (the same chokepoint
/// every user-facing SQL path uses) and execute the result. This is what makes
/// `CREATE UNIQUE INDEX` reach the bluedb-sql unique-index registry — GlueSQL's
/// own translator silently drops the UNIQUE keyword.
async fn exec_rewritten(glue: &mut Glue<SlateDbStorage>, sql: &str) {
    let rewritten = bluedb_sql::prepare_composite_pk(&mut glue.storage, sql, &[])
        .await
        .expect("rewrite");
    glue.execute(&rewritten).await.expect("execute");
}

/// `CREATE UNIQUE INDEX` must enforce uniqueness at the engine level. GlueSQL
/// drops the UNIQUE keyword from its `IndexMut::create_index` trait, so without
/// the bluedb-sql registry + `apply_index_entries` enforcement a unique
/// secondary index is silently non-unique. This runs through the production
/// pre-parse rewrite (where UNIQUE is detected), then exercises insert/update/
/// NULL-distinct/drop-index semantics.
#[tokio::test]
async fn unique_secondary_index_is_enforced_via_rewrite() {
    let mut glue = new_glue().await;
    exec_rewritten(
        &mut glue,
        "CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT);",
    )
    .await;
    exec_rewritten(&mut glue, "CREATE UNIQUE INDEX u_email ON u (email);").await;
    exec_rewritten(&mut glue, "INSERT INTO u VALUES (1, 'a@x');").await;

    // Duplicate indexed value, different PK → rejected.
    let err = glue
        .execute("INSERT INTO u VALUES (2, 'a@x');")
        .await
        .expect_err("duplicate unique-index value must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("u_email"), "error names the index: {msg}");

    // Distinct value inserts cleanly; UPDATE to a colliding value is rejected.
    exec_rewritten(&mut glue, "INSERT INTO u VALUES (2, 'b@x');").await;
    assert!(
        glue.execute("UPDATE u SET email = 'a@x' WHERE id = 2;")
            .await
            .is_err(),
        "update to a colliding unique-index value must be rejected"
    );

    // NULLs are distinct: two rows may both have NULL under a unique index.
    exec_rewritten(
        &mut glue,
        "CREATE TABLE n (id INTEGER PRIMARY KEY, v TEXT);",
    )
    .await;
    exec_rewritten(&mut glue, "CREATE UNIQUE INDEX n_v ON n (v);").await;
    exec_rewritten(&mut glue, "INSERT INTO n (id, v) VALUES (1, NULL);").await;
    exec_rewritten(&mut glue, "INSERT INTO n (id, v) VALUES (2, NULL);").await;

    // DROP INDEX removes enforcement, so a duplicate value inserts afterwards.
    exec_rewritten(&mut glue, "DROP INDEX u.u_email;").await;
    exec_rewritten(&mut glue, "INSERT INTO u VALUES (3, 'a@x');").await;
}
