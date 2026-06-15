//! End-to-end: a strict (user-facing) connection enforces bluedb's schema
//! regime — no schemaless tables, every table needs a PRIMARY KEY — while the
//! unguarded/internal connection keeps GlueSQL's full behavior (so the
//! conformance suite, which uses schemaless / PK-less tables, still passes).

use std::sync::Arc;

use bluedb_sql::Database;
use gluesql_core::prelude::Glue;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn database() -> Database {
    let db = Db::open("schema-regime-test", Arc::new(InMemory::new()))
        .await
        .unwrap();
    Database::new(Arc::new(db))
}

#[tokio::test]
async fn strict_rejects_schemaless_table() {
    let database = database().await;
    let mut user = Glue::new(database.connection_guarded());
    assert!(
        user.execute("CREATE TABLE t").await.is_err(),
        "a schemaless (column-less) CREATE TABLE must be rejected"
    );
}

#[tokio::test]
async fn strict_rejects_table_without_primary_key() {
    let database = database().await;
    let mut user = Glue::new(database.connection_guarded());
    assert!(
        user.execute("CREATE TABLE t (id INTEGER, name TEXT)")
            .await
            .is_err(),
        "a table without a PRIMARY KEY must be rejected"
    );
}

#[tokio::test]
async fn strict_allows_table_with_primary_key() {
    let database = database().await;
    let mut user = Glue::new(database.connection_guarded());
    let res = user
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .await;
    assert!(res.is_ok(), "schema'd table with a PK should be allowed: {res:?}");
}

#[tokio::test]
async fn unguarded_keeps_gluesql_behavior() {
    let database = database().await;
    let mut admin = Glue::new(database.connection());
    // The raw engine still supports schemaless + PK-less tables (conformance).
    assert!(admin.execute("CREATE TABLE sl").await.is_ok());
    assert!(admin
        .execute("CREATE TABLE np (id INTEGER, name TEXT)")
        .await
        .is_ok());
}
