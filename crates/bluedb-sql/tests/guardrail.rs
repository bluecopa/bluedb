//! End-to-end: a guarded connection keeps every read bounded through
//! `Glue::execute` — an unfiltered SELECT is capped to a PK-ordered prefix, a
//! non-indexed WHERE/ORDER BY is rejected — while an unguarded connection (the
//! internal/admin default) scans freely.

use std::sync::Arc;

use bluedb_sql::Database;
use gluesql_core::prelude::{Glue, Payload};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

/// The cap a guarded, unfiltered SELECT is bounded to (mirrors
/// `bluedb_sql::guardrail::UNFILTERED_SCAN_CAP`).
const CAP: usize = 100;

async fn database() -> Database {
    let db = Db::open("guardrail-test", Arc::new(InMemory::new()))
        .await
        .unwrap();
    Database::new(Arc::new(db))
}

async fn seed(database: &Database, rows: usize) {
    let mut glue = Glue::new(database.connection());
    glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    for i in 1..=rows {
        glue.execute(&format!("INSERT INTO t VALUES ({i}, 'n{i}')"))
            .await
            .unwrap();
    }
}

fn row_count(payloads: &[Payload]) -> usize {
    match &payloads[0] {
        Payload::Select { rows, .. } => rows.len(),
        other => panic!("expected a SELECT payload, got {other:?}"),
    }
}

#[tokio::test]
async fn guarded_connection_bounds_bare_scan_instead_of_rejecting() {
    let database = database().await;
    seed(&database, 2).await;
    let mut user = Glue::new(database.connection_guarded());
    // A WHERE-less SELECT is not rejected — it is auto-bounded to a PK-ordered
    // prefix. With only 2 rows it returns both.
    let out = user
        .execute("SELECT * FROM t")
        .await
        .expect("bare scan is bounded, not rejected");
    assert_eq!(row_count(&out), 2);
}

#[tokio::test]
async fn guarded_connection_caps_large_scan_at_100() {
    let database = database().await;
    seed(&database, 150).await;
    let mut user = Glue::new(database.connection_guarded());
    let out = user.execute("SELECT * FROM t").await.unwrap();
    assert_eq!(
        row_count(&out),
        CAP,
        "an unfiltered scan is capped at the ceiling"
    );
}

#[tokio::test]
async fn guarded_connection_clamps_a_larger_explicit_limit() {
    let database = database().await;
    seed(&database, 150).await;
    let mut user = Glue::new(database.connection_guarded());
    let out = user.execute("SELECT * FROM t LIMIT 500").await.unwrap();
    assert_eq!(
        row_count(&out),
        CAP,
        "an explicit LIMIT above the ceiling is clamped"
    );
}

#[tokio::test]
async fn guarded_connection_allows_primary_key_lookup() {
    let database = database().await;
    seed(&database, 2).await;
    let mut user = Glue::new(database.connection_guarded());
    let res = user.execute("SELECT * FROM t WHERE id = 1").await;
    assert!(res.is_ok(), "PK lookup should be allowed, got {res:?}");
}

#[tokio::test]
async fn guarded_connection_rejects_non_indexed_filter() {
    let database = database().await;
    seed(&database, 2).await;
    let mut user = Glue::new(database.connection_guarded());
    assert!(
        user.execute("SELECT * FROM t WHERE name = 'n1'")
            .await
            .is_err(),
        "a filter on a non-indexed column can't be bounded by a LIMIT — must be rejected"
    );
}

#[tokio::test]
async fn guarded_connection_rejects_order_by_unindexed() {
    let database = database().await;
    seed(&database, 2).await;
    let mut user = Glue::new(database.connection_guarded());
    assert!(
        user.execute("SELECT * FROM t WHERE id > 0 ORDER BY name")
            .await
            .is_err(),
        "ORDER BY a non-indexed column is an in-memory sort and must be rejected"
    );
}

#[tokio::test]
async fn unguarded_connection_allows_full_scan() {
    let database = database().await;
    seed(&database, 150).await;
    let mut admin = Glue::new(database.connection());
    let out = admin
        .execute("SELECT * FROM t")
        .await
        .expect("the default/internal connection is unguarded");
    assert_eq!(row_count(&out), 150, "an unguarded scan is not capped");
}
