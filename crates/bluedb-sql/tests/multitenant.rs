//! Integration tests for tenant isolation: two storages over the SAME `Db`,
//! scoped to different tenants via [`SlateDbStorage::new_for_tenant`], must not
//! see each other's data even with identical table and column names.

use std::sync::Arc;

use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn exec_one(glue: &mut Glue<SlateDbStorage>, sql: &str) -> Payload {
    let mut payloads = glue.execute(sql).await.expect("execute sql");
    assert_eq!(payloads.len(), 1, "expected one payload for: {sql}");
    payloads.pop().unwrap()
}

fn select_rows(payload: Payload) -> Vec<Vec<Value>> {
    match payload {
        Payload::Select { rows, .. } => rows,
        other => panic!("expected Select payload, got {other:?}"),
    }
}

#[tokio::test]
async fn two_tenants_share_a_db_without_cross_reads() {
    // One physical SlateDB, two logical tenants.
    let db = Arc::new(
        Db::open("bluedb-sql-multitenant", Arc::new(InMemory::new()))
            .await
            .expect("open slatedb"),
    );
    let mut alice = Glue::new(SlateDbStorage::new_for_tenant(Arc::clone(&db), "alice"));
    let mut bob = Glue::new(SlateDbStorage::new_for_tenant(Arc::clone(&db), "bob"));

    // Same table name, same schema, different rows per tenant.
    for glue in [&mut alice, &mut bob] {
        exec_one(glue, "CREATE TABLE acct (id INTEGER PRIMARY KEY, owner TEXT);").await;
    }
    exec_one(&mut alice, "INSERT INTO acct VALUES (1, 'alice-row');").await;
    exec_one(&mut bob, "INSERT INTO acct VALUES (2, 'bob-row');").await;

    // Each tenant sees only its own row.
    assert_eq!(
        select_rows(exec_one(&mut alice, "SELECT id, owner FROM acct ORDER BY id;").await),
        vec![vec![Value::I64(1), Value::Str("alice-row".to_owned())]],
    );
    assert_eq!(
        select_rows(exec_one(&mut bob, "SELECT id, owner FROM acct ORDER BY id;").await),
        vec![vec![Value::I64(2), Value::Str("bob-row".to_owned())]],
    );

    // Neither can read the other's row by its primary key.
    assert_eq!(
        select_rows(exec_one(&mut alice, "SELECT owner FROM acct WHERE id = 2;").await),
        Vec::<Vec<Value>>::new(),
    );
    assert_eq!(
        select_rows(exec_one(&mut bob, "SELECT owner FROM acct WHERE id = 1;").await),
        Vec::<Vec<Value>>::new(),
    );

    // A mutation in one tenant does not touch the other's data.
    exec_one(&mut alice, "DELETE FROM acct WHERE id = 1;").await;
    assert_eq!(
        select_rows(exec_one(&mut alice, "SELECT id FROM acct;").await),
        Vec::<Vec<Value>>::new(),
    );
    assert_eq!(
        select_rows(exec_one(&mut bob, "SELECT id FROM acct;").await),
        vec![vec![Value::I64(2)]],
    );
}
