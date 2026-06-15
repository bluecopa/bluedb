//! Table-id indirection: RENAME TABLE is O(1). Rows + index entries are keyed by
//! a stable table id, so a rename moves only the schema / id-mapping / meta
//! singletons — never a row. Verify rows survive a rename (readable under the new
//! name, gone under the old) and the id is unchanged.

use std::sync::Arc;

use bluedb_sql::Database;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn database() -> Database {
    let db = Db::open("rename-test", Arc::new(InMemory::new()))
        .await
        .unwrap();
    Database::new(Arc::new(db))
}

async fn exec(glue: &mut Glue<bluedb_sql::SlateDbStorage>, sql: &str) -> Payload {
    glue.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec {sql:?}: {e:?}"))
        .pop()
        .unwrap()
}

fn rows(payload: Payload) -> Vec<Vec<Value>> {
    match payload {
        Payload::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    }
}

#[tokio::test]
async fn rename_table_preserves_rows_and_id() {
    let database = database().await;
    let mut c = Glue::new(database.connection());
    exec(&mut c, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)").await;
    exec(&mut c, "INSERT INTO t VALUES (1, 10)").await;
    exec(&mut c, "INSERT INTO t VALUES (2, 20)").await;
    let id_before = database.table_id("t").await.unwrap().expect("id assigned");

    exec(&mut c, "ALTER TABLE t RENAME TO t2").await;

    // Rows are readable under the new name, unchanged.
    let r = rows(exec(&mut c, "SELECT id, a FROM t2 ORDER BY id").await);
    assert_eq!(
        r,
        vec![
            vec![Value::I64(1), Value::I64(10)],
            vec![Value::I64(2), Value::I64(20)],
        ]
    );
    // The stable id is unchanged — the rename did not re-key any row.
    let id_after = database.table_id("t2").await.unwrap().expect("id present");
    assert_eq!(id_before, id_after, "table id must be stable across rename");

    // The old name no longer resolves.
    assert!(database.table_id("t").await.unwrap().is_none());
    assert!(
        c.execute("SELECT * FROM t WHERE id = 1").await.is_err(),
        "old table name must be gone after rename"
    );
}

#[tokio::test]
async fn renamed_table_keeps_working_indexes() {
    let database = database().await;
    let mut c = Glue::new(database.connection());
    exec(&mut c, "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)").await;
    exec(&mut c, "CREATE INDEX t_email ON t (email)").await;
    exec(&mut c, "INSERT INTO t VALUES (1, 'a@b.c')").await;
    exec(&mut c, "ALTER TABLE t RENAME TO users").await;
    // The index still resolves under the new name (entries keyed by table id).
    let r = rows(exec(&mut c, "SELECT id FROM users WHERE email = 'a@b.c'").await);
    assert_eq!(r, vec![vec![Value::I64(1)]]);
}
