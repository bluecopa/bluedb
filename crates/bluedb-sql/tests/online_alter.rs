//! Online ALTER (field-ids): ADD / DROP / RENAME COLUMN are metadata-only (no
//! row rewrite), and the column catalog is *persisted* — a fresh connection
//! re-reads it and still pads pre-existing rows with the added column's default.

use std::sync::Arc;

use bluedb_sql::Database;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn database() -> Database {
    let db = Db::open("online-alter-test", Arc::new(InMemory::new()))
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
async fn add_column_pads_old_rows_and_survives_a_fresh_connection() {
    let database = database().await;
    {
        let mut c = Glue::new(database.connection());
        exec(&mut c, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)").await;
        exec(&mut c, "INSERT INTO t VALUES (1, 10)").await;
        exec(&mut c, "INSERT INTO t VALUES (2, 20)").await;
        // Metadata-only add with a non-null default — no row rewrite.
        exec(&mut c, "ALTER TABLE t ADD COLUMN b INTEGER DEFAULT 99").await;
    }
    // A brand-new connection re-reads the persisted catalog from storage.
    let mut c2 = Glue::new(database.connection());
    let r = rows(exec(&mut c2, "SELECT id, a, b FROM t ORDER BY id").await);
    assert_eq!(
        r,
        vec![
            vec![Value::I64(1), Value::I64(10), Value::I64(99)],
            vec![Value::I64(2), Value::I64(20), Value::I64(99)],
        ],
        "old rows must read back the column default"
    );
    // A new row carries its own value, not the default.
    exec(&mut c2, "INSERT INTO t VALUES (3, 30, 300)").await;
    let r = rows(exec(&mut c2, "SELECT b FROM t WHERE id = 3").await);
    assert_eq!(r, vec![vec![Value::I64(300)]]);
}

#[tokio::test]
async fn drop_column_projects_it_out() {
    let database = database().await;
    let mut c = Glue::new(database.connection());
    exec(
        &mut c,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
    )
    .await;
    exec(&mut c, "INSERT INTO t VALUES (1, 10, 100)").await;
    exec(&mut c, "ALTER TABLE t DROP COLUMN a").await;
    // Remaining logical columns are (id, b); the dropped value is gone.
    let r = rows(exec(&mut c, "SELECT * FROM t WHERE id = 1").await);
    assert_eq!(r, vec![vec![Value::I64(1), Value::I64(100)]]);
}

#[tokio::test]
async fn rename_column_keeps_data() {
    let database = database().await;
    let mut c = Glue::new(database.connection());
    exec(&mut c, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)").await;
    exec(&mut c, "INSERT INTO t VALUES (1, 10)").await;
    exec(&mut c, "ALTER TABLE t RENAME COLUMN a TO b").await;
    let r = rows(exec(&mut c, "SELECT b FROM t WHERE id = 1").await);
    assert_eq!(r, vec![vec![Value::I64(10)]]);
}
