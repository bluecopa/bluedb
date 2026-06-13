//! Read-replica SQL: a writer creates + inserts, a `DbReader`-backed `Database`
//! over the SAME object store serves `SELECT`s and refuses writes. One process,
//! no cluster — the standby-reads-the-writer model, verifiable here.

use std::sync::Arc;

use bluedb_sql::Database;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::{Db, DbReader};

#[tokio::test]
async fn replica_serves_reads_and_refuses_writes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Active writer creates the table + rows, then flushes for durability.
    let writer_db = Arc::new(Db::open("bluedb-sql-ha", store.clone()).await.expect("open writer"));
    {
        let mut glue = Glue::new(Database::new(writer_db.clone()).connection());
        glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);").await.unwrap();
        glue.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob');").await.unwrap();
    }
    writer_db.flush().await.expect("flush");

    // Read replica over the SAME store.
    let reader = Arc::new(
        DbReader::builder("bluedb-sql-ha", store.clone())
            .build()
            .await
            .expect("open reader"),
    );
    let replica = Database::reader(reader);
    assert!(!replica.is_writer());
    let mut rglue = Glue::new(replica.connection());

    // SELECT through the replica returns the writer's committed data.
    let payload = rglue
        .execute("SELECT id, name FROM t ORDER BY id;")
        .await
        .expect("select on replica")
        .pop()
        .unwrap();
    let rows = match payload {
        Payload::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    };
    assert_eq!(
        rows,
        vec![
            vec![Value::I64(1), Value::Str("alice".to_owned())],
            vec![Value::I64(2), Value::Str("bob".to_owned())],
        ]
    );

    // A replica is read-only: both an autocommit write and an explicit BEGIN fail.
    assert!(
        rglue.execute("INSERT INTO t VALUES (3, 'carol');").await.is_err(),
        "INSERT on a read replica must fail"
    );
    assert!(
        rglue.execute("BEGIN;").await.is_err(),
        "BEGIN on a read replica must fail (no writer to snapshot)"
    );
}
