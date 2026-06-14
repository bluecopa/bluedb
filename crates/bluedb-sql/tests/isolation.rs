//! Cross-connection isolation tests for [`bluedb_sql::Database`].
//!
//! A `Database` vends multiple `Glue` connections over one SlateDB `Db`. These
//! tests prove the guarantees:
//!   1. **Snapshot isolation** — a connection's in-flight transaction reads a
//!      stable point-in-time view; another connection's writes (uncommitted, or
//!      even committed after BEGIN) are invisible to it.
//!   2. **Serializable explicit write transactions** — two concurrent
//!      read-modify-write `BEGIN..COMMIT` blocks serialize on the shared write
//!      lease, so neither update is lost.
//!
//! Autocommit statements never take the lease: reads run lock-free against an
//! MVCC snapshot, and writes commit concurrently (auto-increment keys stay
//! collision-free via a shared counter).

use std::sync::Arc;

use bluedb_sql::{Database, SlateDbStorage};
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn open_database(name: &str) -> Database {
    let object_store = Arc::new(InMemory::new());
    let db = Arc::new(Db::open(name, object_store).await.expect("open slatedb"));
    Database::new(db)
}

async fn scalar_i64(glue: &mut Glue<SlateDbStorage>, sql: &str) -> i64 {
    match glue.execute(sql).await.expect("execute").pop().unwrap() {
        Payload::Select { rows, .. } => match &rows[0][0] {
            Value::I64(n) => *n,
            other => panic!("expected I64, got {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    }
}

async fn row_count(glue: &mut Glue<SlateDbStorage>, sql: &str) -> usize {
    match glue.execute(sql).await.expect("execute").pop().unwrap() {
        Payload::Select { rows, .. } => rows.len(),
        other => panic!("expected Select, got {other:?}"),
    }
}

#[tokio::test]
async fn uncommitted_writes_are_invisible_to_other_connections() {
    let database = open_database("iso-visibility").await;
    let mut a = Glue::new(database.connection());
    let mut b = Glue::new(database.connection());

    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);").await.unwrap();
    a.execute("INSERT INTO t VALUES (1, 10);").await.unwrap();

    // A opens a transaction and mutates — buffered in A's overlay, not flushed.
    a.execute("BEGIN;").await.unwrap();
    a.execute("UPDATE t SET v = 99 WHERE id = 1;").await.unwrap();

    // B (a separate connection) must still see the committed value, not A's
    // uncommitted change.
    assert_eq!(scalar_i64(&mut b, "SELECT v FROM t WHERE id = 1;").await, 10);
    // A sees its own write (read-your-own-writes).
    assert_eq!(scalar_i64(&mut a, "SELECT v FROM t WHERE id = 1;").await, 99);

    // After A commits, B sees the new value.
    a.execute("COMMIT;").await.unwrap();
    assert_eq!(scalar_i64(&mut b, "SELECT v FROM t WHERE id = 1;").await, 99);
}

#[tokio::test]
async fn a_transaction_reads_a_stable_snapshot() {
    let database = open_database("iso-snapshot").await;
    let mut a = Glue::new(database.connection());

    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY);").await.unwrap();
    a.execute("INSERT INTO t VALUES (1);").await.unwrap();

    a.execute("BEGIN;").await.unwrap();
    assert_eq!(row_count(&mut a, "SELECT id FROM t;").await, 1);

    // Another connection commits an insert while A's transaction is open.
    // Autocommit writes don't take A's lease, so this commits immediately.
    let mut b = Glue::new(database.connection());
    b.execute("INSERT INTO t VALUES (2);").await.unwrap();

    // A's snapshot is stable: it still sees only the row that existed at BEGIN.
    assert_eq!(
        row_count(&mut a, "SELECT id FROM t;").await,
        1,
        "A's snapshot must not see B's concurrently-committed insert"
    );

    a.execute("COMMIT;").await.unwrap();
    // A fresh read (new snapshot) now sees both rows.
    assert_eq!(row_count(&mut a, "SELECT id FROM t;").await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_write_transactions_do_not_lose_updates() {
    let database = open_database("iso-lost-update").await;

    {
        let mut g = Glue::new(database.connection());
        g.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER);").await.unwrap();
        g.execute("INSERT INTO c VALUES (1, 0);").await.unwrap();
    }

    // Spawn N concurrent read-modify-write transactions, each incrementing n by
    // 1 inside a BEGIN..COMMIT block. The shared write lease serializes them, so
    // each reads the previous committer's value — none is lost.
    const N: i64 = 8;
    let mut handles = Vec::new();
    for _ in 0..N {
        let database = database.clone();
        handles.push(tokio::spawn(async move {
            let mut g = Glue::new(database.connection());
            g.execute("BEGIN;").await.unwrap();
            g.execute("UPDATE c SET n = n + 1 WHERE id = 1;").await.unwrap();
            g.execute("COMMIT;").await.unwrap();
        }));
    }
    for h in handles {
        h.await.expect("task");
    }

    let mut g = Glue::new(database.connection());
    let final_n = scalar_i64(&mut g, "SELECT n FROM c WHERE id = 1;").await;
    assert_eq!(final_n, N, "every increment landed: {N} serialized RMW transactions");
}
