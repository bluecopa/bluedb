//! The durable CDC log that backs the lakehouse mirror: a global monotonic
//! sequence, change capture into the same `WriteBatch` as the data, and
//! scan/gc over the log.

use std::sync::Arc;

use bluedb_sql::Database;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

use bluedb_sql::CdcConfig;
use gluesql_core::prelude::Glue;

/// The default tenant the server-style connections (`connection_serialized`,
/// `connection_with_cdc`) write under.
const T: &str = "_";

#[tokio::test]
async fn cdc_seq_is_monotonic_and_starts_at_one() {
    let db = Arc::new(Db::open("cdc", Arc::new(InMemory::new())).await.unwrap());
    let database = Database::new(db);
    assert_eq!(database.next_cdc_seq(T).await.unwrap(), 1);
    assert_eq!(database.next_cdc_seq(T).await.unwrap(), 2);
}

/// CDC captures changes even with no commit observer installed — the mirror is
/// driven by `CdcConfig`, not the FTS observer seam.
#[tokio::test]
async fn changes_recorded_when_cdc_enabled_without_observer() {
    let db = Arc::new(Db::open("cdc2", Arc::new(InMemory::new())).await.unwrap());
    let database = Database::new(db);
    let cdc = CdcConfig::default();
    cdc.set_default(T, true);

    {
        let mut g = Glue::new(database.connection_serialized());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
    }
    {
        let mut g = Glue::new(database.connection_with_cdc(cdc.clone()));
        g.execute("INSERT INTO docs VALUES (1,'a');").await.unwrap();
    }

    let entries = database.scan_cdc(T, 0).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1.table, "docs");
    assert!(entries[0].1.row.is_some());
}

/// CDC entries are durable (survive in object storage) and ordered by a global
/// sequence; inserts/updates carry the new row, deletes carry `None`.
#[tokio::test]
async fn cdc_entries_are_durable_and_ordered() {
    let store = Arc::new(InMemory::new());
    let database = Database::new(Arc::new(Db::open("cdc3", store.clone()).await.unwrap()));
    let cdc = CdcConfig::default();
    cdc.set_default(T, true);
    {
        let mut g = Glue::new(database.connection_serialized());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
    }
    {
        let mut g = Glue::new(database.connection_with_cdc(cdc.clone()));
        g.execute("INSERT INTO docs VALUES (1,'a'),(2,'b');")
            .await
            .unwrap();
        g.execute("UPDATE docs SET body='c' WHERE id=1;")
            .await
            .unwrap();
        g.execute("DELETE FROM docs WHERE id=2;").await.unwrap();
    }
    let entries = database.scan_cdc(T, 0).await.unwrap();
    // 2 inserts + 1 update + 1 delete = 4 entries, seqs strictly increasing.
    assert_eq!(entries.len(), 4);
    let seqs: Vec<i64> = entries.iter().map(|(s, _)| *s).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]));
    assert!(entries.last().unwrap().1.row.is_none()); // delete last
}

/// `gc_cdc(through)` removes every entry with sequence `<= through`, leaving the
/// tail the seal loop hasn't published yet.
#[tokio::test]
async fn gc_cdc_removes_entries_through_watermark() {
    let database = Database::new(Arc::new(
        Db::open("cdc4", Arc::new(InMemory::new())).await.unwrap(),
    ));
    let cdc = CdcConfig::default();
    cdc.set_default(T, true);
    {
        let mut g = Glue::new(database.connection_serialized());
        g.execute("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
    }
    {
        let mut g = Glue::new(database.connection_with_cdc(cdc.clone()));
        g.execute("INSERT INTO t VALUES (1),(2),(3);").await.unwrap();
    }
    let all = database.scan_cdc(T, 0).await.unwrap();
    assert_eq!(all.len(), 3);
    let mid = all[1].0; // second seq
    database.gc_cdc(T, mid).await.unwrap();
    let remaining = database.scan_cdc(T, 0).await.unwrap();
    assert_eq!(remaining.len(), 1); // only seq > mid remains
    assert!(remaining[0].0 > mid);
}
