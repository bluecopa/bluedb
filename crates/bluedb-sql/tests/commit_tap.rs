//! The commit-observer seam: an installed [`CommitObserver`] sees the row
//! changes a connection committed, reported only after the durable write.

use std::sync::{Arc, Mutex};

use bluedb_sql::{CommitObserver, Database, RowChange};
use gluesql_core::data::Key;
use gluesql_core::prelude::Glue;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

#[derive(Default)]
struct Recorder {
    seen: Mutex<Vec<RowChange>>,
}

impl CommitObserver for Recorder {
    fn on_commit(&self, changes: &[RowChange]) {
        self.seen.lock().unwrap().extend_from_slice(changes);
    }
}

#[tokio::test]
async fn observer_sees_committed_inserts_and_deletes() {
    let db = Arc::new(Db::open("tap", Arc::new(InMemory::new())).await.unwrap());
    let database = Database::new(db);
    let rec = Arc::new(Recorder::default());

    {
        let mut g = Glue::new(database.connection_serialized());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
    }
    {
        let mut g = Glue::new(database.connection().with_commit_observer(rec.clone()));
        g.execute("INSERT INTO docs (id, body) VALUES (1, 'hello'), (2, 'world');")
            .await
            .unwrap();
    }
    {
        let mut g = Glue::new(database.connection().with_commit_observer(rec.clone()));
        g.execute("DELETE FROM docs WHERE id = 2;").await.unwrap();
    }

    let seen = rec.seen.lock().unwrap();
    // two inserts (row Some) for docs, then one delete (row None) for id=2.
    let inserts: Vec<_> = seen
        .iter()
        .filter(|c| c.table == "docs" && c.row.is_some())
        .collect();
    assert_eq!(inserts.len(), 2);
    assert!(seen
        .iter()
        .any(|c| c.table == "docs" && c.row.is_none() && c.key == Key::I64(2)));
}
