//! End-to-end read-your-writes through SQL: an `@@` query in a fresh
//! connection sees a row committed moments earlier on an observed connection,
//! with no explicit flush (Spec B §5). A delete is reflected immediately too.

use std::sync::Arc;

use bluedb_engine::FtsEngine;
use bluedb_sql::Database;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

#[tokio::test]
async fn read_your_writes_through_sql() {
    let db = Arc::new(Db::open("ryw", Arc::new(InMemory::new())).await.unwrap());
    let database = Database::new(db);
    let fts = FtsEngine::new();

    {
        let mut g = Glue::new(database.connection_serialized());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
    }

    // Declare a fulltext index on docs.body (pk = id), english analyzer.
    fts.create_fulltext_index(&database.connection(), "docs", "body", "id", "english")
        .await
        .unwrap();

    // Insert WITH the observer installed → live segment maintained on commit.
    {
        let mut g = Glue::new(database.connection().with_commit_observer(fts.clone()));
        g.execute(
            "INSERT INTO docs (id, body) VALUES (1, 'quarterly invoice overdue'), (2, 'weather sunny skies');",
        )
        .await
        .unwrap();
    }

    // @@ query in a fresh connection sees the just-committed row 1 (RYW via the
    // shared FtsEngine).
    let mut g = Glue::new(database.connection_serialized());
    let sql =
        "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')";
    let out = fts.execute_fts(&mut g, sql, &[]).await.unwrap();
    match out.into_iter().next().unwrap() {
        Payload::Select { rows, .. } => {
            let ids: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
            assert_eq!(
                ids,
                vec![Value::I64(1)],
                "only the matching row, fetched by pk IN (...)"
            );
        }
        other => panic!("{other:?}"),
    }

    // A delete is reflected immediately too.
    {
        let mut g = Glue::new(database.connection().with_commit_observer(fts.clone()));
        g.execute("DELETE FROM docs WHERE id = 1;").await.unwrap();
    }
    let mut g2 = Glue::new(database.connection_serialized());
    let out2 = fts.execute_fts(&mut g2, sql, &[]).await.unwrap();
    match out2.into_iter().next().unwrap() {
        Payload::Select { rows, .. } => assert!(rows.is_empty(), "deleted row must not match"),
        other => panic!("{other:?}"),
    }
}
