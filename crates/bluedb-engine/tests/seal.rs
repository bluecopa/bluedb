//! End-to-end durable seal: the live segment folds into a durable
//! `bluedb_fts::FtsIndex` split off the commit path, and the union searcher
//! (live ∪ durable, live authoritative for any pk it covers) keeps
//! read-your-writes holding across a seal for insert / update / delete
//! (Spec B §4.2/§5, B4 part 1).

use std::sync::Arc;

use bluedb_engine::FtsEngine;
use bluedb_sql::Database;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

/// Run an `@@` query through the engine and return the matching `id`s.
async fn query_ids(fts: &FtsEngine, database: &Database, sql: &str) -> Vec<i64> {
    let mut g = Glue::new(database.connection_serialized());
    let out = fts.execute_fts(&mut g, sql, &[]).await.unwrap();
    match out.into_iter().next().unwrap() {
        Payload::Select { rows, .. } => rows
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                other => panic!("expected I64 id, got {other:?}"),
            })
            .collect(),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn seal_folds_live_into_durable_and_union_keeps_ryw() {
    let db = Arc::new(Db::open("seal", Arc::new(InMemory::new())).await.unwrap());
    let database = Database::new(db);
    // Durable engine: backed by the SAME substrate as the SQL data.
    let fts = FtsEngine::new_durable(database.substrate());

    {
        let mut g = Glue::new(database.connection_serialized());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
    }
    fts.create_fulltext_index(&database.connection(), "docs", "body", "id", "english")
        .await
        .unwrap();

    let sql =
        "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')";

    // Insert rows 1, 2 on an observed connection → live segment.
    {
        let mut g = Glue::new(database.connection().with_commit_observer(fts.clone()));
        g.execute(
            "INSERT INTO docs (id, body) VALUES (1, 'quarterly invoice overdue'), (2, 'weather sunny skies');",
        )
        .await
        .unwrap();
    }

    // Pre-seal: union = live; row 1 matches.
    assert_eq!(query_ids(&fts, &database, sql).await, vec![1], "pre-seal: live");

    // Seal: fold the live segment into a durable split, reset the live segment.
    fts.seal().await.unwrap();

    // Post-seal: live is empty, so this hit comes from the DURABLE tier.
    assert_eq!(query_ids(&fts, &database, sql).await, vec![1], "post-seal: durable");

    // Update row 1 so it no longer matches, insert row 3 that does → live segment.
    {
        let mut g = Glue::new(database.connection().with_commit_observer(fts.clone()));
        g.execute("UPDATE docs SET body = 'sunny skies forecast' WHERE id = 1;")
            .await
            .unwrap();
        g.execute("INSERT INTO docs (id, body) VALUES (3, 'overdue invoice reminder');")
            .await
            .unwrap();
    }

    // Union: row 3 (live) matches; row 1's stale DURABLE hit is masked because
    // the live segment now COVERS pk 1 (it was re-indexed with non-matching text).
    assert_eq!(
        query_ids(&fts, &database, sql).await,
        vec![3],
        "live covers pk 1 → stale durable hit masked; only the live match remains"
    );

    // Delete row 3 → live tombstone; @@ empty for row 3 immediately.
    {
        let mut g = Glue::new(database.connection().with_commit_observer(fts.clone()));
        g.execute("DELETE FROM docs WHERE id = 3;").await.unwrap();
    }
    assert!(
        query_ids(&fts, &database, sql).await.is_empty(),
        "row 3 deleted (live tombstone) and row 1 still masked → no matches"
    );

    // Seal again: the durable tier now learns row 1's new (non-matching) text and
    // row 3's tombstone. Live is empty afterwards, so the query stays empty —
    // proving the durable tombstone + durable update survived the fold.
    fts.seal().await.unwrap();
    assert!(
        query_ids(&fts, &database, sql).await.is_empty(),
        "post-second-seal: durable tombstone for 3 + durable update of 1 → still empty"
    );
}
