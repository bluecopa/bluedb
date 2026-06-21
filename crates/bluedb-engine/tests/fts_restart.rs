//! Simulated restart of the durable FTS tier: an engine declares an index,
//! indexes + seals it into the durable splits, then the engine is dropped (its
//! in-memory live segment + def map gone). A second engine `reopen`ed over the
//! SAME substrate rebuilds the index defs from the durable registry and
//! reconnects each durable `FtsIndex` to its existing splits (via the stable
//! `fts/{table}/{column}` index_id) — so an `@@` query served by the reopened
//! engine still finds the sealed row (Spec B B4-3).

use std::sync::Arc;

use bluedb_engine::FtsEngine;
use bluedb_sql::Database;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

/// Run an `@@` query through `fts` on a fresh serialized connection over
/// `database` and return the matching `id`s.
async fn query_ids(fts: &FtsEngine, database: &Database, sql: &str) -> Vec<i64> {
    let mut g = Glue::new(database.connection_serialized());
    let out = fts.execute_fts(&mut g, sql, &[], None).await.unwrap();
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
async fn reopen_rebuilds_defs_and_reconnects_durable_splits() {
    // One SlateDB `Db` over InMemory is the persistent substrate; only the
    // FtsEngine's in-memory state is dropped between "boots".
    let db = Arc::new(Db::open("fts-restart", Arc::new(InMemory::new())).await.unwrap());
    let database = Database::new(db);

    let sql =
        "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')";

    // --- boot 1: declare the index, index two rows, seal to durable, drop ---
    {
        let engine1 = FtsEngine::new_durable(database.substrate());

        {
            let mut g = Glue::new(database.connection_serialized());
            g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
                .await
                .unwrap();
        }
        // _auto resolves pk=id from the schema and persists the def to the registry.
        engine1
            .create_fulltext_index_auto(&database.connection(), "docs", "body", "english")
            .await
            .unwrap();

        {
            let mut g = Glue::new(database.connection().with_commit_observer(engine1.clone()));
            g.execute(
                "INSERT INTO docs (id, body) VALUES (1, 'quarterly invoice overdue'), (2, 'weather sunny skies');",
            )
            .await
            .unwrap();
        }

        // Fold the live segment into the durable splits — all data now durable.
        engine1.seal().await.unwrap();

        // Sanity: engine1 still serves the durable hit.
        assert_eq!(query_ids(&engine1, &database, sql).await, vec![1], "boot 1: durable hit");

        // Drop the engine: the in-memory live segment + def map are gone. The
        // durable splits + registry survive in the substrate.
        drop(engine1);
    }

    // --- boot 2: reopen over the SAME substrate; defs + splits reconnect ---
    let engine2 = FtsEngine::reopen(database.substrate()).await.unwrap();

    assert_eq!(
        query_ids(&engine2, &database, sql).await,
        vec![1],
        "reopen: the registry rebuilt the def and reconnected the durable splits"
    );
}
