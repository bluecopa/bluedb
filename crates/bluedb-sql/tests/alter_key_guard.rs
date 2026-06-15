//! `ALTER TABLE … DROP COLUMN` of a primary-key column (single or composite
//! component) is rejected — the key is the merge-on-read identity and the
//! clustering key (mirrors the UPDATE-of-key-column non-goal). Non-key columns
//! still drop.

use std::sync::Arc;

use bluedb_sql::{Database, SlateDbStorage};
use gluesql_core::prelude::{Glue, Payload};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn new_glue(name: &str) -> Glue<SlateDbStorage> {
    let db = Arc::new(Db::open(name, Arc::new(InMemory::new())).await.unwrap());
    Glue::new(Database::new(db).connection_serialized())
}

/// Apply the composite-PK rewrite, then execute — matching the production path
/// (composite `PRIMARY KEY (a, b)` only parses through the rewrite).
async fn exec(g: &mut Glue<SlateDbStorage>, sql: &str) -> Result<Payload, String> {
    let prepared = bluedb_sql::prepare_composite_pk(&mut g.storage, sql, &[])
        .await
        .map_err(|e| e.to_string())?;
    let mut payloads = g.execute(&prepared).await.map_err(|e| e.to_string())?;
    Ok(payloads.pop().unwrap())
}

#[tokio::test]
async fn cannot_drop_single_pk_column() {
    let mut g = new_glue("dropkey-single").await;
    exec(&mut g, "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)")
        .await
        .unwrap();
    let err = exec(&mut g, "ALTER TABLE t DROP COLUMN id").await.unwrap_err();
    assert!(
        err.to_lowercase().contains("primary key"),
        "unexpected error: {err}"
    );
    // A non-key column still drops fine.
    exec(&mut g, "ALTER TABLE t DROP COLUMN a").await.unwrap();
}

#[tokio::test]
async fn cannot_drop_composite_component_column() {
    let mut g = new_glue("dropkey-composite").await;
    exec(
        &mut g,
        "CREATE TABLE t (a INTEGER, b INTEGER, c TEXT, PRIMARY KEY (a, b))",
    )
    .await
    .unwrap();
    let err = exec(&mut g, "ALTER TABLE t DROP COLUMN a").await.unwrap_err();
    assert!(
        err.to_lowercase().contains("primary key"),
        "unexpected error: {err}"
    );
    // A non-key column still drops fine.
    exec(&mut g, "ALTER TABLE t DROP COLUMN c").await.unwrap();
}
