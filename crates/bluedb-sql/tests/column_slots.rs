//! `SlateDbStorage::column_slots` exposes the column-catalog slot mapping that
//! the lakehouse mirror turns into stable Iceberg field-ids (`field_id = slot
//! + 1`). A never-altered table has no catalog persisted ⇒ `None` (identity).

use std::sync::Arc;

use bluedb_sql::{Database, SlateDbStorage};
use gluesql_core::prelude::Glue;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn new_glue(name: &str) -> Glue<SlateDbStorage> {
    let db = Arc::new(Db::open(name, Arc::new(InMemory::new())).await.unwrap());
    Glue::new(Database::new(db).connection_serialized())
}

#[tokio::test]
async fn never_altered_table_has_no_slots() {
    let mut g = new_glue("colslots-identity").await;
    g.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)")
        .await
        .unwrap();
    // Identity (no catalog persisted) ⇒ None.
    assert_eq!(g.storage.column_slots("t").await.unwrap(), None);
}

#[tokio::test]
async fn dropped_column_leaves_a_gap_in_slots() {
    let mut g = new_glue("colslots-drop").await;
    g.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
        .await
        .unwrap();
    g.execute("ALTER TABLE t DROP COLUMN a").await.unwrap();
    // slots were [0,1,2]; dropping logical idx 1 (`a`) leaves [0,2].
    assert_eq!(g.storage.column_slots("t").await.unwrap(), Some(vec![0, 2]));
}

#[tokio::test]
async fn added_column_appends_a_new_slot() {
    let mut g = new_glue("colslots-add").await;
    g.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)")
        .await
        .unwrap();
    g.execute("ALTER TABLE t ADD COLUMN c TEXT").await.unwrap();
    // [0,1] then append slot 2 ⇒ [0,1,2].
    assert_eq!(g.storage.column_slots("t").await.unwrap(), Some(vec![0, 1, 2]));
}
