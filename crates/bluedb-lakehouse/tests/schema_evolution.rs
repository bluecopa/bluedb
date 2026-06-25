//! Schema-evolution reconciliation: after `ALTER TABLE` ADD/DROP/RENAME column
//! on a mirrored table, the Iceberg mirror reflects the change on the next seal.
//! Each case is read back through iceberg-rust's own reader (the spec arbiter).
//!
//! The DROP case is the critical one: it asserts the surviving column's values
//! are *not* shifted from the dropped column — proving the field-id reconciliation
//! keeps rows aligned (the bug before this feature).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use bluedb_lakehouse::LakehouseEngine;
use bluedb_sql::{CdcConfig, Database};
use futures::TryStreamExt;
use gluesql_core::prelude::Glue;
use iceberg::io::FileIO;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn make_db(name: &str) -> Database {
    Database::new(Arc::new(
        Db::open(name, Arc::new(InMemory::new())).await.unwrap(),
    ))
}

async fn engine(root: &str, db: Database, cdc: CdcConfig) -> LakehouseEngine {
    LakehouseEngine::reopen(FileIO::new_with_fs(), root, "_", db, cdc)
        .await
        .unwrap()
}

async fn exec(glue: &mut Glue<bluedb_sql::SlateDbStorage>, sql: &str) {
    let prepared = bluedb_sql::prepare_composite_pk(&mut glue.storage, sql, &[])
        .await
        .unwrap();
    glue.execute(&prepared).await.unwrap();
}

/// The current Iceberg column names of the mirror (read back from metadata).
async fn mirror_field_names(eng: &LakehouseEngine, table: &str) -> Vec<String> {
    let schema = eng.fetch_schema(table).await.unwrap();
    let writer = eng.writer_for(table, &schema, &[]).await.unwrap();
    writer
        .to_table()
        .unwrap()
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect()
}

/// Scan the mirror's current rows back as RecordBatches (equality deletes applied).
async fn read_batches(eng: &LakehouseEngine, table: &str) -> Vec<RecordBatch> {
    let schema = eng.fetch_schema(table).await.unwrap();
    let writer = eng.writer_for(table, &schema, &[]).await.unwrap();
    writer
        .to_table()
        .unwrap()
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

fn int_col<'a>(b: &'a RecordBatch, name: &str) -> &'a Int64Array {
    b.column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
}

fn str_col<'a>(b: &'a RecordBatch, name: &str) -> &'a StringArray {
    b.column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}

#[tokio::test]
async fn add_column_appears_in_mirror() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db = make_db("schemaevo-add").await;
    let cdc = CdcConfig::default();
    let eng = engine(root, db.clone(), cdc.clone()).await;
    eng.enable_table("t").await.unwrap();

    {
        let mut g = Glue::new(db.connection_serialized());
        exec(&mut g, "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)").await;
    }
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        exec(&mut g, "INSERT INTO t (id, a) VALUES (1, 'x')").await;
    }
    eng.seal().await.unwrap();

    // Evolve: add column `c`, write a row carrying it, seal again.
    {
        let mut g = Glue::new(db.connection_serialized());
        exec(&mut g, "ALTER TABLE t ADD COLUMN c INTEGER").await;
    }
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        exec(&mut g, "INSERT INTO t (id, a, c) VALUES (2, 'y', 7)").await;
    }
    eng.seal().await.unwrap();

    assert_eq!(mirror_field_names(&eng, "t").await, vec!["id", "a", "c"]);

    // id=2 carries c=7; id=1 (written before the column existed) reads c = NULL.
    let mut c_by_id: BTreeMap<i64, Option<i64>> = BTreeMap::new();
    for b in read_batches(&eng, "t").await {
        let ids = int_col(&b, "id");
        let cs = int_col(&b, "c");
        for i in 0..b.num_rows() {
            c_by_id.insert(ids.value(i), (!cs.is_null(i)).then(|| cs.value(i)));
        }
    }
    assert_eq!(
        c_by_id.get(&1),
        Some(&None),
        "old row has NULL for added column"
    );
    assert_eq!(c_by_id.get(&2), Some(&Some(7)), "new row carries c=7");
}

#[tokio::test]
async fn drop_column_removed_without_misalignment() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db = make_db("schemaevo-drop").await;
    let cdc = CdcConfig::default();
    let eng = engine(root, db.clone(), cdc.clone()).await;
    eng.enable_table("t").await.unwrap();

    {
        let mut g = Glue::new(db.connection_serialized());
        exec(
            &mut g,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)",
        )
        .await;
    }
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        exec(
            &mut g,
            "INSERT INTO t (id, a, b) VALUES (1, 'aval', 'keep')",
        )
        .await;
    }
    eng.seal().await.unwrap();

    // Drop the middle column `a`, write another row, seal again.
    {
        let mut g = Glue::new(db.connection_serialized());
        exec(&mut g, "ALTER TABLE t DROP COLUMN a").await;
    }
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        exec(&mut g, "INSERT INTO t (id, b) VALUES (2, 'kept2')").await;
    }
    eng.seal().await.unwrap();

    assert_eq!(mirror_field_names(&eng, "t").await, vec!["id", "b"]);

    // The critical assertion: id=1's `b` is still 'keep' (NOT 'aval', the dropped
    // column's value) — i.e. the old data file is projected through the evolved
    // schema by field-id, with no positional shift.
    let mut b_by_id: BTreeMap<i64, String> = BTreeMap::new();
    for batch in read_batches(&eng, "t").await {
        let ids = int_col(&batch, "id");
        let bs = str_col(&batch, "b");
        for i in 0..batch.num_rows() {
            b_by_id.insert(ids.value(i), bs.value(i).to_string());
        }
    }
    assert_eq!(b_by_id.get(&1).map(String::as_str), Some("keep"));
    assert_eq!(b_by_id.get(&2).map(String::as_str), Some("kept2"));
}

#[tokio::test]
async fn rename_column_uses_new_name_in_mirror() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db = make_db("schemaevo-rename").await;
    let cdc = CdcConfig::default();
    let eng = engine(root, db.clone(), cdc.clone()).await;
    eng.enable_table("t").await.unwrap();

    {
        let mut g = Glue::new(db.connection_serialized());
        exec(&mut g, "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)").await;
    }
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        exec(&mut g, "INSERT INTO t (id, a) VALUES (1, 'x')").await;
    }
    eng.seal().await.unwrap();

    {
        let mut g = Glue::new(db.connection_serialized());
        exec(&mut g, "ALTER TABLE t RENAME COLUMN a TO alpha").await;
    }
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        exec(&mut g, "INSERT INTO t (id, alpha) VALUES (2, 'y')").await;
    }
    eng.seal().await.unwrap();

    assert_eq!(mirror_field_names(&eng, "t").await, vec!["id", "alpha"]);

    // Field-id 2 was preserved through the rename, so both rows read under `alpha`.
    let mut by_id: BTreeMap<i64, String> = BTreeMap::new();
    for batch in read_batches(&eng, "t").await {
        let ids = int_col(&batch, "id");
        let al = str_col(&batch, "alpha");
        for i in 0..batch.num_rows() {
            by_id.insert(ids.value(i), al.value(i).to_string());
        }
    }
    assert_eq!(by_id.get(&1).map(String::as_str), Some("x"));
    assert_eq!(by_id.get(&2).map(String::as_str), Some("y"));
}

/// Regression for UAT-SQL-009: a composite-PK table read through the
/// lakehouse front door after ADD + DROP COLUMN. bluedb derives Iceberg
/// field-ids from the column-catalog slots (`slot + 1`); DROP frees a slot,
/// and the composite-PK surrogate's slot-derived id can then match a
/// *different* persisted field when `reconcile_schema` looked fields up by id.
/// That mistyped the non-nullable `__bluedb_pk` (Binary) as String, so the
/// reloaded schema declared a non-nullable column that decoded to null, and
/// `current_record_batch` errored "Column '__bluedb_pk' is declared as
/// non-nullable but contains null values". The fix matches persisted fields by
/// NAME (the stable identity), not id.
///
/// This test does NOT mirror/seal the table: the bug fires on the row-store
/// read path (`current_record_batch`), the first call of which persists
/// `v0.metadata.json`, and the second of which reloads + evolves it (where the
/// mis-typing happened). So we call it twice.
#[tokio::test]
async fn composite_pk_after_drop_column_reads_via_lakehouse() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db = make_db("schemaevo-cpk-drop").await;
    let cdc = CdcConfig::default();
    let eng = engine(root, db.clone(), cdc).await;

    {
        let mut g = Glue::new(db.connection_serialized());
        // Composite PK (org_id, user_id) → hidden __bluedb_pk surrogate.
        exec(&mut g, "CREATE TABLE m (org_id INTEGER, user_id INTEGER, role TEXT, age INTEGER, PRIMARY KEY (org_id, user_id))").await;
        exec(
            &mut g,
            "INSERT INTO m (org_id, user_id, role, age) VALUES (1, 7, 'owner', 40)",
        )
        .await;
        // ADD then DROP frees a slot, exercising the id-collision path.
        exec(&mut g, "ALTER TABLE m ADD COLUMN nickname TEXT").await;
        exec(&mut g, "ALTER TABLE m RENAME COLUMN nickname TO handle").await;
        exec(&mut g, "ALTER TABLE m DROP COLUMN age").await;
    }

    // First read persists v0.metadata.json; second read reloads + evolves it.
    // Before the fix, the second (and every later) read errored.
    for label in ["first", "second"] {
        let batch = eng
            .current_record_batch("m")
            .await
            .unwrap_or_else(|e| panic!("current_record_batch ({label}): {e}"))
            .expect("table exists");
        assert_eq!(batch.num_rows(), 1, "one row ({label})");
        // The bug errored before the row could be materialized at all; reaching
        // here with the role column present is the regression assertion.
        assert!(
            batch.column_by_name("role").is_some(),
            "role column ({label})"
        );
    }

    // The surrogate stays Binary (LargeBinary in Arrow) across the evolve.
    let schema = eng.fetch_schema("m").await.unwrap();
    let arrow = eng
        .writer_for("m", &schema, &[])
        .await
        .unwrap()
        .arrow_schema()
        .unwrap();
    let pk_field = arrow
        .field_with_name("__bluedb_pk")
        .expect("surrogate column present");
    use arrow_schema::DataType;
    assert!(
        matches!(
            pk_field.data_type(),
            DataType::LargeBinary | DataType::Binary
        ),
        "surrogate stays binary, got {:?}",
        pk_field.data_type()
    );
}
