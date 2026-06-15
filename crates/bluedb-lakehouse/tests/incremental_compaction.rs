//! Incremental (minor) bin-packed compaction: small data files are merged
//! without rewriting the whole table, and merge-on-read is preserved (an update
//! and a delete materialized into the cohort must NOT be resurrected by the
//! sequence bump). The major (whole-table) compaction still reclaims the
//! accumulated equality-delete files.

use std::collections::BTreeMap;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use bluedb_lakehouse::writer::LakehouseWriter;
use futures::TryStreamExt;
use gluesql_core::data::{Key, Value};
use gluesql_core::store::DataRow;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

/// `id BIGINT PRIMARY KEY, body TEXT` — field-ids 1 and 2, identifier = id.
fn docs_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_identifier_field_ids(vec![1])
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "body", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap()
}

fn row(id: i64, body: &str) -> (Key, DataRow) {
    (
        Key::I64(id),
        DataRow::Vec(vec![Value::I64(id), Value::Str(body.to_string())]),
    )
}

/// Read the mirror back as a sorted map id -> body (equality deletes applied).
async fn read_back(writer: &LakehouseWriter) -> BTreeMap<i64, String> {
    let table = writer.to_table().unwrap();
    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut out = BTreeMap::new();
    for batch in batches {
        let ids = batch.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let bodies = batch.column_by_name("body").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            out.insert(ids.value(i), bodies.value(i).to_string());
        }
    }
    out
}

#[tokio::test]
async fn incremental_compaction_preserves_merge_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut w = LakehouseWriter::open_local(root, "main", "docs", docs_schema(), 1)
        .await
        .unwrap();

    // Several small seals: inserts, an update (key 1), a delete (key 2).
    w.upsert(&[row(1, "a"), row(2, "b")]).await.unwrap();
    w.commit_snapshot(1).await.unwrap();
    w.upsert(&[row(3, "c")]).await.unwrap();
    w.commit_snapshot(2).await.unwrap();
    w.upsert(&[row(1, "A")]).await.unwrap(); // update key 1
    w.commit_snapshot(3).await.unwrap();
    w.delete(&[Key::I64(2)]).await.unwrap(); // delete key 2
    w.commit_snapshot(4).await.unwrap();

    let before = read_back(&w).await; // {1:"A", 3:"c"}  (2 deleted)
    assert_eq!(before.get(&1).map(String::as_str), Some("A"));
    assert_eq!(before.get(&3).map(String::as_str), Some("c"));
    assert_eq!(before.get(&2), None);
    let files_before = w.data_file_count().await.unwrap();
    assert!(files_before >= 2, "need multiple small files to compact");

    // Target huge so every small file is a candidate (force a real bin-pack).
    w.compact_incremental(1 << 30, 4).await.unwrap();

    let after = read_back(&w).await;
    assert_eq!(after, before, "merge-on-read result must be unchanged by compaction");
    assert!(
        w.data_file_count().await.unwrap() < files_before,
        "incremental compaction should reduce the data-file count"
    );
}

#[tokio::test]
async fn major_after_incremental_reclaims_delete_files() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut w = LakehouseWriter::open_local(root, "main", "docs", docs_schema(), 1)
        .await
        .unwrap();
    w.upsert(&[row(1, "a")]).await.unwrap();
    w.commit_snapshot(1).await.unwrap();
    w.upsert(&[row(1, "A")]).await.unwrap();
    w.commit_snapshot(2).await.unwrap();
    w.upsert(&[row(2, "b")]).await.unwrap();
    w.commit_snapshot(3).await.unwrap();

    w.compact_incremental(1 << 30, 3).await.unwrap();
    // Incremental keeps delete files (a delete may still target a survivor).
    assert!(w.delete_file_count().await.unwrap() > 0, "minor keeps deletes");

    w.compact(3).await.unwrap(); // major: whole-table rewrite
    assert_eq!(
        w.delete_file_count().await.unwrap(),
        0,
        "major compaction reclaims delete files"
    );
    let after = read_back(&w).await;
    assert_eq!(after.get(&1).map(String::as_str), Some("A"));
    assert_eq!(after.get(&2).map(String::as_str), Some("b"));
}
