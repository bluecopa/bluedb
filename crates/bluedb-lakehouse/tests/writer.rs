//! End-to-end writer tests: self-authored snapshots are read back by iceberg's
//! own reader (the spec arbiter) — append, full CRUD via equality deletes, and
//! the CDC watermark in the snapshot summary.

use std::collections::BTreeMap;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use bluedb_lakehouse::writer::LakehouseWriter;
use futures::TryStreamExt;
use gluesql_core::data::Key;
use gluesql_core::data::Value;
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

/// Read the mirror back as a sorted map id -> body.
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
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let bodies = batch
            .column_by_name("body")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            out.insert(ids.value(i), bodies.value(i).to_string());
        }
    }
    out
}

#[tokio::test]
async fn write_then_read_back_rows() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut w = LakehouseWriter::open_local(root, "main", "docs", docs_schema(), 1)
        .await
        .unwrap();

    w.upsert(&[row(1, "a"), row(2, "b")]).await.unwrap();
    w.commit_snapshot(2).await.unwrap();

    let rows = read_back(&w).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.get(&1).map(String::as_str), Some("a"));
    assert_eq!(rows.get(&2).map(String::as_str), Some("b"));
}

#[tokio::test]
async fn full_crud_via_equality_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut w = LakehouseWriter::open_local(root, "main", "docs", docs_schema(), 1)
        .await
        .unwrap();

    // Snapshot 1: insert id=1,2.
    w.upsert(&[row(1, "a"), row(2, "b")]).await.unwrap();
    w.commit_snapshot(2).await.unwrap();

    // Snapshot 2: update id=1 -> "c", delete id=2.
    w.upsert(&[row(1, "c")]).await.unwrap();
    w.delete(&[Key::I64(2)]).await.unwrap();
    w.commit_snapshot(4).await.unwrap();

    let rows = read_back(&w).await;
    assert_eq!(rows.len(), 1, "id=2 deleted, id=1 has one (new) version");
    assert_eq!(rows.get(&1).map(String::as_str), Some("c"));
    assert_eq!(rows.get(&2), None);
}

#[tokio::test]
async fn watermark_round_trips_in_snapshot_summary() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut w = LakehouseWriter::open_local(root, "main", "docs", docs_schema(), 1)
        .await
        .unwrap();

    assert_eq!(w.current_watermark(), None);
    w.upsert(&[row(1, "a")]).await.unwrap();
    w.commit_snapshot(42).await.unwrap();
    assert_eq!(w.current_watermark(), Some(42));

    // Reopen from object storage: the watermark survives a restart.
    let w2 = LakehouseWriter::open_local(root, "main", "docs", docs_schema(), 1)
        .await
        .unwrap();
    assert_eq!(w2.current_watermark(), Some(42));
    assert_eq!(read_back(&w2).await.get(&1).map(String::as_str), Some("a"));
}

/// `id BIGINT PK, body TEXT, extra INT` — `docs_schema` plus an added column.
fn docs_schema_plus_extra() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_identifier_field_ids(vec![1])
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "body", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "extra", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap()
}

fn field_names(writer: &LakehouseWriter) -> Vec<String> {
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

#[tokio::test]
async fn reopen_with_added_column_evolves_schema() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();

    // Create + seal a row under [id, body].
    let mut w = LakehouseWriter::open_local(root, "main", "docs", docs_schema(), 1)
        .await
        .unwrap();
    w.upsert(&[row(1, "a")]).await.unwrap();
    w.commit_snapshot(1).await.unwrap();
    assert_eq!(field_names(&w), vec!["id", "body"]);
    drop(w);

    // Reopen with [id, body, extra] — `extra` added. open() reconciles.
    let w2 = LakehouseWriter::open_local(root, "main", "docs", docs_schema_plus_extra(), 1)
        .await
        .unwrap();
    assert_eq!(field_names(&w2), vec!["id", "body", "extra"]);
    // Old row still reads back (extra is null for it).
    assert_eq!(read_back(&w2).await.get(&1).map(String::as_str), Some("a"));
}

#[tokio::test]
async fn reopen_with_unchanged_schema_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();

    let mut w = LakehouseWriter::open_local(root, "main", "docs", docs_schema(), 1)
        .await
        .unwrap();
    w.upsert(&[row(1, "a")]).await.unwrap();
    w.commit_snapshot(7).await.unwrap();
    drop(w);

    // Reopen with the identical schema: no schema commit, version unchanged.
    let w2 = LakehouseWriter::open_local(root, "main", "docs", docs_schema(), 1)
        .await
        .unwrap();
    assert_eq!(field_names(&w2), vec!["id", "body"]);
    assert_eq!(w2.current_watermark(), Some(7));
}
