//! Cross-engine compatibility: **DuckDB's** Iceberg reader (independent of
//! iceberg-rust) reads our self-authored tables, including equality-delete
//! merge-on-read. This is the real product check — a warehouse, not our own
//! writer's ecosystem, reading the mirror.
//!
//! `#[ignore]` — needs `python3` + the DuckDB `iceberg` extension (a one-time
//! network install). Run with:
//!   cargo test -p bluedb-lakehouse --test duckdb_compat -- --ignored --nocapture

use std::process::Command;

use bluedb_lakehouse::writer::LakehouseWriter;
use gluesql_core::data::Key;
use gluesql_core::data::Value;
use gluesql_core::store::DataRow;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

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

/// Run a DuckDB python snippet, returning stdout (panics on error with stderr).
fn duckdb(script: &str) -> String {
    let out = Command::new("python3")
        .arg("-c")
        .arg(script)
        .output()
        .expect("spawn python3");
    if !out.status.success() {
        panic!(
            "duckdb script failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[tokio::test]
#[ignore = "needs python3 + duckdb iceberg extension (network install)"]
async fn duckdb_reads_self_authored_table_with_equality_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut w = LakehouseWriter::open_local(root, "default", "docs", docs_schema(), 1)
        .await
        .unwrap();

    // Snapshot 1: insert id=1,2,3.
    w.upsert(&[row(1, "a"), row(2, "b"), row(3, "c")]).await.unwrap();
    w.commit_snapshot(3).await.unwrap();
    // Snapshot 2: update id=1 -> "x", delete id=2 (equality deletes).
    w.upsert(&[row(1, "x")]).await.unwrap();
    w.delete(&[Key::I64(2)]).await.unwrap();
    w.commit_snapshot(5).await.unwrap();

    let table_dir = format!("{root}/default/docs");
    let version = std::fs::read_to_string(format!("{table_dir}/metadata/version-hint.text"))
        .unwrap()
        .trim()
        .to_string();
    let metadata = format!("{table_dir}/metadata/v{version}.metadata.json");

    // DuckDB reads the merged-on-read state via its own Iceberg reader.
    let script = format!(
        r#"
import duckdb, json
con = duckdb.connect()
con.execute("INSTALL iceberg"); con.execute("LOAD iceberg")
rows = con.execute(
    "SELECT id, body FROM iceberg_scan('{metadata}') ORDER BY id"
).fetchall()
print(json.dumps(rows))
"#
    );
    let stdout = duckdb(&script);
    let last = stdout.lines().last().unwrap_or("");
    let rows: Vec<(i64, String)> = serde_json::from_str::<Vec<(i64, String)>>(last)
        .unwrap_or_else(|e| panic!("parse duckdb rows from {last:?}: {e}"));

    // Expected final state: id=1 updated to "x", id=2 deleted, id=3 unchanged.
    assert_eq!(
        rows,
        vec![(1, "x".to_string()), (3, "c".to_string())],
        "DuckDB must see the merged-on-read state (equality deletes applied)"
    );
}
