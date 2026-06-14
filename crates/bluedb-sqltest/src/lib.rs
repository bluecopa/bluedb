//! `bluedb-sqltest` — a [sqllogictest] backend that drives bluedb's GlueSQL
//! engine directly (embedded, no wire protocol) against an in-memory SlateDB.
//!
//! This is the **correctness / standard-SQL-conformance** harness: point it at
//! a directory of `.slt` files and it executes every statement and query through
//! `Glue::execute`, so we can measure how much of standard SQL the engine
//! actually covers and track that number as features land.
//!
//! [sqllogictest]: https://github.com/risinglightdb/sqllogictest-rs

use std::sync::Arc;

use async_trait::async_trait;
use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::config::Settings;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use std::time::Duration;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};

/// A GlueSQL engine error, surfaced to sqllogictest as the backend `Error`.
///
/// We keep the message as a `String` so the conformance runner can classify it
/// (e.g. "unsupported" → a missing feature vs. a genuine wrong-result).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct GlueError(pub String);

/// One sqllogictest "connection": a fresh in-memory SlateDB wrapped in GlueSQL.
pub struct GlueTester {
    glue: Glue<SlateDbStorage>,
}

impl GlueTester {
    /// Open a brand-new in-memory-backed engine. Each `.slt` file gets its own,
    /// so files never see each other's schema or rows.
    pub async fn connect() -> Result<Self, GlueError> {
        // The test DB is ephemeral in-memory, so durability is irrelevant — but
        // SlateDB's default 100ms flush interval makes each durable autocommit
        // insert wait ~100ms. A 1ms interval makes serial corpus loads ~100x
        // faster. (This is a harness-only knob; it does not touch bluedb-sql.)
        let settings = Settings {
            flush_interval: Some(Duration::from_millis(1)),
            ..Default::default()
        };
        let db = Db::builder("bluedb-slt", Arc::new(InMemory::new()))
            .with_settings(settings)
            .build()
            .await
            .map_err(|e| GlueError(format!("open slatedb: {e}")))?;
        Ok(Self {
            glue: Glue::new(SlateDbStorage::new(Arc::new(db))),
        })
    }
}

#[async_trait]
impl AsyncDB for GlueTester {
    type Error = GlueError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        // Apply bluedb's SQL-compat rewrites, matching how bluedb-sql would
        // preprocess SQL in production: set ops first (UNION/INTERSECT/EXCEPT ->
        // joins/subqueries), then comma-join folding + VARCHAR(n) normalization.
        let sql = bluedb_sql::rewrite_set_ops(sql);
        let sql = bluedb_sql::rewrite_multitable(&sql);
        let mut payloads = self
            .glue
            .execute(&sql)
            .await
            .map_err(|e| GlueError(e.to_string()))?;

        // sqllogictest feeds one record (statement or query) at a time; take the
        // last payload as the result of that record.
        match payloads.pop() {
            Some(payload) => Ok(payload_to_output(payload)),
            None => Ok(DBOutput::StatementComplete(0)),
        }
    }

    async fn shutdown(&mut self) {}

    fn engine_name(&self) -> &str {
        "gluesql-slatedb"
    }
}

/// Convert a GlueSQL payload into the shape sqllogictest compares against.
fn payload_to_output(payload: Payload) -> DBOutput<DefaultColumnType> {
    match payload {
        Payload::Select { labels, rows } => rows_to_output(labels.len(), rows),
        Payload::SelectMap(maps) => {
            let rows: Vec<Vec<Value>> = maps.into_iter().map(|m| m.into_values().collect()).collect();
            let width = rows.first().map(Vec::len).unwrap_or(0);
            rows_to_output(width, rows)
        }
        Payload::Insert(n) | Payload::Delete(n) | Payload::Update(n) => {
            DBOutput::StatementComplete(n as u64)
        }
        _ => DBOutput::StatementComplete(0),
    }
}

fn rows_to_output(width_hint: usize, rows: Vec<Vec<Value>>) -> DBOutput<DefaultColumnType> {
    let width = width_hint.max(rows.first().map(Vec::len).unwrap_or(0));
    // `Any` means "don't type-check this column" — right for a baseline; the
    // value strings are what get compared.
    let types = vec![DefaultColumnType::Any; width];
    let rows = rows
        .into_iter()
        .map(|row| row.iter().map(value_to_string).collect())
        .collect();
    DBOutput::Rows { types, rows }
}

/// Render a value the way sqllogictest expects (one token per cell).
///
/// Covers the scalar types the seed corpus exercises; exotic types fall back to
/// debug formatting (flagged as a known approximation until the corpus needs
/// them).
fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::I64(n) => n.to_string(),
        Value::F64(x) => x.to_string(),
        Value::Str(s) => s.clone(),
        other => format!("{other:?}"),
    }
}
