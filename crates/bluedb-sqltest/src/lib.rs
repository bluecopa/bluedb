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
use sqllogictest::{default_validator, AsyncDB, DBOutput, DefaultColumnType, Normalizer};

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
        // Reject SQL GlueSQL would silently mis-execute (window functions) with
        // a clear error rather than returning wrong rows.
        if let Some(reason) = bluedb_sql::unsupported_reason(sql) {
            return Err(GlueError(reason));
        }

        // Apply bluedb's SQL-compat rewrites, matching how bluedb-sql would
        // preprocess SQL in production: CTE inlining first (WITH -> derived
        // tables), then set ops (UNION/INTERSECT/EXCEPT -> joins/subqueries),
        // then comma-join folding + data-type normalization.
        let sql = bluedb_sql::inline_ctes(sql);
        let sql = bluedb_sql::rewrite_set_ops(&sql);
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

/// A layout-tolerant result validator.
///
/// sqllogictest has two legal ways to write a multi-column result block, and the
/// DuckDB corpus uses both: **tab-separated rows** (one row per line, e.g.
/// `NULL⇥11.000000`) and **one value per line** (a 2-column row spans 2 lines).
/// The default validator joins each actual row's columns with a space and
/// compares one line per row — so a result whose *values are correct* still
/// fails whenever the file chose the value-per-line form (this is most of the
/// apparent "wrong" aggregate results: `AVG(i)=2` is computed correctly, it just
/// renders across multiple expected lines).
///
/// This validator accepts a result when it matches under **either** layout, and
/// when numeric cells are **numerically equal** even if textually different
/// (`11` vs sqllogictest's `R`-column `11.000000`). It first defers to the exact
/// [`default_validator`] (which also handles `<slt:ignore>` and hashed results),
/// then retries row-wise and value-wise with numeric-aware token comparison. It
/// is strictly more permissive than the default, so it can only turn a
/// layout-or-formatting-only mismatch into a pass — never mask a genuine value
/// difference (e.g. `0.333333` vs `0.3333333333` stays a mismatch; only exact
/// numeric equality is accepted, not approximate).
pub fn lenient_validator(
    normalizer: Normalizer,
    actual: &[Vec<String>],
    expected: &[String],
) -> bool {
    // 1. Exact comparison (preserves ignore-marker + hash handling).
    if default_validator(normalizer, actual, expected) {
        return true;
    }
    // 2. Row-wise, numeric-aware (recovers `11` == `11.000000`).
    let row_wise: Vec<String> = actual.iter().map(|row| row.join(" ")).collect();
    if lines_match(&row_wise, expected) {
        return true;
    }
    // 3. Value-wise, numeric-aware (recovers one-value-per-line files).
    let value_wise: Vec<String> = actual.iter().flat_map(|row| row.iter()).cloned().collect();
    lines_match(&value_wise, expected)
}

fn lines_match(actual: &[String], expected: &[String]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(a, e)| tokens_match(a, e))
}

/// Compare two whitespace-separated lines token by token, treating two tokens as
/// equal if they are byte-identical or parse to the same `f64`.
fn tokens_match(actual: &str, expected: &str) -> bool {
    let a: Vec<&str> = actual.split_ascii_whitespace().collect();
    let e: Vec<&str> = expected.split_ascii_whitespace().collect();
    a.len() == e.len() && a.iter().zip(e).all(|(x, y)| token_eq(x, y))
}

fn token_eq(x: &str, y: &str) -> bool {
    x == y
        || matches!(
            (x.parse::<f64>(), y.parse::<f64>()),
            (Ok(a), Ok(b)) if a == b
        )
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
