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

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeStringArray, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use async_trait::async_trait;
use bluedb_lakehouse::{object_store_file_io, LakehouseEngine};
use bluedb_query::query_via_catalog;
use bluedb_sql::{CdcConfig, Database, SlateDbStorage};
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::ObjectStore;
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
    /// Session `default_null_order`: `None` = engine default (NULLs largest),
    /// `Some(true)` = NULLS FIRST, `Some(false)` = NULLS LAST. Set by
    /// `SET default_null_order = …` and applied to every subsequent `ORDER BY`.
    nulls_first: Option<bool>,
    /// View definitions (name → CTE-inlined body SQL) captured from
    /// `CREATE VIEW`, inlined into later queries (GlueSQL has no views).
    views: std::collections::HashMap<String, String>,
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
            nulls_first: None,
            views: std::collections::HashMap::new(),
        })
    }
}

#[async_trait]
impl AsyncDB for GlueTester {
    type Error = GlueError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        // `SET default_null_order = …` is session state GlueSQL doesn't model;
        // capture it and apply it to ORDER BY ourselves (see below).
        if let Some(nulls_first) = bluedb_sql::parse_default_null_order(sql) {
            self.nulls_first = Some(nulls_first);
            return Ok(DBOutput::StatementComplete(0));
        }
        // Other `SET`/`PRAGMA` knobs (e.g. `SET debug_force_external`) are engine
        // config GlueSQL has no concept of; treat them as no-op session settings
        // rather than rejecting (which would also cascade-fail the rest of the
        // file). `default_null_order` is excluded above so its negative-value
        // tests still surface an error.
        if is_ignorable_setting(sql) {
            return Ok(DBOutput::StatementComplete(0));
        }
        // GlueSQL has no views: capture `CREATE VIEW` definitions (CTE-inlined)
        // and inline references in later queries; swallow `DROP VIEW`.
        if let Some((name, body)) = bluedb_sql::parse_create_view(sql) {
            self.views.insert(name, bluedb_sql::inline_ctes(&body));
            return Ok(DBOutput::StatementComplete(0));
        }
        if let Some(names) = bluedb_sql::parse_drop_view(sql) {
            for name in names {
                self.views.remove(&name);
            }
            return Ok(DBOutput::StatementComplete(0));
        }

        // Reject SQL GlueSQL would silently mis-execute (window functions) with
        // a clear error rather than returning wrong rows.
        if let Some(reason) = bluedb_sql::unsupported_reason(sql) {
            return Err(GlueError(reason));
        }

        // Apply bluedb's SQL-compat rewrites, matching how bluedb-sql would
        // preprocess SQL in production: CTE inlining first (WITH -> derived
        // tables), view-reference inlining, then set ops
        // (UNION/INTERSECT/EXCEPT -> joins/subqueries), then comma-join folding +
        // data-type normalization.
        let sql = bluedb_sql::inline_ctes(sql);
        let sql = bluedb_sql::inline_views(&sql, &self.views);
        let sql = bluedb_sql::rewrite_set_ops(&sql);
        let sql = bluedb_sql::rewrite_multitable(&sql);
        // Normalize NULL placement: strip explicit `NULLS FIRST/LAST` (GlueSQL
        // rejects it) and apply any active `default_null_order`, both via
        // injected `(key IS NULL)` sort keys.
        let sql = bluedb_sql::rewrite_null_order(&sql, self.nulls_first);
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

/// True for a `SET`/`PRAGMA` session knob we treat as a no-op (everything except
/// `default_null_order`, which is handled separately so its negative-value tests
/// still error).
fn is_ignorable_setting(sql: &str) -> bool {
    let lower = sql.trim().to_ascii_lowercase();
    let body = lower.strip_suffix(';').unwrap_or(&lower).trim();
    (body.starts_with("set ") || body.starts_with("pragma "))
        && !body.contains("default_null_order")
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

// ───────────────────────── DataFusion front-door backend ─────────────────────
//
// The second sqllogictest backend drives bluedb's **read front door** exactly as
// the server does: a single top-level `SELECT` is planned + executed by DataFusion
// over the `BluedbSchemaProvider` (joins / window functions / aggregates / CTEs /
// set operations), while writes + DDL stay on the GlueSQL path. Point a corpus at
// this backend to measure the analytical read dialect — the features GlueSQL
// cannot serve and the front-door flip now does.

/// A DataFusion / engine error, surfaced to sqllogictest as the backend `Error`.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct DfError(pub String);

/// One sqllogictest "connection" for the read front door: a fresh in-memory
/// SlateDB shared by a GlueSQL write connection and a [`LakehouseEngine`] whose
/// tables DataFusion reads. Each `.slt` file gets its own, so files never share
/// schema or rows.
pub struct DataFusionTester {
    db: Database,
    eng: Arc<LakehouseEngine>,
}

impl DataFusionTester {
    /// Open a brand-new in-memory engine: SlateDB → `Database` (GlueSQL writes) +
    /// `LakehouseEngine` (DataFusion reads), both over the one object store.
    pub async fn connect() -> Result<Self, DfError> {
        // 1ms flush (vs SlateDB's 100ms default) keeps serial corpus loads fast;
        // harness-only, does not touch bluedb-sql.
        let settings = Settings {
            flush_interval: Some(Duration::from_millis(1)),
            ..Default::default()
        };
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let raw = Db::builder("bluedb-slt-df", store.clone())
            .with_settings(settings)
            .build()
            .await
            .map_err(|e| DfError(format!("open slatedb: {e}")))?;
        let db = Database::new(Arc::new(raw));
        let file_io = object_store_file_io(store.clone(), "");
        let eng =
            LakehouseEngine::reopen(file_io, "lakehouse", "_", db.clone(), CdcConfig::default())
                .await
                .map_err(|e| DfError(format!("open lakehouse engine: {e}")))?;
        Ok(Self {
            db,
            eng: Arc::new(eng),
        })
    }
}

#[async_trait]
impl AsyncDB for DataFusionTester {
    type Error = DfError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        // SET/PRAGMA session knobs: no-op (don't cascade-fail the rest of a file).
        if is_ignorable_setting(sql) {
            return Ok(DBOutput::StatementComplete(0));
        }
        if is_read(sql) {
            // The read front door: DataFusion over the bluedb schema provider —
            // the same entry point the server's `POST /sql` uses for SELECTs.
            let batches = query_via_catalog(self.eng.clone(), sql, &[])
                .await
                .map_err(|e| DfError(e.to_string()))?;
            return Ok(render_batches(&batches));
        }
        // Writes + DDL stay on the GlueSQL path, on the shared SlateDB.
        let mut glue = Glue::new(self.db.connection_serialized());
        let mut payloads = glue
            .execute(sql)
            .await
            .map_err(|e| DfError(e.to_string()))?;
        match payloads.pop() {
            Some(p) => Ok(payload_to_output(p)),
            None => Ok(DBOutput::StatementComplete(0)),
        }
    }

    async fn shutdown(&mut self) {}

    fn engine_name(&self) -> &str {
        "datafusion-frontdoor"
    }
}

/// Classify a statement as a read the same way the server's `/sql` front door does
/// (`is_read_query`): a single top-level `SELECT`/`Query` goes to DataFusion;
/// everything else (DDL / DML / SET) goes to the GlueSQL write path.
fn is_read(sql: &str) -> bool {
    use gluesql_core::sqlparser::ast::Statement;
    match gluesql_core::parse_sql::parse(sql) {
        Ok(stmts) if stmts.len() == 1 => matches!(stmts[0], Statement::Query(_)),
        _ => false,
    }
}

/// Render DataFusion result batches into sqllogictest's row/cell strings. The
/// column count comes from the first batch; the [`lenient_validator`] makes the
/// value comparison numeric-aware, so `11` and `11.0` match.
fn render_batches(batches: &[RecordBatch]) -> DBOutput<DefaultColumnType> {
    let width = batches.first().map(RecordBatch::num_columns).unwrap_or(0);
    let mut rows = Vec::new();
    for b in batches {
        for r in 0..b.num_rows() {
            let row = (0..b.num_columns())
                .map(|c| render_cell(b.column(c).as_ref(), r))
                .collect();
            rows.push(row);
        }
    }
    DBOutput::Rows {
        types: vec![DefaultColumnType::Any; width],
        rows,
    }
}

/// Render one Arrow cell as the single token sqllogictest compares. Covers the
/// scalar types the read dialect produces; other types fall back to a tagged
/// debug form (a known rendering gap until a corpus needs them).
fn render_cell(col: &dyn Array, i: usize) -> String {
    use arrow_schema::DataType as Dt;
    if col.is_null(i) {
        return "NULL".to_string();
    }
    macro_rules! val {
        ($t:ty) => {
            col.as_any().downcast_ref::<$t>().unwrap().value(i)
        };
    }
    match col.data_type() {
        Dt::Boolean => val!(BooleanArray).to_string(),
        Dt::Int8 => val!(Int8Array).to_string(),
        Dt::Int16 => val!(Int16Array).to_string(),
        Dt::Int32 => val!(Int32Array).to_string(),
        Dt::Int64 => val!(Int64Array).to_string(),
        Dt::UInt8 => val!(UInt8Array).to_string(),
        Dt::UInt16 => val!(UInt16Array).to_string(),
        Dt::UInt32 => val!(UInt32Array).to_string(),
        Dt::UInt64 => val!(UInt64Array).to_string(),
        Dt::Float32 => val!(Float32Array).to_string(),
        Dt::Float64 => val!(Float64Array).to_string(),
        Dt::Utf8 => val!(StringArray).to_string(),
        Dt::LargeUtf8 => val!(LargeStringArray).to_string(),
        other => format!("<{other:?}>"),
    }
}
