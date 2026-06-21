//! bluedb analytical read engine — DataFusion over sealed Iceberg snapshots.
//!
//! The lakehouse mirror writes each bluedb table to Iceberg on every seal pass.
//! This crate's single entry point, [`query_sql`], lets callers run arbitrary
//! SQL (filter/sort/aggregate) against a table's latest sealed snapshot via
//! DataFusion — the "analytical tier" of the HTAP unified-query design.
//!
//! # Design
//!
//! 1. Fetch the current sealed [`iceberg::table::Table`] via
//!    [`LakehouseEngine::current_iceberg_table`].
//! 2. Wrap it in [`iceberg_datafusion::IcebergStaticTableProvider`], which
//!    projects Iceberg schema to Arrow and drives columnar I/O.
//! 3. Register the provider under the table's name in a fresh
//!    [`datafusion::prelude::SessionContext`].
//! 4. Execute `sql` and collect the [`arrow_array::RecordBatch`] results.
//!
//! # Out of scope
//!
//! The guardrail reject→route wiring, REST endpoints, watermark/freshness-gate
//! changes, and datafusion-postgres are later sub-tasks. This crate delivers
//! only the engine core.

use std::sync::Arc;

use anyhow::{anyhow, Context};
use arrow_array::RecordBatch;
use bluedb_lakehouse::LakehouseEngine;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use iceberg_datafusion::IcebergStaticTableProvider;

mod catalog;
mod format_udfs;
mod gluesql_compat;
mod hll;
mod json_ops;
mod json_path;
mod json_udfs;
mod provider;
pub use catalog::BluedbSchemaProvider;
pub use provider::{BluedbTableProvider, ProviderStats};

/// Register bluedb's scalar-function extensions on a [`SessionContext`]: the JSON
/// accessors (`->`, `->>`, `json_get`, `json_get_str`), the Postgres formatting
/// functions (`to_number`, `format`, numeric `to_char`), and the GlueSQL/Postgres
/// name-compatibility shim ([`gluesql_compat`] — `sign`, `add_month`, `variance`,
/// `approx_count_distinct`, …). [`query_via_catalog`] calls this; it is public so
/// other front doors (e.g. the conformance harness) can build an identical context.
pub fn register_extensions(ctx: &mut SessionContext) -> datafusion::error::Result<()> {
    json_udfs::register(ctx)?;
    json_ops::register(ctx)?;
    json_path::register(ctx)?;
    format_udfs::register(ctx)?;
    gluesql_compat::register(ctx)?;
    hll::register(ctx)?;
    Ok(())
}

/// Build a fresh DataFusion [`SessionContext`] with bluedb's analytical
/// extensions: the default features (built-in functions / optimizers), a
/// [`json_udfs::JsonTypePlanner`] mapping `JSON`/`JSONB` SQL types to `Utf8`
/// (text-backed JSON — set at build time, the only place a `TypePlanner` can be
/// installed), and the scalar-function extensions ([`register_extensions`]). The
/// caller registers its own schema provider. Used by [`query_via_catalog`] and
/// the conformance harness so both plan identically.
pub fn analytical_context() -> datafusion::error::Result<SessionContext> {
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::config::SessionConfig;
    // DataFusion implements recursive CTEs but ships them behind a flag that
    // defaults to off. Turn it on so `WITH RECURSIVE` works on the analytical
    // path (the engine handles the fixed-point iteration; bluedb has no reason
    // to forbid it). Bounded by the query's own terminator (`WHERE n < k`).
    let mut config = SessionConfig::new();
    config.options_mut().execution.enable_recursive_ctes = true;
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_type_planner(Arc::new(json_udfs::JsonTypePlanner))
        .build();
    let mut ctx = SessionContext::new_with_state(state);
    register_extensions(&mut ctx)?;
    Ok(ctx)
}

/// Build the analytical [`SessionContext`] with the tenant's collections/tables
/// registered (via [`BluedbSchemaProvider`]), ready for `ctx.table(name).await?`
/// → `DataFrame`. This is the same context [`query_via_catalog`] runs SQL against.
///
/// Callers (e.g. MongoDB aggregation pipeline builders) can obtain a `DataFrame`
/// per collection directly:
/// ```ignore
/// let ctx = session_with_catalog(engine).await?;
/// let df = ctx.table("orders").await?;
/// ```
///
/// # Errors
///
/// - [`analytical_context`] fails (e.g. UDF registration conflict).
/// - The default `datafusion` catalog is missing (should never happen with
///   DataFusion's own `SessionContext`).
pub async fn session_with_catalog(engine: Arc<LakehouseEngine>) -> anyhow::Result<SessionContext> {
    let ctx = analytical_context().with_context(|| "building analytical context")?;
    ctx.catalog("datafusion")
        .ok_or_else(|| anyhow!("default catalog 'datafusion' missing"))?
        .register_schema("public", Arc::new(BluedbSchemaProvider::new(engine)))
        .with_context(|| "registering bluedb schema provider")?;
    Ok(ctx)
}

/// Run a read `sql` through the DataFusion front door: register the tenant's
/// tables via [`BluedbSchemaProvider`] and execute, binding positional params.
///
/// This is the server's `POST /sql` SELECT entry point. The schema provider
/// resolves every referenced table (joins / CTEs / subqueries) to a streaming
/// read-your-writes union (or a PK point read). `params` are JSON values bound
/// positionally; `?` placeholders are rewritten to DataFusion's `$N` form first.
///
/// # Errors
///
/// - `sql` is malformed, references unknown tables/columns, or uses a feature the
///   engine can't plan.
/// - A referenced table has no single-column primary key.
/// - DataFusion / Iceberg I/O fails.
pub async fn query_via_catalog(
    engine: Arc<LakehouseEngine>,
    sql: &str,
    params: &[serde_json::Value],
) -> anyhow::Result<Vec<RecordBatch>> {
    let ctx = session_with_catalog(engine).await?;

    let sql = rewrite_placeholders(sql);
    let df = ctx
        .sql(&sql)
        .await
        .with_context(|| format!("planning SQL: {sql}"))?;
    let df = if params.is_empty() {
        df
    } else {
        let scalars: Vec<ScalarValue> = params.iter().map(json_to_scalar).collect();
        df.with_param_values(scalars)
            .with_context(|| "binding query parameters")?
    };
    let batches = df
        .collect()
        .await
        .with_context(|| format!("executing SQL: {sql}"))?;
    Ok(batches)
}

/// Rewrite `?` positional placeholders to DataFusion's `$1, $2, …`, skipping
/// quoted string literals. (bluedb's REST/SQL surface uses `?`; DataFusion uses
/// `$N`.) Note: does not special-case `''` escapes inside string literals.
fn rewrite_placeholders(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 8);
    let mut n = 0usize;
    let (mut in_single, mut in_double) = (false, false);
    for c in sql.chars() {
        match c {
            '\'' if !in_double => {
                in_single = !in_single;
                out.push(c);
            }
            '"' if !in_single => {
                in_double = !in_double;
                out.push(c);
            }
            '?' if !in_single && !in_double => {
                n += 1;
                out.push('$');
                out.push_str(&n.to_string());
            }
            _ => out.push(c),
        }
    }
    out
}

/// Convert a JSON parameter to a DataFusion [`ScalarValue`] for positional binding.
fn json_to_scalar(v: &serde_json::Value) -> ScalarValue {
    use serde_json::Value as J;
    match v {
        J::Null => ScalarValue::Null,
        J::Bool(b) => ScalarValue::Boolean(Some(*b)),
        J::Number(n) => n
            .as_i64()
            .map(|i| ScalarValue::Int64(Some(i)))
            .unwrap_or_else(|| ScalarValue::Float64(n.as_f64())),
        J::String(s) => ScalarValue::Utf8(Some(s.clone())),
        // Arrays/objects: bind as their JSON text (rarely parameterized).
        other => ScalarValue::Utf8(Some(other.to_string())),
    }
}

/// Run `sql` against the current sealed Iceberg snapshot of `table` using
/// DataFusion, and return the resulting [`RecordBatch`]es.
///
/// The query runs entirely in-process; the only I/O is reading the table's
/// Parquet data files and Iceberg metadata from object storage (whatever
/// [`LakehouseEngine`] was opened against — local temp dir, GCS, etc.).
///
/// # Errors
///
/// - The table has never been sealed (no Iceberg snapshot exists yet).
/// - `sql` is malformed or references columns that do not exist.
/// - DataFusion or Iceberg I/O fails.
pub async fn query_sql(
    engine: &LakehouseEngine,
    table: &str,
    sql: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    let iceberg_table = engine
        .current_iceberg_table(table)
        .await
        .with_context(|| format!("loading sealed Iceberg table '{table}'"))?
        .ok_or_else(|| anyhow!("table '{table}' has not been sealed yet"))?;

    let provider = IcebergStaticTableProvider::try_new_from_table(iceberg_table)
        .await
        .with_context(|| format!("building IcebergStaticTableProvider for '{table}'"))?;

    let ctx = SessionContext::new();
    ctx.register_table(table, Arc::new(provider))
        .with_context(|| format!("registering '{table}' in DataFusion session"))?;

    let batches = ctx
        .sql(sql)
        .await
        .with_context(|| format!("planning SQL: {sql}"))?
        .collect()
        .await
        .with_context(|| format!("executing SQL: {sql}"))?;

    Ok(batches)
}

/// Run `sql` against the **fresh, current** rows of `table` — read straight from
/// the active writer's live store, *including writes not yet sealed into
/// Iceberg* — using DataFusion, and return the resulting [`RecordBatch`]es.
///
/// This is the HTAP P4 writer-local path: the active writer holds rows at least
/// as fresh as any acknowledged write, so a freshness-gated analytical query
/// (`X-Bluedb-Min-Watermark` outrunning the sealed watermark) can be answered
/// here instead of 503-ing until the next seal. Rows are converted to Arrow with
/// the *same* gluesql-`Value`→Arrow path the seal uses
/// ([`LakehouseEngine::current_record_batch`]), so results render identically to
/// the sealed-Iceberg path.
///
/// First cut: the **whole** current table is materialized into an in-memory
/// DataFusion [`MemTable`]; the Iceberg ∪ unsealed-delta union optimization is
/// out of scope.
///
/// # Errors
///
/// - `table` does not exist (no gluesql schema).
/// - `sql` is malformed or references columns that do not exist.
/// - DataFusion execution or the row→Arrow conversion fails.
pub async fn query_sql_fresh(
    engine: &LakehouseEngine,
    table: &str,
    sql: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    let batch = engine
        .current_record_batch(table)
        .await
        .with_context(|| format!("reading fresh rows of '{table}' from the writer"))?
        .ok_or_else(|| anyhow!("table '{table}' does not exist"))?;

    let schema = batch.schema();
    let provider = MemTable::try_new(schema, vec![vec![batch]])
        .with_context(|| format!("building in-memory provider for '{table}'"))?;

    let ctx = SessionContext::new();
    ctx.register_table(table, Arc::new(provider))
        .with_context(|| format!("registering fresh '{table}' in DataFusion session"))?;

    let batches = ctx
        .sql(sql)
        .await
        .with_context(|| format!("planning SQL (fresh): {sql}"))?
        .collect()
        .await
        .with_context(|| format!("executing SQL (fresh): {sql}"))?;

    Ok(batches)
}

/// Run `sql` against the exact read-your-writes **union** of `table`'s sealed
/// Iceberg snapshot and its unsealed CDC tail — the analytical read of the
/// DataFusion front-door design — via DataFusion.
///
/// Unlike [`query_sql`] (sealed Iceberg only) and [`query_sql_fresh`] (whole
/// current table materialized), this reads the columnar bulk from Iceberg and
/// merges only the *unsealed delta* on the primary key, so results are both
/// fresh (read-your-writes) and cheap (the row store is touched only for the
/// tail). See [`LakehouseEngine::merged_record_batch`].
///
/// # Errors
///
/// - `table` does not exist, or has no single-column primary key.
/// - `sql` is malformed, or DataFusion / Iceberg I/O fails.
pub async fn query_sql_unified(
    engine: Arc<LakehouseEngine>,
    table: &str,
    sql: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    query_sql_unified_multi(engine, &[table], sql).await
}

/// Like [`query_sql_unified`] but registers **several** tables, so `sql` may JOIN
/// across them. Each table is registered as a [`BluedbTableProvider`]; DataFusion
/// then plans the join / window / aggregate over them — the mechanism by which
/// multi-table and window-function support "fall out" of the front-door flip
/// (GlueSQL handles neither correctly). The provider serves each table as the
/// streaming Iceberg ∪ unsealed-tail union (or a row-store point read for a PK
/// filter).
pub async fn query_sql_unified_multi(
    engine: Arc<LakehouseEngine>,
    tables: &[&str],
    sql: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new();
    for &table in tables {
        let provider = BluedbTableProvider::try_new(engine.clone(), table)
            .await
            .with_context(|| format!("building provider for '{table}'"))?;
        ctx.register_table(table, Arc::new(provider))
            .with_context(|| format!("registering '{table}' in DataFusion session"))?;
    }
    let batches = ctx
        .sql(sql)
        .await
        .with_context(|| format!("planning SQL (unified): {sql}"))?
        .collect()
        .await
        .with_context(|| format!("executing SQL (unified): {sql}"))?;
    Ok(batches)
}
