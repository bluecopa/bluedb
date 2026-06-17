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
use iceberg_datafusion::IcebergStaticTableProvider;

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
