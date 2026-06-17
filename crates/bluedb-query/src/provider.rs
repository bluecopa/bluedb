//! [`BluedbTableProvider`] — the DataFusion front-door's per-table read provider.
//!
//! It forks each scan by predicate — the HTAP fast/slow split at the provider
//! seam:
//!   - a `pk = <literal>` filter → a point read from the row store (SlateDB,
//!     always fresh), **never touching Iceberg** — the OLTP fast path that keeps
//!     PK reads cheap after the front-door flip;
//!   - anything else → the exact read-your-writes union (Iceberg ∪ unsealed CDC
//!     tail) via [`LakehouseEngine::merged_record_batch`] — the OLAP path.
//!
//! A [`ProviderStats`] counter records which path each scan took, so a test can
//! prove a PK read skips Iceberg.
//!
//! Spike scope: the row-store side and the merge side each materialize into an
//! in-memory provider before handing back to DataFusion; a streaming
//! `ExecutionPlan` (and secondary-index pushdown) is the productionization.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bluedb_lakehouse::LakehouseEngine;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use gluesql_core::data::Key;

/// Per-provider counters proving which read path each scan took.
#[derive(Default, Debug)]
pub struct ProviderStats {
    fast_path: AtomicUsize,
    merge_path: AtomicUsize,
}

impl ProviderStats {
    /// Scans served by the PK point-read fast path (row store, no Iceberg).
    pub fn fast_path(&self) -> usize {
        self.fast_path.load(Ordering::Relaxed)
    }

    /// Scans served by the Iceberg ∪ unsealed-tail merge path.
    pub fn merge_path(&self) -> usize {
        self.merge_path.load(Ordering::Relaxed)
    }
}

/// A DataFusion [`TableProvider`] over one bluedb table that forks per predicate:
/// PK equality → row-store point read (fresh, skips Iceberg); everything else →
/// the read-your-writes Iceberg ∪ tail merge.
pub struct BluedbTableProvider {
    engine: Arc<LakehouseEngine>,
    table: String,
    schema: SchemaRef,
    pk_name: String,
    stats: Arc<ProviderStats>,
}

impl std::fmt::Debug for BluedbTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BluedbTableProvider")
            .field("table", &self.table)
            .field("pk_name", &self.pk_name)
            .finish()
    }
}

impl BluedbTableProvider {
    /// Build the provider for `table`, fetching its Arrow schema and PK column
    /// once. Errors if the table does not exist or has no single-column primary
    /// key (PK-less tables are rejected — the pushdown / merge keys on the PK).
    pub async fn try_new(engine: Arc<LakehouseEngine>, table: &str) -> anyhow::Result<Self> {
        let schema = engine
            .arrow_schema_for(table)
            .await?
            .ok_or_else(|| anyhow::anyhow!("table '{table}' does not exist"))?;
        let pk_name = engine.pk_column_name(table).await?;
        Ok(Self {
            engine,
            table: table.to_string(),
            schema,
            pk_name,
            stats: Arc::new(ProviderStats::default()),
        })
    }

    /// A handle to this provider's path counters — clone it before wrapping the
    /// provider in `Arc<dyn TableProvider>` to observe routing from a test.
    pub fn stats_handle(&self) -> Arc<ProviderStats> {
        self.stats.clone()
    }
}

#[async_trait]
impl TableProvider for BluedbTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// A `pk = <literal>` filter is pushed down **exactly** (the fast path
    /// applies it via the point read); every other filter is left for DataFusion
    /// to apply above the merge scan.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if pk_eq_key(f, &self.pk_name).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let batch = match filters.iter().find_map(|f| pk_eq_key(f, &self.pk_name)) {
            Some(key) => {
                // OLTP fast path: point-read the row store, skip Iceberg.
                self.stats.fast_path.fetch_add(1, Ordering::Relaxed);
                self.engine
                    .record_batch_for_pk(&self.table, key)
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("pk fast path: {e}")))?
            }
            None => {
                // OLAP path: exact read-your-writes union (Iceberg ∪ tail).
                self.stats.merge_path.fetch_add(1, Ordering::Relaxed);
                self.engine
                    .merged_record_batch(&self.table)
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("merge path: {e}")))?
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!("table '{}' does not exist", self.table))
                    })?
            }
        };
        let mem = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        // Non-PK filters are reported Unsupported, so DataFusion applies them in a
        // FilterExec above this scan; nothing is pushed into the in-memory source.
        mem.scan(state, projection, &[], limit).await
    }
}

/// If `filter` is `pk = <literal>` (either argument order) on column `pk_name`
/// with a literal we can encode, return the row-store [`Key`]; else `None`.
fn pk_eq_key(filter: &Expr, pk_name: &str) -> Option<Key> {
    let Expr::BinaryExpr(be) = filter else {
        return None;
    };
    if be.op != Operator::Eq {
        return None;
    }
    let (col, lit) = match (be.left.as_ref(), be.right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v, _)) => (c, v),
        (Expr::Literal(v, _), Expr::Column(c)) => (c, v),
        _ => return None,
    };
    if col.name.as_str() != pk_name {
        return None;
    }
    scalar_to_key(lit)
}

/// Convert a DataFusion literal to a gluesql primary [`Key`] (spike: the common
/// single-column PK types).
fn scalar_to_key(v: &ScalarValue) -> Option<Key> {
    match v {
        ScalarValue::Int64(Some(n)) => Some(Key::I64(*n)),
        ScalarValue::Int32(Some(n)) => Some(Key::I32(*n)),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(Key::Str(s.clone())),
        _ => None,
    }
}
