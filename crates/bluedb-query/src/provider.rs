//! [`BluedbTableProvider`] — the DataFusion front-door's per-table read provider.
//!
//! It forks each scan by predicate — the HTAP fast/slow split at the provider
//! seam:
//!   - a `pk = <literal>` filter → a point read from the row store (SlateDB,
//!     always fresh), **never touching Iceberg** — the OLTP fast path that keeps
//!     PK reads cheap after the front-door flip;
//!   - anything else → the exact read-your-writes union, built as a **streaming**
//!     plan: the sealed Iceberg snapshot streams from Parquet, anti-joined
//!     against the unsealed CDC tail's touched keys, `UNION ALL` the tail's
//!     upserts. Only the small tail is held in memory — the bulk never is.
//!
//! The anti-join is expressed as SQL `NOT IN`, so it is key-type-agnostic: a
//! single-column PK and the composite-PK `__bluedb_pk BYTEA` surrogate are
//! handled the same way.
//!
//! A [`ProviderStats`] counter records which path each scan took, so a test can
//! prove a PK read skips Iceberg.

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
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use gluesql_core::data::Key;
use iceberg_datafusion::IcebergStaticTableProvider;

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

    /// Scans served by the streaming Iceberg ∪ unsealed-tail merge path.
    pub fn merge_path(&self) -> usize {
        self.merge_path.load(Ordering::Relaxed)
    }
}

/// A DataFusion [`TableProvider`] over one bluedb table that forks per predicate:
/// PK equality → row-store point read (fresh, skips Iceberg); everything else →
/// the streaming read-your-writes Iceberg ∪ tail merge.
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

    /// The OLAP path: a streaming plan unioning the sealed Iceberg snapshot (read
    /// columnar from Parquet) with the unsealed CDC tail, anti-joined on the PK.
    async fn scan_merge(
        &self,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let merge_err = |ctx: &str, e: String| DataFusionError::Execution(format!("{ctx}: {e}"));
        let inner = SessionContext::new();

        // Tail: upserts + the touched-key set (both small, in-memory).
        let (upserts, touched) = self
            .engine
            .unsealed_delta(&self.table)
            .await
            .map_err(|e| merge_err("unsealed delta", e.to_string()))?;
        inner.register_table(
            "__dup",
            Arc::new(MemTable::try_new(upserts.schema(), vec![vec![upserts]])?),
        )?;
        inner.register_table(
            "__dk",
            Arc::new(MemTable::try_new(touched.schema(), vec![vec![touched]])?),
        )?;

        // Bulk: the sealed Iceberg snapshot streams from Parquet (no whole-table
        // materialization). Absent until the first seal.
        let has_iceberg = match self
            .engine
            .current_iceberg_table(&self.table)
            .await
            .map_err(|e| merge_err("load iceberg", e.to_string()))?
        {
            Some(t) => {
                let p = IcebergStaticTableProvider::try_new_from_table(t)
                    .await
                    .map_err(|e| merge_err("iceberg provider", e.to_string()))?;
                inner.register_table("__ice", Arc::new(p))?;
                true
            }
            None => false,
        };

        let pk = &self.pk_name;
        // Iceberg rows whose PK is NOT touched by the tail, UNION ALL the tail's
        // surviving upserts. `NOT IN` is key-type-agnostic (int / utf8 / the
        // composite-PK BYTEA surrogate alike).
        let merged = if has_iceberg {
            let ice = inner
                .sql(&format!(
                    "SELECT * FROM __ice WHERE \"{pk}\" NOT IN (SELECT \"{pk}\" FROM __dk)"
                ))
                .await?;
            ice.union(inner.table("__dup").await?)?
        } else {
            inner.table("__dup").await?
        };

        // Apply DataFusion's projection (indices into our schema) and limit.
        let merged = match projection {
            Some(idxs) => {
                let names: Vec<&str> = idxs
                    .iter()
                    .map(|i| self.schema.field(*i).name().as_str())
                    .collect();
                merged.select_columns(&names)?
            }
            None => merged,
        };
        let merged = match limit {
            Some(n) => merged.limit(0, Some(n))?,
            None => merged,
        };
        merged.create_physical_plan().await
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
        if let Some(key) = filters.iter().find_map(|f| pk_eq_key(f, &self.pk_name)) {
            // OLTP fast path: point-read the row store, skip Iceberg.
            self.stats.fast_path.fetch_add(1, Ordering::Relaxed);
            let batch = self
                .engine
                .record_batch_for_pk(&self.table, key)
                .await
                .map_err(|e| DataFusionError::Execution(format!("pk fast path: {e}")))?;
            let mem = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
            return mem.scan(state, projection, &[], limit).await;
        }
        // OLAP path: the streaming Iceberg ∪ tail merge.
        self.stats.merge_path.fetch_add(1, Ordering::Relaxed);
        self.scan_merge(projection, limit).await
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

/// Convert a DataFusion literal to a gluesql primary [`Key`] (the common
/// single-column PK types; composite-PK point reads fall to the merge path).
fn scalar_to_key(v: &ScalarValue) -> Option<Key> {
    match v {
        ScalarValue::Int64(Some(n)) => Some(Key::I64(*n)),
        ScalarValue::Int32(Some(n)) => Some(Key::I32(*n)),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(Key::Str(s.clone())),
        _ => None,
    }
}
