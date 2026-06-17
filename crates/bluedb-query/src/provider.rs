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

use arrow_schema::{DataType, SchemaRef};
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

/// The composite-PK surrogate column name (mirrors `bluedb_sql::compositepk::PK_COL`).
/// Hidden from the provider's user-facing schema and projected out of results,
/// while the merge still references it internally for the anti-join.
const SURROGATE_PK: &str = "__bluedb_pk";

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
    pk_type: DataType,
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
        let full_schema = engine
            .arrow_schema_for(table)
            .await?
            .ok_or_else(|| anyhow::anyhow!("table '{table}' does not exist"))?;
        let pk_name = engine.pk_column_name(table).await?;
        let pk_type = full_schema
            .field_with_name(&pk_name)
            .map_err(|e| anyhow::anyhow!("pk column '{pk_name}' not in schema: {e}"))?
            .data_type()
            .clone();
        // Hide the composite-PK surrogate from the user-facing schema.
        let schema: SchemaRef = if full_schema.fields().iter().any(|f| f.name() == SURROGATE_PK) {
            let fields: Vec<_> = full_schema
                .fields()
                .iter()
                .filter(|f| f.name() != SURROGATE_PK)
                .cloned()
                .collect();
            Arc::new(arrow_schema::Schema::new_with_metadata(
                fields,
                full_schema.metadata().clone(),
            ))
        } else {
            full_schema
        };
        Ok(Self {
            engine,
            table: table.to_string(),
            schema,
            pk_name,
            pk_type,
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

        // The Iceberg ∪ tail merge is only *complete* for mirrored tables; a
        // non-mirrored table's rows live solely in the row store. A bare
        // `LakehouseWriter` handle can leave a spurious empty snapshot, so gate on
        // mirror-enablement, not snapshot existence.
        let iceberg = if self.engine.is_table_mirrored(&self.table) {
            self.engine
                .current_iceberg_table(&self.table)
                .await
                .map_err(|e| merge_err("load iceberg", e.to_string()))?
        } else {
            None
        };

        let merged = match iceberg {
            // A sealed snapshot exists → stream the columnar Iceberg bulk and
            // merge the unsealed CDC tail (read-your-writes). Only the tail is in
            // memory; the bulk streams from Parquet.
            Some(t) => {
                let p = IcebergStaticTableProvider::try_new_from_table(t)
                    .await
                    .map_err(|e| merge_err("iceberg provider", e.to_string()))?;
                inner.register_table("__ice", Arc::new(p))?;
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
                let pk = &self.pk_name;
                // Iceberg rows whose PK the tail did NOT touch, UNION ALL the tail's
                // surviving upserts. `NOT IN` is key-type-agnostic (int / utf8 / the
                // composite-PK BYTEA surrogate alike).
                let ice = inner
                    .sql(&format!(
                        "SELECT * FROM __ice WHERE \"{pk}\" NOT IN (SELECT \"{pk}\" FROM __dk)"
                    ))
                    .await?;
                ice.union(inner.table("__dup").await?)?
            }
            // No snapshot yet — a never-sealed table, OR one not mirrored to the
            // lakehouse at all. Serve every current row straight from the row store
            // (the source of truth): always correct, just not columnar.
            None => {
                let batch = self
                    .engine
                    .current_record_batch(&self.table)
                    .await
                    .map_err(|e| merge_err("current rows", e.to_string()))?
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "table '{}' does not exist",
                            self.table
                        ))
                    })?;
                inner.register_table(
                    "__all",
                    Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?),
                )?;
                inner.table("__all").await?
            }
        };

        // Project to the provider's user-facing columns (indices into our
        // schema) — this also drops the composite-PK surrogate the union carries
        // internally — then apply the limit.
        let names: Vec<&str> = match projection {
            Some(idxs) => idxs
                .iter()
                .map(|i| self.schema.field(*i).name().as_str())
                .collect(),
            None => self
                .schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect(),
        };
        let merged = merged.select_columns(&names)?;
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
                if pk_eq_key(f, &self.pk_name, &self.pk_type).is_some() {
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
        if let Some(key) = filters
            .iter()
            .find_map(|f| pk_eq_key(f, &self.pk_name, &self.pk_type))
        {
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

/// If `filter` is `pk = <literal>` (either argument order) on column `pk_name`,
/// and the PK column's Arrow type is one we can encode to a row-store [`Key`]
/// that matches the stored key exactly, return that `Key`; else `None`.
///
/// Only `Int64` and `Utf8`/`LargeUtf8` PKs fast-path. Other PK types (decimal /
/// u128 / date / the composite BYTEA surrogate) fall through to the merge path —
/// correct, just not a single-row point read. (Encoding a `= 1` literal as
/// `Key::I64(1)` for, say, a `u128` PK would build a key that never matches.)
fn pk_eq_key(filter: &Expr, pk_name: &str, pk_type: &DataType) -> Option<Key> {
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
    match (pk_type, lit) {
        (DataType::Int64, ScalarValue::Int64(Some(n))) => Some(Key::I64(*n)),
        (DataType::Utf8, ScalarValue::Utf8(Some(s))) => Some(Key::Str(s.clone())),
        (DataType::LargeUtf8, ScalarValue::LargeUtf8(Some(s))) => Some(Key::Str(s.clone())),
        _ => None,
    }
}
