//! [`BluedbSchemaProvider`] — a DataFusion `SchemaProvider` that resolves a table
//! name to a [`BluedbTableProvider`] on demand, so a query's joins / CTEs /
//! subqueries resolve all their tables uniformly through one catalog seam (no
//! explicit `register_table`).
//!
//! Logically stateless: each resolution mints a fresh provider that reads the
//! table's current watermark + unsealed tail, so freshness is per-query.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use bluedb_lakehouse::LakehouseEngine;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::Result as DfResult;

use crate::provider::BluedbTableProvider;

/// Resolves bluedb table names to [`BluedbTableProvider`]s for a tenant's engine.
pub struct BluedbSchemaProvider {
    engine: Arc<LakehouseEngine>,
}

impl BluedbSchemaProvider {
    pub fn new(engine: Arc<LakehouseEngine>) -> Self {
        Self { engine }
    }
}

impl std::fmt::Debug for BluedbSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BluedbSchemaProvider").finish()
    }
}

#[async_trait]
impl SchemaProvider for BluedbSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Best-effort: query planning resolves each referenced name via
    /// [`Self::table`], which is the authority. A full catalog listing would need
    /// an engine-wide table enumeration, not required for resolution.
    fn table_names(&self) -> Vec<String> {
        Vec::new()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        match BluedbTableProvider::try_new(self.engine.clone(), name).await {
            Ok(p) => Ok(Some(Arc::new(p) as Arc<dyn TableProvider>)),
            // Unknown table or PK-less → not resolvable here.
            Err(_) => Ok(None),
        }
    }

    fn table_exist(&self, _name: &str) -> bool {
        // Optimistic: `table()` is the authority. Returning true keeps DataFusion
        // from short-circuiting resolution before the async `table()` runs.
        true
    }
}
