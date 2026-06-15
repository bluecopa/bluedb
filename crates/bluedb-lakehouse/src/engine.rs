//! [`LakehouseEngine`] — owns the durable opt-out registry and the seal that
//! drains the CDC log into Iceberg snapshots (spec §3, §5, §8).
//!
//! The engine reads committed changes from the bluedb-sql CDC log
//! ([`Database::scan_cdc`]), collapses them last-writer-wins per primary key,
//! and publishes each table's final state through a [`LakehouseWriter`] as one
//! Iceberg snapshot — then garbage-collects the log through the sealed
//! watermark. The opt-out registry (which tables are mirrored) is persisted
//! alongside the tables so a freshly promoted node reopens with the same
//! mirror set. The event-driven seal loop and compaction land in later tasks.

use std::collections::HashMap;
use std::sync::RwLock;

use bluedb_sql::{collapse_lww, CdcConfig, Database};
use gluesql_core::store::{DataRow, Store};
use iceberg::io::FileIO;
use serde::{Deserialize, Serialize};

use crate::schema::table_to_iceberg;
use crate::writer::LakehouseWriter;
use crate::{LakehouseError, Result};

/// On-disk mirror registry (JSON at `<root>/lakehouse/_registry.json`).
#[derive(Default, Serialize, Deserialize)]
struct Registry {
    /// Mirror tables by default (opt-out) vs. only-when-enabled (opt-in).
    default_on: bool,
    /// Explicit per-table enable flags, overriding `default_on`.
    tables: HashMap<String, bool>,
}

/// The lakehouse mirror engine.
pub struct LakehouseEngine {
    file_io: FileIO,
    /// Object-storage root under which both the registry and the Iceberg tables
    /// live (`<root>/lakehouse/_registry.json`, `<root>/<namespace>/<table>/…`).
    root: String,
    /// Iceberg namespace for the mirrored tables (one per tenant; default tenant
    /// for v1).
    namespace: String,
    db: Database,
    /// Shared CDC control (also consulted by the bluedb-sql commit path).
    cdc: CdcConfig,
    /// In-memory copy of the registry's per-table flags + default.
    state: RwLock<Registry>,
}

impl LakehouseEngine {
    fn registry_path(root: &str) -> String {
        format!("{root}/lakehouse/_registry.json")
    }

    /// Reopen the engine over `root`, restoring the persisted registry and
    /// applying it to `cdc` so the commit path mirrors the same tables. Creates
    /// an empty registry if none exists.
    pub async fn reopen(
        file_io: FileIO,
        root: impl Into<String>,
        namespace: impl Into<String>,
        db: Database,
        cdc: CdcConfig,
    ) -> Result<Self> {
        let root = root.into();
        let path = Self::registry_path(&root);
        let registry: Registry = if file_io.exists(&path).await? {
            let bytes = file_io.new_input(&path)?.read().await?;
            serde_json::from_slice(&bytes)?
        } else {
            Registry::default()
        };
        // Mirror the registry into the shared CDC control.
        cdc.default_on
            .store(registry.default_on, std::sync::atomic::Ordering::Relaxed);
        for (table, on) in &registry.tables {
            cdc.set_table(table, *on);
        }
        Ok(Self {
            file_io,
            root,
            namespace: namespace.into(),
            db,
            cdc,
            state: RwLock::new(registry),
        })
    }

    /// Persist the in-memory registry back to object storage.
    async fn persist_registry(&self) -> Result<()> {
        let bytes = {
            let state = self.state.read().unwrap();
            serde_json::to_vec(&*state)?
        };
        let path = Self::registry_path(&self.root);
        self.file_io
            .new_output(&path)?
            .write(bytes.into())
            .await?;
        Ok(())
    }

    /// Enable mirroring for `table` (persisted). Backfill of pre-existing rows
    /// lands in a later task; this records intent and updates the CDC control.
    pub async fn enable_table(&self, table: &str) -> Result<()> {
        self.cdc.set_table(table, true);
        self.state
            .write()
            .unwrap()
            .tables
            .insert(table.to_string(), true);
        self.persist_registry().await
    }

    /// Disable mirroring for `table` (persisted).
    pub async fn disable_table(&self, table: &str) -> Result<()> {
        self.cdc.set_table(table, false);
        self.state
            .write()
            .unwrap()
            .tables
            .insert(table.to_string(), false);
        self.persist_registry().await
    }

    /// Is `table` currently mirrored (effective `default_on XOR override`)?
    pub fn is_mirrored(&self, table: &str) -> bool {
        self.cdc.is_enabled(table)
    }

    /// Tables explicitly enabled in the registry.
    pub fn mirrored_tables(&self) -> Vec<String> {
        let state = self.state.read().unwrap();
        state
            .tables
            .iter()
            .filter(|(_, on)| **on)
            .map(|(t, _)| t.clone())
            .collect()
    }

    /// Drain the CDC log into Iceberg: collapse changes last-writer-wins per
    /// table, publish each table's final state as one snapshot, then GC the log
    /// through the sealed watermark. A no-op when the log is empty.
    pub async fn seal(&self) -> Result<()> {
        let entries = self.db.scan_cdc(0).await?;
        let Some(watermark) = entries.iter().map(|(seq, _)| *seq).max() else {
            return Ok(()); // nothing to seal
        };
        let collapsed = collapse_lww(entries);

        for (table, changes) in collapsed {
            self.seal_table(&table, &changes, watermark).await?;
        }

        // Every entry up to `watermark` is now durably published.
        self.db.gc_cdc(watermark).await?;
        Ok(())
    }

    /// Publish one table's collapsed changes as a single Iceberg snapshot.
    async fn seal_table(
        &self,
        table: &str,
        changes: &std::collections::BTreeMap<gluesql_core::data::Key, Option<DataRow>>,
        watermark: i64,
    ) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        let schema = self.fetch_schema(table).await?;

        // Upserts carry the new row; deletes carry just the key.
        let upserts: Vec<(gluesql_core::data::Key, DataRow)> = changes
            .iter()
            .filter_map(|(k, row)| row.clone().map(|r| (k.clone(), r)))
            .collect();
        let deletes: Vec<gluesql_core::data::Key> = changes
            .iter()
            .filter(|(_, row)| row.is_none())
            .map(|(k, _)| k.clone())
            .collect();

        // Sample the upsert rows so complex (list/map) columns infer their inner
        // types; scalar tables need no samples.
        let sample_rows: Vec<&[gluesql_core::data::Value]> = upserts
            .iter()
            .filter_map(|(_, row)| match row {
                DataRow::Vec(vals) => Some(vals.as_slice()),
                DataRow::Map(_) => None,
            })
            .collect();
        let mut writer = self.writer_for(table, &schema, &sample_rows).await?;

        writer.upsert(&upserts).await?;
        writer.delete(&deletes).await?;
        writer.commit_snapshot(watermark).await?;
        Ok(())
    }

    /// Open the [`LakehouseWriter`] for `table` (creating the Iceberg table on
    /// first use, else loading it). `sample_rows` only matter when the table is
    /// being created — they infer `list`/`map` inner types; on reload the
    /// persisted schema wins. Reusable for read-back and compaction.
    pub async fn writer_for(
        &self,
        table: &str,
        schema: &gluesql_core::data::Schema,
        sample_rows: &[&[gluesql_core::data::Value]],
    ) -> Result<LakehouseWriter> {
        let (iceberg_schema, pk_field_id) = table_to_iceberg(schema, sample_rows)?;
        LakehouseWriter::open(
            self.file_io.clone(),
            &self.root,
            &self.namespace,
            table,
            iceberg_schema,
            pk_field_id,
        )
        .await
    }

    /// Fetch a table's gluesql schema through a read connection.
    pub async fn fetch_schema(&self, table: &str) -> Result<gluesql_core::data::Schema> {
        let conn = self.db.connection();
        conn.fetch_schema(table)
            .await
            .map_err(|e| LakehouseError::Sql(bluedb_sql::SqlError::Serde(e.to_string())))?
            .ok_or_else(|| LakehouseError::Schema(format!("table '{table}' not found")))
    }
}
