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
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bluedb_sql::{collapse_lww, CdcConfig, Database, LhPragma, DEFAULT_TENANT};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use gluesql_core::store::{DataRow, Store};
use iceberg::io::FileIO;
use serde::{Deserialize, Serialize};

use crate::schema::table_to_iceberg;
use crate::writer::LakehouseWriter;
use crate::{LakehouseError, Result};

/// The Iceberg namespace a tenant publishes under. The default tenant (`"_"`)
/// maps to the conventional `default` namespace; any other tenant uses its own
/// id verbatim, giving each tenant an isolated namespace in the shared catalog.
pub fn namespace_for_tenant(tenant: &str) -> String {
    if tenant == DEFAULT_TENANT {
        "default".to_string()
    } else {
        tenant.to_string()
    }
}

/// Inverse of [`namespace_for_tenant`]: the tenant that owns Iceberg namespace
/// `ns`. Used to authorize catalog access against the namespace's tenant.
pub fn tenant_for_namespace(ns: &str) -> String {
    if ns == "default" {
        DEFAULT_TENANT.to_string()
    } else {
        ns.to_string()
    }
}

/// On-disk mirror registry (JSON at `<root>/lakehouse/_registry.json`).
#[derive(Default, Serialize, Deserialize)]
struct Registry {
    /// Mirror tables by default (opt-out) vs. only-when-enabled (opt-in).
    default_on: bool,
    /// Explicit per-table enable flags, overriding `default_on`.
    tables: HashMap<String, bool>,
    /// Tables that have actually been materialized to Iceberg (sealed at least
    /// once). The authoritative list for the REST catalog — independent of
    /// whether a table is enabled explicitly or by the opt-out default.
    #[serde(default)]
    materialized: std::collections::BTreeSet<String>,
}

/// The lakehouse mirror engine — **one per tenant**. Its `tenant` scopes which
/// CDC log it drains and which connection it reads through; its `namespace`
/// scopes where the Iceberg tables and registry live.
pub struct LakehouseEngine {
    file_io: FileIO,
    /// Object-storage root under which every tenant's registry and Iceberg
    /// tables live (`<root>/<namespace>/_registry.json`,
    /// `<root>/<namespace>/<table>/…`).
    root: String,
    /// The tenant whose CDC log this engine drains (its keyspace prefix).
    tenant: String,
    /// Iceberg namespace for this tenant's mirrored tables (one per tenant).
    namespace: String,
    db: Database,
    /// Shared CDC control (also consulted by the bluedb-sql commit path).
    cdc: CdcConfig,
    /// In-memory copy of the registry's per-table flags + default.
    state: RwLock<Registry>,
}

impl LakehouseEngine {
    fn registry_path(root: &str, namespace: &str) -> String {
        format!("{root}/{namespace}/_registry.json")
    }

    /// Reopen the engine for `tenant` over `root`, restoring that tenant's
    /// persisted registry and applying it to `cdc` so the commit path mirrors
    /// the same tables. Creates an empty registry if none exists.
    pub async fn reopen(
        file_io: FileIO,
        root: impl Into<String>,
        tenant: impl Into<String>,
        db: Database,
        cdc: CdcConfig,
    ) -> Result<Self> {
        let root = root.into();
        let tenant = tenant.into();
        let namespace = namespace_for_tenant(&tenant);
        let path = Self::registry_path(&root, &namespace);
        let registry: Registry = if file_io.exists(&path).await? {
            let bytes = file_io.new_input(&path)?.read().await?;
            serde_json::from_slice(&bytes)?
        } else {
            Registry::default()
        };
        // Mirror the registry into the shared CDC control, scoped to this tenant.
        cdc.set_default(&tenant, registry.default_on);
        for (table, on) in &registry.tables {
            cdc.set_table(&tenant, table, *on);
        }
        Ok(Self {
            file_io,
            root,
            tenant,
            namespace,
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
        let path = Self::registry_path(&self.root, &self.namespace);
        self.file_io
            .new_output(&path)?
            .write(bytes.into())
            .await?;
        Ok(())
    }

    /// Enable mirroring for `table` (persisted), then **backfill** any rows that
    /// already exist (committed before CDC was on) into Iceberg as one snapshot,
    /// so the mirror starts complete and subsequent CDC layers on top.
    pub async fn enable_table(&self, table: &str) -> Result<()> {
        self.cdc.set_table(&self.tenant, table, true);
        self.state
            .write()
            .unwrap()
            .tables
            .insert(table.to_string(), true);
        self.persist_registry().await?;
        self.backfill(table).await
    }

    /// Publish a table's current rows as one Iceberg snapshot. A no-op if the
    /// table doesn't exist or is empty. Used by [`Self::enable_table`].
    async fn backfill(&self, table: &str) -> Result<()> {
        let Some(schema) = self.try_fetch_schema(table).await? else {
            return Ok(()); // table not created yet — nothing to backfill
        };
        let rows = self.scan_all_rows(table).await?;
        if rows.is_empty() {
            return Ok(());
        }
        let sample_rows: Vec<&[gluesql_core::data::Value]> = rows
            .iter()
            .filter_map(|(_, row)| match row {
                DataRow::Vec(vals) => Some(vals.as_slice()),
                DataRow::Map(_) => None,
            })
            .collect();
        // Watermark = current max CDC sequence, so the snapshot reflects state up
        // to now and later seals (higher sequences) layer on cleanly.
        let watermark = self
            .db
            .scan_cdc(&self.tenant, 0)
            .await?
            .last()
            .map(|(seq, _)| *seq)
            .unwrap_or(0);
        let mut writer = self.writer_for(table, &schema, &sample_rows).await?;
        writer.upsert(&rows).await?;
        writer.commit_snapshot(watermark).await?;
        self.mark_materialized(table).await?;
        Ok(())
    }

    /// Disable mirroring for `table` (persisted).
    pub async fn disable_table(&self, table: &str) -> Result<()> {
        self.cdc.set_table(&self.tenant, table, false);
        self.state
            .write()
            .unwrap()
            .tables
            .insert(table.to_string(), false);
        self.persist_registry().await
    }

    /// Apply a parsed `PRAGMA lakehouse_mirror` directive (the runtime opt-out
    /// control, spec §8): flip the global default or override one table. Persisted
    /// to the registry; flipping a table on backfills it.
    pub async fn apply_pragma(&self, pragma: LhPragma) -> Result<()> {
        match pragma {
            LhPragma::GlobalDefault(on) => {
                self.cdc.set_default(&self.tenant, on);
                {
                    let mut st = self.state.write().unwrap();
                    st.default_on = on;
                    // Re-apply explicit per-table flags relative to the new default.
                    for (table, flag) in &st.tables {
                        self.cdc.set_table(&self.tenant, table, *flag);
                    }
                }
                self.persist_registry().await
            }
            LhPragma::Table(table, true) => self.enable_table(&table).await,
            LhPragma::Table(table, false) => self.disable_table(&table).await,
        }
    }

    /// Is `table` currently mirrored (effective `default_on XOR override`)?
    pub fn is_mirrored(&self, table: &str) -> bool {
        self.cdc.is_enabled(&self.tenant, table)
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

    /// Spawn the event-driven, debounced seal loop. It blocks until a
    /// mirror-enabled commit signals new changes, coalesces a burst for up to
    /// `debounce` (but never delays a steady stream past `max_interval`), then
    /// seals once. An idle table never produces a snapshot — the loop simply
    /// waits. Replaces any fixed-interval scheduler.
    pub fn spawn_seal_loop(
        self: Arc<Self>,
        debounce: Duration,
        max_interval: Duration,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                // Block (cheaply) until the first change of a quiet period.
                self.cdc.wait_for_changes().await;
                // Coalesce a burst: keep extending by `debounce` as long as new
                // changes keep arriving, capped at `max_interval` from the first.
                let deadline = Instant::now() + max_interval;
                loop {
                    tokio::select! {
                        _ = self.cdc.wait_for_changes() => {
                            if Instant::now() >= deadline {
                                break;
                            }
                        }
                        _ = tokio::time::sleep(debounce) => break,
                    }
                }
                if let Err(err) = self.seal().await {
                    eprintln!("lakehouse: seal failed: {err}");
                }
            }
        })
    }

    /// Drain the CDC log into Iceberg: collapse changes last-writer-wins per
    /// table, publish each table's final state as one snapshot, then GC the log
    /// through the sealed watermark. A no-op when the log is empty.
    pub async fn seal(&self) -> Result<()> {
        let entries = self.db.scan_cdc(&self.tenant, 0).await?;
        let Some(watermark) = entries.iter().map(|(seq, _)| *seq).max() else {
            return Ok(()); // nothing to seal
        };
        let collapsed = collapse_lww(entries);

        for (table, changes) in collapsed {
            self.seal_table(&table, &changes, watermark).await?;
        }

        // Every entry up to `watermark` is now durably published.
        self.db.gc_cdc(&self.tenant, watermark).await?;
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
        self.mark_materialized(table).await?;
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
        let sort_field_ids = self.sort_field_ids(table, schema, pk_field_id).await?;
        LakehouseWriter::open(
            self.file_io.clone(),
            &self.root,
            &self.namespace,
            table,
            iceberg_schema,
            pk_field_id,
            &sort_field_ids,
        )
        .await
    }

    /// The Iceberg field-ids the table's data is sorted by (declared as the sort
    /// order at creation). For a composite-PK table these are the user component
    /// columns (resolved from the Pk catalog); otherwise the single PK. Field-ids
    /// are positional in gluesql schema order (matching [`table_to_iceberg`]).
    async fn sort_field_ids(
        &self,
        table: &str,
        schema: &gluesql_core::data::Schema,
        pk_field_id: i32,
    ) -> Result<Vec<i32>> {
        let components = self
            .db
            .connection_for_tenant(&self.tenant)
            .pk_columns(table)
            .await
            .map_err(LakehouseError::Sql)?;
        let Some(components) = components else {
            return Ok(vec![pk_field_id]); // single-column PK
        };
        let names: Vec<&str> = schema
            .column_defs
            .as_ref()
            .map(|defs| defs.iter().map(|c| c.name.as_str()).collect())
            .unwrap_or_default();
        Ok(components
            .iter()
            .filter_map(|c| names.iter().position(|n| n == c).map(|i| i as i32 + 1))
            .collect())
    }

    /// Compact a mirrored table's Iceberg files (memory-bounded streaming
    /// rewrite; see [`LakehouseWriter::compact`]). No-op for ≤1 data file.
    pub async fn compact(&self, table: &str) -> Result<()> {
        let schema = self.fetch_schema(table).await?;
        let mut writer = self.writer_for(table, &schema, &[]).await?;
        let watermark = writer.current_watermark().unwrap_or(0);
        writer.compact(watermark).await
    }

    /// Number of live data files in a table's current Iceberg state.
    pub async fn data_file_count(&self, table: &str) -> Result<usize> {
        let schema = self.fetch_schema(table).await?;
        self.writer_for(table, &schema, &[]).await?.data_file_count().await
    }

    /// Spawn a throttled background worker that compacts mirrored tables whose
    /// data-file count exceeds `min_files`, once per `interval`. Compaction is
    /// memory-bounded regardless of backlog, so this just bounds read
    /// amplification (file count).
    pub fn spawn_compaction_worker(
        self: Arc<Self>,
        interval: Duration,
        min_files: usize,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                for table in self.mirrored_tables() {
                    match self.data_file_count(&table).await {
                        Ok(n) if n > min_files => {
                            if let Err(err) = self.compact(&table).await {
                                eprintln!("lakehouse: compaction of '{table}' failed: {err}");
                            }
                        }
                        Ok(_) => {}
                        Err(err) => {
                            eprintln!("lakehouse: data_file_count('{table}') failed: {err}")
                        }
                    }
                }
            }
        })
    }

    /// The Iceberg namespace this engine publishes under (for the REST catalog).
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The tenant this engine mirrors (its CDC-log / keyspace prefix).
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// The current `metadata.json` location for a sealed table, or `None` if the
    /// table has no Iceberg table yet. Read-only — creates nothing.
    pub async fn table_metadata_location(&self, table: &str) -> Result<Option<String>> {
        let dir = format!("{}/{}/{}/metadata", self.root, self.namespace, table);
        let hint = format!("{dir}/version-hint.text");
        if !self.file_io.exists(&hint).await? {
            return Ok(None);
        }
        let raw = self.file_io.new_input(&hint)?.read().await?;
        let version: u64 = String::from_utf8_lossy(&raw)
            .trim()
            .parse()
            .map_err(|e| LakehouseError::Iceberg(format!("bad version-hint: {e}")))?;
        Ok(Some(format!("{dir}/v{version}.metadata.json")))
    }

    /// Load a table's current `(metadata_location, metadata_json)` for an Iceberg
    /// REST-catalog `loadTable`, or `None` if it isn't mirrored yet.
    pub async fn table_metadata_json(
        &self,
        table: &str,
    ) -> Result<Option<(String, serde_json::Value)>> {
        let Some(loc) = self.table_metadata_location(table).await? else {
            return Ok(None);
        };
        let bytes = self.file_io.new_input(&loc)?.read().await?;
        let json: serde_json::Value = serde_json::from_slice(&bytes)?;
        Ok(Some((loc, json)))
    }

    /// Tables that have actually been materialized to Iceberg (sealed at least
    /// once) and still have metadata on disk — what the REST catalog lists.
    pub async fn list_iceberg_tables(&self) -> Result<Vec<String>> {
        let candidates: Vec<String> = {
            self.state.read().unwrap().materialized.iter().cloned().collect()
        };
        let mut out = Vec::new();
        for table in candidates {
            if self.table_metadata_location(&table).await?.is_some() {
                out.push(table);
            }
        }
        out.sort();
        Ok(out)
    }

    /// Record that `table` now has an Iceberg table (persists on first sight).
    async fn mark_materialized(&self, table: &str) -> Result<()> {
        let newly = self
            .state
            .write()
            .unwrap()
            .materialized
            .insert(table.to_string());
        if newly {
            self.persist_registry().await?;
        }
        Ok(())
    }

    /// Fetch a table's gluesql schema through a read connection.
    pub async fn fetch_schema(&self, table: &str) -> Result<gluesql_core::data::Schema> {
        self.try_fetch_schema(table)
            .await?
            .ok_or_else(|| LakehouseError::Schema(format!("table '{table}' not found")))
    }

    /// Fetch a table's schema, or `None` if it doesn't exist.
    async fn try_fetch_schema(&self, table: &str) -> Result<Option<gluesql_core::data::Schema>> {
        self.db
            .connection_for_tenant(&self.tenant)
            .fetch_schema(table)
            .await
            .map_err(glue_err)
    }

    /// Read every current row of `table` (engine-internal scan — guardrail-exempt,
    /// since the seal path must read full tables for backfill/compaction).
    async fn scan_all_rows(
        &self,
        table: &str,
    ) -> Result<Vec<(gluesql_core::data::Key, DataRow)>> {
        use futures::TryStreamExt;
        let conn = self.db.connection_for_tenant(&self.tenant);
        let iter = conn.scan_data(table).await.map_err(glue_err)?;
        iter.try_collect::<Vec<_>>().await.map_err(glue_err)
    }
}

/// Map a gluesql execution error into a [`LakehouseError`].
fn glue_err(e: gluesql_core::error::Error) -> LakehouseError {
    LakehouseError::Sql(bluedb_sql::SqlError::Serde(e.to_string()))
}
