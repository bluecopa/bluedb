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

use std::collections::{BTreeSet, HashMap};
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
    /// Bin-pack target file size for incremental compaction (set via
    /// `PRAGMA lakehouse_target_file_bytes`). `None` ⇒ use the engine default.
    #[serde(default)]
    target_file_bytes: Option<u64>,
    /// Read-your-writes freshness tolerance for the analytical path, in seal
    /// cycles (set via `PRAGMA bluedb_read_wait_seal_n`). `None` ⇒ default (1).
    #[serde(default)]
    read_wait_seal_n: Option<u64>,
}

/// Default incremental-compaction bin-pack target when no PRAGMA is set:
/// `BLUEDB_LAKEHOUSE_TARGET_FILE_BYTES` if valid, else 128 MiB.
fn default_target_bytes() -> u64 {
    std::env::var("BLUEDB_LAKEHOUSE_TARGET_FILE_BYTES")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(128 * 1024 * 1024)
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
            LhPragma::TargetFileBytes(bytes) => self.set_target_file_bytes(bytes).await,
            LhPragma::ReadWaitSealN(n) => self.set_read_wait_seal_n(n).await,
        }
    }

    /// Set the incremental-compaction bin-pack target file size (persisted to the
    /// registry, restored on promote/failover). `0` clears it (revert to default).
    pub async fn set_target_file_bytes(&self, bytes: u64) -> Result<()> {
        self.state.write().unwrap().target_file_bytes = (bytes > 0).then_some(bytes);
        self.persist_registry().await
    }

    /// The configured bin-pack target file size, or the engine default when
    /// unset (see [`default_target_bytes`]).
    pub fn target_file_bytes(&self) -> u64 {
        self.state
            .read()
            .unwrap()
            .target_file_bytes
            .unwrap_or_else(default_target_bytes)
    }

    /// Set the analytical read-your-writes freshness tolerance, in seal cycles
    /// (persisted to the registry, restored on promote/failover). `0` clears it
    /// (revert to the default of 1).
    pub async fn set_read_wait_seal_n(&self, n: u64) -> Result<()> {
        self.state.write().unwrap().read_wait_seal_n = (n > 0).then_some(n);
        self.persist_registry().await
    }

    /// The configured analytical freshness tolerance in seal cycles, or `1`
    /// (the default) when unset.
    pub fn read_wait_seal_n(&self) -> u64 {
        self.state.read().unwrap().read_wait_seal_n.unwrap_or(1)
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
        let slots = self
            .db
            .connection_for_tenant(&self.tenant)
            .column_slots(table)
            .await
            .map_err(LakehouseError::Sql)?;
        let (iceberg_schema, pk_field_id) =
            table_to_iceberg(schema, sample_rows, slots.as_deref())?;
        let sort_field_ids = self
            .sort_field_ids(table, schema, pk_field_id, slots.as_deref())
            .await?;
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
    /// are slot-based (`slot + 1`, identity when no catalog), matching
    /// [`table_to_iceberg`], so the sort order survives `ALTER`.
    async fn sort_field_ids(
        &self,
        table: &str,
        schema: &gluesql_core::data::Schema,
        pk_field_id: i32,
        slots: Option<&[u32]>,
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
        // Component field-id = slot(pos) + 1 (identity when no catalog), so the
        // sort order stays stable across ALTER.
        let field_id_of = |pos: usize| -> i32 {
            match slots {
                Some(s) => s[pos] as i32 + 1,
                None => pos as i32 + 1,
            }
        };
        Ok(components
            .iter()
            .filter_map(|c| names.iter().position(|n| n == c).map(field_id_of))
            .collect())
    }

    /// **Major** compaction: whole-table memory-bounded rewrite that also
    /// reclaims equality-delete files (see [`LakehouseWriter::compact`]). No-op
    /// for ≤1 data file.
    pub async fn compact(&self, table: &str) -> Result<()> {
        let schema = self.fetch_schema(table).await?;
        let mut writer = self.writer_for(table, &schema, &[]).await?;
        let watermark = writer.current_watermark().unwrap_or(0);
        writer.compact(watermark).await
    }

    /// **Minor** compaction: incremental bin-pack of the table's small data files
    /// (see [`LakehouseWriter::compact_incremental`]), using the configured
    /// [`Self::target_file_bytes`]. Leaves large files and all delete files alone.
    pub async fn compact_incremental(&self, table: &str) -> Result<()> {
        let schema = self.fetch_schema(table).await?;
        let mut writer = self.writer_for(table, &schema, &[]).await?;
        let watermark = writer.current_watermark().unwrap_or(0);
        writer
            .compact_incremental(self.target_file_bytes(), watermark)
            .await
    }

    /// Number of live data files in a table's current Iceberg state.
    pub async fn data_file_count(&self, table: &str) -> Result<usize> {
        let schema = self.fetch_schema(table).await?;
        self.writer_for(table, &schema, &[]).await?.data_file_count().await
    }

    /// Number of live equality-delete files in a table's current Iceberg state
    /// (drives the worker's choice of major vs minor compaction).
    pub async fn delete_file_count(&self, table: &str) -> Result<usize> {
        let schema = self.fetch_schema(table).await?;
        self.writer_for(table, &schema, &[]).await?.delete_file_count().await
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

    /// The CDC watermark that is durably sealed into Iceberg for this tenant.
    ///
    /// Every seal pass stamps the same max-CDC-seq into all tables it touches
    /// (see [`Self::seal`] and [`crate::writer::LakehouseWriter::commit_snapshot`]),
    /// so any sealed table's `current_watermark()` equals the tenant watermark.
    /// This method returns the max watermark across all materialized tables, or 0
    /// if none have been sealed yet.
    ///
    /// Used by the HTAP read tier to check freshness against a client's
    /// `X-Bluedb-Min-Watermark` request header.
    pub async fn sealed_watermark(&self) -> i64 {
        let tables: Vec<String> = {
            self.state.read().unwrap().materialized.iter().cloned().collect()
        };
        let mut max = 0i64;
        for table in tables {
            let Ok(schema) = self.try_fetch_schema(&table).await else { continue; };
            let Some(schema) = schema else { continue; };
            let Ok(writer) = self.writer_for(&table, &schema, &[]).await else { continue; };
            if let Some(wm) = writer.current_watermark() {
                max = max.max(wm);
            }
        }
        max
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

    /// Load the current sealed [`iceberg::table::Table`] for `table`, for use
    /// by the analytical query engine (DataFusion over sealed Iceberg snapshots).
    /// Returns `None` if the table has not been sealed (materialized) yet.
    pub async fn current_iceberg_table(
        &self,
        table: &str,
    ) -> Result<Option<iceberg::table::Table>> {
        let Some(schema) = self.try_fetch_schema(table).await? else {
            return Ok(None);
        };
        // writer_for loads the existing Iceberg metadata (version-hint.text) if it
        // exists, or creates a new empty table — we only want the former case.
        let hint = format!(
            "{}/{}/{}/metadata/version-hint.text",
            self.root, self.namespace, table
        );
        if !self.file_io.exists(&hint).await? {
            return Ok(None);
        }
        let writer = self.writer_for(table, &schema, &[]).await?;
        Ok(Some(writer.to_table()?))
    }

    /// Build an Arrow [`RecordBatch`](arrow_array::RecordBatch) of the table's
    /// **current** rows read straight from the live store — including writes not
    /// yet sealed into Iceberg. Returns `None` if the table doesn't exist.
    ///
    /// This is the writer-local **fresh** analytical read source (HTAP P4): the
    /// active writer always holds rows at least as fresh as any acknowledged
    /// write, so a freshness-gated analytical query can be answered here instead
    /// of waiting for the next seal. It reuses the seal path's exact
    /// gluesql-`Value`→Arrow conversion ([`LakehouseWriter::rows_to_record_batch`]
    /// over the schema [`writer_for`](Self::writer_for) derives), so a column
    /// renders identically whether served fresh from the writer or from a sealed
    /// Iceberg snapshot.
    ///
    /// First cut: serves the **whole** current table (correctness over
    /// efficiency). The Iceberg ∪ unsealed-delta union is out of scope.
    pub async fn current_record_batch(
        &self,
        table: &str,
    ) -> Result<Option<arrow_array::RecordBatch>> {
        let Some(schema) = self.try_fetch_schema(table).await? else {
            return Ok(None);
        };
        let rows = self.scan_all_rows(table).await?;
        let sample_rows: Vec<&[gluesql_core::data::Value]> = rows
            .iter()
            .filter_map(|(_, row)| match row {
                DataRow::Vec(vals) => Some(vals.as_slice()),
                DataRow::Map(_) => None,
            })
            .collect();
        // writer_for derives the Iceberg/Arrow schema from the gluesql schema +
        // sample rows (decimal precision/scale etc.); we only use it to convert
        // rows — nothing is written to object storage.
        let writer = self.writer_for(table, &schema, &sample_rows).await?;
        Ok(Some(writer.rows_to_record_batch(&rows)?))
    }

    /// Build an Arrow [`RecordBatch`] of `table`'s **current** rows as the exact
    /// read-your-writes union of its sealed Iceberg snapshot and its unsealed
    /// CDC tail — the analytical read source for the DataFusion front-door.
    ///
    /// Bulk rows (CDC seq ≤ the snapshot's sealed watermark) are read columnar
    /// from Iceberg (merge-on-read deletes already applied by the reader); the
    /// small tail (seq > watermark) is replayed from the CDC log, collapsed
    /// last-writer-wins, and merged on the primary key: every key touched in the
    /// tail is dropped from the Iceberg side (anti-join), then surviving tail
    /// upserts are appended. A tail delete drops the key from both sides.
    ///
    /// This is the efficient replacement for [`Self::current_record_batch`]'s
    /// whole-table materialization — it reads only the unsealed delta from the
    /// row store, not every row. Returns `None` if `table` does not exist; errors
    /// if it has no single-column primary key (the merge keys on it), matching
    /// the seal path's requirement.
    ///
    /// First cut: returns a single concatenated batch (the tail is bounded by the
    /// seal cadence). A streaming, pushdown-capable `TableProvider` is the
    /// productionization.
    pub async fn merged_record_batch(
        &self,
        table: &str,
    ) -> Result<Option<arrow_array::RecordBatch>> {
        use futures::StreamExt;

        let Some(schema) = self.try_fetch_schema(table).await? else {
            return Ok(None);
        };
        // Merge key: the single PK column position. Errors on no-PK / composite
        // (PK-less tables cannot be merged on read — front-door design decision).
        let pk_idx = single_pk_index(&schema)?;

        // The writer yields both the shared Arrow schema and the sealed watermark.
        let writer = self.writer_for(table, &schema, &[]).await?;
        let arrow_schema = writer.arrow_schema()?;
        let sealed_seq = writer.current_watermark().unwrap_or(0);

        // --- unsealed delta: CDC entries with seq > sealed_seq ----------------
        let entries = self
            .db
            .scan_cdc(&self.tenant, sealed_seq)
            .await
            .map_err(LakehouseError::Sql)?;
        let mut collapsed = collapse_lww(entries);
        let changes = collapsed.remove(table).unwrap_or_default();
        let touched: BTreeSet<gluesql_core::data::Key> = changes.keys().cloned().collect();
        let upserts: Vec<(gluesql_core::data::Key, DataRow)> = changes
            .into_iter()
            .filter_map(|(k, row)| row.map(|r| (k, r)))
            .collect();

        let mut parts: Vec<arrow_array::RecordBatch> = Vec::new();

        // --- bulk side: sealed Iceberg rows minus any tail-touched key --------
        if self.table_metadata_location(table).await?.is_some() {
            let ice = writer.to_table()?;
            let mut stream = ice.scan().build()?.to_arrow().await?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let kept = if touched.is_empty() {
                    batch
                } else {
                    antijoin_batch(&batch, pk_idx, &touched)?
                };
                if kept.num_rows() > 0 {
                    parts.push(kept);
                }
            }
        }

        // --- tail side: surviving upserts -------------------------------------
        if !upserts.is_empty() {
            parts.push(writer.rows_to_record_batch(&upserts)?);
        }

        let merged = arrow_select::concat::concat_batches(&arrow_schema, &parts)
            .map_err(|e| LakehouseError::Iceberg(format!("merge concat: {e}")))?;
        Ok(Some(merged))
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

/// The position of the single primary-key column — the key the analytical merge
/// joins on. Errors if the table has no primary key or a composite one (the v1
/// merge supports single-column keys only), matching [`table_to_iceberg`].
fn single_pk_index(schema: &gluesql_core::data::Schema) -> Result<usize> {
    let columns = schema.column_defs.as_ref().ok_or_else(|| {
        LakehouseError::Schema(format!(
            "table '{}' is schemaless; the analytical merge requires a primary key",
            schema.table_name
        ))
    })?;
    let pks: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.unique.as_ref().is_some_and(|u| u.is_primary))
        .map(|(i, _)| i)
        .collect();
    match pks.as_slice() {
        [only] => Ok(*only),
        [] => Err(LakehouseError::Schema(format!(
            "table '{}' has no primary key; the analytical merge requires one",
            schema.table_name
        ))),
        _ => Err(LakehouseError::Schema(format!(
            "table '{}' has a composite primary key; the v1 analytical merge \
             supports single-column keys only",
            schema.table_name
        ))),
    }
}

/// Drop every Iceberg row whose primary key was touched in the unsealed CDC
/// tail, so the tail's last-writer-wins value (appended separately) shadows the
/// sealed copy. The retained mask is `pk ∉ touched`.
fn antijoin_batch(
    batch: &arrow_array::RecordBatch,
    pk_idx: usize,
    touched: &BTreeSet<gluesql_core::data::Key>,
) -> Result<arrow_array::RecordBatch> {
    use arrow_array::{Array, BooleanArray, Int32Array, Int64Array, StringArray};
    use gluesql_core::data::Key;

    let pk = batch.column(pk_idx);
    let keep: BooleanArray = match pk.data_type() {
        arrow_schema::DataType::Int64 => {
            let a = pk.as_any().downcast_ref::<Int64Array>().unwrap();
            (0..a.len())
                .map(|i| Some(!touched.contains(&Key::I64(a.value(i)))))
                .collect()
        }
        arrow_schema::DataType::Int32 => {
            let a = pk.as_any().downcast_ref::<Int32Array>().unwrap();
            (0..a.len())
                .map(|i| Some(!touched.contains(&Key::I32(a.value(i)))))
                .collect()
        }
        arrow_schema::DataType::Utf8 => {
            let a = pk.as_any().downcast_ref::<StringArray>().unwrap();
            (0..a.len())
                .map(|i| Some(!touched.contains(&Key::Str(a.value(i).to_string()))))
                .collect()
        }
        other => {
            return Err(LakehouseError::Iceberg(format!(
                "analytical merge: unsupported primary-key Arrow type {other:?} \
                 (spike supports Int32/Int64/Utf8)"
            )))
        }
    };
    arrow_select::filter::filter_record_batch(batch, &keep)
        .map_err(|e| LakehouseError::Iceberg(format!("merge anti-join filter: {e}")))
}
