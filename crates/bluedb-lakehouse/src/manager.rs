//! [`LakehouseManager`] — the **multi-tenant** front for the mirror.
//!
//! Each tenant has its own [`LakehouseEngine`] (own CDC log, own registry, own
//! Iceberg namespace). The manager owns the per-tenant engines in a map, creates
//! one lazily the first time a tenant is mirrored, and restores all previously
//! seen tenants on promote/failover from a small **tenant index**
//! (`<root>/_tenants.json`).
//!
//! It also owns the background loops. Crucially there is **one** seal loop and
//! **one** compaction loop for the whole node, not one per tenant: the CDC seal
//! signal is a single shared [`Notify`](tokio::sync::Notify), and
//! `notify_one()` wakes exactly one waiter — so N per-tenant loops sharing it
//! would starve. The single loop wakes on any mirror-enabled commit and seals
//! every tenant (an idle tenant's `seal()` is a cheap no-op).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bluedb_sql::{CdcConfig, Database, LhPragma};
use iceberg::io::FileIO;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Instant};

use crate::engine::LakehouseEngine;
use crate::Result;

/// Tunables for the background seal + compaction loops (sourced from env by the
/// server; see `bluedb-server`).
#[derive(Clone, Copy)]
pub struct LakehouseConfig {
    /// Coalesce a burst of commits this long before sealing.
    pub seal_debounce: Duration,
    /// Cap on how long a steady write stream delays a seal.
    pub seal_max_interval: Duration,
    /// How often the compaction loop runs.
    pub compaction_interval: Duration,
    /// Minor-compact (bin-pack small data files) once a table exceeds this many
    /// data files.
    pub max_data_files: usize,
    /// Major-compact (whole-table rewrite, which reclaims equality-delete files)
    /// once a table exceeds this many delete files. Checked first, since only the
    /// major pass clears deletes.
    pub max_delete_files: usize,
}

/// Which compaction (if any) a table needs this round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Compaction {
    None,
    /// Incremental bin-pack of small data files.
    Minor,
    /// Whole-table rewrite that also reclaims delete files.
    Major,
}

/// Decide the compaction for a table from its live file counts. Major (which is
/// the only pass that reclaims delete files) wins when deletes have piled up;
/// otherwise a minor bin-pack runs once data files exceed the threshold.
fn choose_compaction(data_files: usize, delete_files: usize, cfg: &LakehouseConfig) -> Compaction {
    if delete_files > cfg.max_delete_files {
        Compaction::Major
    } else if data_files > cfg.max_data_files {
        Compaction::Minor
    } else {
        Compaction::None
    }
}

/// Owns one [`LakehouseEngine`] per tenant plus the shared background loops.
pub struct LakehouseManager {
    file_io: FileIO,
    root: String,
    db: Database,
    cdc: CdcConfig,
    cfg: LakehouseConfig,
    /// tenant → engine. Populated on promote from the tenant index and lazily on
    /// first mirror of a new tenant.
    engines: RwLock<HashMap<String, Arc<LakehouseEngine>>>,
    /// Serializes read-modify-write of the on-disk tenant index.
    index_lock: tokio::sync::Mutex<()>,
    /// Background seal + compaction task handles (aborted on [`Self::shutdown`]).
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl LakehouseManager {
    fn index_path(&self) -> String {
        format!("{}/_tenants.json", self.root)
    }

    /// Open the manager over `root`, restore every previously-seen tenant's
    /// engine, and spawn the shared seal + compaction loops.
    pub async fn open(
        file_io: FileIO,
        root: impl Into<String>,
        db: Database,
        cdc: CdcConfig,
        cfg: LakehouseConfig,
    ) -> Result<Arc<Self>> {
        let mgr = Arc::new(Self {
            file_io,
            root: root.into(),
            db,
            cdc,
            cfg,
            engines: RwLock::new(HashMap::new()),
            index_lock: tokio::sync::Mutex::new(()),
            handles: Mutex::new(Vec::new()),
        });
        mgr.reopen_all().await?;
        mgr.clone().spawn_loops();
        Ok(mgr)
    }

    /// Warm an engine for every tenant recorded in the index (restores the
    /// durable mirror set on promote/failover). New clusters have no index → no
    /// engines until the first `PRAGMA lakehouse_mirror`.
    async fn reopen_all(&self) -> Result<()> {
        for tenant in self.read_index().await? {
            self.engine_for(&tenant).await?;
        }
        Ok(())
    }

    /// The engine for `tenant`, creating (and persisting to the index) on first
    /// use. Idempotent and race-safe.
    pub async fn engine_for(&self, tenant: &str) -> Result<Arc<LakehouseEngine>> {
        if let Some(e) = self.engines.read().await.get(tenant) {
            return Ok(e.clone());
        }
        let engine = {
            let mut map = self.engines.write().await;
            if let Some(e) = map.get(tenant) {
                return Ok(e.clone()); // lost a race; reuse the winner
            }
            let engine = Arc::new(
                LakehouseEngine::reopen(
                    self.file_io.clone(),
                    &self.root,
                    tenant,
                    self.db.clone(),
                    self.cdc.clone(),
                )
                .await?,
            );
            map.insert(tenant.to_string(), engine.clone());
            engine
        };
        self.register_tenant(tenant).await?;
        Ok(engine)
    }

    /// Apply a `PRAGMA lakehouse_mirror` for `tenant` (creating its engine).
    pub async fn apply_pragma(&self, tenant: &str, pragma: LhPragma) -> Result<()> {
        self.engine_for(tenant).await?.apply_pragma(pragma).await
    }

    /// Seal every tenant's pending CDC into Iceberg (a no-op per idle tenant).
    pub async fn seal_all(&self) -> Result<()> {
        for engine in self.snapshot_engines().await {
            if let Err(err) = engine.seal().await {
                eprintln!("lakehouse: seal({}) failed: {err}", engine.namespace());
            }
        }
        Ok(())
    }

    /// Compact every tenant's tables: a major (delete-reclaiming) rewrite when
    /// delete files have piled up, else a minor bin-pack when data files exceed
    /// the threshold (see [`choose_compaction`]).
    async fn compact_all(&self) {
        for engine in self.snapshot_engines().await {
            for table in engine.mirrored_tables() {
                let data_files = match engine.data_file_count(&table).await {
                    Ok(n) => n,
                    Err(err) => {
                        eprintln!(
                            "lakehouse: data_file_count('{}/{table}') failed: {err}",
                            engine.namespace()
                        );
                        continue;
                    }
                };
                let delete_files = engine.delete_file_count(&table).await.unwrap_or(0);
                let result = match choose_compaction(data_files, delete_files, &self.cfg) {
                    Compaction::Major => engine.compact(&table).await,
                    Compaction::Minor => engine.compact_incremental(&table).await,
                    Compaction::None => Ok(()),
                };
                if let Err(err) = result {
                    eprintln!(
                        "lakehouse: compaction of '{}/{table}' failed: {err}",
                        engine.namespace()
                    );
                }
            }
        }
    }

    /// All Iceberg namespaces with a live engine (sorted), for the REST catalog.
    pub async fn namespaces(&self) -> Vec<String> {
        let mut ns: Vec<String> = self
            .snapshot_engines()
            .await
            .iter()
            .map(|e| e.namespace().to_string())
            .collect();
        ns.sort();
        ns.dedup();
        ns
    }

    /// The engine publishing under Iceberg namespace `ns`, if any.
    pub async fn engine_for_namespace(&self, ns: &str) -> Option<Arc<LakehouseEngine>> {
        self.snapshot_engines()
            .await
            .into_iter()
            .find(|e| e.namespace() == ns)
    }

    /// Stop the background loops (called on demote; the next promote reopens).
    pub fn shutdown(&self) {
        for h in self.handles.lock().unwrap().drain(..) {
            h.abort();
        }
    }

    // --- internals ---------------------------------------------------------

    async fn snapshot_engines(&self) -> Vec<Arc<LakehouseEngine>> {
        self.engines.read().await.values().cloned().collect()
    }

    /// The shared seal loop (event-driven, debounced) + compaction loop.
    fn spawn_loops(self: Arc<Self>) {
        let seal = {
            let me = self.clone();
            tokio::spawn(async move {
                loop {
                    me.cdc.wait_for_changes().await;
                    let deadline = Instant::now() + me.cfg.seal_max_interval;
                    loop {
                        tokio::select! {
                            _ = me.cdc.wait_for_changes() => {
                                if Instant::now() >= deadline { break; }
                            }
                            _ = sleep(me.cfg.seal_debounce) => break,
                        }
                    }
                    if let Err(err) = me.seal_all().await {
                        eprintln!("lakehouse: seal_all failed: {err}");
                    }
                }
            })
        };
        let compact = {
            let me = self.clone();
            tokio::spawn(async move {
                loop {
                    sleep(me.cfg.compaction_interval).await;
                    me.compact_all().await;
                }
            })
        };
        *self.handles.lock().unwrap() = vec![seal, compact];
    }

    async fn read_index(&self) -> Result<Vec<String>> {
        let path = self.index_path();
        if !self.file_io.exists(&path).await? {
            return Ok(Vec::new());
        }
        let bytes = self.file_io.new_input(&path)?.read().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Record `tenant` in the durable index (idempotent), so a future promote
    /// restores its engine. Serialized against concurrent registrations.
    async fn register_tenant(&self, tenant: &str) -> Result<()> {
        let _g = self.index_lock.lock().await;
        let mut tenants = self.read_index().await?;
        if tenants.iter().any(|t| t == tenant) {
            return Ok(());
        }
        tenants.push(tenant.to_string());
        let bytes = serde_json::to_vec(&tenants)?;
        self.file_io
            .new_output(self.index_path())?
            .write(bytes.into())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(max_data_files: usize, max_delete_files: usize) -> LakehouseConfig {
        LakehouseConfig {
            seal_debounce: Duration::from_millis(1),
            seal_max_interval: Duration::from_millis(1),
            compaction_interval: Duration::from_millis(1),
            max_data_files,
            max_delete_files,
        }
    }

    #[test]
    fn chooses_major_when_delete_files_pile_up() {
        // Deletes over threshold → major, even with few data files.
        assert_eq!(choose_compaction(1, 5, &cfg(8, 4)), Compaction::Major);
    }

    #[test]
    fn chooses_minor_when_only_data_files_exceed() {
        assert_eq!(choose_compaction(10, 0, &cfg(8, 4)), Compaction::Minor);
    }

    #[test]
    fn major_takes_precedence_over_minor() {
        // Both thresholds exceeded → major (it also reclaims deletes).
        assert_eq!(choose_compaction(20, 9, &cfg(8, 4)), Compaction::Major);
    }

    #[test]
    fn no_compaction_under_both_thresholds() {
        assert_eq!(choose_compaction(8, 4, &cfg(8, 4)), Compaction::None);
    }
}
