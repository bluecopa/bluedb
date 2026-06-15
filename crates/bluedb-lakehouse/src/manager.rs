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
    /// Compact a table once it exceeds this many data files.
    pub max_data_files: usize,
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

    /// Compact every tenant's tables that exceed the file-count threshold.
    async fn compact_all(&self) {
        for engine in self.snapshot_engines().await {
            for table in engine.mirrored_tables() {
                match engine.data_file_count(&table).await {
                    Ok(n) if n > self.cfg.max_data_files => {
                        if let Err(err) = engine.compact(&table).await {
                            eprintln!(
                                "lakehouse: compaction of '{}/{table}' failed: {err}",
                                engine.namespace()
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(err) => eprintln!(
                        "lakehouse: data_file_count('{}/{table}') failed: {err}",
                        engine.namespace()
                    ),
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
