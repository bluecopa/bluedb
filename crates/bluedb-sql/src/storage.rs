//! [`SlateDbStorage`] — a GlueSQL custom storage backed by SlateDB.
//!
//! This wires GlueSQL's pluggable [`Store`]/[`StoreMut`] traits — plus real
//! [`Transaction`], [`Index`]/[`IndexMut`] and [`Metadata`] — onto bluedb's
//! object-storage substrate. Tables (schema'd *and* schemaless), rows, inserts,
//! updates, deletes, ordered scans, secondary indexes and multi-statement
//! transactions all land in one SlateDB keyspace using the encoding documented
//! in [`crate::keyspace`].
//!
//! ## Values
//!
//! Both schemas and rows are serialized with `serde_json` (GlueSQL's `Schema`,
//! `Key` and `DataRow` all derive `serde`). JSON keeps the on-disk form
//! debuggable; swap to `postcard`/`bincode` later if size matters — the seam is
//! [`encode`]/[`decode`].
//!
//! Each stored row value is a [`StoredRow`] holding *both* the GlueSQL `Key`
//! and the `DataRow`. We keep the `Key` in the value (not just baked into the
//! storage key) because [`Key::to_cmp_be_bytes`](gluesql_core::data::Key::to_cmp_be_bytes)
//! is a one-way, order-preserving encoding: a `scan` needs to hand GlueSQL back
//! the original `Key`, which we can only recover by storing it verbatim.
//!
//! ## Multi-tenancy
//!
//! Every key is prefixed with a tenant namespace (see [`crate::keyspace`]), so
//! many tenants can share one [`Db`]. Construct per-tenant storages with
//! [`SlateDbStorage::new_for_tenant`]; [`SlateDbStorage::new`] uses the default
//! tenant [`DEFAULT_TENANT`](crate::keyspace::DEFAULT_TENANT).
//!
//! ## Transactions & isolation
//!
//! GlueSQL drives `BEGIN`/`COMMIT`/`ROLLBACK` through the [`Transaction`] trait.
//! We implement them with a **write-buffer overlay + atomic batch commit** on
//! SlateDB's single-writer model:
//!
//! * `begin` captures a [`DbSnapshot`] (a consistent point-in-time read view)
//!   and opens an empty overlay — an ordered `BTreeMap<Vec<u8>, Option<Vec<u8>>>`
//!   keyed by the encoded storage key (`Some` = put, `None` = tombstone).
//! * **Writes** inside the txn mutate the overlay only. On the writer *every*
//!   statement runs inside such a txn (autocommit statements included — see
//!   `begin`), so all writes are buffered and flushed atomically at commit.
//! * **Reads** inside the txn merge the overlay over the *snapshot*: overlay
//!   entries shadow the base, tombstones hide base rows, and `scan_data`
//!   performs an ordered merge so reads see the txn's own writes in correct
//!   key order (read-your-own-writes).
//! * `commit` applies the whole overlay as a single atomic SlateDB
//!   [`WriteBatch`](slatedb::WriteBatch) via [`Db::write`], then clears it.
//!   `rollback` just drops the overlay.
//!
//! **Isolation level: snapshot isolation.** Base reads inside a transaction go
//! through the snapshot captured at `begin`, so a long-running txn never sees
//! writes committed by others after it began, and its own buffered writes are
//! layered on top. Commit is all-or-nothing (one `WriteBatch`). SlateDB is a
//! single-writer store, so there is no concurrent-writer conflict to detect:
//! the `&mut self` GlueSQL hands us, plus SlateDB's single-writer guarantee,
//! means only one transaction mutates a given `Db` at a time. Index entries and
//! schema records flow through the same overlay, so DDL/`CREATE INDEX`/index
//! maintenance inside a txn roll back too.
//!
//! ## Secondary indexes
//!
//! `CREATE INDEX`/`DROP INDEX` and index-backed query plans go through
//! [`IndexMut`]/[`Index`]. Index definitions and entries live in their own
//! keyspace namespaces (see [`crate::keyspace`]); an index entry's key embeds
//! the order-preserving evaluated value followed by the row's primary key, so a
//! byte-ordered range scan yields rows in indexed-value order. Index entries
//! are maintained on every row mutation **through the same overlay**, so they
//! are transactional and stay consistent across INSERT/UPDATE/DELETE.
//!
//! ## Concurrency / `&mut self`
//!
//! `StoreMut`/`Transaction` methods take `&mut self`, so GlueSQL guarantees
//! exclusivity *within one connection*. But many connections (one per request)
//! share a single [`Db`], and `&mut self` says nothing across them. Two seams
//! coordinate them:
//!
//! * **Auto-increment keys** ([`SeqAllocator`]). `append_data` assigns row keys
//!   from a shared in-memory per-table counter (atomically, under a brief map
//!   lock), *not* by scanning for the current max. So two concurrent autocommit
//!   `INSERT`s always get distinct keys and can't clobber each other — without
//!   any global lock on the write path. The counter lazily initializes from the
//!   live committed max (once per table per writer; re-derived after failover).
//! * **Write lease** ([`WriteLease`]). An explicit `BEGIN` holds it for the whole
//!   block, snapshotting under it, so its read-modify-write (e.g. an `UPDATE`'s
//!   row scan) is serialized against other explicit transactions. Autocommit
//!   statements do *not* take it — their durable commits run concurrently so
//!   SlateDB's WAL coalesces them into one object-store flush (group commit),
//!   which is the throughput path. Read replicas take no lease either.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use gluesql_core::ast::{IndexOperator, OrderByExpr};
use gluesql_core::data::{Key, Schema, SchemaIndex, SchemaIndexOrd, Value};
use gluesql_core::error::Result as GlueResult;
use gluesql_core::executor::evaluate_stateless;
use gluesql_core::store::{
    AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexError, IndexMut, Metadata,
    MetaIter, Planner, RowIter, Store, StoreMut, Transaction,
};
use serde::{Deserialize, Serialize};
use slatedb::config::ScanOptions;
use slatedb::{Db, DbSnapshot, WriteBatch};
use tokio::sync::{Mutex, OwnedMutexGuard};

use bluedb_storage::Substrate;

use crate::cdc::{next_cdc_seq, CdcConfig, CdcEntry, CdcSeq};
use crate::colcat::ColumnCatalog;
use crate::error::SqlError;
use crate::keyspace::{prefix_upper_bound, Keyspace, DEFAULT_TENANT, TAG_CDC};

/// A shared **write lease** — the async mutex that serializes *explicit* write
/// transactions across connections to one [`Db`]. Connections created from the
/// same [`crate::Database`] share one of these; a standalone
/// [`SlateDbStorage::new`] gets its own (it is the sole writer).
pub type WriteLease = Arc<Mutex<()>>;

/// Shared per-table auto-increment row-key counters (keyed by the table's data
/// keyspace prefix, so tenants don't collide). Holds the *next* key already
/// handed out; `append_data` bumps it under the map lock. Lazily seeded from the
/// live committed max the first time a table is appended to on a given writer,
/// so it stays correct across failover (a freshly promoted writer re-derives it
/// from object storage). In-memory only — gaps from rolled-back/failed appends
/// are fine, exactly like a SQL sequence.
pub(crate) type SeqAllocator = Arc<Mutex<HashMap<Vec<u8>, i64>>>;

/// The stored form of a data row: the primary key plus the row payload.
///
/// `pub(crate)` so [`crate::projection`] can build the exact same on-disk row
/// form that GlueSQL's own store writes, letting a layer above bluedb-sql
/// hand-encode rows into its own atomic batch.
#[derive(Serialize, Deserialize)]
pub(crate) struct StoredRow {
    pub(crate) key: Key,
    pub(crate) row: DataRow,
}

/// A single committed row change reported to a [`CommitObserver`] after the
/// durable write. `row` is `Some` for an insert/update (the new row), `None`
/// for a delete.
#[derive(Debug, Clone)]
pub struct RowChange {
    pub table: String,
    pub key: Key,
    pub row: Option<DataRow>,
}

/// Observes committed row changes on a [`SlateDbStorage`] connection — the seam
/// the FTS engine uses to maintain its live index synchronously with SQL commit
/// (Spec B §4.2). Called **after** the durable `WriteBatch` write succeeds, so
/// an observer never sees a change that didn't commit. `on_commit` runs inline
/// on the commit path: keep it cheap (in-memory work only).
pub trait CommitObserver: Send + Sync {
    fn on_commit(&self, changes: &[RowChange]);
}

/// State for an in-flight transaction. Every statement on the writer runs
/// inside one of these (autocommit statements get a short-lived one — see
/// [`SlateDbStorage::begin`]); explicit `BEGIN ... COMMIT` blocks keep one open
/// across statements.
struct TxnState {
    /// Buffered mutations keyed by encoded storage key: `Some` = put, `None` =
    /// delete (tombstone). Ordered so range merges over the base are cheap.
    overlay: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Point-in-time read view of SlateDB captured at `begin`, giving snapshot
    /// isolation. Autocommit statements snapshot *without* the lease, so they
    /// never block (or are blocked by) writers.
    snapshot: Arc<DbSnapshot>,
    /// Exclusive write lease, held only for an **explicit** `BEGIN` (acquired
    /// eagerly at `begin`, before the snapshot, so the transaction's own reads —
    /// e.g. an `UPDATE`'s row scan — are serialized against other explicit
    /// transactions). `None` for autocommit statements, which don't serialize on
    /// it (key allocation is collision-free via the [`SeqAllocator`], and their
    /// durable commits run concurrently for group commit). Held until
    /// `commit`/`rollback`; dropping it releases the lease.
    _lease: Option<OwnedMutexGuard<()>>,
    /// Storage keys this txn inserted that were **absent at its snapshot** (fresh
    /// keyed inserts — e.g. `INSERT` into a table with a primary key; not
    /// updates, not auto-increment appends). At commit they are re-checked
    /// against live committed state under the write lease, so two concurrent
    /// inserts of the same primary key can't both succeed (the loser aborts with
    /// [`SqlError::UniqueViolation`]).
    unique_checks: Vec<Vec<u8>>,
    /// Decoded row mutations this txn made, buffered for the
    /// [`CommitObserver`] and flushed to it **after** the durable commit write
    /// (Spec B §4.2). Only populated when an observer is installed; empty
    /// otherwise so non-observed commits pay nothing.
    changes: Vec<RowChange>,
}

/// GlueSQL custom storage over a SlateDB database.
///
/// Construct with [`SlateDbStorage::new`] (default tenant) or
/// [`SlateDbStorage::new_for_tenant`] from an already-open [`Db`], then hand it
/// to `gluesql_core::prelude::Glue::new`.
pub struct SlateDbStorage {
    /// The SlateDB handle this connection reads/writes through: a writer `Db`
    /// (active node) or a read-only `DbReader` (replica). Writes and `BEGIN`
    /// require the writer.
    substrate: Substrate,
    keyspace: Keyspace,
    /// Shared write lease serializing explicit write transactions over this `Db`.
    write_lease: WriteLease,
    /// Shared lock held *briefly* at commit while a fresh keyed insert
    /// re-validates uniqueness and writes — separate from `write_lease` so an
    /// autocommit insert doesn't block on a long-held explicit-transaction lease
    /// (it serializes only against other inserters). See `commit`.
    insert_lock: WriteLease,
    /// Shared per-table auto-increment counters (see [`SeqAllocator`]).
    seq: SeqAllocator,
    /// When set, autocommit statements on this connection also take the write
    /// lease eagerly at `begin` (snapshotting under it) — so a single-statement
    /// read-modify-write like `UPDATE n = n + 1` is serialized and can't lose an
    /// update against a concurrent writer. Set by the server for the routes that
    /// can RMW (`/sql`, `PATCH`, `DELETE`); left off for the append/insert path
    /// so it keeps group-committing. See [`SlateDbStorage::serialize_writes`].
    serialize_writes: bool,
    /// "Strict" bluedb policy mode for user-facing connections: a `Planner::plan`
    /// pass rejects full-scan / in-memory-sort queries (see [`crate::guardrail`]),
    /// and `insert_schema` rejects schemaless / PK-less tables (see
    /// [`crate::schema_rules`]). Off by default so the raw engine and internal /
    /// conformance paths keep GlueSQL's full behavior; user-facing connections
    /// enable it (see [`crate::Database::connection_guarded`]).
    strict: bool,
    /// Optional commit observer (e.g. the FTS live-index maintainer). When set,
    /// data mutations are buffered as decoded [`RowChange`]s and reported via
    /// [`CommitObserver::on_commit`] after the durable commit write. `None` by
    /// default → zero behavior change and no per-row clone cost.
    commit_observer: Option<Arc<dyn CommitObserver>>,
    /// Optional lakehouse CDC control. When set, committed changes on
    /// mirror-enabled tables are also written as [`CdcEntry`]s into the same
    /// `WriteBatch` as the data (see `commit`). `None` → no CDC overhead.
    cdc: Option<CdcConfig>,
    /// Shared global CDC sequence counter (the one held by [`crate::Database`]),
    /// used to stamp CDC entries at commit. Unused when `cdc` is `None`.
    cdc_seq: CdcSeq,
    /// `Some` while a `BEGIN ... COMMIT/ROLLBACK` block is open.
    txn: Option<TxnState>,
}

impl SlateDbStorage {
    /// Wrap an already-open SlateDB database under the default tenant, as a
    /// **standalone** connection (its own private write lease — it is the sole
    /// writer). To run *several* coordinated connections over one `Db` (so their
    /// write transactions serialize against each other), create them from a
    /// shared [`crate::Database`] instead.
    pub fn new(db: Arc<Db>) -> Self {
        Self::new_for_tenant(db, DEFAULT_TENANT)
    }

    /// Wrap an already-open SlateDB database scoped to `tenant`, standalone.
    ///
    /// Every key this storage reads or writes is namespaced to `tenant`, so two
    /// storages over the same [`Db`] with different tenants share no data.
    pub fn new_for_tenant(db: Arc<Db>, tenant: &str) -> Self {
        Self::with_substrate(
            Substrate::writer(db),
            tenant,
            Arc::new(Mutex::new(())),
            Arc::new(Mutex::new(())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(None)),
        )
    }

    /// Construct over any [`Substrate`] (writer or read replica), sharing the
    /// `write_lease`, `insert_lock`, `seq` allocator and `cdc_seq` counter. A
    /// reader substrate yields a **read-only** connection: reads work, but writes
    /// and `BEGIN` error (`require_writer`).
    pub(crate) fn with_substrate(
        substrate: Substrate,
        tenant: &str,
        write_lease: WriteLease,
        insert_lock: WriteLease,
        seq: SeqAllocator,
        cdc_seq: CdcSeq,
    ) -> Self {
        Self {
            substrate,
            keyspace: Keyspace::new(tenant),
            write_lease,
            insert_lock,
            seq,
            serialize_writes: false,
            strict: false,
            commit_observer: None,
            cdc: None,
            cdc_seq,
            txn: None,
        }
    }

    /// Enable the lakehouse CDC mirror on this connection: committed changes on
    /// tables that `cdc` marks enabled are written as [`CdcEntry`]s into the same
    /// atomic `WriteBatch` as the data (see `commit`). Pairs with
    /// [`crate::Database::connection_with_cdc`], which also serializes writes so
    /// read-modify-write updates are captured in full.
    pub fn with_cdc(mut self, cdc: CdcConfig) -> Self {
        self.cdc = Some(cdc);
        self
    }

    /// Make autocommit statements on this connection serialize on the write lease
    /// (snapshotting under it), so a single-statement read-modify-write like
    /// `UPDATE n = n + 1` can't lose an update against a concurrent writer. Use
    /// for RMW-capable request routes; leave off so the append/insert path keeps
    /// group-committing.
    pub fn serialize_writes(mut self) -> Self {
        self.serialize_writes = true;
        self
    }

    /// Enable "strict" bluedb policy on this connection: reject full-scan /
    /// in-memory-sort queries (see [`crate::guardrail`]) and schemaless / PK-less
    /// table creation (see [`crate::schema_rules`]). Use for user-facing request
    /// surfaces; leave off for internal/admin connections that legitimately scan
    /// or need GlueSQL's full schema behavior.
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    /// Install a commit observer (e.g. the FTS live-index maintainer). Reported
    /// changes are decoded row mutations, emitted after the durable commit write.
    pub fn with_commit_observer(mut self, observer: Arc<dyn CommitObserver>) -> Self {
        self.commit_observer = Some(observer);
        self
    }

    /// Does this connection need committed changes buffered for `table` — either
    /// a [`CommitObserver`] is installed, or the CDC mirror is enabled for it?
    /// Call sites guard the (cloning) `record_change` call on this so an
    /// unobserved, non-mirrored commit pays nothing.
    fn wants_changes(&self, table: &str) -> bool {
        self.commit_observer.is_some()
            || self.cdc.as_ref().is_some_and(|c| c.is_enabled(table))
    }

    /// Buffer a row change for the commit observer and/or the CDC log, when one
    /// is wanted for `table` and a txn is active (gluesql always runs statements
    /// inside a txn). The buffered changes are drained at `commit`: written into
    /// the CDC namespace of the batch and/or handed to the observer.
    fn record_change(&mut self, table: &str, key: Key, row: Option<DataRow>) {
        if !self.wants_changes(table) {
            return;
        }
        if let Some(txn) = self.txn.as_mut() {
            txn.changes.push(RowChange {
                table: table.to_string(),
                key,
                row,
            });
        }
    }

    /// The writer handle, or a read-only error if this connection is a replica.
    fn writer(&self) -> Result<&Arc<Db>, SqlError> {
        Ok(self.substrate.require_writer()?)
    }

    /// Reserve `count` consecutive auto-increment row keys for `table_name`,
    /// returning the first. Atomic across connections: the shared [`SeqAllocator`]
    /// hands out monotonically increasing keys under a brief map lock, so
    /// concurrent autocommit `INSERT`s never collide — no global write lease and
    /// no max-scan on the hot path. The counter seeds lazily from the live
    /// committed max the first time a table is touched on this writer (re-derived
    /// after a failover, since it's in-memory).
    async fn allocate_keys(&self, table_name: &str, count: usize) -> Result<i64, SqlError> {
        let table_id = self.table_id(table_name).await?;
        let prefix = self.keyspace.data_prefix(table_id);
        let seq = self.seq.clone();
        let mut map = seq.lock().await;
        let current = match map.get(&prefix).copied() {
            Some(n) => n,
            // First append to this table on this writer: seed from object storage.
            None => self.live_max_i64_key(&prefix).await?,
        };
        let start = current + 1;
        map.insert(prefix, current + count as i64);
        Ok(start)
    }

    /// Highest `I64` row key among the **live committed** rows under `prefix`
    /// (read straight from the writer `Db`, not a snapshot). Used only to seed the
    /// auto-increment counter. Returns 0 for an empty table.
    async fn live_max_i64_key(&self, prefix: &[u8]) -> Result<i64, SqlError> {
        let end = prefix_upper_bound(prefix);
        let mut max = 0i64;
        let mut iter = self.substrate.scan_range(prefix, end.as_deref()).await?;
        while let Some(kv) = iter.next().await? {
            let stored: StoredRow = decode(&kv.value)?;
            if let Key::I64(n) = stored.key {
                max = max.max(n);
            }
        }
        Ok(max)
    }

    // --- Unified read/write through the optional transaction overlay. -------

    /// Write a key (put). Buffered in the overlay when a txn is active,
    /// otherwise written straight through to SlateDB.
    async fn write_key(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), SqlError> {
        match self.txn.as_mut() {
            Some(txn) => {
                txn.overlay.insert(key, Some(value));
                Ok(())
            }
            None => {
                self.writer()?.put(&key, &value).await?;
                Ok(())
            }
        }
    }

    /// Delete a key. Buffered as a tombstone in the overlay when a txn is
    /// active, otherwise deleted straight through.
    async fn delete_key(&mut self, key: Vec<u8>) -> Result<(), SqlError> {
        match self.txn.as_mut() {
            Some(txn) => {
                txn.overlay.insert(key, None);
                Ok(())
            }
            None => {
                self.writer()?.delete(&key).await?;
                Ok(())
            }
        }
    }

    /// Read one key, honoring the overlay (and the txn snapshot) when a txn is
    /// active.
    async fn read_key(&self, key: &[u8]) -> Result<Option<Vec<u8>>, SqlError> {
        match self.txn.as_ref() {
            Some(txn) => match txn.overlay.get(key) {
                // Overlay shadows the base: a put or a tombstone wins.
                Some(Some(value)) => Ok(Some(value.clone())),
                Some(None) => Ok(None),
                None => Ok(txn.snapshot.get(key).await?.map(|b| b.to_vec())),
            },
            None => Ok(self.substrate.get(key).await?.map(|b| b.to_vec())),
        }
    }

    /// Scan all `(storage_key, value)` pairs whose key falls in
    /// `[start, end)` (or `[start, ..)` when `end` is `None`), in key order,
    /// merging the overlay over the base. Tombstones drop base rows; overlay
    /// puts shadow base values.
    async fn scan_range(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SqlError> {
        // 1. Pull the base rows (from the snapshot inside a txn, else live db).
        let base = self.scan_range_base(&start, end.as_deref()).await?;

        let txn = match self.txn.as_ref() {
            None => return Ok(base),
            Some(txn) => txn,
        };

        // 2. Ordered merge of base with the overlay slice in the same range.
        let upper = match &end {
            Some(end) => Bound::Excluded(end.clone()),
            None => Bound::Unbounded,
        };
        let overlay_slice = txn
            .overlay
            .range((Bound::Included(start.clone()), upper))
            .map(|(k, v)| (k.clone(), v.clone()));

        Ok(merge_sorted(base, overlay_slice))
    }

    /// Raw base scan (no overlay) over `[start, end)`, reading from the txn
    /// snapshot when one is open and from the live db otherwise.
    async fn scan_range_base(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SqlError> {
        let mut out = Vec::new();
        // SlateDB's snapshot and db share the same scan surface but are distinct
        // types, so we branch and reuse a small drain helper.
        match self.txn.as_ref() {
            Some(txn) => {
                let mut iter = match end {
                    Some(end) => {
                        txn.snapshot
                            .scan_with_options(start.to_vec()..end.to_vec(), &ScanOptions::default())
                            .await?
                    }
                    None => {
                        txn.snapshot
                            .scan_with_options(start.to_vec().., &ScanOptions::default())
                            .await?
                    }
                };
                while let Some(kv) = iter.next().await? {
                    out.push((kv.key.to_vec(), kv.value.to_vec()));
                }
            }
            None => {
                // Live read through the substrate (writer Db or read replica).
                let mut iter = self.substrate.scan_range(start, end).await?;
                while let Some(kv) = iter.next().await? {
                    out.push((kv.key.to_vec(), kv.value.to_vec()));
                }
            }
        }
        Ok(out)
    }

    /// Drain every row of `table_name` in primary-key order, honoring the txn
    /// overlay. Rows come back sorted because storage keys sort by encoded pk.
    async fn collect_rows(&self, table_name: &str) -> Result<Vec<(Key, DataRow)>, SqlError> {
        let table_id = self.table_id(table_name).await?;
        let prefix = self.keyspace.data_prefix(table_id);
        let end = prefix_upper_bound(&prefix);
        let pairs = self.scan_range(prefix, end).await?;
        let catalog = self.read_catalog(table_name).await?;
        let mut rows = Vec::with_capacity(pairs.len());
        for (_, value) in pairs {
            let stored: StoredRow = decode(&value)?;
            let row = match &catalog {
                Some(cat) => cat.to_logical(stored.row),
                None => stored.row,
            };
            rows.push((stored.key, row));
        }
        Ok(rows)
    }

    /// Read a table's schema honoring the overlay (used by index maintenance,
    /// which runs inside `StoreMut` calls that may be within a txn).
    async fn read_schema(&self, table_name: &str) -> Result<Option<Schema>, SqlError> {
        let key = self.keyspace.schema_key(table_name);
        match self.read_key(&key).await? {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Read a table's column catalog (the online-ALTER field-id mapping), honoring
    /// the overlay. `None` means the table was never altered, so rows are stored
    /// in schema order and need no translation (see [`crate::colcat`]).
    async fn read_catalog(&self, table_name: &str) -> Result<Option<ColumnCatalog>, SqlError> {
        let key = self.keyspace.colcat_key(table_name);
        match self.read_key(&key).await? {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Persist a table's column catalog (written on the first diverging ALTER).
    async fn write_catalog(
        &mut self,
        table_name: &str,
        catalog: &ColumnCatalog,
    ) -> Result<(), SqlError> {
        let key = self.keyspace.colcat_key(table_name);
        self.write_key(key, encode(catalog)?).await
    }

    /// Resolve a table's stable id (assigned at CREATE). Data and index keys are
    /// keyed by this id — not the table name — so it must exist for any table
    /// that has rows. Reading it is how name→id resolution happens on every data
    /// path. (One extra point read per op; fine for a spike — cacheable later.)
    /// The stable id for a table, if it has one (name→id mapping). Public so a
    /// layer that hand-writes SQL-projection rows (e.g. `bluedb-ledger`) can build
    /// row keys by id, the same way the engine does.
    pub async fn resolve_table_id(&self, table_name: &str) -> Result<Option<u64>, SqlError> {
        match self.read_key(&self.keyspace.tableid_key(table_name)).await? {
            Some(bytes) => Ok(Some(decode_table_id(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn table_id(&self, table_name: &str) -> Result<u64, SqlError> {
        self.resolve_table_id(table_name).await?.ok_or_else(|| {
            SqlError::KeyEncode(format!("no stable id registered for table `{table_name}`"))
        })
    }

    /// Return the table's stable id, allocating one (and bumping the per-tenant
    /// counter) on first call. Used at CREATE time.
    async fn ensure_table_id(&mut self, table_name: &str) -> Result<u64, SqlError> {
        let key = self.keyspace.tableid_key(table_name);
        if let Some(bytes) = self.read_key(&key).await? {
            return decode_table_id(&bytes);
        }
        let seq_key = self.keyspace.tableid_seq_key();
        let next = match self.read_key(&seq_key).await? {
            Some(bytes) => decode_table_id(&bytes)? + 1,
            None => 1,
        };
        self.write_key(seq_key, next.to_be_bytes().to_vec()).await?;
        self.write_key(key, next.to_be_bytes().to_vec()).await?;
        Ok(next)
    }

    // --- Secondary-index helpers. -------------------------------------------

    /// All index definitions for `table_name`, honoring the overlay.
    async fn index_defs(&self, table_name: &str) -> Result<Vec<SchemaIndex>, SqlError> {
        match self.read_schema(table_name).await? {
            Some(schema) => Ok(schema.indexes),
            None => Ok(Vec::new()),
        }
    }

    /// Evaluate one index's expression against a row to get its indexed value,
    /// then convert it to an order-preserving [`Key`].
    async fn index_value(
        index: &SchemaIndex,
        columns: Option<&[String]>,
        row: &DataRow,
    ) -> Result<Key, SqlError> {
        let context = row.as_context(columns);
        let evaluated = evaluate_stateless(Some(context), &index.expr)
            .await
            .map_err(|e| SqlError::IndexEval(e.to_string()))?;
        let value = Value::try_from(evaluated).map_err(|e| SqlError::IndexEval(e.to_string()))?;
        Key::try_from(value).map_err(|e| SqlError::IndexEval(e.to_string()))
    }

    /// Column names for a table (empty for schemaless tables — index exprs on
    /// schemaless tables are rejected by GlueSQL's `validate_index_expr`).
    fn schema_columns(schema: &Schema) -> Vec<String> {
        schema
            .column_defs
            .as_ref()
            .map(|defs| defs.iter().map(|c| c.name.clone()).collect())
            .unwrap_or_default()
    }

    /// Add or remove every index entry for `row` (keyed by `pk`).
    async fn apply_index_entries(
        &mut self,
        table_name: &str,
        pk: &Key,
        row: &DataRow,
        insert: bool,
    ) -> Result<(), SqlError> {
        let defs = self.index_defs(table_name).await?;
        if defs.is_empty() {
            return Ok(());
        }
        let table_id = self.table_id(table_name).await?;
        let columns = match self.read_schema(table_name).await? {
            Some(schema) => Self::schema_columns(&schema),
            None => Vec::new(),
        };
        let cols = if columns.is_empty() {
            None
        } else {
            Some(columns.as_slice())
        };
        for def in &defs {
            let value = Self::index_value(def, cols, row).await?;
            let entry_key = self
                .keyspace
                .index_entry_key(table_id, &def.name, &value, pk)?;
            if insert {
                self.write_key(entry_key, encode(pk)?).await?;
            } else {
                self.delete_key(entry_key).await?;
            }
        }
        Ok(())
    }
}

/// Serialize a value to bytes (JSON). `pub(crate)` so [`crate::projection`]
/// reuses the exact row encoding GlueSQL's store reads back.
pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, SqlError> {
    Ok(serde_json::to_vec(value)?)
}

/// Deserialize bytes (JSON) back into a value.
pub(crate) fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, SqlError> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Decode a big-endian `u64` table id from its stored 8-byte value.
pub(crate) fn decode_table_id(bytes: &[u8]) -> Result<u64, SqlError> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| SqlError::KeyEncode("corrupt table id".to_owned()))?;
    Ok(u64::from_be_bytes(arr))
}

/// Merge an already-sorted base scan with an already-sorted overlay slice.
///
/// Both inputs are ordered by key. Overlay entries shadow base entries with the
/// same key; an overlay `None` (tombstone) drops the key entirely. The result
/// stays sorted by key.
fn merge_sorted(
    base: Vec<(Vec<u8>, Vec<u8>)>,
    overlay: impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut base = base.into_iter().peekable();
    let mut overlay = overlay.peekable();
    let mut out = Vec::new();

    loop {
        match (base.peek(), overlay.peek()) {
            (Some((bk, _)), Some((ok, _))) => {
                use std::cmp::Ordering::*;
                match bk.cmp(ok) {
                    Less => out.push(base.next().unwrap()),
                    Greater => {
                        let (k, v) = overlay.next().unwrap();
                        if let Some(v) = v {
                            out.push((k, v));
                        }
                    }
                    Equal => {
                        // Overlay wins; drop the base value.
                        base.next();
                        let (k, v) = overlay.next().unwrap();
                        if let Some(v) = v {
                            out.push((k, v));
                        }
                    }
                }
            }
            (Some(_), None) => out.push(base.next().unwrap()),
            (None, Some(_)) => {
                let (k, v) = overlay.next().unwrap();
                if let Some(v) = v {
                    out.push((k, v));
                }
            }
            (None, None) => break,
        }
    }
    out
}

#[async_trait]
impl Store for SlateDbStorage {
    async fn fetch_schema(&self, table_name: &str) -> GlueResult<Option<Schema>> {
        Ok(self.read_schema(table_name).await?)
    }

    async fn fetch_all_schemas(&self) -> GlueResult<Vec<Schema>> {
        let prefix = self.keyspace.schema_prefix();
        let end = prefix_upper_bound(&prefix);
        let pairs = self.scan_range(prefix, end).await?;
        let mut schemas = Vec::with_capacity(pairs.len());
        for (_, value) in pairs {
            schemas.push(decode::<Schema>(&value)?);
        }
        // GlueSQL expects schemas sorted by table name. The scan is already in
        // key order, but the byte order of <table_utf8> matches lexical name
        // order so this is mostly a safety net for the merge path.
        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));
        Ok(schemas)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> GlueResult<Option<DataRow>> {
        let table_id = self.table_id(table_name).await?;
        let storage_key = self.keyspace.row_key(table_id, key)?;
        match self.read_key(&storage_key).await? {
            Some(bytes) => {
                let stored: StoredRow = decode(&bytes)?;
                let row = match self.read_catalog(table_name).await? {
                    Some(cat) => cat.to_logical(stored.row),
                    None => stored.row,
                };
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> GlueResult<RowIter<'a>> {
        let rows = self.collect_rows(table_name).await?;
        Ok(Box::pin(stream::iter(rows.into_iter().map(Ok))))
    }
}

#[async_trait]
impl StoreMut for SlateDbStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> GlueResult<()> {
        if self.strict {
            crate::schema_rules::enforce(schema)?;
        }
        // Assign a stable table id on first creation — data and index keys are
        // keyed by it, so it must exist before any row is written. Idempotent on
        // re-insert (ALTER re-inserts the schema).
        self.ensure_table_id(&schema.table_name).await?;
        let key = self.keyspace.schema_key(&schema.table_name);
        self.write_key(key, encode(schema)?).await?;
        // Stamp the table's creation time once, for `GLUE_OBJECTS.CREATED`. Set
        // it only if absent so `ALTER TABLE` (which re-inserts the schema) keeps
        // the original creation time. Stored as i64 microseconds, big-endian.
        let meta_key = self.keyspace.meta_key(&schema.table_name);
        if self.read_key(&meta_key).await?.is_none() {
            let micros = chrono::Utc::now().timestamp_micros();
            self.write_key(meta_key, micros.to_be_bytes().to_vec()).await?;
        }
        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> GlueResult<()> {
        // Drop every index entry, every row, then the schema record (which
        // carries the index *definitions*) itself.
        let table_id = self.table_id(table_name).await?;
        let rows = self.collect_rows(table_name).await?;
        for (key, row) in &rows {
            self.apply_index_entries(table_name, key, row, false).await?;
        }
        for (key, _) in &rows {
            let storage_key = self.keyspace.row_key(table_id, key)?;
            self.delete_key(storage_key).await?;
        }
        self.delete_key(self.keyspace.schema_key(table_name)).await?;
        self.delete_key(self.keyspace.meta_key(table_name)).await?;
        self.delete_key(self.keyspace.colcat_key(table_name)).await?;
        self.delete_key(self.keyspace.tableid_key(table_name)).await?;
        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> GlueResult<()> {
        // `append_data` feeds schemaless / auto-incremented tables. Reserve a
        // contiguous run of keys from the shared counter (atomic across
        // connections), so concurrent autocommit `INSERT`s never collide — no
        // global write lease, no max-scan on the hot path, so their durable
        // commits run concurrently and SlateDB's WAL group-commits them.
        if rows.is_empty() {
            return Ok(());
        }
        let mut next = self.allocate_keys(table_name, rows.len()).await?;
        let table_id = self.table_id(table_name).await?;
        let catalog = self.read_catalog(table_name).await?;

        for row in rows {
            let key = Key::I64(next);
            next += 1;
            let storage_key = self.keyspace.row_key(table_id, &key)?;
            // Index entries + the commit tap see the *logical* row (schema order).
            self.apply_index_entries(table_name, &key, &row, true)
                .await?;
            // Commit tap: buffer the committed insert for the observer/CDC log
            // (guarded so the clone only happens when a consumer wants it).
            if self.wants_changes(table_name) {
                self.record_change(table_name, key.clone(), Some(row.clone()));
            }
            // Persist the *physical* row (online-ALTER slot order) when a catalog
            // exists; otherwise store as-is (identity).
            let row = match &catalog {
                Some(cat) => cat.to_physical(row),
                None => row,
            };
            let stored = StoredRow {
                key: key.clone(),
                row,
            };
            self.write_key(storage_key, encode(&stored)?).await?;
        }
        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> GlueResult<()> {
        // Used for keyed inserts and updates (UPDATE re-inserts the same key).
        let catalog = self.read_catalog(table_name).await?;
        let table_id = self.table_id(table_name).await?;
        for (key, row) in rows {
            // For UPDATE, the old row's index entries must be removed first.
            let storage_key = self.keyspace.row_key(table_id, &key)?;
            if let Some(bytes) = self.read_key(&storage_key).await? {
                let old: StoredRow = decode(&bytes)?;
                // Index removal evaluates against the *logical* old row.
                let old_row = match &catalog {
                    Some(cat) => cat.to_logical(old.row),
                    None => old.row,
                };
                self.apply_index_entries(table_name, &key, &old_row, false)
                    .await?;
            } else if let Some(txn) = self.txn.as_mut() {
                // Fresh keyed insert (absent at our snapshot): remember it so
                // commit can re-validate uniqueness against live committed state.
                txn.unique_checks.push(storage_key.clone());
            }
            self.apply_index_entries(table_name, &key, &row, true)
                .await?;
            // Commit tap: buffer the committed insert/update (UPDATE re-inserts
            // the same key with the new row → the engine's `index()` supersedes).
            if self.wants_changes(table_name) {
                self.record_change(table_name, key.clone(), Some(row.clone()));
            }
            // Persist the *physical* row (online-ALTER slot order) when a catalog
            // exists; otherwise store as-is (identity).
            let row = match &catalog {
                Some(cat) => cat.to_physical(row),
                None => row,
            };
            let stored = StoredRow {
                key: key.clone(),
                row,
            };
            self.write_key(storage_key, encode(&stored)?).await?;
        }
        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> GlueResult<()> {
        let catalog = self.read_catalog(table_name).await?;
        let table_id = self.table_id(table_name).await?;
        for key in keys {
            let storage_key = self.keyspace.row_key(table_id, &key)?;
            if let Some(bytes) = self.read_key(&storage_key).await? {
                let old: StoredRow = decode(&bytes)?;
                let old_row = match &catalog {
                    Some(cat) => cat.to_logical(old.row),
                    None => old.row,
                };
                self.apply_index_entries(table_name, &key, &old_row, false)
                    .await?;
            }
            // Commit tap: buffer the committed delete (row None).
            if self.wants_changes(table_name) {
                self.record_change(table_name, key.clone(), None);
            }
            self.delete_key(storage_key).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Transaction for SlateDbStorage {
    async fn begin(&mut self, autocommit: bool) -> GlueResult<bool> {
        if self.txn.is_some() {
            // A transaction is already open. GlueSQL's `execute` wraps every
            // non-transaction statement in `begin(true)` and auto-commits iff
            // that call returns `true` (see gluesql-core executor/execute.rs:
            // `let autocommit = storage.begin(true).await?; ... if autocommit {
            // commit() }`). While an explicit `BEGIN` block is open we must
            // return `false` so each statement's writes stay buffered in the
            // overlay until the user's explicit COMMIT/ROLLBACK — returning
            // `true` here would auto-commit (flush) after every statement and
            // make ROLLBACK a no-op. (For the `StartTransaction` statement the
            // return value is ignored, so nested BEGIN is harmless.)
            return Ok(false);
        }
        if !self.substrate.is_writer() {
            // Read replica: serve reads lock-free, but refuse an explicit BEGIN
            // (it implies write intent). Autocommit statements read straight
            // through the substrate; any write they attempt is rejected later by
            // `writer()`.
            if !autocommit {
                self.writer()?; // -> read-only error for explicit BEGIN
            }
            return Ok(false);
        }
        // Acquire the write lease eagerly when this is an explicit `BEGIN`, or an
        // autocommit statement on a connection that opted into serialized writes
        // (the read-modify-write routes). The lease is taken FIRST, then the
        // snapshot under it, so the statement's reads see every prior commit and
        // its read-modify-write can't lose against a concurrent writer. A plain
        // autocommit statement (unflagged) snapshots lock-free — it never blocks
        // (or is blocked by) a writer; auto-increment keys stay collision-free
        // via the shared counter (see `append_data`), and concurrent commits
        // batch at the SlateDB WAL (group commit).
        let eager = !autocommit || self.serialize_writes;
        let lease = if eager {
            Some(self.write_lease.clone().lock_owned().await)
        } else {
            None
        };
        let snapshot = self.writer()?.snapshot().await.map_err(SqlError::from)?;
        self.txn = Some(TxnState {
            overlay: BTreeMap::new(),
            snapshot,
            _lease: lease,
            unique_checks: Vec::new(),
            changes: Vec::new(),
        });
        // Return `autocommit` so GlueSQL commits after a plain statement; the
        // value is ignored for the explicit `StartTransaction` statement.
        Ok(autocommit)
    }

    async fn rollback(&mut self) -> GlueResult<()> {
        // Drop the overlay; nothing reached SlateDB.
        self.txn = None;
        Ok(())
    }

    async fn commit(&mut self) -> GlueResult<()> {
        if let Some(txn) = self.txn.take() {
            if !txn.overlay.is_empty() {
                // If this txn made fresh keyed inserts, validate uniqueness while
                // holding `insert_lock` across the re-check AND the batch write,
                // so the check is atomic against other inserters. This is a
                // distinct, briefly-held lock — NOT `write_lease` — so an
                // autocommit insert doesn't stall behind a long-held explicit
                // transaction (and a read-only explicit txn never blocks it).
                // Pure appends/updates have no checks and stay lock-free (group
                // commit). Acquired regardless of whether `write_lease` is already
                // held, so explicit-transaction inserts serialize here too.
                let _ilock = if txn.unique_checks.is_empty() {
                    None
                } else {
                    Some(self.insert_lock.clone().lock_owned().await)
                };
                for key in &txn.unique_checks {
                    if self
                        .substrate
                        .get(key)
                        .await
                        .map_err(SqlError::from)?
                        .is_some()
                    {
                        // Another connection inserted this primary key since our
                        // snapshot — first committer wins, we abort.
                        return Err(SqlError::UniqueViolation(format!("{key:?}")).into());
                    }
                }
                let mut batch = WriteBatch::new();
                // Borrow the overlay so `txn.changes` is still usable after the
                // write to fire the observer (Spec B §4.2).
                for (key, op) in &txn.overlay {
                    match op {
                        Some(value) => batch.put(key, value),
                        None => batch.delete(key),
                    }
                }
                // Lakehouse CDC: append one entry per mirror-enabled change into
                // the SAME batch as the data, stamped with a global monotonic
                // sequence. Because it rides the one atomic `WriteBatch`, a CDC
                // entry is durable iff its data row is — exactly-once capture for
                // the seal loop. Default-tenant only for v1 (see `crate::cdc`).
                let mut cdc_to_signal = None;
                if let Some(cdc) = self.cdc.clone() {
                    let mut wrote_cdc = false;
                    for ch in &txn.changes {
                        if !cdc.is_enabled(&ch.table) {
                            continue;
                        }
                        let seq = next_cdc_seq(&self.cdc_seq, &self.substrate).await?;
                        let entry = CdcEntry {
                            table: ch.table.clone(),
                            key: ch.key.clone(),
                            row: ch.row.clone(),
                        };
                        let cdc_key = self.keyspace.external_key(TAG_CDC, &seq.to_be_bytes());
                        batch.put(&cdc_key, &entry.encode()?);
                        wrote_cdc = true;
                    }
                    if wrote_cdc {
                        cdc_to_signal = Some(cdc);
                    }
                }
                self.writer()?.write(batch).await.map_err(SqlError::from)?;
                // Wake the lakehouse seal loop now that mirror-enabled changes are
                // durable (event-driven freshness).
                if let Some(cdc) = cdc_to_signal {
                    cdc.signal_seal();
                }
                // Commit tap: the durable write succeeded → report the buffered
                // changes. An observer thus never sees a change that didn't commit.
                if let Some(obs) = self.commit_observer.as_ref() {
                    if !txn.changes.is_empty() {
                        obs.on_commit(&txn.changes);
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Index for SlateDbStorage {
    async fn scan_indexed_data<'a>(
        &'a self,
        table_name: &str,
        index_name: &str,
        asc: Option<bool>,
        cmp_value: Option<(&IndexOperator, Value)>,
    ) -> GlueResult<RowIter<'a>> {
        let table_id = self.table_id(table_name).await?;
        // Resolve the scan byte range from the optional comparison.
        let full_prefix = self.keyspace.index_prefix(table_id, index_name);
        let full_end = prefix_upper_bound(&full_prefix);

        let (start, end) = match &cmp_value {
            None => (full_prefix.clone(), full_end.clone()),
            Some((op, value)) => {
                let key = Key::try_from(value.clone())
                    .map_err(|e| SqlError::IndexEval(e.to_string()))?;
                // The value-bounded prefix: all entries for exactly this value.
                let value_prefix = self
                    .keyspace
                    .index_value_prefix(table_id, index_name, &key)?;
                let value_end = prefix_upper_bound(&value_prefix);
                match op {
                    IndexOperator::Eq => (
                        value_prefix.clone(),
                        value_end.clone().or_else(|| full_end.clone()),
                    ),
                    // value < cmp : [index_prefix, value_prefix)
                    IndexOperator::Lt => (full_prefix.clone(), Some(value_prefix.clone())),
                    // value <= cmp : [index_prefix, value_end)
                    IndexOperator::LtEq => (
                        full_prefix.clone(),
                        value_end.clone().or_else(|| full_end.clone()),
                    ),
                    // value > cmp : [value_end, index_end)
                    IndexOperator::Gt => (
                        value_end.clone().unwrap_or_else(|| full_prefix.clone()),
                        full_end.clone(),
                    ),
                    // value >= cmp : [value_prefix, index_end)
                    IndexOperator::GtEq => (value_prefix.clone(), full_end.clone()),
                }
            }
        };

        // Scan the index-entry range (overlay-aware), each value is the pk.
        let entries = self.scan_range(start, end).await?;

        // Resolve each pk to its row (also overlay-aware).
        let mut rows: Vec<(Key, DataRow)> = Vec::with_capacity(entries.len());
        for (_, pk_bytes) in entries {
            let pk: Key = decode(&pk_bytes)?;
            let row_key = self.keyspace.row_key(table_id, &pk)?;
            if let Some(bytes) = self.read_key(&row_key).await? {
                let stored: StoredRow = decode(&bytes)?;
                rows.push((stored.key, stored.row));
            }
            // A missing row means a tombstoned/stale entry; skip it.
        }

        // `entries` came back in ascending (value, pk) order. Reverse for DESC.
        if asc == Some(false) {
            rows.reverse();
        }

        Ok(Box::pin(stream::iter(rows.into_iter().map(Ok))))
    }
}

#[async_trait]
impl IndexMut for SlateDbStorage {
    async fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &OrderByExpr,
    ) -> GlueResult<()> {
        let mut schema = self
            .read_schema(table_name)
            .await?
            .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()))?;
        if schema.indexes.iter().any(|i| i.name == index_name) {
            return Err(IndexError::IndexNameAlreadyExists(index_name.to_owned()).into());
        }

        let order = column
            .asc
            .map(|asc| {
                if asc {
                    SchemaIndexOrd::Asc
                } else {
                    SchemaIndexOrd::Desc
                }
            })
            .unwrap_or(SchemaIndexOrd::Both);
        let def = SchemaIndex {
            name: index_name.to_owned(),
            expr: column.expr.clone(),
            order,
            created: chrono::Utc::now().naive_utc(),
        };

        // Back-fill entries for every existing row, then persist the new schema.
        let columns = Self::schema_columns(&schema);
        let cols = if columns.is_empty() {
            None
        } else {
            Some(columns.as_slice())
        };
        let table_id = self.table_id(table_name).await?;
        let rows = self.collect_rows(table_name).await?;
        for (pk, row) in &rows {
            let value = Self::index_value(&def, cols, row).await?;
            let entry_key = self
                .keyspace
                .index_entry_key(table_id, index_name, &value, pk)?;
            self.write_key(entry_key, encode(pk)?).await?;
        }

        schema.indexes.push(def);
        let schema_key = self.keyspace.schema_key(table_name);
        self.write_key(schema_key, encode(&schema)?).await?;
        Ok(())
    }

    async fn drop_index(&mut self, table_name: &str, index_name: &str) -> GlueResult<()> {
        let mut schema = self
            .read_schema(table_name)
            .await?
            .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()))?;
        if !schema.indexes.iter().any(|i| i.name == index_name) {
            return Err(IndexError::IndexNameDoesNotExist(index_name.to_owned()).into());
        }

        // Remove all entries for this index.
        let table_id = self.table_id(table_name).await?;
        let prefix = self.keyspace.index_prefix(table_id, index_name);
        let end = prefix_upper_bound(&prefix);
        for (k, _) in self.scan_range(prefix, end).await? {
            self.delete_key(k).await?;
        }

        schema.indexes.retain(|i| i.name != index_name);
        let schema_key = self.keyspace.schema_key(table_name);
        self.write_key(schema_key, encode(&schema)?).await?;
        Ok(())
    }
}

// --- Marker traits whose gluesql-core defaults are sufficient. -------------
//
// GlueSQL composes its store bounds as:
//   GStore    = Store + Index + Metadata + CustomFunction
//   GStoreMut = StoreMut + IndexMut + AlterTable + Transaction
//               + CustomFunction + CustomFunctionMut
//   (and Glue additionally requires Planner)
//
// The traits below ship full default method implementations in gluesql-core:
//   * `CustomFunction(Mut)` — user-defined functions: default "not supported".
//   * `AlterTable`          — default impls drive RENAME/ADD/DROP COLUMN on top
//                             of our Store + StoreMut, so they work for free.
//                             (DROP COLUMN with a dropped index calls
//                             `drop_index`, which we now implement.)
//   * `Planner`             — overridden above (pushdown / index / coercion).

// `Metadata` backs the `GLUE_OBJECTS` introspection table. We return each
// table's creation timestamp (stamped in `insert_schema`); GlueSQL synthesizes
// `OBJECT_NAME`/`OBJECT_TYPE` and merges these in (see `executor::fetch`).
#[async_trait]
impl Metadata for SlateDbStorage {
    async fn scan_table_meta(&self) -> GlueResult<MetaIter> {
        let prefix = self.keyspace.meta_prefix();
        let strip = prefix.len();
        let end = prefix_upper_bound(&prefix);
        let pairs = self.scan_range(prefix, end).await?;

        let mut metas = Vec::with_capacity(pairs.len());
        for (key, value) in pairs {
            // Recover the table name (the bytes after the meta prefix) and the
            // i64-microsecond creation time. Skip any malformed record rather
            // than failing the whole introspection scan.
            let Ok(micros_bytes) = <[u8; 8]>::try_from(value.as_slice()) else {
                continue;
            };
            let Some(created) =
                chrono::DateTime::from_timestamp_micros(i64::from_be_bytes(micros_bytes))
                    .map(|dt| dt.naive_utc())
            else {
                continue;
            };
            let table_name = String::from_utf8_lossy(&key[strip..]).into_owned();
            let meta = BTreeMap::from([("CREATED".to_owned(), Value::Timestamp(created))]);
            metas.push(Ok((table_name, meta)));
        }
        Ok(Box::new(metas.into_iter()))
    }
}

impl CustomFunction for SlateDbStorage {}
impl CustomFunctionMut for SlateDbStorage {}
// Online schema evolution: ADD/DROP/RENAME COLUMN are O(1) metadata ops — they
// update the schema (and, for ADD/DROP, the column catalog) without rewriting any
// rows. The catalog (see [`crate::colcat`]) decouples a column's logical position
// from its physical slot, so reads/writes translate on the fly. RENAME TABLE
// (`rename_schema`) keeps GlueSQL's default (eager re-key) for now — table-id
// indirection makes it O(1). This replaces GlueSQL's default `AlterTable`, whose
// methods all rewrite every row.
#[async_trait]
impl AlterTable for SlateDbStorage {
    async fn add_column(
        &mut self,
        table_name: &str,
        column_def: &gluesql_core::ast::ColumnDef,
    ) -> GlueResult<()> {
        use gluesql_core::error::AlterTableError;
        let mut schema = self
            .fetch_schema(table_name)
            .await?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        // Resolve the fill value for rows that predate the column (GlueSQL's rule:
        // a default, else NULL if nullable, else reject).
        let default_value: Value = match (column_def.default.as_ref(), column_def.nullable) {
            (Some(default), _) => evaluate_stateless(None, default).await?.try_into()?,
            (None, true) => Value::Null,
            (None, false) => {
                return Err(AlterTableError::DefaultValueRequired(column_def.clone()).into())
            }
        };
        let column_defs = schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;
        if column_defs.iter().any(|d| d.name == column_def.name) {
            return Err(AlterTableError::AlreadyExistingColumn(column_def.name.clone()).into());
        }
        let old_ncols = column_defs.len();
        column_defs.push(column_def.clone());

        let mut catalog = self
            .read_catalog(table_name)
            .await?
            .unwrap_or_else(|| ColumnCatalog::identity(old_ncols));
        catalog.add_column(default_value);
        self.insert_schema(&schema).await?;
        self.write_catalog(table_name, &catalog).await?;
        Ok(())
    }

    async fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> GlueResult<()> {
        use gluesql_core::error::AlterTableError;
        let mut schema = self
            .fetch_schema(table_name)
            .await?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        let column_defs = schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;
        let i = match column_defs.iter().position(|d| d.name == column_name) {
            Some(i) => i,
            None if if_exists => return Ok(()),
            None => {
                return Err(AlterTableError::DroppingColumnNotFound(column_name.to_owned()).into())
            }
        };
        let old_ncols = column_defs.len();
        column_defs.remove(i);

        let mut catalog = self
            .read_catalog(table_name)
            .await?
            .unwrap_or_else(|| ColumnCatalog::identity(old_ncols));
        catalog.drop_column(i);
        self.insert_schema(&schema).await?;
        self.write_catalog(table_name, &catalog).await?;
        Ok(())
    }

    async fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> GlueResult<()> {
        use gluesql_core::error::AlterTableError;
        let mut schema = self
            .fetch_schema(table_name)
            .await?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        let column_defs = schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;
        if column_defs.iter().any(|d| d.name == new_column_name) {
            return Err(AlterTableError::AlreadyExistingColumn(new_column_name.to_owned()).into());
        }
        let col = column_defs
            .iter_mut()
            .find(|d| d.name == old_column_name)
            .ok_or(AlterTableError::RenamingColumnNotFound)?;
        new_column_name.clone_into(&mut col.name);
        // Names live only in the schema, not in rows — no row rewrite, no catalog
        // change.
        self.insert_schema(&schema).await?;
        Ok(())
    }

    async fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> GlueResult<()> {
        use gluesql_core::error::AlterTableError;
        let mut schema = self
            .fetch_schema(table_name)
            .await?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        let table_id = self.table_id(table_name).await?;
        // O(1): move the per-table singletons (schema record, name→id mapping,
        // creation-time meta, column catalog) to the new name. Data and index keys
        // are keyed by `table_id` — unchanged — so not one row or index entry moves.
        new_table_name.clone_into(&mut schema.table_name);
        let new_schema_key = self.keyspace.schema_key(new_table_name);
        self.write_key(new_schema_key, encode(&schema)?).await?;
        let old_schema_key = self.keyspace.schema_key(table_name);
        self.delete_key(old_schema_key).await?;

        let new_id_key = self.keyspace.tableid_key(new_table_name);
        self.write_key(new_id_key, table_id.to_be_bytes().to_vec()).await?;
        let old_id_key = self.keyspace.tableid_key(table_name);
        self.delete_key(old_id_key).await?;

        let old_meta = self.keyspace.meta_key(table_name);
        if let Some(meta) = self.read_key(&old_meta).await? {
            let new_meta = self.keyspace.meta_key(new_table_name);
            self.write_key(new_meta, meta).await?;
            self.delete_key(old_meta).await?;
        }
        let old_cat = self.keyspace.colcat_key(table_name);
        if let Some(cat) = self.read_key(&old_cat).await? {
            let new_cat = self.keyspace.colcat_key(new_table_name);
            self.write_key(new_cat, cat).await?;
            self.delete_key(old_cat).await?;
        }
        Ok(())
    }
}
// Override the default planner with two schema-aware passes inserted into
// gluesql's own plan pipeline (`fetch_schema_map` gives us column types here):
//   1. `pushdown_equijoins` — the comma-join shim leaves join keys in the
//      `WHERE` as `... JOIN b ON TRUE`; pushing them into the `ON` lets
//      `plan_join` build hash joins instead of nested-loop cartesian products
//      (`reject_cross_products` fast-fails anything left without a key).
//   2. `coerce_comparisons` — insert implicit `CAST`s so a numeric operand vs a
//      numeric string literal compares numerically, instead of GlueSQL's silent
//      text/number mismatch. See `crate::coerce`.
// Everything else is gluesql's own public plan helpers; this is the default
// `plan()` pipeline with our two passes inserted.
#[async_trait]
impl Planner for SlateDbStorage {
    async fn plan(
        &self,
        statement: gluesql_core::ast::Statement,
    ) -> GlueResult<gluesql_core::ast::Statement> {
        use gluesql_core::plan::{
            fetch_schema_map, plan_index, plan_join, plan_primary_key, validate,
        };

        let schema_map = fetch_schema_map(self, &statement).await?;
        validate(&schema_map, &statement)?;
        let statement = crate::pushdown::pushdown_equijoins(&schema_map, statement);
        crate::pushdown::reject_cross_products(&statement)?;
        let statement = crate::coerce::coerce_comparisons(&schema_map, statement);
        // On a guarded (user-facing) connection, bound every read: an unfiltered
        // SELECT is capped to a PK-ordered prefix, and a non-indexed WHERE/ORDER BY
        // is rejected. Internal/admin connections (`strict == false`) are exempt.
        let statement = if self.strict {
            crate::guardrail::bound_or_reject(&schema_map, statement)?
        } else {
            statement
        };
        let statement = plan_primary_key(&schema_map, statement);
        // Secondary-index selection: route eligible `WHERE` predicates to
        // `Index::scan_indexed_data`. The default `Planner::plan` omits this, so
        // overriding `plan()` dropped it — without this pass our `CREATE INDEX`es
        // are built but never used (correct results, but full scans).
        let statement = plan_index(&schema_map, statement);
        let statement = plan_join(&schema_map, statement);
        Ok(statement)
    }
}
