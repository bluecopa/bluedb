//! CDC log for the lakehouse mirror.
//!
//! One [`CdcEntry`] is written per committed row change on a mirror-enabled
//! table, into the **same** [`WriteBatch`](slatedb::WriteBatch) as the data (see
//! [`crate::storage`]'s `commit`), so an entry exists if and only if the data
//! committed — the exactly-once seam the lakehouse seal loop drains. Entries are
//! keyed `external_key(TAG_CDC, seq.to_be_bytes())` with a per-tenant, monotonic,
//! 1-based sequence so a byte-ordered scan yields them in commit order.
//!
//! **Multi-tenant.** Each tenant has its own CDC log (under its own key prefix)
//! and its own sequence space starting at 1; enablement and the opt-out default
//! are tracked per `(tenant, table)`. The seal signal is global — one loop wakes
//! and seals every tenant's pending changes.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use gluesql_core::data::Key;
use gluesql_core::store::DataRow;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify};

use bluedb_storage::Substrate;

use crate::error::SqlError;
use crate::keyspace::{prefix_upper_bound, Keyspace, TAG_CDC};

/// A single committed change in CDC-log form. `row` is `Some` for an
/// insert/update (the new row), `None` for a delete — mirroring
/// [`RowChange`](crate::RowChange), from which entries are built at commit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CdcEntry {
    pub table: String,
    pub key: Key,
    pub row: Option<DataRow>,
}

impl CdcEntry {
    /// Encode to the compact binary form stored under the CDC key.
    pub fn encode(&self) -> Result<Vec<u8>, SqlError> {
        postcard::to_stdvec(self).map_err(|e| SqlError::Serde(e.to_string()))
    }

    /// Decode an entry read back from the log.
    pub fn decode(bytes: &[u8]) -> Result<Self, SqlError> {
        postcard::from_bytes(bytes).map_err(|e| SqlError::Serde(e.to_string()))
    }
}

/// Per-database CDC control, **scoped per tenant**: whether the mirror is on by
/// default for a tenant, plus the set of `(tenant, table)` pairs whose choice is
/// the **opposite** of that tenant's default.
///
/// Cloning shares the same inner state (Arc), so a PRAGMA toggle on one
/// connection is seen by every live connection over the same database. The
/// effective decision for a `(tenant, table)` is `default_for(tenant) XOR
/// overridden(tenant, table)`:
///
/// | `default_for(tenant)` | in `overrides` | mirrored? |
/// |-----------------------|----------------|-----------|
/// | true (opt-out default) | no  | yes |
/// | true                   | yes | no (explicitly excluded) |
/// | false (opt-in)         | yes | yes (explicitly included) |
/// | false                  | no  | no |
#[derive(Clone, Default)]
pub struct CdcConfig {
    /// `(tenant, table)` pairs whose mirror state is the negation of that
    /// tenant's default.
    overrides: Arc<RwLock<HashSet<(String, String)>>>,
    /// Per-tenant opt-out default. An absent tenant falls back to `global_default`.
    defaults: Arc<RwLock<HashMap<String, bool>>>,
    /// Fallback mirror default for tenants with no explicit per-tenant setting.
    /// The testkit's mirror mode flips this on so every tenant mirrors by default;
    /// prod leaves it false (opt-in via `PRAGMA lakehouse_mirror`).
    global_default: Arc<AtomicBool>,
    /// Tenants that have written CDC entries this process, so the seal path can
    /// ensure a lakehouse engine exists for each — auto-mirror of arbitrary
    /// tenants without a per-tenant PRAGMA.
    seen: Arc<RwLock<HashSet<String>>>,
    /// Pinged after a commit writes CDC entries, so the lakehouse seal loop wakes
    /// promptly (event-driven freshness) instead of polling on a fixed interval.
    seal_signal: Arc<Notify>,
}

impl CdcConfig {
    /// The opt-out default for `tenant`: its explicit per-tenant setting if any,
    /// else the process-wide `global_default` (false ⇒ opt-in, the initial state).
    pub fn default_for(&self, tenant: &str) -> bool {
        self.defaults
            .read()
            .unwrap()
            .get(tenant)
            .copied()
            .unwrap_or_else(|| self.global_default.load(Ordering::Relaxed))
    }

    /// Set the fallback mirror default for tenants without an explicit setting
    /// (testkit mirror mode). A per-tenant `set_default` still overrides it.
    pub fn set_global_default(&self, on: bool) {
        self.global_default.store(on, Ordering::Relaxed);
    }

    /// Record that `tenant` has written CDC entries (so seal can ensure its engine).
    pub fn mark_seen(&self, tenant: &str) {
        self.seen.write().unwrap().insert(tenant.to_string());
    }

    /// Tenants that have written CDC entries this process.
    pub fn seen_tenants(&self) -> Vec<String> {
        self.seen.read().unwrap().iter().cloned().collect()
    }

    /// Set `tenant`'s opt-out default. Per-table overrides are unaffected — a
    /// caller that flips the default re-applies its explicit flags via
    /// [`Self::set_table`] (see the lakehouse engine).
    pub fn set_default(&self, tenant: &str, on: bool) {
        self.defaults
            .write()
            .unwrap()
            .insert(tenant.to_string(), on);
    }

    /// Should `(tenant, table)` be mirrored? `default_for(tenant) XOR override`.
    pub fn is_enabled(&self, tenant: &str, table: &str) -> bool {
        let key = (tenant.to_string(), table.to_string());
        let overridden = self.overrides.read().unwrap().contains(&key);
        self.default_for(tenant) ^ overridden
    }

    /// Force `(tenant, table)` to `enabled`, regardless of the tenant's current
    /// default — records an override iff the request differs from that default.
    pub fn set_table(&self, tenant: &str, table: &str, enabled: bool) {
        let key = (tenant.to_string(), table.to_string());
        let mut set = self.overrides.write().unwrap();
        if enabled == self.default_for(tenant) {
            set.remove(&key); // matches the default → no override needed
        } else {
            set.insert(key);
        }
    }

    /// Signal that mirror-enabled changes just committed — wakes one waiter on
    /// [`Self::wait_for_changes`]. Called by the commit path after the durable
    /// write.
    pub fn signal_seal(&self) {
        self.seal_signal.notify_one();
    }

    /// Wait until the next [`Self::signal_seal`]. A permit set before this is
    /// awaited returns immediately, so commits are never missed.
    pub async fn wait_for_changes(&self) {
        self.seal_signal.notified().await;
    }
}

/// Shared, lazily-seeded **per-tenant** CDC sequence counters (tenant → last seq
/// handed out). A tenant absent from the map is seeded from its persisted max on
/// first allocation, so a freshly promoted writer re-derives each counter from
/// object storage after failover.
pub(crate) type CdcSeq = Arc<Mutex<HashMap<String, i64>>>;

/// Allocate `tenant`'s next CDC sequence (1-based, monotonic within the tenant).
/// Seeds lazily from [`max_persisted_cdc_seq`] on first use, then increments in
/// memory under the shared lock so concurrent commits get distinct, ordered
/// sequences.
pub(crate) async fn next_cdc_seq(
    handle: &CdcSeq,
    substrate: &Substrate,
    tenant: &str,
) -> Result<i64, SqlError> {
    let mut guard = handle.lock().await;
    let cur = match guard.get(tenant) {
        Some(n) => *n,
        None => max_persisted_cdc_seq(substrate, tenant).await?,
    };
    let next = cur + 1;
    guard.insert(tenant.to_string(), next);
    Ok(next)
}

/// The largest persisted CDC sequence for `tenant`, or 0 if its log is empty.
/// Scans the tenant's CDC namespace and decodes the trailing big-endian seq.
pub(crate) async fn max_persisted_cdc_seq(
    substrate: &Substrate,
    tenant: &str,
) -> Result<i64, SqlError> {
    let ks = Keyspace::new(tenant);
    let prefix = ks.external_prefix(TAG_CDC);
    let end = prefix_upper_bound(&prefix);
    let mut max = 0i64;
    let mut it = substrate.scan_range(&prefix, end.as_deref()).await?;
    while let Some(kv) = it.next().await? {
        if let Some(seq) = cdc_seq_from_key(kv.key.as_ref()) {
            max = max.max(seq);
        }
    }
    Ok(max)
}

/// The final state per table after collapsing a CDC scan: `Some(row)` is the
/// last insert/update for that primary key, `None` a final delete.
pub type CollapsedChanges = HashMap<String, BTreeMap<Key, Option<DataRow>>>;

/// Collapse CDC entries (already in ascending sequence order) to their final
/// per-`(table, primary key)` state — last-writer-wins. An insert then update
/// then delete on one key collapses to a single `None`; insert then update to a
/// single `Some(latest row)`. The seal loop turns `Some` into an upsert and
/// `None` into an equality-delete.
pub fn collapse_lww(entries: Vec<(i64, CdcEntry)>) -> CollapsedChanges {
    let mut out: CollapsedChanges = HashMap::new();
    for (_seq, entry) in entries {
        out.entry(entry.table)
            .or_default()
            .insert(entry.key, entry.row);
    }
    out
}

/// Recover the sequence from a CDC storage key (its trailing 8 big-endian bytes).
pub(crate) fn cdc_seq_from_key(key: &[u8]) -> Option<i64> {
    let n = key.len();
    let bytes: [u8; 8] = key.get(n.checked_sub(8)?..)?.try_into().ok()?;
    Some(i64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gluesql_core::data::Key;

    #[test]
    fn cdc_entry_round_trips() {
        let e = CdcEntry {
            table: "docs".into(),
            key: Key::I64(7),
            row: None,
        };
        let bytes = e.encode().unwrap();
        assert_eq!(CdcEntry::decode(&bytes).unwrap(), e);
    }

    #[test]
    fn cdc_config_default_off_then_opt_in() {
        let cfg = CdcConfig::default();
        assert!(!cfg.is_enabled("_", "docs")); // off by default
        cfg.set_table("_", "docs", true);
        assert!(cfg.is_enabled("_", "docs"));
        assert!(!cfg.is_enabled("_", "other"));
    }

    #[test]
    fn cdc_config_default_on_then_opt_out() {
        let cfg = CdcConfig::default();
        cfg.set_default("_", true);
        assert!(cfg.is_enabled("_", "docs")); // on by default
        cfg.set_table("_", "docs", false);
        assert!(!cfg.is_enabled("_", "docs")); // explicitly excluded
        assert!(cfg.is_enabled("_", "other"));
    }

    #[test]
    fn cdc_config_is_isolated_per_tenant() {
        let cfg = CdcConfig::default();
        // Enabling a table for tenant A leaves tenant B's identically-named table
        // untouched; defaults are per-tenant too.
        cfg.set_table("a", "docs", true);
        assert!(cfg.is_enabled("a", "docs"));
        assert!(!cfg.is_enabled("b", "docs"));
        cfg.set_default("b", true);
        assert!(cfg.is_enabled("b", "docs"));
        assert!(!cfg.is_enabled("a", "other")); // tenant a still opt-in
    }

    #[test]
    fn collapse_is_last_writer_wins_per_key() {
        use gluesql_core::store::DataRow;
        let e = |seq: i64, key: i64, row: Option<Vec<i64>>| {
            (
                seq,
                CdcEntry {
                    table: "t".into(),
                    key: Key::I64(key),
                    row: row.map(|vs| {
                        DataRow::Vec(vs.into_iter().map(gluesql_core::data::Value::I64).collect())
                    }),
                },
            )
        };
        // key 1: insert -> update -> survives as latest; key 2: insert -> delete.
        let collapsed = collapse_lww(vec![
            e(1, 1, Some(vec![1])),
            e(2, 2, Some(vec![2])),
            e(3, 1, Some(vec![99])),
            e(4, 2, None),
        ]);
        let t = &collapsed["t"];
        assert_eq!(t.len(), 2);
        assert!(
            matches!(&t[&Key::I64(1)], Some(DataRow::Vec(v)) if v == &[gluesql_core::data::Value::I64(99)])
        );
        assert_eq!(t[&Key::I64(2)], None);
    }

    #[test]
    fn seq_decodes_from_key_suffix() {
        let ks = Keyspace::new(crate::keyspace::DEFAULT_TENANT);
        let key = ks.external_key(TAG_CDC, &42i64.to_be_bytes());
        assert_eq!(cdc_seq_from_key(&key), Some(42));
    }
}
