//! CDC log for the lakehouse mirror.
//!
//! One [`CdcEntry`] is written per committed row change on a mirror-enabled
//! table, into the **same** [`WriteBatch`](slatedb::WriteBatch) as the data (see
//! [`crate::storage`]'s `commit`), so an entry exists if and only if the data
//! committed — the exactly-once seam the lakehouse seal loop drains. Entries are
//! keyed `external_key(TAG_CDC, seq.to_be_bytes())` with a global, monotonic,
//! 1-based sequence so a byte-ordered scan yields them in commit order.
//!
//! Multi-tenant CDC is out of scope for v1: the log lives under the default
//! tenant only (see [`crate::keyspace::DEFAULT_TENANT`]).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use gluesql_core::data::Key;
use gluesql_core::store::DataRow;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use bluedb_storage::Substrate;

use crate::error::SqlError;
use crate::keyspace::{prefix_upper_bound, Keyspace, DEFAULT_TENANT, TAG_CDC};

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

/// Per-database CDC control: whether the mirror is on by default, plus the set
/// of tables whose choice is the **opposite** of that default.
///
/// Cloning shares the same inner state (Arc), so a PRAGMA toggle on one
/// connection is seen by every live connection over the same database. The
/// effective decision is `default_on XOR overridden(table)`:
///
/// | `default_on` | in `overrides` | mirrored? |
/// |--------------|----------------|-----------|
/// | true (opt-out default) | no  | yes |
/// | true                   | yes | no (explicitly excluded) |
/// | false (opt-in)         | yes | yes (explicitly included) |
/// | false                  | no  | no |
///
/// Phase 4 wires the PRAGMA that mutates these; Phase 1 just needs `default_on`.
#[derive(Clone)]
pub struct CdcConfig {
    /// Tables whose mirror state is the negation of `default_on`.
    overrides: Arc<RwLock<HashSet<String>>>,
    /// Mirror tables by default (opt-out). Public so callers/tests can toggle it.
    pub default_on: Arc<AtomicBool>,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            overrides: Arc::new(RwLock::new(HashSet::new())),
            default_on: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl CdcConfig {
    /// Should `table` be mirrored? `default_on XOR overridden(table)`.
    pub fn is_enabled(&self, table: &str) -> bool {
        let overridden = self.overrides.read().unwrap().contains(table);
        self.default_on.load(Ordering::Relaxed) ^ overridden
    }

    /// Force `table` to `enabled`, regardless of the current default — records an
    /// override iff the request differs from `default_on`. Used by the PRAGMA
    /// surface (Phase 4).
    pub fn set_table(&self, table: &str, enabled: bool) {
        let mut set = self.overrides.write().unwrap();
        if enabled == self.default_on.load(Ordering::Relaxed) {
            set.remove(table); // matches the default → no override needed
        } else {
            set.insert(table.to_string());
        }
    }
}

/// Shared, lazily-seeded global CDC sequence counter (the last seq handed out).
/// `None` until the first allocation seeds it from the persisted max, so a
/// freshly promoted writer re-derives it from object storage after failover.
pub(crate) type CdcSeq = Arc<Mutex<Option<i64>>>;

/// Allocate the next global CDC sequence (1-based, monotonic). Seeds lazily from
/// [`max_persisted_cdc_seq`] on first use, then increments in memory under the
/// shared lock so concurrent commits get distinct, ordered sequences.
pub(crate) async fn next_cdc_seq(handle: &CdcSeq, substrate: &Substrate) -> Result<i64, SqlError> {
    let mut guard = handle.lock().await;
    let cur = match *guard {
        Some(n) => n,
        None => max_persisted_cdc_seq(substrate).await?,
    };
    let next = cur + 1;
    *guard = Some(next);
    Ok(next)
}

/// The largest persisted CDC sequence, or 0 if the log is empty. Scans the
/// default-tenant CDC namespace and decodes the trailing big-endian seq.
pub(crate) async fn max_persisted_cdc_seq(substrate: &Substrate) -> Result<i64, SqlError> {
    let ks = Keyspace::new(DEFAULT_TENANT);
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
        assert!(!cfg.is_enabled("docs")); // off by default
        cfg.set_table("docs", true);
        assert!(cfg.is_enabled("docs"));
        assert!(!cfg.is_enabled("other"));
    }

    #[test]
    fn cdc_config_default_on_then_opt_out() {
        let cfg = CdcConfig::default();
        cfg.default_on.store(true, Ordering::Relaxed);
        assert!(cfg.is_enabled("docs")); // on by default
        cfg.set_table("docs", false);
        assert!(!cfg.is_enabled("docs")); // explicitly excluded
        assert!(cfg.is_enabled("other"));
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
        assert!(matches!(&t[&Key::I64(1)], Some(DataRow::Vec(v)) if v == &[gluesql_core::data::Value::I64(99)]));
        assert_eq!(t[&Key::I64(2)], None);
    }

    #[test]
    fn seq_decodes_from_key_suffix() {
        let ks = Keyspace::new(DEFAULT_TENANT);
        let key = ks.external_key(TAG_CDC, &42i64.to_be_bytes());
        assert_eq!(cdc_seq_from_key(&key), Some(42));
    }
}
