//! Native record I/O: `postcard` (de)serialization and point reads of accounts
//! and transfers through a [`Substrate`].

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bluedb_storage::Substrate;
use serde::{de::DeserializeOwned, Serialize};

use crate::keyspace::LedgerKeyspace;
use crate::model::{Account, PendingStatus, Transfer};

/// Encode a native record with `postcard` (compact, fast fixed-struct encoding).
pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(value).context("postcard encode ledger record")
}

/// Decode a native record from `postcard` bytes.
pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    postcard::from_bytes(bytes).context("postcard decode ledger record")
}

/// Point read of an account by id (committed state), or `None` if absent.
pub(crate) async fn get_account(
    substrate: &Substrate,
    ks: &LedgerKeyspace,
    id: u128,
) -> Result<Option<Account>> {
    match substrate.get(&ks.account_key(id)).await? {
        Some(bytes) => Ok(Some(decode(&bytes)?)),
        None => Ok(None),
    }
}

/// Point read of a transfer by id (committed state), or `None` if absent.
pub(crate) async fn get_transfer(
    substrate: &Substrate,
    ks: &LedgerKeyspace,
    id: u128,
) -> Result<Option<Transfer>> {
    match substrate.get(&ks.transfer_key(id)).await? {
        Some(bytes) => Ok(Some(decode(&bytes)?)),
        None => Ok(None),
    }
}

/// Read the pending-state record for a pending transfer (present ⇒ already
/// posted or voided), or `None` if it is still outstanding.
pub(crate) async fn get_pending_state(
    substrate: &Substrate,
    ks: &LedgerKeyspace,
    pending_id: u128,
) -> Result<Option<PendingStatus>> {
    match substrate.get(&ks.pending_state_key(pending_id)).await? {
        Some(bytes) => Ok(Some(decode(&bytes)?)),
        None => Ok(None),
    }
}

/// Read the persisted timestamp watermark (the last assigned timestamp), or `0`
/// if none has been written yet.
pub(crate) async fn get_watermark(substrate: &Substrate, ks: &LedgerKeyspace) -> Result<u64> {
    match substrate.get(&ks.watermark_key()).await? {
        Some(bytes) => {
            let arr: [u8; 8] = bytes.as_ref().try_into().context("watermark must be 8 bytes")?;
            Ok(u64::from_be_bytes(arr))
        }
        None => Ok(0),
    }
}

/// Expiry-index entries with `expires_at <= now`, as `(full_index_key,
/// pending_id)` pairs in ascending `expires_at` order — the timed pendings the
/// sweep must auto-void. The full key is returned so the caller can delete it.
pub(crate) async fn scan_expired(
    substrate: &Substrate,
    ks: &LedgerKeyspace,
    now: u64,
) -> Result<Vec<(Vec<u8>, u128)>> {
    let start = ks.expiry_prefix();
    let end = ks.expiry_scan_end(now);
    let mut out = Vec::new();
    let mut iter = substrate.scan_range(&start, Some(&end)).await?;
    while let Some(kv) = iter.next().await? {
        let key = kv.key.to_vec();
        // Suffix layout: <expires_at::8> <pending_id::16>; id is the last 16 bytes.
        let tail: [u8; 16] = key
            .get(key.len().saturating_sub(16)..)
            .filter(|t| t.len() == 16)
            .and_then(|t| t.try_into().ok())
            .with_context(|| format!("expiry index key too short: {} bytes", key.len()))?;
        out.push((key, u128::from_be_bytes(tail)));
    }
    Ok(out)
}

/// Wall clock in nanoseconds since the Unix epoch — the engine's timestamp
/// source (combined with the persisted watermark to stay strictly monotonic).
pub(crate) fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) mod test_harness {
    //! Shared test helper: an in-memory writer `Database` over a fresh
    //! `InMemory` object store.
    use std::sync::Arc;

    use bluedb_sql::Database;
    use slatedb::object_store::memory::InMemory;
    use slatedb::Db;

    /// Open a brand-new in-memory writer database for a test.
    pub(crate) async fn writer_database() -> Database {
        let db = Db::open("ledger-test", Arc::new(InMemory::new()))
            .await
            .expect("open in-memory db");
        Database::new(Arc::new(db))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Account, AccountFlags};

    #[test]
    fn account_round_trips_through_postcard() {
        let a = Account {
            id: 99,
            debits_pending: 1,
            debits_posted: 2,
            credits_pending: 3,
            credits_posted: 4,
            user_data_128: u128::MAX,
            user_data_64: 64,
            user_data_32: 32,
            reserved: 0,
            ledger: 1,
            code: 7,
            flags: AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS,
            timestamp: 123,
        };
        let bytes = encode(&a).unwrap();
        let back: Account = decode(&bytes).unwrap();
        assert_eq!(a, back);
    }

    #[tokio::test]
    async fn pending_state_round_trips() {
        use crate::model::PendingStatus;
        let database = test_harness::writer_database().await;
        let substrate = database.substrate();
        let ks = LedgerKeyspace::new(bluedb_sql::DEFAULT_TENANT);
        assert!(get_pending_state(&substrate, &ks, 9).await.unwrap().is_none());
        let writer = substrate.require_writer().unwrap();
        writer.put(&ks.pending_state_key(9), &encode(&PendingStatus::Posted).unwrap()).await.unwrap();
        assert_eq!(get_pending_state(&substrate, &ks, 9).await.unwrap(), Some(PendingStatus::Posted));
    }

    #[tokio::test]
    async fn watermark_round_trips() {
        let database = test_harness::writer_database().await;
        let substrate = database.substrate();
        let ks = LedgerKeyspace::new(bluedb_sql::DEFAULT_TENANT);
        assert_eq!(get_watermark(&substrate, &ks).await.unwrap(), 0);
        let writer = substrate.require_writer().unwrap();
        writer.put(&ks.watermark_key(), &1234u64.to_be_bytes()).await.unwrap();
        assert_eq!(get_watermark(&substrate, &ks).await.unwrap(), 1234);
    }

    #[tokio::test]
    async fn get_account_reads_what_was_written() {
        let database = test_harness::writer_database().await;
        let substrate = database.substrate();
        let ks = LedgerKeyspace::new(bluedb_sql::DEFAULT_TENANT);

        assert!(get_account(&substrate, &ks, 1).await.unwrap().is_none());

        let a = Account {
            id: 1, debits_pending: 0, debits_posted: 10, credits_pending: 0, credits_posted: 0,
            user_data_128: 0, user_data_64: 0, user_data_32: 0,
            reserved: 0, ledger: 1, code: 0, flags: AccountFlags::NONE, timestamp: 5,
        };
        let writer = substrate.require_writer().unwrap();
        writer.put(&ks.account_key(1), &encode(&a).unwrap()).await.unwrap();

        let back = get_account(&substrate, &ks, 1).await.unwrap().unwrap();
        assert_eq!(back, a);
    }
}
