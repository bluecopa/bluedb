//! Native record I/O: `postcard` (de)serialization and point reads of accounts
//! and transfers through a [`Substrate`].

use anyhow::{Context, Result};
use bluedb_storage::Substrate;
use serde::{de::DeserializeOwned, Serialize};

use crate::keyspace::LedgerKeyspace;
use crate::model::{Account, Transfer};

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
            ledger: 1,
            code: 7,
            flags: AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS,
            debits_pending: 1,
            debits_posted: 2,
            credits_pending: 3,
            credits_posted: 4,
            user_data_128: u128::MAX,
            user_data_64: 64,
            user_data_32: 32,
            timestamp: 123,
        };
        let bytes = encode(&a).unwrap();
        let back: Account = decode(&bytes).unwrap();
        assert_eq!(a, back);
    }

    #[tokio::test]
    async fn get_account_reads_what_was_written() {
        let database = test_harness::writer_database().await;
        let substrate = database.substrate();
        let ks = LedgerKeyspace::new(bluedb_sql::DEFAULT_TENANT);

        assert!(get_account(&substrate, &ks, 1).await.unwrap().is_none());

        let a = Account {
            id: 1, ledger: 1, code: 0, flags: AccountFlags::NONE,
            debits_pending: 0, debits_posted: 10, credits_pending: 0, credits_posted: 0,
            user_data_128: 0, user_data_64: 0, user_data_32: 0, timestamp: 5,
        };
        let writer = substrate.require_writer().unwrap();
        writer.put(&ks.account_key(1), &encode(&a).unwrap()).await.unwrap();

        let back = get_account(&substrate, &ks, 1).await.unwrap().unwrap();
        assert_eq!(back, a);
    }
}
