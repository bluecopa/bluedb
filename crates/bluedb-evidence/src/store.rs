//! Postcard (de)serialization and point reads of evidence records through a
//! [`Substrate`]. Mirrors `bluedb-ledger/src/store.rs`.

use anyhow::{Context, Result};
use bluedb_storage::Substrate;
use serde::{de::DeserializeOwned, Serialize};

use crate::keyspace::EvidenceKeyspace;
use crate::model::{ChainMeta, EntryRecord, IdemRecord};

/// Encode a native record with `postcard`.
pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(value).context("postcard encode evidence record")
}

/// Decode a native record from `postcard` bytes.
pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    postcard::from_bytes(bytes).context("postcard decode evidence record")
}

/// Point read of chain metadata, or `None` if the chain has no metadata record.
pub(crate) async fn get_chain_meta(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
) -> Result<Option<ChainMeta>> {
    match substrate.get(&ks.chain_meta_key(chain)).await? {
        Some(b) => Ok(Some(decode(&b)?)),
        None => Ok(None),
    }
}

/// Last-assigned seq for `chain`, or 0 if empty/absent.
pub(crate) async fn get_seq(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
) -> Result<i64> {
    match substrate.get(&ks.seq_key(chain)).await? {
        Some(b) => {
            let arr: [u8; 8] = b.as_ref().try_into().context("seq counter must be 8 bytes")?;
            Ok(i64::from_be_bytes(arr))
        }
        None => Ok(0),
    }
}

/// Point read of an idempotency record, or `None` if the token is unseen.
#[allow(dead_code)]
pub(crate) async fn get_idem(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
    idem: &str,
) -> Result<Option<IdemRecord>> {
    match substrate.get(&ks.idem_key(chain, idem)).await? {
        Some(b) => Ok(Some(decode(&b)?)),
        None => Ok(None),
    }
}

/// Point read of one entry by seq, or `None` if absent.
#[allow(dead_code)]
pub(crate) async fn get_entry(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
    seq: i64,
) -> Result<Option<EntryRecord>> {
    match substrate.get(&ks.entry_key(chain, seq)).await? {
        Some(b) => Ok(Some(decode(&b)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ChainMeta, EntryRecord};

    #[test]
    fn entry_record_encode_decode_roundtrip() {
        let rec = EntryRecord {
            etype: "test.event".into(),
            payload: vec![1, 2, 3],
            at: "2026-06-15T00:00:00Z".into(),
            edges: vec![],
            leaf_hash: None,
            redacted: false,
        };
        let bytes = encode(&rec).unwrap();
        let back: EntryRecord = decode(&bytes).unwrap();
        assert_eq!(rec, back);
    }

    #[test]
    fn chain_meta_encode_decode_roundtrip() {
        let meta = ChainMeta { verified: true };
        let bytes = encode(&meta).unwrap();
        let back: ChainMeta = decode(&bytes).unwrap();
        assert_eq!(meta, back);
    }
}
