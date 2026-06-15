//! The public [`Evidence`] handle: build from a [`bluedb_sql::Database`], it
//! runs evidence append and read operations inside that database's active
//! writer. Mirrors the `bluedb-ledger` subsystem shape.

use bluedb_sql::{Database, WriteLease};
use bluedb_storage::Substrate;
use sha2::{Digest, Sha256};
use slatedb::config::WriteOptions;
use slatedb::WriteBatch;

use crate::error::EvidenceError;
use crate::keyspace::EvidenceKeyspace;
use crate::model::{ChainMeta, EdgeDelta, EntryRecord, IdemRecord};
use crate::store;

/// A handle to evidence chains in one bluedb database.
///
/// Construct one per use from the live [`Database`] (e.g. per request on the
/// server), so it always reflects the node's current writer/replica role. All
/// writes require the active writer; reads work on a replica.
pub struct Evidence {
    substrate: Substrate,
    write_lease: WriteLease,
    keyspace: EvidenceKeyspace,
}

/// The caller-supplied description of one event to append.
#[derive(Debug, Clone)]
pub struct EntryInput {
    pub etype: String,
    pub payload: Vec<u8>,
    pub at: String,
    pub edges: Vec<EdgeDelta>,
}

/// Result of a successful append: the server-assigned sequence range.
#[derive(Debug, Clone, PartialEq)]
pub struct Appended {
    /// The head seq _before_ this append (0 if the chain was empty).
    pub base_seq: i64,
    /// The server-assigned seqs for each entry in input order.
    pub seqs: Vec<i64>,
}

const MAX_PAGE: usize = 1000;

impl Evidence {
    /// Build an evidence handle over `database` for the given `tenant`, sharing
    /// its substrate and write lease.
    pub fn new(database: &Database, tenant: &str) -> Self {
        Self {
            substrate: database.substrate(),
            write_lease: database.write_lease(),
            keyspace: EvidenceKeyspace::new(tenant),
        }
    }

    /// Read the metadata for `chain`, or `None` if the chain has never been
    /// explicitly created.
    pub async fn chain_meta(&self, chain: &str) -> Result<Option<ChainMeta>, EvidenceError> {
        Ok(store::get_chain_meta(&self.substrate, &self.keyspace, chain).await?)
    }

    /// Ensure `chain` exists with the given `verified` mode. If the chain
    /// already exists with the same mode this is a no-op (idempotent). If it
    /// exists with a different mode, returns [`EvidenceError::ChainModeConflict`].
    pub async fn create_chain(&self, chain: &str, verified: bool) -> Result<(), EvidenceError> {
        let _lease = self.write_lease.lock().await;
        let writer = self.substrate.require_writer().map_err(|_| EvidenceError::NotWriter)?;
        if let Some(existing) = store::get_chain_meta(&self.substrate, &self.keyspace, chain).await? {
            if existing.verified != verified {
                return Err(EvidenceError::ChainModeConflict(chain.to_string()));
            }
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        batch.put(
            self.keyspace.chain_meta_key(chain),
            &store::encode(&ChainMeta { verified })?,
        );
        writer
            .write_with_options(batch, &WriteOptions { await_durable: false, ..Default::default() })
            .await
            .map_err(|e| EvidenceError::Storage(anyhow::anyhow!("{e}")))?;
        drop(_lease);
        writer.flush().await.map_err(|e| EvidenceError::Storage(anyhow::anyhow!("{e}")))?;
        Ok(())
    }

    // ---- Append ----

    /// Compute a fingerprint of `entries` for idempotency comparison.
    fn fingerprint(entries: &[EntryInput]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update((entries.len() as u64).to_be_bytes());
        for e in entries {
            for field in [e.etype.as_bytes(), &e.payload, e.at.as_bytes()] {
                h.update((field.len() as u64).to_be_bytes());
                h.update(field);
            }
        }
        h.finalize().into()
    }

    /// Return the current head seq (last assigned seq) for `chain`, or `0` if
    /// the chain is empty or does not exist.
    pub async fn head(&self, chain: &str) -> Result<i64, EvidenceError> {
        Ok(store::get_seq(&self.substrate, &self.keyspace, chain).await?)
    }

    /// Atomically append `entries` to `chain`, assigning dense server-ordered
    /// sequences starting at `head + 1`. If `idem_key` is provided the result
    /// is idempotent: a second call with the same key and identical entries
    /// returns the original seqs; a second call with different entries returns
    /// [`EvidenceError::IdemConflict`].
    ///
    /// If the chain has never been explicitly created, it is auto-created with
    /// `verified = true` in the same atomic batch.
    pub async fn append(
        &self,
        chain: &str,
        entries: Vec<EntryInput>,
        idem_key: Option<&str>,
    ) -> Result<Appended, EvidenceError> {
        let _lease = self.write_lease.lock().await;
        let writer = self.substrate.require_writer().map_err(|_| EvidenceError::NotWriter)?;

        let existing_meta =
            store::get_chain_meta(&self.substrate, &self.keyspace, chain).await?;
        let fp = Self::fingerprint(&entries);

        // Idempotency check: if we have seen this key before, either replay or
        // reject (conflict).
        if let Some(key) = idem_key {
            if let Some(rec) =
                store::get_idem(&self.substrate, &self.keyspace, chain, key).await?
            {
                if rec.fingerprint == fp {
                    return Ok(Appended { base_seq: rec.base_seq, seqs: rec.seqs });
                }
                return Err(EvidenceError::IdemConflict);
            }
        }

        let base = store::get_seq(&self.substrate, &self.keyspace, chain).await?;
        let k = entries.len() as i64;
        let seqs: Vec<i64> = (base + 1..=base + k).collect();

        let mut batch = WriteBatch::new();

        // Auto-create the chain meta if it has never been explicitly created.
        if existing_meta.is_none() {
            batch.put(
                self.keyspace.chain_meta_key(chain),
                &store::encode(&ChainMeta { verified: true })?,
            );
        }

        // Write each entry record.
        for (entry, &seq) in entries.iter().zip(&seqs) {
            let rec = EntryRecord {
                etype: entry.etype.clone(),
                payload: entry.payload.clone(),
                at: entry.at.clone(),
                edges: entry.edges.clone(),
                leaf_hash: None,
                redacted: false,
            };
            batch.put(self.keyspace.entry_key(chain, seq), &store::encode(&rec)?);
        }

        // Advance the sequence counter.
        batch.put(self.keyspace.seq_key(chain), &(base + k).to_be_bytes());

        // Persist the idempotency record if requested.
        if let Some(key) = idem_key {
            let rec = IdemRecord { base_seq: base, seqs: seqs.clone(), fingerprint: fp };
            batch.put(self.keyspace.idem_key(chain, key), &store::encode(&rec)?);
        }

        writer
            .write_with_options(batch, &WriteOptions { await_durable: false, ..Default::default() })
            .await
            .map_err(|e| EvidenceError::Storage(anyhow::anyhow!("{e}")))?;
        drop(_lease);
        writer.flush().await.map_err(|e| EvidenceError::Storage(anyhow::anyhow!("{e}")))?;

        Ok(Appended { base_seq: base, seqs })
    }

    // ---- Reads ----

    /// Read entries with seq in `[lo, hi]` (inclusive both ends). Returns
    /// entries in ascending seq order. Empty if `hi < lo`.
    pub async fn read_range(
        &self,
        chain: &str,
        lo: i64,
        hi: i64,
    ) -> Result<Vec<(i64, EntryRecord)>, EvidenceError> {
        if hi < lo {
            return Ok(Vec::new());
        }
        let start = self.keyspace.entry_key(chain, lo);
        let end = match hi.checked_add(1) {
            Some(next) => self.keyspace.entry_key(chain, next),
            None => self.keyspace.entry_prefix_end(chain),
        };
        self.scan_entries(&start, &end, usize::MAX).await
    }

    /// Read entries with seq > `after`, up to `limit` (default `MAX_PAGE`, max
    /// `MAX_PAGE`). Returns entries in ascending seq order; returns empty when
    /// there are no more entries.
    pub async fn read_from(
        &self,
        chain: &str,
        after: i64,
        limit: Option<usize>,
    ) -> Result<Vec<(i64, EntryRecord)>, EvidenceError> {
        let cap = limit.unwrap_or(MAX_PAGE).min(MAX_PAGE);
        let start = self.keyspace.entry_key(chain, after.saturating_add(1));
        let end = self.keyspace.entry_prefix_end(chain);
        self.scan_entries(&start, &end, cap).await
    }

    /// Internal: scan entry keys in `[start, end)`, yielding up to `cap` items.
    async fn scan_entries(
        &self,
        start: &[u8],
        end: &[u8],
        cap: usize,
    ) -> Result<Vec<(i64, EntryRecord)>, EvidenceError> {
        let mut out = Vec::new();
        let mut iter = self.substrate.scan_range(start, Some(end)).await?;
        while let Some(kv) = iter.next().await.map_err(|e| EvidenceError::Storage(anyhow::anyhow!("{e}")))? {
            if out.len() >= cap {
                break;
            }
            let key = kv.key.as_ref();
            // The seq is the last 8 bytes of the key.
            let tail: [u8; 8] = key[key.len() - 8..].try_into().unwrap();
            let seq = i64::from_be_bytes(tail);
            let rec: EntryRecord = store::decode(&kv.value)?;
            out.push((seq, rec));
        }
        Ok(out)
    }
}
