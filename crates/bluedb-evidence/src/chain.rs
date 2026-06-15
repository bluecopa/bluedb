//! The public [`Evidence`] handle: build from a [`bluedb_sql::Database`], it
//! runs evidence append and read operations inside that database's active
//! writer. Mirrors the `bluedb-ledger` subsystem shape.

use std::collections::HashMap;

use bluedb_sql::{Database, WriteLease};
use bluedb_storage::Substrate;
use sha2::{Digest as Sha2Digest, Sha256};
use slatedb::config::WriteOptions;
use slatedb::WriteBatch;

use crate::error::EvidenceError;
use crate::graph::apply_edge_delta;
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

/// `{ size, root }` — the Merkle digest of a verified chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Digest {
    pub size: i64,
    pub root: [u8; 32],
}

/// An RFC 6962 inclusion proof for `seq` (1-based) against tree size `size`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InclusionProof {
    pub seq: i64,
    pub size: i64,
    pub audit_path: Vec<[u8; 32]>,
}

/// An RFC 6962 consistency proof between sizes `first` and `second`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsistencyProof {
    pub first: i64,
    pub second: i64,
    pub proof: Vec<[u8; 32]>,
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
            .map_err(Self::storage_err)?;
        drop(_lease);
        writer.flush().await.map_err(Self::storage_err)?;
        Ok(())
    }

    // ---- Append ----

    /// Map any `Display`-able error into [`EvidenceError::Storage`].
    fn storage_err(e: impl std::fmt::Display) -> EvidenceError {
        EvidenceError::Storage(anyhow::anyhow!("{e}"))
    }

    /// Compute a fingerprint of `entries` for idempotency comparison.
    /// Covers etype, payload, at, AND edges so that same-key replays with
    /// different edges are correctly detected as IdemConflict.
    fn fingerprint(entries: &[EntryInput]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update((entries.len() as u64).to_be_bytes());
        for e in entries {
            for field in [e.etype.as_bytes(), &e.payload, e.at.as_bytes()] {
                h.update((field.len() as u64).to_be_bytes());
                h.update(field);
            }
            // Fix 1: fold edges into the fingerprint.
            h.update((e.edges.len() as u64).to_be_bytes());
            for ed in &e.edges {
                for field in [ed.graph.as_bytes(), ed.src.as_bytes(), ed.dst.as_bytes(), ed.etype.as_bytes()] {
                    h.update((field.len() as u64).to_be_bytes());
                    h.update(field);
                }
                h.update(ed.weight.to_be_bytes());
                let discriminant: u8 = match &ed.op {
                    crate::model::EdgeOp::Upsert { merge: crate::model::Merge::Set } => 0,
                    crate::model::EdgeOp::Upsert { merge: crate::model::Merge::Max } => 1,
                    crate::model::EdgeOp::Delete => 2,
                };
                h.update([discriminant]);
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
        // Fix 4: short-circuit an empty append — no lease, no write, no flush.
        if entries.is_empty() {
            return Ok(Appended { base_seq: self.head(chain).await?, seqs: Vec::new() });
        }

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

        let verified = existing_meta.map(|m| m.verified).unwrap_or(true);

        let mut batch = WriteBatch::new();

        // Auto-create the chain meta if it has never been explicitly created.
        if existing_meta.is_none() {
            batch.put(
                self.keyspace.chain_meta_key(chain),
                &store::encode(&ChainMeta { verified: true })?,
            );
        }

        // Write each entry record — compute leaf_hash on verified chains; collect
        // edges to apply to the graph store in this same batch.
        let mut leaves: Vec<[u8; 32]> = Vec::new();
        let mut all_edges: Vec<EdgeDelta> = Vec::new();
        for (entry, &seq) in entries.into_iter().zip(&seqs) {
            let mut rec = EntryRecord {
                etype: entry.etype,
                payload: entry.payload,
                at: entry.at,
                edges: entry.edges,
                leaf_hash: None,
                redacted: false,
            };
            if verified {
                let lh = crate::merkle::leaf_hash(&rec.etype, &rec.payload, &rec.at, &rec.edges);
                rec.leaf_hash = Some(lh);
                leaves.push(lh);
            }
            all_edges.extend(rec.edges.iter().cloned());
            batch.put(self.keyspace.entry_key(chain, seq), &store::encode(&rec)?);
        }

        // Materialize the graph projection for every entry's edges, in this batch.
        // One shared overlay across all entries gives read-your-own-writes when
        // several deltas touch the same edge identity within the batch.
        if !all_edges.is_empty() {
            let mut overlay: HashMap<Vec<u8>, Option<i64>> = HashMap::new();
            for d in &all_edges {
                apply_edge_delta(&self.substrate, &self.keyspace, &mut batch, &mut overlay, d).await?;
            }
        }

        // Advance the sequence counter.
        batch.put(self.keyspace.seq_key(chain), (base + k).to_be_bytes());

        // Advance the Merkle frontier on verified chains (same batch — crash-consistent).
        // Each push's carry-merge emits complete-subtree parents; persist them in the
        // same batch so O(log N) proofs can read them instead of all leaves.
        if verified {
            let mut frontier = store::get_frontier(&self.substrate, &self.keyspace, chain)
                .await?
                .unwrap_or_default();
            for lh in leaves {
                for (level, index, hash) in frontier.push_emit(lh) {
                    batch.put(self.keyspace.merkle_node_key(chain, level, index), &hash);
                }
            }
            batch.put(self.keyspace.merkle_key(chain), &store::encode(&frontier)?);
        }

        // Persist the idempotency record if requested.
        if let Some(key) = idem_key {
            let rec = IdemRecord { base_seq: base, seqs: seqs.clone(), fingerprint: fp };
            batch.put(self.keyspace.idem_key(chain, key), &store::encode(&rec)?);
        }

        writer
            .write_with_options(batch, &WriteOptions { await_durable: false, ..Default::default() })
            .await
            .map_err(Self::storage_err)?;
        drop(_lease);
        writer.flush().await.map_err(Self::storage_err)?;

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
        // Fix 5: check cap BEFORE fetching the next kv so we never pull an
        // extra SlateDB read once the cap is reached.
        while out.len() < cap {
            let Some(kv) = iter.next().await.map_err(Self::storage_err)? else { break };
            let key = kv.key.as_ref();
            // The seq is the last 8 bytes of the key.
            let tail: [u8; 8] = key[key.len() - 8..]
                .try_into()
                .map_err(|_| Self::storage_err("entry key too short: expected trailing 8-byte seq"))?;
            let seq = i64::from_be_bytes(tail);
            let rec: EntryRecord = store::decode(&kv.value)?;
            out.push((seq, rec));
        }
        Ok(out)
    }

    // ---- Merkle reads ----

    /// Require that `chain` is a verified chain (auto-created chains are
    /// verified). Returns `NotVerified` on an explicitly-plain chain.
    async fn require_verified(&self, chain: &str) -> Result<(), EvidenceError> {
        match store::get_chain_meta(&self.substrate, &self.keyspace, chain).await? {
            Some(m) if !m.verified => Err(EvidenceError::NotVerified(chain.to_string())),
            _ => Ok(()),
        }
    }

    /// Read the dense leaf hashes for seqs `1..=upto` on a verified chain.
    /// Errors if a slot is missing or lacks a `leaf_hash` (would indicate a
    /// non-verified or corrupted chain).
    async fn leaf_hashes(&self, chain: &str, upto: i64) -> Result<Vec<[u8; 32]>, EvidenceError> {
        let rows = self.read_range(chain, 1, upto).await?;
        if rows.len() as i64 != upto {
            return Err(Self::storage_err(format!(
                "expected {upto} dense entries for proof, found {}",
                rows.len()
            )));
        }
        let mut out = Vec::with_capacity(rows.len());
        for (seq, rec) in rows {
            let lh = rec
                .leaf_hash
                .ok_or_else(|| Self::storage_err(format!("entry {seq} has no leaf_hash")))?;
            out.push(lh);
        }
        Ok(out)
    }

    /// Merkle digest `{ size, root }` for a verified chain. O(log N) — folds the
    /// persisted frontier. Empty/never-appended verified chain → size 0,
    /// `empty_root`.
    pub async fn digest(&self, chain: &str) -> Result<Digest, EvidenceError> {
        self.require_verified(chain).await?;
        match store::get_frontier(&self.substrate, &self.keyspace, chain).await? {
            Some(f) => Ok(Digest { size: f.size, root: f.root() }),
            None => Ok(Digest { size: 0, root: crate::merkle::empty_root() }),
        }
    }

    /// Inclusion proof for `seq` (1-based) against tree size `size` (defaults to
    /// `head`). O(N) — reads leaf hashes for `1..=size`.
    pub async fn inclusion(
        &self,
        chain: &str,
        seq: i64,
        size: Option<i64>,
    ) -> Result<InclusionProof, EvidenceError> {
        self.require_verified(chain).await?;
        let head = self.head(chain).await?;
        let size = size.unwrap_or(head);
        if size < 1 || size > head {
            return Err(EvidenceError::InvalidArgument(format!(
                "size {size} out of range (head={head})"
            )));
        }
        if seq < 1 || seq > size {
            return Err(EvidenceError::InvalidArgument(format!(
                "seq {seq} out of range (size={size})"
            )));
        }
        let leaves = self.leaf_hashes(chain, size).await?;
        let audit_path = crate::merkle::inclusion_proof(&leaves, (seq - 1) as usize);
        Ok(InclusionProof { seq, size, audit_path })
    }

    /// Consistency proof between sizes `first` and `second` (second defaults to
    /// `head`). O(N).
    pub async fn consistency(
        &self,
        chain: &str,
        first: i64,
        second: Option<i64>,
    ) -> Result<ConsistencyProof, EvidenceError> {
        self.require_verified(chain).await?;
        let head = self.head(chain).await?;
        let second = second.unwrap_or(head);
        if first < 1 || first > second || second > head {
            return Err(EvidenceError::InvalidArgument(format!(
                "require 1 <= first <= second <= head ({first}, {second}, head={head})"
            )));
        }
        let leaves = self.leaf_hashes(chain, second).await?;
        let proof = crate::merkle::consistency_proof(&leaves, first as usize);
        Ok(ConsistencyProof { first, second, proof })
    }

    // ---- Erasure ----

    /// Redact the payload of entry `seq` on `chain` (any mode). Blanks the
    /// payload and sets `redacted = true`, **keeping** seq/type/at/edges and
    /// (on verified chains) `leaf_hash` — so digest and proofs still verify.
    /// Idempotent. 404 if the entry is absent. Caller must hold `schema:admin`.
    pub async fn redact(&self, chain: &str, seq: i64) -> Result<(), EvidenceError> {
        let _lease = self.write_lease.lock().await;
        let writer = self.substrate.require_writer().map_err(|_| EvidenceError::NotWriter)?;
        let mut rec = store::get_entry(&self.substrate, &self.keyspace, chain, seq)
            .await?
            .ok_or(EvidenceError::EntryNotFound { chain: chain.to_string(), seq })?;
        rec.payload = Vec::new();
        rec.redacted = true;
        let mut batch = WriteBatch::new();
        batch.put(self.keyspace.entry_key(chain, seq), &store::encode(&rec)?);
        writer
            .write_with_options(batch, &WriteOptions { await_durable: false, ..Default::default() })
            .await
            .map_err(Self::storage_err)?;
        drop(_lease);
        writer.flush().await.map_err(Self::storage_err)?;
        Ok(())
    }

    /// Hard-delete entry `seq` on a **plain** chain: removes the slot (leaving a
    /// gap); the counter does not decrement so `head` is unchanged. Verified
    /// chains return [`EvidenceError::VerifiedNoDelete`] (dropping a slot would
    /// break the consistency proof). 404 if the entry is absent. Caller must
    /// hold `schema:admin`.
    ///
    /// When `retract_edges` is true, the entry's `edges[]` are also retracted
    /// from the graph projection in the same batch — each edge identity the
    /// entry referenced (canonical + out + in) is deleted, so replaying the
    /// chain no longer reproduces this event's edges. This does NOT restore a
    /// prior weight: the graph is a rebuildable projection.
    pub async fn hard_delete(
        &self,
        chain: &str,
        seq: i64,
        retract_edges: bool,
    ) -> Result<(), EvidenceError> {
        let _lease = self.write_lease.lock().await;
        let writer = self.substrate.require_writer().map_err(|_| EvidenceError::NotWriter)?;
        let verified = store::get_chain_meta(&self.substrate, &self.keyspace, chain)
            .await?
            .map(|m| m.verified)
            .unwrap_or(true);
        if verified {
            return Err(EvidenceError::VerifiedNoDelete(chain.to_string()));
        }
        let rec = store::get_entry(&self.substrate, &self.keyspace, chain, seq)
            .await?
            .ok_or(EvidenceError::EntryNotFound { chain: chain.to_string(), seq })?;

        let mut batch = WriteBatch::new();
        batch.delete(self.keyspace.entry_key(chain, seq));

        // Retract the entry's edges from the graph projection (default on).
        if retract_edges && !rec.edges.is_empty() {
            let mut overlay: HashMap<Vec<u8>, Option<i64>> = HashMap::new();
            for e in &rec.edges {
                let d = EdgeDelta {
                    graph: e.graph.clone(),
                    src: e.src.clone(),
                    dst: e.dst.clone(),
                    weight: 0,
                    etype: e.etype.clone(),
                    op: crate::model::EdgeOp::Delete,
                };
                apply_edge_delta(&self.substrate, &self.keyspace, &mut batch, &mut overlay, &d).await?;
            }
        }

        writer
            .write_with_options(batch, &WriteOptions { await_durable: false, ..Default::default() })
            .await
            .map_err(Self::storage_err)?;
        drop(_lease);
        writer.flush().await.map_err(Self::storage_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use bluedb_sql::Database;
    use slatedb::{object_store::memory::InMemory, Db};

    async fn db() -> Database {
        let d = Db::open("chain-test", Arc::new(InMemory::new())).await.unwrap();
        Database::new(Arc::new(d))
    }

    /// Value-correctness for the persisted nodes: every persisted `(level, index)`
    /// node equals `merkle_root` of its leaf range over the actual stored leaves.
    /// In-crate test so it can reach `crate::merkle::{leaf_hash, merkle_root}` and
    /// `store::get_merkle_node` (all `pub(crate)`).
    #[tokio::test]
    async fn persisted_nodes_equal_merkle_root_of_their_leaf_range() {
        let database = db().await;
        let ev = Evidence::new(&database, "_");
        let substrate = database.substrate();
        let ks = EvidenceKeyspace::new("_");
        let mut leaves: Vec<[u8; 32]> = Vec::new();
        for i in 0..16u8 {
            let payload = vec![i];
            ev.append("c", vec![EntryInput { etype: "t".into(), payload: payload.clone(), at: String::new(), edges: vec![] }], None)
                .await
                .unwrap();
            leaves.push(crate::merkle::leaf_hash("t", &payload, "", &[]));
            let size = leaves.len() as u64;
            // Check every complete subtree fully inside [0, size).
            for level in 1u8..64 {
                let span = 1u64 << level;
                if span > size {
                    break;
                }
                let mut index = 0u64;
                while (index + 1) * span <= size {
                    let lo = (index * span) as usize;
                    let hi = lo + span as usize;
                    let got = store::get_merkle_node(&substrate, &ks, "c", level, index)
                        .await
                        .unwrap()
                        .unwrap_or_else(|| panic!("node (L{level},{index}) missing at size {size}"));
                    assert_eq!(
                        got,
                        crate::merkle::merkle_root(&leaves[lo..hi]),
                        "node (L{level},{index}) wrong at size {size}"
                    );
                    index += 1;
                }
            }
        }
    }
}
