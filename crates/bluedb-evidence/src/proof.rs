//! Storage-backed RFC 6962 proofs: O(log N) inclusion/consistency assembled
//! from persisted complete-subtree nodes (tag 0x1F) instead of reading all
//! leaves. Mirrors the pure recursion in `merkle.rs` exactly (same algorithm,
//! same byte output) but sources each subtree root from storage. No backward
//! compatibility: every complete subtree within [0, head) was persisted on
//! append, so a missing node is a bug → a loud error.

use bluedb_storage::Substrate;

use crate::error::EvidenceError;
use crate::keyspace::EvidenceKeyspace;
use crate::merkle::{largest_pow2_lt, node_hash};
use crate::store;

fn err(msg: impl std::fmt::Display) -> EvidenceError {
    EvidenceError::Storage(anyhow::anyhow!("{msg}"))
}

/// Merkle root over leaves `[lo, hi)` (0-based), O(log N), from persisted nodes.
/// Level-0 (single leaf) reads the entry's `leaf_hash`. A complete aligned range
/// is one node `get`; otherwise split RFC-style and recurse the right spine.
pub(crate) async fn subtree_root(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
    lo: u64,
    hi: u64,
) -> Result<[u8; 32], EvidenceError> {
    let n = hi - lo;
    if n == 1 {
        // seq is 1-based: leaf index lo → seq lo+1.
        let rec = store::get_entry(substrate, ks, chain, (lo + 1) as i64)
            .await?
            .ok_or_else(|| err(format!("entry {} missing for proof", lo + 1)))?;
        return rec.leaf_hash.ok_or_else(|| err(format!("entry {} has no leaf_hash", lo + 1)));
    }
    if n.is_power_of_two() && lo % n == 0 {
        let level = n.trailing_zeros() as u8;
        let index = lo >> level;
        return store::get_merkle_node(substrate, ks, chain, level, index)
            .await?
            .ok_or_else(|| err(format!("merkle node (L{level},{index}) missing")));
    }
    let k = largest_pow2_lt(n as usize) as u64;
    let left = Box::pin(subtree_root(substrate, ks, chain, lo, lo + k)).await?;
    let right = Box::pin(subtree_root(substrate, ks, chain, lo + k, hi)).await?;
    Ok(node_hash(&left, &right))
}

/// O(log N) inclusion audit path for 0-based `index` against tree size `size`.
/// Byte-identical to `merkle::inclusion_proof(&leaves[..size], index)`.
pub(crate) async fn inclusion(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
    index: u64,
    size: u64,
) -> Result<Vec<[u8; 32]>, EvidenceError> {
    incl(substrate, ks, chain, 0, size, index).await
}

async fn incl(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
    lo: u64,
    hi: u64,
    index: u64,
) -> Result<Vec<[u8; 32]>, EvidenceError> {
    let n = hi - lo;
    if n == 1 {
        return Ok(Vec::new());
    }
    let k = largest_pow2_lt(n as usize) as u64;
    let mid = lo + k;
    if index < mid {
        let mut p = Box::pin(incl(substrate, ks, chain, lo, mid, index)).await?;
        p.push(subtree_root(substrate, ks, chain, mid, hi).await?);
        Ok(p)
    } else {
        let mut p = Box::pin(incl(substrate, ks, chain, mid, hi, index)).await?;
        p.push(subtree_root(substrate, ks, chain, lo, mid).await?);
        Ok(p)
    }
}

/// O(log N) consistency proof (size `first` is a prefix of size `second`).
/// Byte-identical to `merkle::consistency_proof(&leaves[..second], first)`.
pub(crate) async fn consistency(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
    first: u64,
    second: u64,
) -> Result<Vec<[u8; 32]>, EvidenceError> {
    if first == 0 {
        return Ok(Vec::new());
    }
    subproof(substrate, ks, chain, first, 0, second, true).await
}

// Mirrors merkle::subproof over the range [lo, hi) (global coords). `m` is the
// prefix size measured from `lo`.
async fn subproof(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
    m: u64,
    lo: u64,
    hi: u64,
    b: bool,
) -> Result<Vec<[u8; 32]>, EvidenceError> {
    let n = hi - lo;
    if m == n {
        return Ok(if b { Vec::new() } else { vec![subtree_root(substrate, ks, chain, lo, hi).await?] });
    }
    let k = largest_pow2_lt(n as usize) as u64;
    if m <= k {
        let mut p = Box::pin(subproof(substrate, ks, chain, m, lo, lo + k, b)).await?;
        p.push(subtree_root(substrate, ks, chain, lo + k, hi).await?);
        Ok(p)
    } else {
        let mut p = Box::pin(subproof(substrate, ks, chain, m - k, lo + k, hi, false)).await?;
        p.push(subtree_root(substrate, ks, chain, lo, lo + k).await?);
        Ok(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use bluedb_sql::Database;
    use slatedb::{object_store::memory::InMemory, Db};
    use crate::chain::{Evidence, EntryInput};
    use crate::merkle;

    async fn db() -> Database {
        let d = Db::open("proof-test", Arc::new(InMemory::new())).await.unwrap();
        Database::new(Arc::new(d))
    }

    #[tokio::test]
    async fn storage_proofs_match_pure_for_many_sizes() {
        let database = db().await;
        let ev = Evidence::new(&database, "_");
        let substrate = database.substrate();
        let ks = EvidenceKeyspace::new("_");
        let mut leaves: Vec<[u8; 32]> = Vec::new();
        for i in 0..64u32 {
            let payload = i.to_be_bytes().to_vec();
            ev.append("c", vec![EntryInput { etype: "t".into(), payload: payload.clone(), at: String::new(), edges: vec![] }], None).await.unwrap();
            leaves.push(merkle::leaf_hash("t", &payload, "", &[]));
            let size = leaves.len();
            for index in 0..size {
                let got = inclusion(&substrate, &ks, "c", index as u64, size as u64).await.unwrap();
                assert_eq!(got, merkle::inclusion_proof(&leaves[..size], index), "inclusion {index}/{size}");
            }
            for first in 1..=size {
                let got = consistency(&substrate, &ks, "c", first as u64, size as u64).await.unwrap();
                assert_eq!(got, merkle::consistency_proof(&leaves[..size], first), "consistency {first}->{size}");
            }
        }
    }
}
