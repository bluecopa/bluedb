# Evidence A — O(log N) Merkle proofs (persisted internal nodes)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Make `inclusion`/`consistency` proofs O(log N) by persisting complete-subtree node hashes (tag `0x1F`) on append and reading them at proof time, instead of reading all leaves (O(N)). Emitted proofs stay **byte-identical**.

**Spec:** `docs/superpowers/specs/2026-06-16-evidence-hardening-design.md` (Component A). Read it.

**Worktree:** Work ONLY in this worktree on branch `feat/evidence-hardening`. Commit-only, NEVER push. Co-author trailer `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. NO `cargo fmt`. Run tests directly with `2>&1` (no tail/grep pipes). "bluedb"/"bluecopa" lowercase. No backward-compat path (nothing deployed) — a missing node is a loud error, not a fallback.

**Touches (disjoint from Component B, which only touches `traverse.rs`):** `keyspace.rs`, `store.rs`, `merkle.rs`, `chain.rs`, new `proof.rs`, `lib.rs`, `tests/merkle_chain.rs`, `docs/evidence/chains.md`.

## Background facts (from the merged code)

- `model::Frontier { size: i64, peaks: Vec<[u8;32]> }`. `merkle::Frontier::push(&mut self, leaf)` carry-merges equal-height peaks (RFC 6962). `chain.rs::append` calls `frontier.push(lh)` per leaf and `batch.put(merkle_key, encode(&frontier))`.
- `chain.rs::inclusion(seq,size)`/`consistency(first,second)` currently call `self.leaf_hashes(chain, upto)` (reads ALL leaves via `read_range(1,upto)`) then `merkle::inclusion_proof(&leaves, seq-1)` / `merkle::consistency_proof(&leaves, first)`.
- `merkle.rs` pure fns: `node_hash`, `merkle_root(&[[u8;32]])`, `largest_pow2_lt(n)` (n≥2), `inclusion_proof(leaves,index)`, `consistency_proof(leaves,first)`, `empty_root`. These STAY as the test reference.
- `store::get_entry(substrate, ks, chain, seq) -> Option<EntryRecord>`; `EntryRecord.leaf_hash: Option<[u8;32]>` is `Some` on verified chains and retained across redaction. Verified chains can't hard-delete, so every seq `1..=head` exists with a `leaf_hash`.
- `EvidenceKeyspace { ks: bluedb_sql::Keyspace }`, `external_key(tag, suffix)`, existing `chain_suffix` = `(len u32-be)‖chain`. Tags: evidence `0x17–0x1B`, graph `0x1C–0x1E`. Next free: `0x1F`.

---

## Task A1: keyspace — Merkle node tag + key

**Files:** `crates/bluedb-evidence/src/keyspace.rs`

- [ ] **Step 1:** Add the tag (after `TAG_GRAPH_IN`):
```rust
pub(crate) const TAG_EVIDENCE_MERKLE_NODE: u8 = TAG_EXTERNAL_BASE + 15; // 0x1F  complete-subtree node
```
- [ ] **Step 2:** Add the key builder on `impl EvidenceKeyspace`:
```rust
/// Key for a persisted complete-subtree Merkle node at `(level, index)`:
/// `TAG_EVIDENCE_MERKLE_NODE <chain_suffix> level::u8 index::u64-be`. The node
/// is the Merkle root over leaves `[index·2^level, (index+1)·2^level)`.
pub(crate) fn merkle_node_key(&self, chain: &str, level: u8, index: u64) -> Vec<u8> {
    let mut s = Self::chain_suffix(chain);
    s.push(level);
    s.extend_from_slice(&index.to_be_bytes());
    self.ks.external_key(TAG_EVIDENCE_MERKLE_NODE, &s)
}
```
- [ ] **Step 3:** Test (in `mod tests`):
```rust
#[test]
fn merkle_node_keys_are_distinct_and_tenant_scoped() {
    let ks = EvidenceKeyspace::new("acme");
    assert_ne!(ks.merkle_node_key("c", 1, 0), ks.merkle_node_key("c", 1, 1));
    assert_ne!(ks.merkle_node_key("c", 1, 0), ks.merkle_node_key("c", 2, 0));
    assert_ne!(ks.merkle_node_key("c", 1, 0), EvidenceKeyspace::new("globex").merkle_node_key("c", 1, 0));
    // Distinct namespace from the frontier (0x1A) and chains.
    assert_ne!(ks.merkle_node_key("c", 1, 0), ks.merkle_key("c"));
}
```
- [ ] **Step 4:** `cargo test -p bluedb-evidence keyspace 2>&1` — PASS (dead_code on the new builder is expected until A2/A4).
- [ ] **Step 5:** Commit `feat(evidence): merkle node keyspace tag 0x1F`.

## Task A2: store — node reader

**Files:** `crates/bluedb-evidence/src/store.rs`

- [ ] **Step 1:** Add (after `get_frontier`):
```rust
/// Point read of a persisted complete-subtree Merkle node, or `None` if absent.
pub(crate) async fn get_merkle_node(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
    level: u8,
    index: u64,
) -> Result<Option<[u8; 32]>> {
    match substrate.get(&ks.merkle_node_key(chain, level, index)).await? {
        Some(b) => Ok(Some(b.as_ref().try_into().context("merkle node must be 32 bytes")?)),
        None => Ok(None),
    }
}
```
- [ ] **Step 2:** Test:
```rust
#[tokio::test]
async fn get_merkle_node_absent_then_present() {
    let database = writer_database().await;
    let substrate = database.substrate();
    let ks = EvidenceKeyspace::new("acme");
    assert_eq!(get_merkle_node(&substrate, &ks, "c", 1, 0).await.unwrap(), None);
    let writer = substrate.require_writer().unwrap();
    let h = [7u8; 32];
    writer.put(&ks.merkle_node_key("c", 1, 0), &h).await.unwrap();
    assert_eq!(get_merkle_node(&substrate, &ks, "c", 1, 0).await.unwrap(), Some(h));
}
```
- [ ] **Step 3:** `cargo test -p bluedb-evidence store 2>&1` — PASS.
- [ ] **Step 4:** Commit `feat(evidence): point read of a persisted merkle node`.

## Task A3: merkle — `push_emit` (yield merged complete-subtree parents)

**Files:** `crates/bluedb-evidence/src/merkle.rs`

- [ ] **Step 1:** Add a push variant that returns each merged parent's `(level, index, hash)`, and make `push` delegate to it. Replace the existing `push` with:
```rust
/// Like [`push`], but returns the complete-subtree parent nodes created by the
/// carry-merge, each as `(level, index, hash)` where the node is the root over
/// leaves `[index·2^level, (index+1)·2^level)`. Used to persist internal nodes.
pub(crate) fn push_emit(&mut self, leaf: [u8; 32]) -> Vec<(u8, u64, [u8; 32])> {
    let mut emitted = Vec::new();
    let mut carry = leaf;
    let mut carry_level: u8 = 0;
    let mut carry_start: u64 = self.size as u64; // leaf offset of the carry's range
    let mut s = self.size;
    while s & 1 == 1 {
        let left = self.peaks.pop().expect("frontier peak underflow");
        carry = node_hash(&left, &carry);
        carry_start -= 1u64 << carry_level; // left covers 2^carry_level leaves to the left
        carry_level += 1;
        let index = carry_start >> carry_level;
        emitted.push((carry_level, index, carry));
        s >>= 1;
    }
    self.peaks.push(carry);
    self.size += 1;
    emitted
}

/// Fold one new leaf into the frontier (carry-merge). O(log N).
pub(crate) fn push(&mut self, leaf: [u8; 32]) {
    let _ = self.push_emit(leaf);
}
```
- [ ] **Step 2:** Property test — every emitted node equals `merkle_root` of its leaf range, across sizes 0..N:
```rust
#[test]
fn push_emit_nodes_match_reference_subtree_roots() {
    use crate::model::Frontier;
    let leaves: Vec<[u8; 32]> = (0..300).map(leaf_n).collect();
    let mut f = Frontier::default();
    for n in 0..leaves.len() {
        for (level, index, hash) in f.push_emit(leaves[n]) {
            let lo = (index as usize) << level;
            let hi = lo + (1usize << level);
            assert!(hi <= n + 1, "node (L{level},{index}) exceeds appended leaves at size {}", n + 1);
            assert_eq!(hash, merkle_root(&leaves[lo..hi]), "node (L{level},{index}) wrong at size {}", n + 1);
        }
    }
    // push (delegating) still yields the same root as the reference.
    let mut g = Frontier::default();
    for (i, &lh) in leaves.iter().enumerate() {
        assert_eq!(g.root(), merkle_root(&leaves[..i]));
        g.push(lh);
    }
}
```
- [ ] **Step 3:** `cargo test -p bluedb-evidence merkle 2>&1` and `cargo test -p bluedb-evidence 2>&1` — PASS (the existing `frontier_root_agrees_*` still passes via the delegating `push`).
- [ ] **Step 4:** Commit `feat(evidence): Frontier::push_emit yields complete-subtree nodes`.

## Task A4: chain — persist nodes in the append batch

**Files:** `crates/bluedb-evidence/src/chain.rs`

- [ ] **Step 1:** In `append`, the verified-chain frontier block currently does:
```rust
        if verified {
            let mut frontier = store::get_frontier(...).await?.unwrap_or_default();
            for lh in leaves {
                frontier.push(lh);
            }
            batch.put(self.keyspace.merkle_key(chain), &store::encode(&frontier)?);
        }
```
Replace the inner loop so each push's emitted nodes are persisted in the same batch:
```rust
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
```
(`leaves: Vec<[u8;32]>` is already collected above. No other change to `append`.)
- [ ] **Step 2:** Test (extend `tests/merkle_chain.rs`) — after appends, persisted nodes equal recomputed subtree roots. Reuse the file's helpers; re-derive node keys via the public `bluedb_sql::Keyspace` (independent verifier):
```rust
#[tokio::test]
async fn append_persists_complete_subtree_nodes() {
    use bluedb_sql::Keyspace;
    let database = db().await; // existing helper in this test file
    let ev = Evidence::new(&database, "_");
    // Append 7 single-entry leaves on a verified (default) chain.
    let mut leaf_hashes = Vec::new();
    for i in 0..7u8 {
        let payload = vec![i];
        ev.append("c", vec![EntryInput { etype: "t".into(), payload: payload.clone(), at: String::new(), edges: vec![] }], None).await.unwrap();
        leaf_hashes.push(bluedb_evidence::leaf_hash_for_test("t", &payload, "", &[]));
        // ^ If no such test helper is exported, instead recompute the expected
        //   node from the digest path below; see note.
    }
    // node (level 2, index 0) must be the root over leaves [0,4).
    let ks = Keyspace::new("_");
    let mut suffix = (1u32).to_be_bytes().to_vec(); suffix.extend_from_slice(b"c"); // chain "c"
    suffix.push(2u8); suffix.extend_from_slice(&0u64.to_be_bytes());
    let key = ks.external_key(0x1F, &suffix);
    let node = database.substrate().get(&key).await.unwrap().expect("node (2,0) present");
    assert_eq!(node.len(), 32);
}
```
> **Implementer note:** `leaf_hash` is `pub(crate)` in `merkle.rs`, so an external integration test can't call it. Prefer asserting node *presence + size* (as above) plus a correctness check that does NOT need the raw leaf hash: e.g. assert that the persisted `(level,index)` node count after N appends equals `N − popcount(N)` by scanning the `0x1F` tag range for tenant `_`. If you want a value-correctness assertion, add it as a `#[cfg(test)]` UNIT test inside `chain.rs` (which can call `crate::merkle::leaf_hash` + `merkle_root`) rather than the integration test. Choose whichever gives a real assertion without weakening it.
- [ ] **Step 3:** `cargo test -p bluedb-evidence 2>&1` — PASS (existing merkle_chain/digest tests unchanged; digest still correct).
- [ ] **Step 4:** Commit `feat(evidence): persist complete-subtree nodes in the append batch`.

## Task A5: proof — storage-backed `subtree_root` + inclusion/consistency

**Files:** new `crates/bluedb-evidence/src/proof.rs`; `crates/bluedb-evidence/src/lib.rs`; `crates/bluedb-evidence/src/chain.rs`

- [ ] **Step 1:** Create `proof.rs`:
```rust
//! Storage-backed RFC 6962 proofs: O(log N) inclusion/consistency assembled
//! from persisted complete-subtree nodes (tag 0x1F) instead of reading all
//! leaves. Mirrors the pure recursion in `merkle.rs` exactly (same algorithm,
//! same byte output) but sources each subtree root from storage. No backward
//! compatibility: every complete subtree within [0, head) was persisted on
//! append, so a missing node is a bug → a loud error.

use bluedb_storage::Substrate;

use crate::error::EvidenceError;
use crate::keyspace::EvidenceKeyspace;
use crate::merkle::{largest_pow2_lt_pub as largest_pow2_lt, node_hash};
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
```
> **Note on `largest_pow2_lt`:** it is currently a private free fn in `merkle.rs`. Expose it to the crate — add `pub(crate) fn largest_pow2_lt_pub(n: usize) -> usize { largest_pow2_lt(n) }` in `merkle.rs` (or change the existing fn to `pub(crate)` and import it directly; pick the lower-churn option and keep `merkle.rs`'s own tests working). Likewise `node_hash` is already `pub(crate)`.
- [ ] **Step 2:** `lib.rs`: add `mod proof;`.
- [ ] **Step 3:** Rewrite `chain.rs::inclusion` and `consistency` to use the storage-backed module instead of `leaf_hashes` + `merkle::*`. Keep all the existing argument validation (size/seq range checks, `require_verified`, head lookup) — only swap the proof computation:
```rust
        // inclusion(): after validation computes `size` and checks seq/size ranges:
        let audit_path = crate::proof::inclusion(&self.substrate, &self.keyspace, chain, (seq - 1) as u64, size as u64).await?;
        Ok(InclusionProof { seq, size, audit_path })
```
```rust
        // consistency(): after validation computes `second` and checks first/second:
        let proof = crate::proof::consistency(&self.substrate, &self.keyspace, chain, first as u64, second as u64).await?;
        Ok(ConsistencyProof { first, second, proof })
```
Remove the now-unused `leaf_hashes` helper (and its `read_range(1,upto)` density check) **if** nothing else uses it (grep first; `digest` does not).
- [ ] **Step 4:** Property tests in `proof.rs` `#[cfg(test)]` — storage-backed == pure, byte-for-byte, over many sizes. Build a chain in an in-memory db, then compare:
```rust
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
```
(`merkle::leaf_hash`/`inclusion_proof`/`consistency_proof` are `pub(crate)` — reachable from this in-crate test module.)
- [ ] **Step 5:** `cargo test -p bluedb-evidence 2>&1` — PASS (proof module + existing merkle_chain + server e2e via the crate). `cargo clippy -p bluedb-evidence --all-targets 2>&1` — no new warnings.
- [ ] **Step 6:** Commit `feat(evidence): O(log N) storage-backed inclusion/consistency proofs`.

## Task A6: integration + docs + green

**Files:** `crates/bluedb-evidence/tests/merkle_chain.rs`, `docs/evidence/chains.md`

- [ ] **Step 1:** Extend `merkle_chain.rs`: after several separate appends on a verified chain, assert `inclusion` reconstructs the `digest` root (already a pattern there) AND that proof length matches the RFC 6962 expectation (O(log N), not O(N)). Confirm a redacted entry still yields verifying proofs (leaf_hash retained).
- [ ] **Step 2:** Update `docs/evidence/chains.md`: in **Merkle verification** and **Limitations (v1)**, remove the "proofs are O(N), computed on demand / no persisted internal-node store" bullet and replace with: proofs are **O(log N)** via a persisted complete-subtree node store (tag `0x1F`), written in the same atomic batch as entries. Keep the **unsigned digests / trust model** limitation (unchanged).
- [ ] **Step 3:** `cargo test -p bluedb-evidence 2>&1` and `cargo test -p bluedb-server --test evidence 2>&1` — PASS. `cargo clippy -p bluedb-evidence --all-targets 2>&1` — clean.
- [ ] **Step 4:** Commit `docs(evidence): O(log N) proofs; persisted node store`.

## Acceptance
- Storage-backed proofs are byte-identical to the pure `merkle` proofs for all `(index,size)`/`(first,second)` over the tested size range, and verify against the independent RFC 6962 verifiers.
- Persisted `(level,index)` nodes equal `merkle_root` of their leaf ranges for all sizes.
- `inclusion`/`consistency` no longer read all leaves (no `leaf_hashes` in the proof path); `digest` unchanged.
- `cargo test -p bluedb-evidence` green; no new clippy warnings; `chains.md` limitation updated.
