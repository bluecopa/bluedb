# bluedb-evidence Plan 2 — Merkle verification + erasure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. **Work in the worktree `/Users/satya/work/bc/bluedb-evidence-wt`** (branch `feat/evidence-substrate`). Commit-only, **never push**. **No `cargo fmt`** (repo has no rustfmt.toml; it reformats ~90 files — match style by hand). Run tests directly with `2>&1` (no `tail`/`grep` pipes). End every commit message with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. "bluedb"/"bluecopa" are always lowercase.

**Goal:** Add per-chain RFC 6962 Merkle verification (leaf/node hashing, an incremental frontier persisted with each append, digest + inclusion + consistency proofs) and GDPR-grade erasure (redaction-in-place on any chain; hard-delete on plain chains only) to the `bluedb-evidence` crate, exposed over HTTP.

**Architecture:** A new pure `merkle` module implements RFC 6962 (the Certificate Transparency / Trillian model). Verified chains compute a `leaf_hash` per entry and fold it into a persisted `Frontier` (≤ log N perfect-subtree peaks) **inside the same `WriteBatch`** as the entry — so the tree advances atomically and crash-consistently with the log. Proofs are computed **on demand from the stored `leaf_hash`es** (O(N), no persisted internal-node store — the v1 simplification from spec §7.3). Redaction blanks the payload but **keeps** the seq slot and `leaf_hash`, so digests and proofs still verify (crypto-shred / redactable Merkle). Hard-delete (plain chains only) removes the slot, leaving a gap. Verification is **client-side**; the crate ships proof *generation* only, and an independent RFC 6962 *verifier* lives in the test module as the correctness oracle.

**Tech Stack:** Rust, `sha2` (already a dep), `slatedb::WriteBatch`, `postcard`, `serde`, `axum`. No new runtime dependencies.

---

## Background the implementer needs

The `bluedb-evidence` crate (Plan 1) is complete and green. Read these files before starting; the tasks below extend them:

- `crates/bluedb-evidence/src/model.rs` — record types. `EntryRecord { etype, payload, at, edges: Vec<EdgeDelta>, leaf_hash: Option<[u8;32]>, redacted: bool }` and `ChainMeta { verified: bool }` already exist. `EdgeDelta { graph, src, dst, weight: i64, etype, op: EdgeOp }`, `EdgeOp::{ Upsert { merge: Merge }, Delete }`, `Merge::{ Set, Max }`.
- `crates/bluedb-evidence/src/keyspace.rs` — `EvidenceKeyspace`. Tags: `TAG_EVIDENCE_ENTRY=0x17`, `TAG_EVIDENCE_SEQ=0x18`, `TAG_EVIDENCE_IDEM=0x19`, `TAG_EVIDENCE_CHAIN=0x1B`. **`0x1A` (Merkle) is reserved and unused** — this plan claims it. Helpers: `entry_key(chain, seq)`, `entry_prefix(chain)`, `entry_prefix_end(chain)`, `seq_key`, `idem_key`, `chain_meta_key`, plus private `chain_suffix(chain)`.
- `crates/bluedb-evidence/src/store.rs` — `encode`/`decode` (postcard), `get_chain_meta`, `get_seq`, `get_idem`, and `get_entry` (currently `#[allow(dead_code)]` — this plan uses it, so remove that allow when it becomes live). The `writer_database()` test harness opens an in-memory `Db` and wraps it via `Database::new(Arc::new(db))`.
- `crates/bluedb-evidence/src/chain.rs` — the `Evidence` handle. `new(&Database, tenant)` captures `substrate`, `write_lease`, `keyspace`. `append` is the reference write path: lock the lease → `require_writer` → idempotency → seq RMW → build one `WriteBatch` → `write_with_options(await_durable:false)` → `drop(lease)` → `flush()`. The helper `fn storage_err(e: impl Display) -> EvidenceError` wraps any error into `EvidenceError::Storage(anyhow!)`. Reads (`read_range`, `read_from`, `scan_entries`) decode `EntryRecord`s.
- `crates/bluedb-evidence/src/error.rs` — `EvidenceError::{ IdemConflict, ChainModeConflict(String), EntryNotFound{chain,seq}, NotWriter, Storage(anyhow::Error) }`.
- `crates/bluedb-server/src/evidence_api.rs` — HTTP handlers. `map_evidence_err` maps `EvidenceError → AppError`. Handlers call `state.require_active()?` (writes), `state.authorize(&headers, Scope::…)?`, `state.tenant(&headers)?`. base64 wire via `B64` (`base64::engine::general_purpose::STANDARD`).
- `crates/bluedb-server/src/lib.rs` — `AppState::evidence(&tenant)` (line ~562), `tenant`/`authorize`/`require_active`, `AppError::{ bad_request, internal, not_found, conflict, service_unavailable }` (line ~1062), the `build_app` route block (evidence routes ~617).
- `crates/bluedb-server/src/authz.rs` — `Scope::{ DataRead, DataWrite, DataQuery, SchemaAdmin, Superuser }`; `data:read`/`data:write`/`schema:admin` token strings.
- `crates/bluedb-server/tests/evidence.rs` — the e2e harness (`spawn_app`, `call`, `x-bluedb-tenant`). Extend this file's tests.

**RFC 6962 conventions used throughout:** leaves are **already** `leaf_hash`es (`SHA256(0x00 ‖ frame)`), so the Merkle Tree Hash of a single leaf is the leaf itself. Internal node = `SHA256(0x01 ‖ left ‖ right)`. Empty tree = `SHA256(<empty>)`. `k` in the recursion is the **largest power of two strictly less than `n`** (the left subtree is the largest perfect subtree). Indices inside `merkle` are **0-based**; the public `seq` is **1-based** (leaf index = `seq - 1`); the handler does the mapping.

---

## File Structure

- **Create** `crates/bluedb-evidence/src/merkle.rs` — pure RFC 6962: `empty_root`, `leaf_hash`, `node_hash`, `merkle_root` (reference), `Frontier` impl (`push`/`root`), `inclusion_proof`, `consistency_proof`. Plus `#[cfg(test)]` independent verifiers (`verify_inclusion`, `verify_consistency`). One responsibility: tree math over `[u8;32]` hashes. No I/O.
- **Modify** `crates/bluedb-evidence/src/model.rs` — add `Frontier { size: i64, peaks: Vec<[u8;32]> }` (serde).
- **Modify** `crates/bluedb-evidence/src/keyspace.rs` — add `TAG_EVIDENCE_MERKLE=0x1A` + `merkle_key(chain)`.
- **Modify** `crates/bluedb-evidence/src/store.rs` — add `get_frontier`; drop the `#[allow(dead_code)]` on `get_entry` once used.
- **Modify** `crates/bluedb-evidence/src/error.rs` — add `NotVerified`, `VerifiedNoDelete`, `InvalidArgument(String)`.
- **Modify** `crates/bluedb-evidence/src/chain.rs` — wire `leaf_hash`+frontier into `append`; add `digest`/`inclusion`/`consistency` reads and `redact`/`hard_delete` writes; add public result types `Digest`, `InclusionProof`, `ConsistencyProof`.
- **Modify** `crates/bluedb-evidence/src/lib.rs` — `mod merkle;` (private) and `pub use` the new result types.
- **Modify** `crates/bluedb-server/src/evidence_api.rs` — handlers `redact`, `hard_delete`, `digest`, `inclusion`, `consistency`; extend `map_evidence_err`; a `hex32` helper.
- **Modify** `crates/bluedb-server/src/lib.rs` — 5 new routes; `delete` import.
- **Modify** `crates/bluedb-server/tests/evidence.rs` — e2e for Merkle + erasure.
- **Test** `crates/bluedb-evidence/tests/merkle_chain.rs` — integration tests for the wired append/digest/proof/erasure paths (uses the existing `harness::memory_db`).

---

### Task 1: Frontier model + Merkle keyspace tag + store reader

**Files:**
- Modify: `crates/bluedb-evidence/src/model.rs`
- Modify: `crates/bluedb-evidence/src/keyspace.rs`
- Modify: `crates/bluedb-evidence/src/store.rs`

- [ ] **Step 1: Add the `Frontier` record to `model.rs`**

Append after `ChainMeta`:

```rust
/// RFC 6962 incremental Merkle frontier: the ≤ log N perfect-subtree roots
/// ("peaks") covering `size` leaves, ordered left→right (largest subtree first).
/// Persisted per verified chain and advanced in the same WriteBatch as entries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frontier {
    pub size: i64,
    pub peaks: Vec<[u8; 32]>,
}
```

- [ ] **Step 2: Add a roundtrip test in `model.rs`**

```rust
#[test]
fn frontier_roundtrips_through_postcard() {
    let f = Frontier { size: 3, peaks: vec![[1u8; 32], [2u8; 32]] };
    let bytes = postcard::to_allocvec(&f).unwrap();
    let back: Frontier = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(f, back);
}
```

- [ ] **Step 3: Add the Merkle tag + key in `keyspace.rs`**

Replace the reserved-tag comment and add the constant next to the others:

```rust
pub(crate) const TAG_EVIDENCE_MERKLE: u8 = TAG_EXTERNAL_BASE + 10; // 0x1A
```

Update the trailing comment to: `// 0x1C-0x1E (graph) reserved for later plans.`

Add the key builder after `chain_meta_key`:

```rust
    /// Key for the per-chain Merkle frontier (verified chains only).
    pub(crate) fn merkle_key(&self, chain: &str) -> Vec<u8> {
        self.ks.external_key(TAG_EVIDENCE_MERKLE, &Self::chain_suffix(chain))
    }
```

- [ ] **Step 4: Extend the keyspace namespace test**

In `keyspace.rs` `tests::seq_and_idem_and_chain_meta_are_distinct_namespaces`, add the merkle key and assert ordering `IDEM(0x19) < MERKLE(0x1A) < CHAIN(0x1B)`:

```rust
        let merkle = ks.merkle_key("c");
        assert_ne!(merkle, entry);
        assert_ne!(merkle, seq);
        assert_ne!(merkle, idem);
        assert_ne!(merkle, meta);
        assert!(idem < merkle);
        assert!(merkle < meta);
```

- [ ] **Step 5: Add `get_frontier` to `store.rs`**

Add the import of `Frontier` to the `use crate::model::{...}` line, then:

```rust
/// Point read of the Merkle frontier for `chain`, or `None` if the chain has no
/// frontier yet (never appended-to as a verified chain).
pub(crate) async fn get_frontier(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
) -> Result<Option<crate::model::Frontier>> {
    match substrate.get(&ks.merkle_key(chain)).await? {
        Some(b) => Ok(Some(decode(&b)?)),
        None => Ok(None),
    }
}
```

- [ ] **Step 6: Build + test**

Run: `cargo test -p bluedb-evidence 2>&1`
Expected: PASS (existing 21 tests + the 2 new ones).

- [ ] **Step 7: Commit**

```bash
git add crates/bluedb-evidence/src/model.rs crates/bluedb-evidence/src/keyspace.rs crates/bluedb-evidence/src/store.rs
git commit -m "feat(evidence): Frontier record + 0x1A Merkle keyspace tag + reader

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Merkle hashing primitives (`empty_root`, `node_hash`, `leaf_hash`)

**Files:**
- Create: `crates/bluedb-evidence/src/merkle.rs`
- Modify: `crates/bluedb-evidence/src/lib.rs`

- [ ] **Step 1: Create `merkle.rs` with the hashing primitives**

```rust
//! Pure RFC 6962 (Certificate Transparency / Trillian) Merkle tree math over
//! 32-byte hashes. No I/O. Leaves passed to `merkle_root`/proofs are already
//! `leaf_hash`es, so the Merkle Tree Hash of a single leaf is the leaf itself.
//!
//! Domain separation: leaf prefix `0x00`, node prefix `0x01` (RFC 6962 §2.1).
//! Indices here are 0-based; the public `seq` is 1-based (leaf index = seq - 1).

use sha2::{Digest, Sha256};

use crate::model::{EdgeDelta, EdgeOp, Merge};

/// Stable 1-byte discriminant for an edge op, used in the leaf framing so the
/// hash attests the exact op. Matches the idempotency fingerprint in `chain`.
fn op_discriminant(op: &EdgeOp) -> u8 {
    match op {
        EdgeOp::Upsert { merge: Merge::Set } => 0,
        EdgeOp::Upsert { merge: Merge::Max } => 1,
        EdgeOp::Delete => 2,
    }
}

/// Digest of an empty tree: `SHA256(<empty>)` (RFC 6962 §2.1).
pub(crate) fn empty_root() -> [u8; 32] {
    Sha256::new().finalize().into()
}

/// Internal node hash: `SHA256(0x01 ‖ left ‖ right)`.
pub(crate) fn node_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x01]);
    h.update(left);
    h.update(right);
    h.finalize().into()
}

/// Leaf hash: `SHA256(0x00 ‖ frame(type, payload, at, edges))`. Each field is
/// length-delimited (u64-be length, then bytes); edges are sorted by
/// `(graph, src, dst, etype, op)` then framed (each field length-delimited,
/// then `weight` as 8 BE bytes, then the op discriminant). A copy of the exact
/// bytes — no payload normalization (R3 byte-exactness preserved).
pub(crate) fn leaf_hash(etype: &str, payload: &[u8], at: &str, edges: &[EdgeDelta]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x00]);
    for field in [etype.as_bytes(), payload, at.as_bytes()] {
        h.update((field.len() as u64).to_be_bytes());
        h.update(field);
    }
    let mut sorted: Vec<&EdgeDelta> = edges.iter().collect();
    sorted.sort_by(|a, b| {
        (&a.graph, &a.src, &a.dst, &a.etype, op_discriminant(&a.op))
            .cmp(&(&b.graph, &b.src, &b.dst, &b.etype, op_discriminant(&b.op)))
    });
    h.update((sorted.len() as u64).to_be_bytes());
    for e in sorted {
        for field in [e.graph.as_bytes(), e.src.as_bytes(), e.dst.as_bytes(), e.etype.as_bytes()] {
            h.update((field.len() as u64).to_be_bytes());
            h.update(field);
        }
        h.update(e.weight.to_be_bytes());
        h.update([op_discriminant(&e.op)]);
    }
    h.finalize().into()
}
```

- [ ] **Step 2: Register the module in `lib.rs`**

Add `mod merkle;` alongside the other `mod` lines (private — proofs are surfaced through `chain`).

- [ ] **Step 3: Add primitive tests at the bottom of `merkle.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // SHA256("") — the RFC 6962 empty-tree digest. Golden vector.
    const EMPTY_SHA256_HEX: &str =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    fn hex(b: &[u8; 32]) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        for x in b {
            write!(s, "{x:02x}").unwrap();
        }
        s
    }

    #[test]
    fn empty_root_matches_sha256_of_empty() {
        assert_eq!(hex(&empty_root()), EMPTY_SHA256_HEX);
    }

    #[test]
    fn node_hash_is_order_sensitive() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        assert_ne!(node_hash(&a, &b), node_hash(&b, &a));
    }

    #[test]
    fn leaf_hash_is_deterministic_and_field_sensitive() {
        let h1 = leaf_hash("t", b"payload", "at", &[]);
        let h2 = leaf_hash("t", b"payload", "at", &[]);
        assert_eq!(h1, h2);
        assert_ne!(h1, leaf_hash("t", b"payloaD", "at", &[]));
        assert_ne!(h1, leaf_hash("t", b"payload", "AT", &[]));
        // The 0x00 leaf prefix separates leaf and node domains.
        assert_ne!(leaf_hash("", b"", "", &[]), empty_root());
    }

    #[test]
    fn leaf_hash_edge_order_invariant() {
        let e = |dst: &str| EdgeDelta {
            graph: "g".into(),
            src: "a".into(),
            dst: dst.into(),
            weight: 1,
            etype: String::new(),
            op: EdgeOp::Upsert { merge: Merge::Set },
        };
        // Same edge set, different input order → identical leaf hash.
        assert_eq!(
            leaf_hash("t", b"p", "at", &[e("b"), e("c")]),
            leaf_hash("t", b"p", "at", &[e("c"), e("b")]),
        );
        // Different edge content → different hash.
        assert_ne!(
            leaf_hash("t", b"p", "at", &[e("b")]),
            leaf_hash("t", b"p", "at", &[e("b"), e("c")]),
        );
    }
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p bluedb-evidence --lib merkle 2>&1`
Expected: PASS (4 tests). The empty-root golden vector pins the hash domain.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-evidence/src/merkle.rs crates/bluedb-evidence/src/lib.rs
git commit -m "feat(evidence): RFC 6962 leaf/node/empty hashing primitives

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: `merkle_root` (reference) + incremental `Frontier` + agreement property

**Files:**
- Modify: `crates/bluedb-evidence/src/merkle.rs`

- [ ] **Step 1: Add `largest_pow2_lt` + `merkle_root` + `Frontier` impl**

Add to `merkle.rs` (after the primitives, before the test module):

```rust
/// Largest power of two strictly less than `n`. Requires `n >= 2`.
fn largest_pow2_lt(n: usize) -> usize {
    debug_assert!(n >= 2);
    let mut k = 1;
    while k << 1 < n {
        k <<= 1;
    }
    k
}

/// RFC 6962 Merkle Tree Hash over a slice of leaf hashes (the from-scratch
/// reference; O(N)). Empty → `empty_root`; single → that leaf.
pub(crate) fn merkle_root(leaves: &[[u8; 32]]) -> [u8; 32] {
    match leaves.len() {
        0 => empty_root(),
        1 => leaves[0],
        n => {
            let k = largest_pow2_lt(n);
            node_hash(&merkle_root(&leaves[..k]), &merkle_root(&leaves[k..]))
        }
    }
}

impl crate::model::Frontier {
    /// Fold one new leaf into the frontier (RFC 6962 incremental append): push
    /// it as a height-0 peak, then carry-merge equal-height peaks. The number of
    /// merges equals the count of trailing 1-bits in the old size. O(log N).
    pub(crate) fn push(&mut self, leaf: [u8; 32]) {
        let mut carry = leaf;
        let mut s = self.size;
        while s & 1 == 1 {
            let left = self.peaks.pop().expect("frontier peak underflow");
            carry = node_hash(&left, &carry);
            s >>= 1;
        }
        self.peaks.push(carry);
        self.size += 1;
    }

    /// Digest at the current size: "bag the peaks" right→left. O(log N).
    pub(crate) fn root(&self) -> [u8; 32] {
        let mut iter = self.peaks.iter().rev();
        match iter.next() {
            None => empty_root(),
            Some(&last) => {
                let mut acc = last;
                for p in iter {
                    acc = node_hash(p, &acc);
                }
                acc
            }
        }
    }
}
```

- [ ] **Step 2: Add the agreement property test**

Add to the `merkle.rs` `tests` module:

```rust
    // Deterministic pseudo-random leaves (no Math.random; derive from index).
    fn leaf_n(i: usize) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update((i as u64).to_be_bytes());
        h.finalize().into()
    }

    #[test]
    fn frontier_root_agrees_with_reference_root_for_all_sizes() {
        use crate::model::Frontier;
        let leaves: Vec<[u8; 32]> = (0..300).map(leaf_n).collect();
        let mut f = Frontier::default();
        assert_eq!(f.root(), empty_root());
        for n in 0..leaves.len() {
            assert_eq!(
                f.root(),
                merkle_root(&leaves[..n]),
                "frontier root diverged at size {n}"
            );
            // peak count == popcount(size)
            assert_eq!(f.peaks.len(), (f.size as u64).count_ones() as usize);
            f.push(leaves[n]);
        }
    }

    #[test]
    fn merkle_root_small_cases_are_explicit() {
        let h: Vec<[u8; 32]> = (0..4).map(leaf_n).collect();
        // 2 leaves: node(h0, h1)
        assert_eq!(merkle_root(&h[..2]), node_hash(&h[0], &h[1]));
        // 3 leaves: node(node(h0,h1), h2)   (k = 2)
        assert_eq!(
            merkle_root(&h[..3]),
            node_hash(&node_hash(&h[0], &h[1]), &h[2])
        );
        // 4 leaves: node(node(h0,h1), node(h2,h3))
        assert_eq!(
            merkle_root(&h[..4]),
            node_hash(&node_hash(&h[0], &h[1]), &node_hash(&h[2], &h[3]))
        );
    }
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p bluedb-evidence --lib merkle 2>&1`
Expected: PASS. The agreement test is the gate: the incrementally-built frontier must equal the from-scratch root at every size 0..300.

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-evidence/src/merkle.rs
git commit -m "feat(evidence): reference Merkle root + incremental frontier (agree 0..300)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Inclusion proofs + independent verifier + property test

**Files:**
- Modify: `crates/bluedb-evidence/src/merkle.rs`

- [ ] **Step 1: Add `inclusion_proof` generation**

Add to `merkle.rs` (after `merkle_root`):

```rust
/// RFC 6962 inclusion proof (PATH(m, D[n])) for the 0-based `index` into
/// `leaves`. The audit path lists sibling subtree roots bottom-up. O(N).
/// Panics if `index >= leaves.len()`.
pub(crate) fn inclusion_proof(leaves: &[[u8; 32]], index: usize) -> Vec<[u8; 32]> {
    let n = leaves.len();
    assert!(index < n, "inclusion index out of range");
    if n == 1 {
        return Vec::new();
    }
    let k = largest_pow2_lt(n);
    if index < k {
        let mut p = inclusion_proof(&leaves[..k], index);
        p.push(merkle_root(&leaves[k..]));
        p
    } else {
        let mut p = inclusion_proof(&leaves[k..], index - k);
        p.push(merkle_root(&leaves[..k]));
        p
    }
}
```

- [ ] **Step 2: Add the independent verifier + property test**

The verifier is the Trillian `RootFromInclusionProof` formulation (RFC 6962 §2.1.1) — written independently of the generation recursion so the property test is a genuine cross-check. Add to the `tests` module:

```rust
    /// Independent RFC 6962 inclusion verifier (Trillian formulation). Returns
    /// the reconstructed root; the caller compares to the trusted root.
    fn root_from_inclusion(leaf: [u8; 32], index: usize, size: usize, proof: &[[u8; 32]]) -> Option<[u8; 32]> {
        if index >= size {
            return None;
        }
        let x = (index ^ (size - 1)) as u64;
        let inner = (64 - x.leading_zeros()) as usize; // bits.Len64
        let border = ((index >> inner) as u64).count_ones() as usize;
        if proof.len() != inner + border {
            return None;
        }
        let mut res = leaf;
        for (i, h) in proof[..inner].iter().enumerate() {
            if (index >> i) & 1 == 0 {
                res = node_hash(&res, h);
            } else {
                res = node_hash(h, &res);
            }
        }
        for h in &proof[inner..] {
            res = node_hash(h, &res);
        }
        Some(res)
    }

    #[test]
    fn inclusion_proofs_verify_for_every_index_and_size() {
        let leaves: Vec<[u8; 32]> = (0..130).map(leaf_n).collect();
        for size in 1..=leaves.len() {
            let root = merkle_root(&leaves[..size]);
            for index in 0..size {
                let proof = inclusion_proof(&leaves[..size], index);
                assert_eq!(
                    root_from_inclusion(leaves[index], index, size, &proof),
                    Some(root),
                    "inclusion failed: index {index} of size {size}"
                );
                // Tamper: flip a byte of the first proof node → must NOT verify.
                if let Some(first) = proof.first() {
                    let mut bad = proof.clone();
                    bad[0][0] ^= 0xff;
                    let _ = first;
                    assert_ne!(
                        root_from_inclusion(leaves[index], index, size, &bad),
                        Some(root),
                        "tampered inclusion verified: index {index} of size {size}"
                    );
                }
            }
        }
    }
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p bluedb-evidence --lib merkle 2>&1`
Expected: PASS. If a proof fails to verify, RFC 6962 §2.1.1 is the source of truth — fix the generation/verifier to match; do not weaken the assertion.

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-evidence/src/merkle.rs
git commit -m "feat(evidence): RFC 6962 inclusion proofs + independent verifier

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Consistency proofs + independent verifier + property test

**Files:**
- Modify: `crates/bluedb-evidence/src/merkle.rs`

- [ ] **Step 1: Add `consistency_proof` generation**

RFC 6962 PROOF(m, D[n]) = SUBPROOF(m, D[n], true). Add to `merkle.rs`:

```rust
/// RFC 6962 consistency proof that the tree at size `first` is a prefix of the
/// tree formed by all of `leaves` (size = `leaves.len()`). `first` is a leaf
/// count, `1 <= first <= leaves.len()`. O(N). `first == 0` → empty proof.
pub(crate) fn consistency_proof(leaves: &[[u8; 32]], first: usize) -> Vec<[u8; 32]> {
    if first == 0 {
        return Vec::new();
    }
    subproof(first, leaves, true)
}

fn subproof(m: usize, leaves: &[[u8; 32]], b: bool) -> Vec<[u8; 32]> {
    let n = leaves.len();
    if m == n {
        // The subtree is complete: include its root unless it's the original
        // tree the verifier already knows (`b == true`).
        if b {
            Vec::new()
        } else {
            vec![merkle_root(leaves)]
        }
    } else {
        let k = largest_pow2_lt(n);
        if m <= k {
            let mut p = subproof(m, &leaves[..k], b);
            p.push(merkle_root(&leaves[k..]));
            p
        } else {
            let mut p = subproof(m - k, &leaves[k..], false);
            p.push(merkle_root(&leaves[..k]));
            p
        }
    }
}
```

- [ ] **Step 2: Add the independent verifier + property test**

Trillian `VerifyConsistency` formulation (RFC 6962 §2.1.2). Add to the `tests` module:

```rust
    fn chain_border_right(mut seed: [u8; 32], proof: &[[u8; 32]]) -> [u8; 32] {
        for h in proof {
            seed = node_hash(h, &seed);
        }
        seed
    }
    fn chain_inner(mut seed: [u8; 32], proof: &[[u8; 32]], index: usize) -> [u8; 32] {
        for (i, h) in proof.iter().enumerate() {
            if (index >> i) & 1 == 0 {
                seed = node_hash(&seed, h);
            } else {
                seed = node_hash(h, &seed);
            }
        }
        seed
    }
    fn chain_inner_right(mut seed: [u8; 32], proof: &[[u8; 32]], index: usize) -> [u8; 32] {
        for (i, h) in proof.iter().enumerate() {
            if (index >> i) & 1 == 1 {
                seed = node_hash(h, &seed);
            }
        }
        seed
    }

    /// Independent RFC 6962 consistency verifier. True iff `proof` proves the
    /// size-`first` tree (root `root1`) is a prefix of the size-`second` tree
    /// (root `root2`).
    fn verify_consistency(
        first: usize,
        second: usize,
        proof: &[[u8; 32]],
        root1: [u8; 32],
        root2: [u8; 32],
    ) -> bool {
        if first > second {
            return false;
        }
        if first == second {
            return proof.is_empty() && root1 == root2;
        }
        if first == 0 {
            return proof.is_empty();
        }
        // decompInclProof(first-1, second)
        let x = ((first - 1) ^ (second - 1)) as u64;
        let mut inner = (64 - x.leading_zeros()) as usize;
        let border = (((first - 1) >> inner) as u64).count_ones() as usize;
        let shift = (first as u64).trailing_zeros() as usize;
        inner -= shift;
        let (seed, start) = if first == (1usize << shift) {
            (root1, 0)
        } else {
            (proof[0], 1)
        };
        if proof.len() != start + inner + border {
            return false;
        }
        let proof = &proof[start..];
        let mask = (first - 1) >> shift;
        let hash1 = chain_border_right(chain_inner_right(seed, &proof[..inner], mask), &proof[inner..]);
        let hash2 = chain_border_right(chain_inner(seed, &proof[..inner], mask), &proof[inner..]);
        hash1 == root1 && hash2 == root2
    }

    #[test]
    fn consistency_proofs_verify_for_all_first_le_second() {
        let leaves: Vec<[u8; 32]> = (0..70).map(leaf_n).collect();
        for second in 1..=leaves.len() {
            let root2 = merkle_root(&leaves[..second]);
            for first in 1..=second {
                let root1 = merkle_root(&leaves[..first]);
                let proof = consistency_proof(&leaves[..second], first);
                assert!(
                    verify_consistency(first, second, &proof, root1, root2),
                    "consistency failed: {first} -> {second}"
                );
                if !proof.is_empty() {
                    let mut bad = proof.clone();
                    bad[0][0] ^= 0xff;
                    assert!(
                        !verify_consistency(first, second, &bad, root1, root2),
                        "tampered consistency verified: {first} -> {second}"
                    );
                }
            }
        }
    }
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p bluedb-evidence --lib merkle 2>&1`
Expected: PASS for all `first ≤ second ≤ 70`. RFC 6962 §2.1.2 is authoritative if anything diverges.

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-evidence/src/merkle.rs
git commit -m "feat(evidence): RFC 6962 consistency proofs + independent verifier

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: Wire `leaf_hash` + frontier into the verified append path

**Files:**
- Modify: `crates/bluedb-evidence/src/chain.rs`
- Test: `crates/bluedb-evidence/tests/merkle_chain.rs` (create)

- [ ] **Step 1: Write the failing integration test**

Create `crates/bluedb-evidence/tests/merkle_chain.rs`:

```rust
use bluedb_evidence::{EntryInput, Evidence};

mod harness;

fn entry(t: &str) -> EntryInput {
    EntryInput { etype: t.into(), payload: t.as_bytes().to_vec(), at: String::new(), edges: vec![] }
}

#[tokio::test]
async fn verified_append_stores_leaf_hash_and_frontier_matches_digest() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    // default auto-create is verified
    for i in 0..5 {
        ev.append("v", vec![entry(&format!("e{i}"))], None).await.unwrap();
    }
    // Every entry carries a leaf hash on a verified chain.
    let rows = ev.read_range("v", 1, 5).await.unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows.iter().all(|(_, r)| r.leaf_hash.is_some()));

    // Digest size == head, root is non-empty.
    let d = ev.digest("v").await.unwrap();
    assert_eq!(d.size, 5);
}

#[tokio::test]
async fn plain_chain_has_no_leaf_hash_or_frontier() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    ev.create_chain("p", false).await.unwrap();
    ev.append("p", vec![entry("a")], None).await.unwrap();
    let rows = ev.read_range("p", 1, 1).await.unwrap();
    assert!(rows[0].1.leaf_hash.is_none());
    // digest on a plain chain is an error (see Task 7).
    assert!(ev.digest("p").await.is_err());
}
```

This will not compile yet (`digest` lands in Task 7). Implement Step 2 first, then add a temporary stub for `digest` if needed, or implement Task 7's `digest` here — recommended order: do Step 2, then jump to Task 7's `digest` method, then run both tests. (The two tasks are split for review clarity but compile together.)

- [ ] **Step 2: Compute and persist `leaf_hash` + frontier in `append`**

In `chain.rs`, add the import:

```rust
use crate::model::{ChainMeta, EdgeDelta, EntryRecord, Frontier, IdemRecord};
```

Then, in `append`, after `let existing_meta = …;` compute the mode, and replace the entry-writing loop + counter advance. Concretely:

After the idempotency block and `let base = …; let k = …; let seqs = …;`, add:

```rust
        let verified = existing_meta.map(|m| m.verified).unwrap_or(true);
```

Replace the entry loop with one that computes leaf hashes on verified chains:

```rust
        let mut leaves: Vec<[u8; 32]> = Vec::new();
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
            batch.put(self.keyspace.entry_key(chain, seq), &store::encode(&rec)?);
        }
```

After `batch.put(self.keyspace.seq_key(chain), (base + k).to_be_bytes());`, fold the leaves into the frontier on verified chains:

```rust
        if verified {
            let mut frontier = store::get_frontier(&self.substrate, &self.keyspace, chain)
                .await?
                .unwrap_or_default();
            for lh in leaves {
                frontier.push(lh);
            }
            batch.put(self.keyspace.merkle_key(chain), &store::encode(&frontier)?);
        }
```

(The frontier read happens inside the held write lease, after the seq read — same atomic batch, read-your-own-writes consistent, mirroring the counter RMW.)

- [ ] **Step 3: Run the test (after Task 7's `digest` exists)**

Run: `cargo test -p bluedb-evidence --test merkle_chain 2>&1`
Expected: PASS once `digest` (Task 7) is implemented.

- [ ] **Step 4: Commit** (after Task 7 compiles; or commit Step 2 now and the test in Task 7's commit)

```bash
git add crates/bluedb-evidence/src/chain.rs
git commit -m "feat(evidence): compute leaf_hash + advance Merkle frontier in verified append

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 7: `digest` / `inclusion` / `consistency` read methods + `NotVerified`/`InvalidArgument` errors

**Files:**
- Modify: `crates/bluedb-evidence/src/error.rs`
- Modify: `crates/bluedb-evidence/src/chain.rs`
- Modify: `crates/bluedb-evidence/src/lib.rs`
- Test: `crates/bluedb-evidence/tests/merkle_chain.rs`

- [ ] **Step 1: Add error variants**

In `error.rs`, add inside `enum EvidenceError`:

```rust
    #[error("chain '{0}' is not verified; Merkle proofs are unavailable")]
    NotVerified(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
```

- [ ] **Step 2: Add the public proof result types + read methods in `chain.rs`**

Add the result types near `Appended`:

```rust
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
```

Add methods to `impl Evidence` (reads — no write lease needed; they use the substrate snapshot like `read_range`):

```rust
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
```

- [ ] **Step 3: Export the result types in `lib.rs`**

```rust
pub use chain::{Appended, ConsistencyProof, Digest, EntryInput, Evidence, InclusionProof};
```

- [ ] **Step 4: Add proof tests to `merkle_chain.rs`**

```rust
#[tokio::test]
async fn digest_matches_recompute_and_proofs_are_well_formed() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    for i in 0..9 {
        ev.append("v", vec![entry(&format!("e{i}"))], None).await.unwrap();
    }
    let d = ev.digest("v").await.unwrap();
    assert_eq!(d.size, 9);

    // Inclusion proof for a middle seq against full size.
    let inc = ev.inclusion("v", 4, None).await.unwrap();
    assert_eq!(inc.seq, 4);
    assert_eq!(inc.size, 9);
    assert!(!inc.audit_path.is_empty());

    // Consistency proof between an earlier size and head.
    let con = ev.consistency("v", 5, None).await.unwrap();
    assert_eq!((con.first, con.second), (5, 9));

    // Out-of-range args are rejected.
    assert!(ev.inclusion("v", 0, None).await.is_err());
    assert!(ev.inclusion("v", 10, None).await.is_err());
    assert!(ev.consistency("v", 0, None).await.is_err());
    assert!(ev.consistency("v", 6, Some(5)).await.is_err());
}
```

- [ ] **Step 5: Run the merkle_chain + lib tests**

Run: `cargo test -p bluedb-evidence 2>&1`
Expected: PASS (the Task 6 tests now compile and pass too).

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-evidence/src/error.rs crates/bluedb-evidence/src/chain.rs crates/bluedb-evidence/src/lib.rs crates/bluedb-evidence/tests/merkle_chain.rs
git commit -m "feat(evidence): digest + inclusion + consistency reads (NotVerified guard)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 8: Redaction (any chain) + hard-delete (plain only) + `VerifiedNoDelete`

**Files:**
- Modify: `crates/bluedb-evidence/src/error.rs`
- Modify: `crates/bluedb-evidence/src/store.rs`
- Modify: `crates/bluedb-evidence/src/chain.rs`
- Test: `crates/bluedb-evidence/tests/erasure.rs` (create)

- [ ] **Step 1: Add the `VerifiedNoDelete` error variant**

In `error.rs`:

```rust
    #[error("cannot hard-delete from a verified chain '{0}'")]
    VerifiedNoDelete(String),
```

- [ ] **Step 2: Make `get_entry` live**

In `store.rs`, remove the `#[allow(dead_code)]` attribute above `get_entry` (it is used as of this task).

- [ ] **Step 3: Write the failing erasure test**

Create `crates/bluedb-evidence/tests/erasure.rs`:

```rust
use bluedb_evidence::{EntryInput, Evidence};

mod harness;

fn entry(t: &str) -> EntryInput {
    EntryInput { etype: t.into(), payload: t.as_bytes().to_vec(), at: "T".into(), edges: vec![] }
}

#[tokio::test]
async fn redaction_keeps_seq_meta_and_digest_on_verified_chain() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    for i in 0..5 {
        ev.append("v", vec![entry(&format!("e{i}"))], None).await.unwrap();
    }
    let before = ev.digest("v").await.unwrap();

    ev.redact("v", 3).await.unwrap();
    ev.redact("v", 3).await.unwrap(); // idempotent

    let rows = ev.read_range("v", 1, 5).await.unwrap();
    assert_eq!(rows.len(), 5, "redaction keeps the slot");
    let (_, r3) = &rows[2];
    assert_eq!(r3.payload, Vec::<u8>::new(), "payload blanked");
    assert!(r3.redacted);
    assert_eq!(r3.etype, "e2"); // type/at retained
    assert_eq!(r3.at, "T");
    assert!(r3.leaf_hash.is_some(), "leaf_hash retained");

    assert_eq!(ev.head("v").await.unwrap(), 5, "head unchanged");
    let after = ev.digest("v").await.unwrap();
    assert_eq!(before, after, "digest unchanged by redaction");

    // Proofs still well-formed after redaction.
    assert!(ev.inclusion("v", 3, None).await.is_ok());
}

#[tokio::test]
async fn hard_delete_plain_leaves_gap_but_keeps_head() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    ev.create_chain("p", false).await.unwrap();
    for i in 0..3 {
        ev.append("p", vec![entry(&format!("e{i}"))], None).await.unwrap();
    }
    ev.hard_delete("p", 2).await.unwrap();
    let rows = ev.read_range("p", 1, 3).await.unwrap();
    let seqs: Vec<i64> = rows.iter().map(|(s, _)| *s).collect();
    assert_eq!(seqs, vec![1, 3], "seq 2 is a gap");
    assert_eq!(ev.head("p").await.unwrap(), 3, "head unchanged");
}

#[tokio::test]
async fn hard_delete_rejected_on_verified_chain() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    ev.append("v", vec![entry("a")], None).await.unwrap(); // verified by default
    let err = ev.hard_delete("v", 1).await.unwrap_err();
    assert!(matches!(err, bluedb_evidence::EvidenceError::VerifiedNoDelete(_)));
    // Entry remains.
    assert_eq!(ev.read_range("v", 1, 1).await.unwrap().len(), 1);
}

#[tokio::test]
async fn redact_and_delete_missing_entry_is_not_found() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    ev.create_chain("p", false).await.unwrap();
    assert!(matches!(
        ev.redact("p", 99).await.unwrap_err(),
        bluedb_evidence::EvidenceError::EntryNotFound { .. }
    ));
    assert!(matches!(
        ev.hard_delete("p", 99).await.unwrap_err(),
        bluedb_evidence::EvidenceError::EntryNotFound { .. }
    ));
}
```

Run: `cargo test -p bluedb-evidence --test erasure 2>&1` → FAIL (methods missing).

- [ ] **Step 4: Implement `redact` + `hard_delete` in `chain.rs`**

Add to `impl Evidence` (write paths — mirror the `append` lease/flush shape):

```rust
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
    /// Note: retracting the entry's `edges[]` from the graph store is deferred
    /// to Plan 3 (the graph store does not exist yet).
    pub async fn hard_delete(&self, chain: &str, seq: i64) -> Result<(), EvidenceError> {
        let _lease = self.write_lease.lock().await;
        let writer = self.substrate.require_writer().map_err(|_| EvidenceError::NotWriter)?;
        let verified = store::get_chain_meta(&self.substrate, &self.keyspace, chain)
            .await?
            .map(|m| m.verified)
            .unwrap_or(true);
        if verified {
            return Err(EvidenceError::VerifiedNoDelete(chain.to_string()));
        }
        if store::get_entry(&self.substrate, &self.keyspace, chain, seq).await?.is_none() {
            return Err(EvidenceError::EntryNotFound { chain: chain.to_string(), seq });
        }
        let mut batch = WriteBatch::new();
        batch.delete(self.keyspace.entry_key(chain, seq));
        writer
            .write_with_options(batch, &WriteOptions { await_durable: false, ..Default::default() })
            .await
            .map_err(Self::storage_err)?;
        drop(_lease);
        writer.flush().await.map_err(Self::storage_err)?;
        Ok(())
    }
```

- [ ] **Step 5: Run the test**

Run: `cargo test -p bluedb-evidence --test erasure 2>&1`
Expected: PASS (4 tests). The key assertions: redaction leaves the digest byte-identical; hard-delete leaves a gap with `head` unchanged; verified chains reject hard-delete.

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-evidence/src/error.rs crates/bluedb-evidence/src/store.rs crates/bluedb-evidence/src/chain.rs crates/bluedb-evidence/tests/erasure.rs
git commit -m "feat(evidence): redaction-in-place (any) + hard-delete (plain only)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 9: HTTP surface — redact, hard-delete, digest, proof, consistency

**Files:**
- Modify: `crates/bluedb-server/src/evidence_api.rs`
- Modify: `crates/bluedb-server/src/lib.rs`
- Test: `crates/bluedb-server/tests/evidence.rs`

- [ ] **Step 1: Extend `map_evidence_err` + add a hex helper in `evidence_api.rs`**

Add the new arms to `map_evidence_err`:

```rust
        EvidenceError::NotVerified(c) => {
            AppError::bad_request(format!("E_NOT_VERIFIED: chain '{c}' is not a verified chain"))
        }
        EvidenceError::VerifiedNoDelete(c) => {
            AppError::conflict(format!("E_VERIFIED_NO_DELETE: chain '{c}' is verified; cannot hard-delete"))
        }
        EvidenceError::InvalidArgument(m) => AppError::bad_request(m),
```

Add a hex helper (hashes render as lowercase hex in JSON — the convention for Merkle roots/proofs; no new dependency):

```rust
fn hex32(b: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(64);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}
```

- [ ] **Step 2: Add the handlers in `evidence_api.rs`**

```rust
// --- POST /evidence/{chain}/entries/{seq}/redact ----------------------------

/// Redact (blank the payload of) one entry. Requires `schema:admin`.
pub async fn redact(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((chain, seq)): Path<(String, i64)>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    state.evidence(&tenant).await?.redact(&chain, seq).await.map_err(map_evidence_err)?;
    Ok(Json(json!({ "chain": chain, "seq": seq, "redacted": true })))
}

// --- DELETE /evidence/{chain}/entries/{seq} ---------------------------------

/// Hard-delete one entry (plain chains only). Requires `schema:admin`.
pub async fn hard_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((chain, seq)): Path<(String, i64)>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    state.evidence(&tenant).await?.hard_delete(&chain, seq).await.map_err(map_evidence_err)?;
    Ok(Json(json!({ "chain": chain, "seq": seq, "deleted": true })))
}

// --- GET /evidence/{chain}/digest -------------------------------------------

/// Merkle digest `{ size, root_hash }` for a verified chain.
pub async fn digest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let d = state.evidence(&tenant).await?.digest(&chain).await.map_err(map_evidence_err)?;
    Ok(Json(json!({ "size": d.size, "root_hash": hex32(&d.root) })))
}

// --- GET /evidence/{chain}/proof?seq&size -----------------------------------

#[derive(Deserialize)]
pub(crate) struct ProofQuery {
    seq: i64,
    size: Option<i64>,
}

/// Inclusion proof for `seq` against tree `size` (defaults to head).
pub async fn inclusion(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
    Query(q): Query<ProofQuery>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let p = state
        .evidence(&tenant)
        .await?
        .inclusion(&chain, q.seq, q.size)
        .await
        .map_err(map_evidence_err)?;
    let path: Vec<String> = p.audit_path.iter().map(hex32).collect();
    Ok(Json(json!({ "seq": p.seq, "size": p.size, "audit_path": path })))
}

// --- GET /evidence/{chain}/consistency?from&to ------------------------------

#[derive(Deserialize)]
pub(crate) struct ConsistencyQuery {
    from: i64,
    to: Option<i64>,
}

/// Consistency proof between sizes `from` and `to` (defaults to head).
pub async fn consistency(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
    Query(q): Query<ConsistencyQuery>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let c = state
        .evidence(&tenant)
        .await?
        .consistency(&chain, q.from, q.to)
        .await
        .map_err(map_evidence_err)?;
    let proof: Vec<String> = c.proof.iter().map(hex32).collect();
    Ok(Json(json!({ "first": c.first, "second": c.second, "proof": proof })))
}
```

- [ ] **Step 3: Register the routes in `lib.rs`**

Ensure `delete` is imported from `axum::routing` (check the existing `use axum::routing::{...}` line; add `delete` if missing). Add to `build_app`, right after the existing `/evidence/{chain}/head` route:

```rust
        .route("/evidence/{chain}/entries/{seq}/redact", post(evidence_api::redact))
        .route("/evidence/{chain}/entries/{seq}", delete(evidence_api::hard_delete))
        .route("/evidence/{chain}/digest", get(evidence_api::digest))
        .route("/evidence/{chain}/proof", get(evidence_api::inclusion))
        .route("/evidence/{chain}/consistency", get(evidence_api::consistency))
```

- [ ] **Step 4: Add e2e assertions to `tests/evidence.rs`**

Add a test (reusing the file's `spawn_app`/`call`/tenant-header harness — match the existing tests' exact helper signatures):

```rust
#[tokio::test]
async fn evidence_merkle_and_erasure_e2e() {
    // ... spawn app + promote writer exactly like the existing tests ...

    // Verified chain: append 5, digest size == 5, root is 64 hex chars.
    for i in 0..5 {
        let body = json!({ "events": [{ "type": "e", "payload_b64": base64_std(format!("p{i}").as_bytes()) }] });
        // POST /evidence/v/entries → 200
    }
    // GET /evidence/v/digest → { size: 5, root_hash: <64 hex> }
    // GET /evidence/v/proof?seq=3 → audit_path non-empty
    // GET /evidence/v/consistency?from=2 → proof present
    let digest_before = /* root_hash from GET digest */;

    // Redact seq 3 (schema:admin) → 200; entries shows seq 3 redacted, no payload_b64; head still 5.
    // POST /evidence/v/entries/3/redact → 200
    // GET /evidence/v/entries?from=1&to=5 → 5 rows; row seq=3 has redacted:true and no payload_b64
    // GET /evidence/v/head → 5
    // GET /evidence/v/digest → root_hash == digest_before  (redaction keeps the digest)

    // Plain chain: PUT {verified:false}; digest → 400 E_NOT_VERIFIED.
    // PUT /evidence/p {"verified": false} → 200
    // GET /evidence/p/digest → 400

    // Hard-delete on verified chain → 409 E_VERIFIED_NO_DELETE.
    // DELETE /evidence/v/entries/1 → 409

    // Hard-delete on plain chain → 200, leaves a gap, head unchanged.
    // append 2 to /evidence/p ; DELETE /evidence/p/entries/1 → 200
    // GET /evidence/p/entries → seq 1 absent; GET /evidence/p/head unchanged
}
```

Fill in the request mechanics by copying the existing tests' exact `call(...)` usage and JSON assertions. Assert the **status codes** for the error cases (400 for `E_NOT_VERIFIED`, 409 for `E_VERIFIED_NO_DELETE`) and that the redacted row carries `redacted: true` with no `payload_b64` and an unchanged `root_hash`.

- [ ] **Step 5: Run the server tests**

Run: `cargo test -p bluedb-server --test evidence 2>&1`
Expected: PASS (existing 4 + the new e2e).

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-server/src/evidence_api.rs crates/bluedb-server/src/lib.rs crates/bluedb-server/tests/evidence.rs
git commit -m "feat(evidence): HTTP redact/delete/digest/proof/consistency endpoints

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 10: Workspace green + clippy

**Files:** none (verification + commit only)

- [ ] **Step 1: Whole workspace tests**

Run: `cargo test --workspace 2>&1`
Expected: PASS. List any failures and confirm they are unrelated to evidence (check they also fail on the parent commit if in doubt). Do not "fix" unrelated subsystems.

- [ ] **Step 2: Clippy**

Run: `cargo clippy -p bluedb-evidence -p bluedb-server 2>&1`
Fix any NEW warnings introduced by this plan's code. Pre-existing warnings in other crates (`type_complexity` in bluedb-engine, `field_reassign_with_default` in bluedb-server `lib.rs`) are not yours — leave them. **Do not run `cargo fmt`.**

- [ ] **Step 3: Commit any clippy fixes**

```bash
git add -A
git commit -m "chore(evidence): clippy cleanup for Merkle + erasure

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-review notes (addressed)

- **Spec coverage (Plan 2 slice):** §7.1 leaf/node/empty hashing (Task 2); §7.2 incremental frontier persisted in the append batch (Tasks 1, 3, 6); §7.3 digest (frontier fold) + inclusion + consistency, on-demand from stored leaf hashes, client-side verification (Tasks 4, 5, 7); §6.3 verified/plain mode already exists, proofs gated by `NotVerified` (Task 7); §6.6 redaction-in-place keeping seq/leaf_hash + digest (Task 8) and plain-only hard-delete with `E_VERIFIED_NO_DELETE` (Task 8); §10 endpoints + scopes (`schema:admin` for redact/delete, `data:read` for proofs) + named errors `E_NOT_VERIFIED`/`E_VERIFIED_NO_DELETE` (Task 9). The **redaction-keeps-proofs** acceptance (§12) is asserted in Tasks 8 (digest unchanged) and 9 (e2e).
- **Deliberately deferred (not gaps):** digest **signing** (§7.4 — documented extension, not v1); **persisted internal-node store** (§7.3 — proofs are O(N) on demand); **edge retraction on hard-delete** (§6.6 — needs the graph store, which is Plan 3 — `hard_delete` removes only the entry slot for now, noted in its doc comment); the graph store, traversal, and scratch (Plans 3–5).
- **RFC 6962 correctness gate:** generation (`merkle_root`, `inclusion_proof`, `consistency_proof`) and the independent test-module verifiers are written from different formulations (recursive PATH/SUBPROOF generation vs. Trillian bit-decomposition verification). The property tests (every index×size for inclusion; every `first ≤ second` for consistency; plus single-bit tamper rejection) are the gate. **RFC 6962 §2.1.1/§2.1.2 is the source of truth** — if a test fails, fix the code to match the RFC; never weaken the assertion.
- **Type consistency:** `Frontier`/`Digest`/`InclusionProof`/`ConsistencyProof` names and fields match across `model.rs`, `chain.rs`, `lib.rs` exports, and `evidence_api.rs`. `EvidenceError::{NotVerified, VerifiedNoDelete, InvalidArgument}` added once, mapped once in `map_evidence_err`. `leaf_hash(etype, payload, at, edges)` signature is used identically in `merkle.rs` and `chain.rs`.
- **Atomicity / determinism:** leaf hash + frontier advance ride the **same `WriteBatch`** as the entries and counter inside the held write lease (Task 6) → crash-consistent; redaction/hard-delete reuse the `append` lease→write→drop→flush group-commit shape (Task 8). Reads use the substrate snapshot (no lease), repeatable on writer and replicas.

## Next plans (queued)

- **Plan 3 — Graph store + append-with-edges:** `graph.rs` (tags `0x1C–0x1E`, canonical+out+in upsert/delete), apply `EntryInput.edges` inside the append `WriteBatch`, edges API, and wire edge-retraction into `hard_delete`.
- **Plan 4 — Traversal:** `reachable`, `widest_path` over the adjacency.
- **Plan 5 — Scratch:** as-of name-prefix create/drop.
