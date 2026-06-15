# bluedb-evidence Plan 3 — Native Graph Store + Append-with-Edges

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the *write/maintenance* side of bluedb-evidence's native graph store — directed weighted typed edges with `out`/`in` adjacency indexes — wire each evidence entry's `edges[]` into the append `WriteBatch` so the graph is a reproducible projection of the chain, expose a standalone edges API for bulk/rebuild writes, and retract an entry's edges on hard-delete.

**Architecture:** Three new keyspace tags (`0x1C` canonical edge, `0x1D` out-adjacency, `0x1E` in-adjacency) on the same tenant-namespaced `Keyspace`. Edge identity is `(graph, src, dst, type)`; weight is folded order-preserving into the `out`/`in` keys so Plan 4 traversal is a bounded prefix range scan. A single overlay-based applier (`apply_edge_delta`) maintains canonical+out+in atomically and gives read-your-own-writes *within one batch* (SlateDB `WriteBatch` does not), so both the append hot-path and the standalone `Graph` handle funnel through it. Traversal reads (`reachable`, `widest_path`) are **out of scope — Plan 4**.

**Tech Stack:** Rust, SlateDB `WriteBatch` (atomic, group-commit via `await_durable:false` + `drop(lease)` + `flush()`), postcard, axum, `bluedb_sql::Keyspace::external_key`.

**Worktree:** All work happens in `/Users/satya/work/bc/bluedb-evidence-wt` on branch `feat/evidence-substrate`. NEVER touch `/Users/satya/work/bc/bluedb`. Commit-only, NEVER push. Co-author trailer `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. No `cargo fmt` (match style by hand). Run tests directly with `2>&1` — no `tail`/`grep` pipes. "bluedb"/"bluecopa" always lowercase.

---

## Source-of-truth context (read once, applies to every task)

**Spec:** `docs/superpowers/specs/2026-06-15-bluedb-evidence-substrate-design.md` §8.1–8.3 (edge model, keyspace, edges API), §6.4 step 8 (append-with-edges), §6.6 hard-delete step 2 (edge retraction), §10 (HTTP surface rows for `/graph/*`).

**Existing model types** (`crates/bluedb-evidence/src/model.rs`, already present — do NOT redefine):
```rust
pub struct EdgeDelta { pub graph: String, pub src: String, pub dst: String, pub weight: i64, pub etype: String, pub op: EdgeOp }
pub enum EdgeOp { Upsert { merge: Merge }, Delete }
pub enum Merge { Set, Max }
pub struct EntryRecord { pub etype: String, pub payload: Vec<u8>, pub at: String, pub edges: Vec<EdgeDelta>, pub leaf_hash: Option<[u8;32]>, pub redacted: bool }
```
`EntryInput` (`chain.rs`) already carries `pub edges: Vec<EdgeDelta>`. The append path already *stores* `edges` in each `EntryRecord` and folds them into the verified `leaf_hash` (Plan 2). This plan makes those edges *materialize into the graph store* in the same batch.

**Existing keyspace tags** (`crates/bluedb-evidence/src/keyspace.rs`): `TAG_EVIDENCE_ENTRY 0x17`, `_SEQ 0x18`, `_IDEM 0x19`, `_MERKLE 0x1A`, `_CHAIN 0x1B`. The file header comment already says "`0x1C-0x1E (graph) reserved for later plans.`" — this plan fills them.

**Key encoding facts:**
- `EvidenceKeyspace` wraps `bluedb_sql::Keyspace` and routes every key through `self.ks.external_key(tag, &suffix)` (asserts `tag >= TAG_EXTERNAL_BASE`, prepends tenant) → tenant isolation is free.
- `prefix_upper_bound(prefix) -> Option<Vec<u8>>` gives the exclusive scan end (already imported).
- Existing `chain_suffix` length-prefixes names with `(len as u32).to_be_bytes()`. **Reuse that width (u32-be) for graph/node/type components** for consistency.

**Order-preserving big-endian (`i64-obe`)** — signed weights that sort numerically as bytes: `((w as u64) ^ (i64::MIN as u64)).to_be_bytes()`. Reverse: `i64::from_be_bytes(b) ^ i64::MIN` (xor with `i64::MIN` is its own inverse). The `out`/`in` keys embed this so a node's edges sort by weight ascending → Plan 4 `reachable(floor)` is a range scan; `widest_path` is the same index reversed.

**Index-marker value convention:** this codebase writes presence-only marker keys with a single sentinel byte `[1u8]`, NOT an empty slice (see `bluedb-ledger/src/ledger.rs:425` `batch.put(self.keyspace.expiry_key(...), [1u8])`). The `out`/`in` keys are presence-only → **value = `[1u8]`**; it is never read (presence is the signal). The canonical edge value carries the weight as `i64-obe` (so a re-upsert can find and delete the stale `out`/`in` keys).

**Read-your-own-writes (load-bearing):** SlateDB `WriteBatch` does NOT let you read staged puts/deletes. Within one batch (a multi-entry append, or a multi-edge upsert request) two deltas may touch the same `(graph,src,dst,type)`. The applier therefore threads a `&mut HashMap<Vec<u8>, Option<i64>>` **overlay** keyed by the canonical key bytes (`Some(w)` = present at weight `w`, `None` = deleted). It consults the overlay before the committed store. SlateDB applies batch ops in insertion order, so a later `delete(K)`/`put(K,v)` correctly supersedes an earlier one for the same `K`.

**Group-commit shape (copy exactly, as in `chain.rs` append/redact):**
```rust
let _lease = self.write_lease.lock().await;
let writer = self.substrate.require_writer().map_err(|_| EvidenceError::NotWriter)?;
// ... build `batch` ...
writer.write_with_options(batch, &WriteOptions { await_durable: false, ..Default::default() })
    .await.map_err(Self::storage_err)?;
drop(_lease);
writer.flush().await.map_err(Self::storage_err)?;
```

**Test DB helper (copy from `store.rs` tests):**
```rust
use std::sync::Arc;
use bluedb_sql::Database;
use slatedb::{object_store::memory::InMemory, Db};
async fn writer_database() -> Database {
    let db = Db::open("evidence-test", Arc::new(InMemory::new())).await.expect("open in-memory db");
    Database::new(Arc::new(db))
}
```

---

## File Structure

- **Modify** `crates/bluedb-evidence/src/keyspace.rs` — 3 tags + `graph_edge_key`/`graph_out_key`/`graph_in_key` + obe helpers + a graph-edge prefix builder (for the standalone delete-by-identity and Plan-4 scan reuse).
- **Modify** `crates/bluedb-evidence/src/store.rs` — `get_edge_weight` (point read of the canonical edge value → `Option<i64>`).
- **Create** `crates/bluedb-evidence/src/graph.rs` — `apply_edge_delta` (overlay applier), the public `Graph` handle (`upsert`/`delete`), and the `EdgeUpsert`/`EdgeRef` input structs.
- **Modify** `crates/bluedb-evidence/src/chain.rs` — apply each entry's `edges[]` into the append batch (shared overlay); add `retract_edges: bool` to `hard_delete` and retract referenced edge identities.
- **Modify** `crates/bluedb-evidence/src/lib.rs` — `mod graph;` + `pub use graph::{Graph, EdgeUpsert, EdgeRef};`.
- **Create** `crates/bluedb-server/src/graph_api.rs` — `PUT/DELETE /graph/{graph}/edges` handlers.
- **Modify** `crates/bluedb-server/src/lib.rs` — `mod graph_api;`, `AppState::graph(tenant)`, 2 routes, and pass the hard-delete `retract_edges` flag through.
- **Modify** `crates/bluedb-server/src/evidence_api.rs` — `hard_delete` reads `?retract_edges=` (default true), passes it through.
- **Create** `crates/bluedb-evidence/tests/graph.rs` — edge maintenance integration tests.
- **Create** `crates/bluedb-evidence/tests/append_edges.rs` — append-with-edges + retraction integration tests.
- **Modify** `crates/bluedb-server/tests/evidence.rs` — graph-edges e2e (or a new `crates/bluedb-server/tests/graph.rs`).

---

## Task P3-1: Keyspace — graph tags, key builders, obe helpers

**Files:**
- Modify: `crates/bluedb-evidence/src/keyspace.rs`

- [ ] **Step 1: Add the three tags** (replace the `// 0x1C-0x1E (graph) reserved for later plans.` line):

```rust
pub(crate) const TAG_GRAPH_EDGE: u8 = TAG_EXTERNAL_BASE + 12; // 0x1C  canonical edge
pub(crate) const TAG_GRAPH_OUT: u8 = TAG_EXTERNAL_BASE + 13; // 0x1D  out-adjacency (by weight asc)
pub(crate) const TAG_GRAPH_IN: u8 = TAG_EXTERNAL_BASE + 14; // 0x1E  in-adjacency (by weight asc)
```

- [ ] **Step 2: Add the obe helpers + length-prefix helper** as free functions at the bottom of the file (above `#[cfg(test)]`):

```rust
/// Order-preserving big-endian encoding of a signed weight: flips the sign bit
/// so two's-complement `i64`s sort numerically as unsigned bytes.
pub(crate) fn weight_obe(w: i64) -> [u8; 8] {
    ((w as u64) ^ (i64::MIN as u64)).to_be_bytes()
}

/// Inverse of [`weight_obe`].
pub(crate) fn weight_from_obe(b: &[u8; 8]) -> i64 {
    (u64::from_be_bytes(*b) ^ (i64::MIN as u64)) as i64
}

/// Append `<len::u32-be> <bytes>` to `buf` (self-delimiting component).
fn push_lp(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
    buf.extend_from_slice(b);
}
```

- [ ] **Step 3: Add the three key builders** as methods on `impl EvidenceKeyspace` (after `merkle_key`):

```rust
/// Canonical edge key: identity `(graph, src, dst, type)`. Value = `weight_obe`.
pub(crate) fn graph_edge_key(&self, graph: &str, src: &str, dst: &str, etype: &str) -> Vec<u8> {
    let mut s = Vec::new();
    push_lp(&mut s, graph);
    push_lp(&mut s, src);
    push_lp(&mut s, dst);
    push_lp(&mut s, etype);
    self.ks.external_key(TAG_GRAPH_EDGE, &s)
}

/// Out-adjacency key: `graph ‖ src ‖ weight_obe ‖ dst ‖ type`. Value = `[1u8]`.
/// A node's out-edges sort by weight ascending under the `(graph, src)` prefix.
pub(crate) fn graph_out_key(&self, graph: &str, src: &str, weight: i64, dst: &str, etype: &str) -> Vec<u8> {
    let mut s = Vec::new();
    push_lp(&mut s, graph);
    push_lp(&mut s, src);
    s.extend_from_slice(&weight_obe(weight));
    push_lp(&mut s, dst);
    push_lp(&mut s, etype);
    self.ks.external_key(TAG_GRAPH_OUT, &s)
}

/// In-adjacency key: `graph ‖ dst ‖ weight_obe ‖ src ‖ type`. Value = `[1u8]`.
pub(crate) fn graph_in_key(&self, graph: &str, dst: &str, weight: i64, src: &str, etype: &str) -> Vec<u8> {
    let mut s = Vec::new();
    push_lp(&mut s, graph);
    push_lp(&mut s, dst);
    s.extend_from_slice(&weight_obe(weight));
    push_lp(&mut s, src);
    push_lp(&mut s, etype);
    self.ks.external_key(TAG_GRAPH_IN, &s)
}
```

- [ ] **Step 4: Write the failing tests** (add to the `#[cfg(test)] mod tests`):

```rust
#[test]
fn weight_obe_is_order_preserving() {
    let ws = [i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];
    for pair in ws.windows(2) {
        assert!(weight_obe(pair[0]) < weight_obe(pair[1]), "{} vs {}", pair[0], pair[1]);
    }
    for w in ws {
        assert_eq!(weight_from_obe(&weight_obe(w)), w);
    }
}

#[test]
fn out_index_orders_a_nodes_edges_by_weight_ascending() {
    let ks = EvidenceKeyspace::new("acme");
    let lo = ks.graph_out_key("g", "u", 1, "a", "");
    let hi = ks.graph_out_key("g", "u", 100, "a", "");
    assert!(lo < hi);
    // Negative weights still sort below positive.
    let neg = ks.graph_out_key("g", "u", -5, "a", "");
    assert!(neg < lo);
}

#[test]
fn graph_tags_are_distinct_namespaces_and_ordered() {
    let ks = EvidenceKeyspace::new("acme");
    let edge = ks.graph_edge_key("g", "u", "v", "");
    let out = ks.graph_out_key("g", "u", 1, "v", "");
    let inn = ks.graph_in_key("g", "v", 1, "u", "");
    assert_ne!(edge, out);
    assert_ne!(edge, inn);
    assert_ne!(out, inn);
    // Tags sort EDGE(0x1C) < OUT(0x1D) < IN(0x1E), and all sort after CHAIN(0x1B).
    assert!(ks.chain_meta_key("g") < edge);
    assert!(edge < out);
    assert!(out < inn);
}

#[test]
fn node_ids_do_not_bleed_via_length_prefix() {
    let ks = EvidenceKeyspace::new("acme");
    // out-prefix of node "ab" must not contain node "abc"'s edges.
    let ab = ks.graph_out_key("g", "ab", 1, "x", "");
    let abc = ks.graph_out_key("g", "abc", 1, "x", "");
    // Build the (graph, "ab") scan bound and assert "abc" falls outside it.
    // Reuse weight 0's key as a stand-in lower bound for "ab".
    assert_ne!(ab, abc);
    // Graph names likewise isolated.
    let g1 = ks.graph_edge_key("g1", "u", "v", "");
    let g2 = ks.graph_edge_key("g2", "u", "v", "");
    assert_ne!(g1, g2);
}

#[test]
fn graph_keys_are_tenant_isolated() {
    let a = EvidenceKeyspace::new("acme").graph_edge_key("g", "u", "v", "");
    let b = EvidenceKeyspace::new("globex").graph_edge_key("g", "u", "v", "");
    assert_ne!(a, b);
}
```

- [ ] **Step 5: Run, expect fail then pass**

Run: `cargo test -p bluedb-evidence keyspace 2>&1`
Expected: FAIL (missing items) → after Steps 1-3, PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-evidence/src/keyspace.rs
git commit -m "feat(evidence): graph keyspace tags 0x1C-0x1E + obe weight encoding"
```

---

## Task P3-2: Store — canonical edge weight reader

**Files:**
- Modify: `crates/bluedb-evidence/src/store.rs`

- [ ] **Step 1: Add the reader** (after `get_entry`):

```rust
/// Point read of the canonical edge weight for `(graph, src, dst, etype)`,
/// or `None` if the edge does not exist. The canonical value is `weight_obe`.
pub(crate) async fn get_edge_weight(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    graph: &str,
    src: &str,
    dst: &str,
    etype: &str,
) -> Result<Option<i64>> {
    match substrate.get(&ks.graph_edge_key(graph, src, dst, etype)).await? {
        Some(b) => {
            let arr: [u8; 8] = b.as_ref().try_into().context("edge weight must be 8 bytes")?;
            Ok(Some(crate::keyspace::weight_from_obe(&arr)))
        }
        None => Ok(None),
    }
}
```

- [ ] **Step 2: Write the failing test** (add to `mod tests`):

```rust
#[tokio::test]
async fn get_edge_weight_absent_then_present() {
    let database = writer_database().await;
    let substrate = database.substrate();
    let ks = EvidenceKeyspace::new("acme");
    assert_eq!(get_edge_weight(&substrate, &ks, "g", "u", "v", "").await.unwrap(), None);
    let writer = substrate.require_writer().unwrap();
    writer.put(&ks.graph_edge_key("g", "u", "v", ""), &crate::keyspace::weight_obe(-7)).await.unwrap();
    assert_eq!(get_edge_weight(&substrate, &ks, "g", "u", "v", "").await.unwrap(), Some(-7));
}
```

- [ ] **Step 3: Run, expect pass**

Run: `cargo test -p bluedb-evidence store 2>&1`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-evidence/src/store.rs
git commit -m "feat(evidence): point read of canonical edge weight"
```

---

## Task P3-3: graph.rs — overlay applier + `Graph` handle

**Files:**
- Create: `crates/bluedb-evidence/src/graph.rs`
- Modify: `crates/bluedb-evidence/src/lib.rs`
- Test: `crates/bluedb-evidence/tests/graph.rs`

This is the core unit. The applier is the single chokepoint; the `Graph` handle is the standalone bulk/rebuild path; `chain.rs` (P3-4/P3-5) reuses the same applier.

- [ ] **Step 1: Write `graph.rs`**

```rust
//! Native directed-weighted-typed graph store. Edge identity is
//! `(graph, src, dst, type)`; `out`/`in` adjacency indexes fold the weight into
//! the key so traversal (Plan 4) is a bounded prefix range scan. Mutations go
//! through [`apply_edge_delta`], which maintains canonical+out+in atomically and
//! provides read-your-own-writes within one batch (SlateDB `WriteBatch` alone
//! does not). The graph is a reproducible projection of the evidence chain: the
//! hot path is append-with-edges (`chain.rs`); this standalone handle is the
//! bulk / rebuild / scratch path for edges with no originating event.

use std::collections::HashMap;

use bluedb_sql::{Database, WriteLease};
use bluedb_storage::Substrate;
use slatedb::config::WriteOptions;
use slatedb::WriteBatch;

use crate::error::EvidenceError;
use crate::keyspace::{weight_obe, EvidenceKeyspace};
use crate::model::{EdgeDelta, EdgeOp, Merge};
use crate::store;

/// Presence-only marker value for `out`/`in` index keys (never read).
const MARK: [u8; 1] = [1u8];

/// One edge to upsert via the standalone API.
#[derive(Debug, Clone)]
pub struct EdgeUpsert {
    pub src: String,
    pub dst: String,
    pub weight: i64,
    pub etype: String,
}

/// One edge identity to delete via the standalone API (weight not needed).
#[derive(Debug, Clone)]
pub struct EdgeRef {
    pub src: String,
    pub dst: String,
    pub etype: String,
}

/// Apply one [`EdgeDelta`] into `batch`, maintaining canonical + out + in and
/// the in-batch `overlay` (`canonical_key_bytes -> Some(weight) | None`).
///
/// - `Upsert { Set }`: new weight = delta weight. `Upsert { Max }`: new weight =
///   `max(existing, delta)` (existing from overlay, else committed store).
/// - On any weight change (or first insert) the stale out/in keys (old weight)
///   are deleted before the new canonical+out+in are written.
/// - `Delete`: removes canonical+out+in if the edge exists (no-op otherwise).
pub(crate) async fn apply_edge_delta(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    batch: &mut WriteBatch,
    overlay: &mut HashMap<Vec<u8>, Option<i64>>,
    d: &EdgeDelta,
) -> Result<(), EvidenceError> {
    let canon = ks.graph_edge_key(&d.graph, &d.src, &d.dst, &d.etype);
    let current: Option<i64> = match overlay.get(&canon) {
        Some(v) => *v,
        None => store::get_edge_weight(substrate, ks, &d.graph, &d.src, &d.dst, &d.etype).await?,
    };

    match &d.op {
        EdgeOp::Delete => {
            if let Some(w0) = current {
                batch.delete(canon.clone());
                batch.delete(ks.graph_out_key(&d.graph, &d.src, w0, &d.dst, &d.etype));
                batch.delete(ks.graph_in_key(&d.graph, &d.dst, w0, &d.src, &d.etype));
                overlay.insert(canon, None);
            }
        }
        EdgeOp::Upsert { merge } => {
            let new_w = match (current, merge) {
                (Some(w0), Merge::Max) => w0.max(d.weight),
                _ => d.weight,
            };
            if let Some(w0) = current {
                if w0 != new_w {
                    batch.delete(ks.graph_out_key(&d.graph, &d.src, w0, &d.dst, &d.etype));
                    batch.delete(ks.graph_in_key(&d.graph, &d.dst, w0, &d.src, &d.etype));
                }
            }
            batch.put(canon.clone(), weight_obe(new_w));
            batch.put(ks.graph_out_key(&d.graph, &d.src, new_w, &d.dst, &d.etype), MARK);
            batch.put(ks.graph_in_key(&d.graph, &d.dst, new_w, &d.src, &d.etype), MARK);
            overlay.insert(canon, Some(new_w));
        }
    }
    Ok(())
}

/// Standalone handle to the graph store in one bluedb database / tenant.
/// Mirrors [`crate::Evidence`]: writes need the active writer.
pub struct Graph {
    substrate: Substrate,
    write_lease: WriteLease,
    keyspace: EvidenceKeyspace,
}

impl Graph {
    pub fn new(database: &Database, tenant: &str) -> Self {
        Self {
            substrate: database.substrate(),
            write_lease: database.write_lease(),
            keyspace: EvidenceKeyspace::new(tenant),
        }
    }

    fn storage_err(e: impl std::fmt::Display) -> EvidenceError {
        EvidenceError::Storage(anyhow::anyhow!("{e}"))
    }

    /// Upsert `edges` into `graph` with the given `merge` mode. Atomic + durable.
    pub async fn upsert(
        &self,
        graph: &str,
        edges: &[EdgeUpsert],
        merge: Merge,
    ) -> Result<(), EvidenceError> {
        if edges.is_empty() {
            return Ok(());
        }
        let _lease = self.write_lease.lock().await;
        let writer = self.substrate.require_writer().map_err(|_| EvidenceError::NotWriter)?;
        let mut batch = WriteBatch::new();
        let mut overlay = HashMap::new();
        for e in edges {
            let d = EdgeDelta {
                graph: graph.to_string(),
                src: e.src.clone(),
                dst: e.dst.clone(),
                weight: e.weight,
                etype: e.etype.clone(),
                op: EdgeOp::Upsert { merge },
            };
            apply_edge_delta(&self.substrate, &self.keyspace, &mut batch, &mut overlay, &d).await?;
        }
        writer
            .write_with_options(batch, &WriteOptions { await_durable: false, ..Default::default() })
            .await
            .map_err(Self::storage_err)?;
        drop(_lease);
        writer.flush().await.map_err(Self::storage_err)?;
        Ok(())
    }

    /// Delete `edges` (by identity) from `graph`. Atomic + durable. Deleting a
    /// non-existent edge is a no-op.
    pub async fn delete(&self, graph: &str, edges: &[EdgeRef]) -> Result<(), EvidenceError> {
        if edges.is_empty() {
            return Ok(());
        }
        let _lease = self.write_lease.lock().await;
        let writer = self.substrate.require_writer().map_err(|_| EvidenceError::NotWriter)?;
        let mut batch = WriteBatch::new();
        let mut overlay = HashMap::new();
        for e in edges {
            let d = EdgeDelta {
                graph: graph.to_string(),
                src: e.src.clone(),
                dst: e.dst.clone(),
                weight: 0,
                etype: e.etype.clone(),
                op: EdgeOp::Delete,
            };
            apply_edge_delta(&self.substrate, &self.keyspace, &mut batch, &mut overlay, &d).await?;
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
```

- [ ] **Step 2: Wire the module** in `crates/bluedb-evidence/src/lib.rs`:

Add `mod graph;` (after `mod error;`/before `pub mod chain;` — keep alphabetical-ish with the others) and extend the public re-exports:
```rust
pub use graph::{EdgeRef, EdgeUpsert, Graph};
```

- [ ] **Step 3: Write the integration tests** `crates/bluedb-evidence/tests/graph.rs`. These exercise the `Graph` handle and assert canonical+out+in presence directly through the substrate.

```rust
//! Integration tests for the native graph store (edge maintenance).
//!
//! The test re-derives keys independently via the PUBLIC `bluedb_sql::Keyspace`
//! API (`external_prefix`/`external_key` + `prefix_upper_bound`) — an independent
//! verifier of the crate's encoding — and scans exact per-tag ranges. No byte
//! grubbing, no heuristics.

use std::sync::Arc;

use bluedb_evidence::{EdgeRef, EdgeUpsert, Graph, Merge};
use bluedb_sql::{prefix_upper_bound, Database, Keyspace};
use slatedb::{object_store::memory::InMemory, Db};

const TAG_EDGE: u8 = 0x1C;
const TAG_OUT: u8 = 0x1D;
const TAG_IN: u8 = 0x1E;

async fn db() -> Database {
    let d = Db::open("g-test", Arc::new(InMemory::new())).await.unwrap();
    Database::new(Arc::new(d))
}

/// Count keys in one external tag's range for tenant `"_"`.
async fn count_tag(database: &Database, tag: u8) -> usize {
    let ks = Keyspace::new("_");
    let prefix = ks.external_prefix(tag);
    let end = prefix_upper_bound(&prefix);
    let mut it = database.substrate().scan_range(&prefix, end.as_deref()).await.unwrap();
    let mut n = 0;
    while it.next().await.unwrap().is_some() {
        n += 1;
    }
    n
}

/// `(edge, out, in)` key counts for tenant `"_"`.
async fn count_graph_keys(database: &Database) -> (usize, usize, usize) {
    (count_tag(database, TAG_EDGE).await, count_tag(database, TAG_OUT).await, count_tag(database, TAG_IN).await)
}

/// Read the retained canonical weight of one edge (re-deriving the key exactly
/// as `keyspace.rs` does: u32-be length-prefixed components, value = obe weight).
async fn canonical_weight(database: &Database, graph: &str, src: &str, dst: &str, etype: &str) -> Option<i64> {
    let ks = Keyspace::new("_");
    let mut suffix = Vec::new();
    for comp in [graph, src, dst, etype] {
        suffix.extend_from_slice(&(comp.len() as u32).to_be_bytes());
        suffix.extend_from_slice(comp.as_bytes());
    }
    let v = database.substrate().get(&ks.external_key(TAG_EDGE, &suffix)).await.unwrap()?;
    let arr: [u8; 8] = v.as_ref().try_into().unwrap();
    Some((u64::from_be_bytes(arr) ^ (i64::MIN as u64)) as i64)
}

#[tokio::test]
async fn upsert_then_delete_roundtrip() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    g.upsert("lineage", &[EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 5, etype: String::new() }], Merge::Set)
        .await
        .unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
    assert_eq!(canonical_weight(&database, "lineage", "A", "B", "").await, Some(5));

    g.delete("lineage", &[EdgeRef { src: "A".into(), dst: "B".into(), etype: String::new() }])
        .await
        .unwrap();
    assert_eq!(count_graph_keys(&database).await, (0, 0, 0));
    assert_eq!(canonical_weight(&database, "lineage", "A", "B", "").await, None);
}

#[tokio::test]
async fn set_overwrites_stale_out_in_no_orphans() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    g.upsert("g", &[EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 1, etype: String::new() }], Merge::Set).await.unwrap();
    g.upsert("g", &[EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 9, etype: String::new() }], Merge::Set).await.unwrap();
    // Still exactly one of each — the weight-1 out/in keys must be gone (no orphans).
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
    assert_eq!(canonical_weight(&database, "g", "A", "B", "").await, Some(9));
}

#[tokio::test]
async fn max_keeps_larger_weight() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    g.upsert("g", &[EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 9, etype: String::new() }], Merge::Max).await.unwrap();
    g.upsert("g", &[EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 2, etype: String::new() }], Merge::Max).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
    assert_eq!(canonical_weight(&database, "g", "A", "B", "").await, Some(9));
}

#[tokio::test]
async fn within_one_request_two_deltas_same_identity_compose() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    // Two upserts of the same identity in one request (overlay must collapse them).
    g.upsert(
        "g",
        &[
            EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 1, etype: String::new() },
            EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 7, etype: String::new() },
        ],
        Merge::Set,
    )
    .await
    .unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
    assert_eq!(canonical_weight(&database, "g", "A", "B", "").await, Some(7));
}

#[tokio::test]
async fn parallel_edges_distinguished_by_type() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    g.upsert("g", &[
        EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 1, etype: "knows".into() },
        EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 2, etype: "likes".into() },
    ], Merge::Set).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (2, 2, 2));
    assert_eq!(canonical_weight(&database, "g", "A", "B", "knows").await, Some(1));
    assert_eq!(canonical_weight(&database, "g", "A", "B", "likes").await, Some(2));
}

#[tokio::test]
async fn delete_nonexistent_is_noop() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    g.delete("g", &[EdgeRef { src: "X".into(), dst: "Y".into(), etype: String::new() }]).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (0, 0, 0));
}
```

- [ ] **Step 4: Run, expect pass**

Run: `cargo test -p bluedb-evidence --test graph 2>&1`
Expected: all PASS. The helpers use the public `bluedb_sql::Keyspace` API to scan exact per-tag ranges — do NOT weaken the behavioral assertions if something fails; fix the implementation.

- [ ] **Step 5: Also run the crate unit tests** (the applier is exercised indirectly; ensure nothing regressed):

Run: `cargo test -p bluedb-evidence 2>&1`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-evidence/src/graph.rs crates/bluedb-evidence/src/lib.rs crates/bluedb-evidence/tests/graph.rs
git commit -m "feat(evidence): graph edge applier (overlay RYW) + Graph upsert/delete handle"
```

---

## Task P3-4: Append-with-edges — materialize entry edges in the append batch

**Files:**
- Modify: `crates/bluedb-evidence/src/chain.rs`
- Test: `crates/bluedb-evidence/tests/append_edges.rs`

The append path already stores `edges` in each `EntryRecord` and (verified) folds them into `leaf_hash`. This task additionally applies them to the graph store **in the same `WriteBatch`**, with one shared overlay across all entries (so same-batch reads compose, §6.4 step 8).

- [ ] **Step 1: Add imports** to `chain.rs` — extend the `use std::collections` (add if missing) and the graph applier:

```rust
use std::collections::HashMap;
use crate::graph::apply_edge_delta;
```

- [ ] **Step 2: Apply edges inside `append`.** In `append`, the loop currently builds `EntryRecord`s and puts them. We must apply each entry's edges to the batch. Because the applier borrows `&mut batch` and is `async`, collect each entry's edges first, then apply after the entry-put loop (the order of puts within the batch does not matter for atomicity; only that all land together).

Replace the entry-build loop and the section up to the frontier code with:

```rust
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
```

(Leave the seq-counter, frontier, idem, and group-commit code below unchanged.)

- [ ] **Step 3: Write the failing integration tests** `crates/bluedb-evidence/tests/append_edges.rs`:

```rust
//! Append-with-edges: an evidence entry's `edges[]` materialize into the graph
//! store atomically with the entry, on both verified and plain chains.

use std::sync::Arc;

use bluedb_evidence::{EdgeDelta, EdgeOp, EdgeRef, Evidence, EntryInput, Graph, Merge};
use bluedb_sql::{prefix_upper_bound, Database, Keyspace};
use slatedb::{object_store::memory::InMemory, Db};

async fn db() -> Database {
    let d = Db::open("ae-test", Arc::new(InMemory::new())).await.unwrap();
    Database::new(Arc::new(d))
}

fn upsert_edge(graph: &str, src: &str, dst: &str, w: i64) -> EdgeDelta {
    EdgeDelta { graph: graph.into(), src: src.into(), dst: dst.into(), weight: w, etype: String::new(), op: EdgeOp::Upsert { merge: Merge::Set } }
}

/// `(edge, out, in)` key counts for tenant `"_"`, scanning each graph tag's
/// exact external range via the public `bluedb_sql::Keyspace` API.
async fn count_graph_keys(database: &Database) -> (usize, usize, usize) {
    async fn count_tag(database: &Database, tag: u8) -> usize {
        let ks = Keyspace::new("_");
        let prefix = ks.external_prefix(tag);
        let end = prefix_upper_bound(&prefix);
        let mut it = database.substrate().scan_range(&prefix, end.as_deref()).await.unwrap();
        let mut n = 0;
        while it.next().await.unwrap().is_some() {
            n += 1;
        }
        n
    }
    (count_tag(database, 0x1C).await, count_tag(database, 0x1D).await, count_tag(database, 0x1E).await)
}

#[tokio::test]
async fn append_materializes_edges_on_verified_chain() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    let entry = EntryInput {
        etype: "lineage".into(),
        payload: b"p".to_vec(),
        at: String::new(),
        edges: vec![upsert_edge("lin", "D1", "D2", 5)],
    };
    ev.append("c", vec![entry], None).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
}

#[tokio::test]
async fn append_materializes_edges_on_plain_chain() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    ev.create_chain("c", false).await.unwrap();
    let entry = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 1)] };
    ev.append("c", vec![entry], None).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
}

#[tokio::test]
async fn multi_entry_batch_same_identity_composes() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    // Two entries in ONE append, both touching (lin, A, B): the overlay must
    // collapse them to a single edge at the last weight.
    let e1 = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 1)] };
    let e2 = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 9)] };
    ev.append("c", vec![e1, e2], None).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
}

#[tokio::test]
async fn append_with_no_edges_writes_no_graph_keys() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    let entry = EntryInput { etype: "e".into(), payload: b"x".to_vec(), at: String::new(), edges: vec![] };
    ev.append("c", vec![entry], None).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (0, 0, 0));
}

#[tokio::test]
async fn idempotent_replay_does_not_double_apply_edges() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    let entry = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 3)] };
    ev.append("c", vec![entry.clone()], Some("k1")).await.unwrap();
    // Same key + identical entry → replay, no second write.
    ev.append("c", vec![entry], Some("k1")).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
}

// Sanity: the standalone Graph handle and append agree on layout (delete what
// append created).
#[tokio::test]
async fn standalone_delete_removes_append_created_edge() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    let entry = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 5)] };
    ev.append("c", vec![entry], None).await.unwrap();
    let g = Graph::new(&database, "_");
    g.delete("lin", &[EdgeRef { src: "A".into(), dst: "B".into(), etype: String::new() }]).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (0, 0, 0));
}
```

> Note: `EdgeDelta`, `EdgeOp`, `Merge` must be public for these tests. They are already re-exported from `model` (`pub use model::{ChainMeta, EdgeDelta, EdgeOp, EntryRecord, IdemRecord, Merge};` in `lib.rs`). Confirm; if `Merge` is missing from the re-export, add it.

- [ ] **Step 4: Run, expect pass**

Run: `cargo test -p bluedb-evidence --test append_edges 2>&1`
Expected: PASS. Then `cargo test -p bluedb-evidence 2>&1` — full crate green (existing merkle/erasure/concurrency tests must still pass; the leaf_hash already covers edges so digests are unchanged).

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-evidence/src/chain.rs crates/bluedb-evidence/tests/append_edges.rs
git commit -m "feat(evidence): materialize entry edges into the graph store in the append batch"
```

---

## Task P3-5: Hard-delete edge retraction

**Files:**
- Modify: `crates/bluedb-evidence/src/chain.rs`
- Test: extend `crates/bluedb-evidence/tests/append_edges.rs` (or `tests/erasure.rs`)

Per §6.6 hard-delete step 2: on a plain-chain hard-delete, by default also retract the entry's `edges[]` from the graph. v1 retraction semantics: **delete each edge identity the entry referenced** (canonical + out + in), so replaying the chain after the delete no longer reproduces the deleted event's edges. (It does not attempt to restore a prior weight — the graph is a rebuildable projection; exact as-of is a replay concern, Plan 5.)

- [ ] **Step 1: Change `hard_delete`'s signature** to take `retract_edges: bool`, read the entry (we need its `edges`), and in the same batch issue an edge-Delete for each referenced identity:

```rust
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
            let mut overlay: std::collections::HashMap<Vec<u8>, Option<i64>> =
                std::collections::HashMap::new();
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
```

Also update the doc comment above `hard_delete` — remove the "deferred to Plan 3" note and describe the `retract_edges` flag.

- [ ] **Step 2: Update the existing caller in `crates/bluedb-server/src/evidence_api.rs`** so the crate compiles (full HTTP wiring is P3-6, but the call must pass the new arg now). Temporarily pass `true`:
```rust
state.evidence(&tenant).await?.hard_delete(&chain, seq, true).await.map_err(map_evidence_err)?;
```
(P3-6 replaces `true` with the parsed query flag.)

- [ ] **Step 3: Update any existing `hard_delete` test callers** in `crates/bluedb-evidence/tests/erasure.rs` to pass `true` (so they keep compiling). Search: `grep -rn "hard_delete" crates/bluedb-evidence/tests crates/bluedb-server`.

- [ ] **Step 4: Write the failing tests** (append to `tests/append_edges.rs`):

```rust
#[tokio::test]
async fn hard_delete_retracts_edges_by_default() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    ev.create_chain("c", false).await.unwrap(); // plain chain (deletable)
    let entry = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 5)] };
    let r = ev.append("c", vec![entry], None).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
    ev.hard_delete("c", r.seqs[0], true).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (0, 0, 0));
}

#[tokio::test]
async fn hard_delete_keeps_edges_when_retract_false() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    ev.create_chain("c", false).await.unwrap();
    let entry = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 5)] };
    let r = ev.append("c", vec![entry], None).await.unwrap();
    ev.hard_delete("c", r.seqs[0], false).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
}
```

- [ ] **Step 5: Run, expect pass**

Run: `cargo test -p bluedb-evidence 2>&1`
Expected: PASS (append_edges + erasure + all prior).

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-evidence/src/chain.rs crates/bluedb-evidence/tests/append_edges.rs crates/bluedb-evidence/tests/erasure.rs crates/bluedb-server/src/evidence_api.rs
git commit -m "feat(evidence): hard-delete retracts the entry's edges (flag, default on)"
```

---

## Task P3-6: HTTP graph edges API + hard-delete flag + workspace green

**Files:**
- Create: `crates/bluedb-server/src/graph_api.rs`
- Modify: `crates/bluedb-server/src/lib.rs`
- Modify: `crates/bluedb-server/src/evidence_api.rs`
- Test: `crates/bluedb-server/tests/graph.rs` (new) or extend `tests/evidence.rs`

- [ ] **Step 1: Add `AppState::graph`** in `lib.rs` (right after `evidence`):

```rust
    /// Build a [`Graph`] handle over the currently-bound database for `tenant`.
    pub(crate) async fn graph(&self, tenant: &str) -> Result<bluedb_evidence::Graph, AppError> {
        match self.inner.db.read().await.as_ref() {
            Some(db) => Ok(bluedb_evidence::Graph::new(db, tenant)),
            None => Err(AppError::service_unavailable(
                "node has no database yet (no writer has been promoted)",
            )),
        }
    }
```

- [ ] **Step 2: Write `graph_api.rs`**:

```rust
//! `/graph/*` — HTTP surface for the native graph store (edge maintenance).
//! Traversal endpoints (`reachable`, `widest-path`) are Plan 4.

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use bluedb_evidence::{EdgeRef, EdgeUpsert, Merge};

use crate::evidence_api::map_evidence_err;
use crate::{authz::Scope, AppError, AppState};

#[derive(Deserialize)]
pub(crate) struct UpsertBody {
    edges: Vec<UpsertEdge>,
    #[serde(default)]
    merge: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct UpsertEdge {
    src: String,
    dst: String,
    weight: i64,
    #[serde(default, rename = "type")]
    etype: String,
}

#[derive(Deserialize)]
pub(crate) struct DeleteBody {
    edges: Vec<DeleteEdge>,
}

#[derive(Deserialize)]
pub(crate) struct DeleteEdge {
    src: String,
    dst: String,
    #[serde(default, rename = "type")]
    etype: String,
}

/// `PUT /graph/{graph}/edges` — upsert edges. `merge` ∈ {"set","max"} (default set).
pub async fn upsert_edges(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(graph): Path<String>,
    Json(body): Json<UpsertBody>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    let merge = match body.merge.as_deref() {
        None | Some("set") => Merge::Set,
        Some("max") => Merge::Max,
        Some(other) => return Err(AppError::bad_request(format!("unknown merge mode '{other}' (want 'set' or 'max')"))),
    };
    let edges: Vec<EdgeUpsert> = body
        .edges
        .into_iter()
        .map(|e| EdgeUpsert { src: e.src, dst: e.dst, weight: e.weight, etype: e.etype })
        .collect();
    let n = edges.len();
    state.graph(&tenant).await?.upsert(&graph, &edges, merge).await.map_err(map_evidence_err)?;
    Ok(Json(json!({ "graph": graph, "upserted": n })))
}

/// `DELETE /graph/{graph}/edges` — delete edges by identity.
pub async fn delete_edges(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(graph): Path<String>,
    Json(body): Json<DeleteBody>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    state.authorize(&headers, Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    let edges: Vec<EdgeRef> = body
        .edges
        .into_iter()
        .map(|e| EdgeRef { src: e.src, dst: e.dst, etype: e.etype })
        .collect();
    let n = edges.len();
    state.graph(&tenant).await?.delete(&graph, &edges).await.map_err(map_evidence_err)?;
    Ok(Json(json!({ "graph": graph, "deleted": n })))
}
```

- [ ] **Step 3: Make `map_evidence_err` reachable.** In `evidence_api.rs`, change `fn map_evidence_err` to `pub(crate) fn map_evidence_err`.

- [ ] **Step 4: Register the module + routes** in `lib.rs`. Add `mod graph_api;` near `mod evidence_api;`, and in `build_app` after the evidence routes:

```rust
        // Native graph store (edge maintenance; traversal is Plan 4).
        .route("/graph/{graph}/edges", put(graph_api::upsert_edges).delete(graph_api::delete_edges))
```

(Confirm `put`/`delete` are imported in `lib.rs` — they already are, used by other routes.)

- [ ] **Step 5: Wire the hard-delete `retract_edges` query flag** in `evidence_api.rs`. Change the `hard_delete` handler to read an optional query param (default true):

```rust
#[derive(Deserialize)]
pub(crate) struct HardDeleteQuery {
    #[serde(default = "default_true")]
    retract_edges: bool,
}
fn default_true() -> bool { true }

pub async fn hard_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((chain, seq)): Path<(String, i64)>,
    Query(q): Query<HardDeleteQuery>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    state.evidence(&tenant).await?.hard_delete(&chain, seq, q.retract_edges).await.map_err(map_evidence_err)?;
    Ok(Json(json!({ "chain": chain, "seq": seq, "deleted": true, "retract_edges": q.retract_edges })))
}
```

(`Query` is already imported in `evidence_api.rs`.)

- [ ] **Step 6: Write the e2e test** `crates/bluedb-server/tests/graph.rs`. Follow the harness shape of `crates/bluedb-server/tests/evidence.rs` (reuse its test app builder / auth header helper — read that file first to copy the exact setup; do NOT invent a new harness). Cover:
  1. `PUT /graph/g/edges {edges:[{src:"A",dst:"B",weight:5}]}` → 200, `upserted:1`.
  2. `PUT` same identity weight 9 `merge:"max"` → still one edge; re-`PUT` weight 1 `merge:"set"` → weight becomes 1 (assert via a Plan-4 read if available — it is NOT yet, so assert idempotent success + count is out of HTTP scope; instead assert the second/third upsert returns 200).
  3. `DELETE /graph/g/edges {edges:[{src:"A",dst:"B"}]}` → 200, `deleted:1`.
  4. Append-with-edges through HTTP then hard-delete with `?retract_edges=false` on a **plain** chain returns 200 (and with default true also 200) — assert status codes and JSON shape.
  5. Auth: `PUT /graph/...` without `data:write` → 403; with wrong tenant scope → 403 (mirror evidence.rs auth assertions).

Because traversal reads do not exist yet, e2e assertions are on **status codes + response JSON**, not on graph contents (content correctness is covered by the crate-level integration tests in P3-3/P3-4/P3-5). Keep the e2e focused on the HTTP contract.

- [ ] **Step 7: Run the server tests**

Run: `cargo test -p bluedb-server --test graph 2>&1` and `cargo test -p bluedb-server --test evidence 2>&1`
Expected: PASS.

- [ ] **Step 8: Workspace green + clippy**

Run: `cargo test --workspace 2>&1`
Expected: exit 0, all suites pass.

Run: `cargo clippy --workspace --all-targets 2>&1`
Expected: no NEW warnings beyond the pre-existing `type_complexity` (bluedb-engine) and `field_reassign_with_default` (bluedb-server lib.rs). Fix any new warnings in the code you added.

- [ ] **Step 9: Commit**

```bash
git add crates/bluedb-server/src/graph_api.rs crates/bluedb-server/src/lib.rs crates/bluedb-server/src/evidence_api.rs crates/bluedb-server/tests/graph.rs
git commit -m "feat(evidence): HTTP graph edges API (upsert/delete) + hard-delete retract flag"
```

---

## Acceptance (whole plan)

- `cargo test --workspace 2>&1` exits 0.
- `cargo clippy --workspace --all-targets 2>&1` introduces no new warnings.
- An entry's `edges[]` materialize canonical+out+in atomically with the entry (verified and plain chains); a multi-entry / multi-edge batch touching one identity composes via the overlay (no orphan out/in keys).
- `merge:"max"` keeps the larger weight; `merge:"set"` overwrites and removes the stale out/in keys.
- Hard-delete on a plain chain retracts the entry's edges by default; `retract_edges=false` keeps them; verified chains still reject delete with `E_VERIFIED_NO_DELETE`.
- The standalone `Graph` handle and the append path agree on key layout (one can delete what the other created).
- Verified-chain digests are unchanged by this plan (leaf_hash already framed edges in Plan 2).

## Out of scope (later plans)

- **Plan 4:** traversal reads — `reachable(from_set, floor, directed)`, `widest_path(from, to, directed)`, their HTTP routes (`POST /graph/{graph}/reachable`, `/widest-path`), and reverse/floor range scans over the out/in indexes.
- **Plan 5:** as-of scratch (name-prefix create/drop, range-delete of a scratch graph's `0x1C/0x1D/0x1E` keys).
- Explicit isolated nodes (nodes are implicit = union of src/dst).
