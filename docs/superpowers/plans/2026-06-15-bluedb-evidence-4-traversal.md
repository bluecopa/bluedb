# bluedb-evidence Plan 4 — Graph Traversal (reachable + widest_path)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the *read* side of the native graph store — `reachable(from_set, floor, directed)` (BFS over a weight floor) and `widest_path(from, to, directed)` (max-bottleneck / maximin Dijkstra) — over the `out`/`in` adjacency indexes built in Plan 3, plus their HTTP routes.

**Architecture:** Traversal scans the `out` (`0x1D`) and `in` (`0x1E`) adjacency indexes by `(graph, node)` prefix. A node's edges are a bounded prefix range scan with the weight folded order-preserving into the key, so `reachable`'s floor is a range lower-bound and a node's full adjacency is one scan. `reachable` is BFS with a visited set; `widest_path` is a max-heap maximin Dijkstra (we use a max-heap keyed by best-known bottleneck rather than descending in-index iteration — equivalent and simpler with the forward-scan API). Reads use the substrate directly (no write lease; works on replicas). **Out of scope:** as-of scratch (§9, Plan 5).

**Tech Stack:** Rust, `bluedb_storage::Substrate::scan_range` (ordered `[start,end)` iterator), `std::collections::{VecDeque, BinaryHeap, HashMap, HashSet}`, axum.

**Worktree:** All work in `/Users/satya/work/bc/bluedb-evidence-wt` on branch `feat/evidence-substrate`. NEVER touch `/Users/satya/work/bc/bluedb`. Commit-only, NEVER push. Co-author trailer `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. No `cargo fmt` (match style by hand). Run tests directly with `2>&1` — no `tail`/`grep` pipes. "bluedb"/"bluecopa" lowercase.

---

## Source-of-truth context (read once)

**Spec:** `docs/superpowers/specs/2026-06-15-bluedb-evidence-substrate-design.md` §8.4 (traversal), §8.5 (efficiency), §10 (HTTP rows for `reachable` / `widest_path`).

**Key encoding (Plan 3, committed — do NOT change):**
- `tagged(tag, _)` produces `tenant_prefix ‖ tag` (the length arg is a capacity hint only). So `external_key(tag, suffix) = tenant_prefix ‖ tag ‖ suffix`, and `external_prefix(tag) = tenant_prefix ‖ tag` is a clean prefix.
- Out-adjacency key `0x1D`: `external_key(TAG_GRAPH_OUT, graph_lp ‖ src_lp ‖ weight_obe ‖ dst_lp ‖ type_lp)`, value `[1u8]`.
- In-adjacency key `0x1E`: `external_key(TAG_GRAPH_IN, graph_lp ‖ dst_lp ‖ weight_obe ‖ src_lp ‖ type_lp)`, value `[1u8]`.
- `xx_lp` = `(len as u32-be) ‖ bytes` (via `push_lp`). `weight_obe(w) = ((w as u64) ^ (i64::MIN as u64)).to_be_bytes()`, inverse `weight_from_obe`. Both `pub(crate)` in `crate::keyspace`. The out index sorts a node's edges by weight **ascending**.
- `EvidenceKeyspace` (in `crate::keyspace`) wraps `bluedb_sql::Keyspace` as field `ks` and exposes `pub(crate)` builders incl. `graph_out_key`/`graph_in_key`. It does NOT yet expose a *prefix* builder for adjacency scans — P4-1 adds it.

**Parsing a scanned adjacency key:** if you scan with prefix `P = graph_out_prefix(graph, src)` (= `tenant_prefix ‖ TAG_GRAPH_OUT ‖ graph_lp ‖ src_lp`), then for each returned key, `key[P.len()..]` = `weight_obe(8 bytes) ‖ dst_lp ‖ type_lp`. Parse: `weight_from_obe(&key[P.len()..P.len()+8])`, then a u32-be length + that many bytes for `dst`, then a u32-be length + bytes for `type`. (Same shape for `in`, with `src` in the neighbor position.)

**Substrate scan API:** `substrate.scan_range(start: &[u8], end: Option<&[u8]>) -> Result<DbIterator>` yields kv pairs in ascending key order over `[start, end)`. Iterate with `while let Some(kv) = iter.next().await.map_err(...)? { kv.key.as_ref(); kv.value }`. There is **no snapshot primitive** — each `scan_range` is read-committed. v1 traversal accepts read-committed semantics across its many scans (output is sorted / maximin-unique, so deterministic for a fixed graph; concurrent writes during a traversal are an accepted v1 caveat — document it).

**`Graph` handle (Plan 3, `crate::graph`):** `Graph { substrate: Substrate, write_lease: WriteLease, keyspace: EvidenceKeyspace }`, `new(database, tenant)`, plus write methods. Read methods you add use `&self.substrate` + `&self.keyspace` and do NOT take the lease (so they work on a read replica). Error helper: `fn storage_err(e: impl std::fmt::Display) -> EvidenceError` already exists on `Graph`; for free functions in `traverse.rs` use `EvidenceError::Storage(anyhow::anyhow!("{e}"))` (the `?` on `scan_range`'s `anyhow::Error` already converts via `EvidenceError::Storage(#[from] anyhow::Error)`).

**Server wiring (Plan 3, committed):** `AppState::graph(tenant) -> Result<bluedb_evidence::Graph, AppError>`; `crate::graph_api` holds the `/graph/*` handlers (`upsert_edges`/`delete_edges`); `map_evidence_err` is `pub(crate)` in `evidence_api`; routes registered in `lib.rs build_app` (`/graph/{graph}/edges`). Handlers resolve `state.tenant(&headers)?`, reads use `state.authorize(&headers, Scope::DataRead)?` (NO `require_active` for reads — reads work on replicas).

---

## File Structure

- **Modify** `crates/bluedb-evidence/src/keyspace.rs` — add `graph_out_prefix(graph, src)` and `graph_in_prefix(graph, dst)` (the adjacency scan prefixes).
- **Create** `crates/bluedb-evidence/src/traverse.rs` — `out_neighbors`/`in_neighbors` (scan + parse + floor), `reachable`, `widest_path`, and the `WidestPath` result type.
- **Modify** `crates/bluedb-evidence/src/graph.rs` — add `Graph::reachable` and `Graph::widest_path` (thin delegations to `traverse::*`).
- **Modify** `crates/bluedb-evidence/src/lib.rs` — `mod traverse;` + `pub use graph::WidestPath;`.
- **Modify** `crates/bluedb-server/src/graph_api.rs` — `reachable` + `widest_path` HTTP handlers.
- **Modify** `crates/bluedb-server/src/lib.rs` — register the two routes.
- **Create** `crates/bluedb-evidence/tests/traverse.rs` — algorithm integration tests.
- **Modify** `crates/bluedb-server/tests/graph.rs` — traversal e2e.

---

## Task P4-1: Keyspace — adjacency scan prefixes

**Files:** Modify `crates/bluedb-evidence/src/keyspace.rs`

- [ ] **Step 1: Add the two prefix builders** as methods on `impl EvidenceKeyspace` (after `graph_in_key`). They reuse the same `push_lp` framing so they are exact prefixes of the full keys:

```rust
/// Scan prefix for all out-edges of `(graph, src)`: every `graph_out_key`
/// for this node begins with this, then `weight_obe ‖ dst ‖ type`.
pub(crate) fn graph_out_prefix(&self, graph: &str, src: &str) -> Vec<u8> {
    let mut s = Vec::new();
    push_lp(&mut s, graph);
    push_lp(&mut s, src);
    self.ks.external_key(TAG_GRAPH_OUT, &s)
}

/// Scan prefix for all in-edges of `(graph, dst)`.
pub(crate) fn graph_in_prefix(&self, graph: &str, dst: &str) -> Vec<u8> {
    let mut s = Vec::new();
    push_lp(&mut s, graph);
    push_lp(&mut s, dst);
    self.ks.external_key(TAG_GRAPH_IN, &s)
}
```

- [ ] **Step 2: Add tests** (in `#[cfg(test)] mod tests`):

```rust
#[test]
fn out_prefix_is_exact_prefix_of_out_keys() {
    let ks = EvidenceKeyspace::new("acme");
    let p = ks.graph_out_prefix("g", "u");
    let k1 = ks.graph_out_key("g", "u", 1, "v", "");
    let k2 = ks.graph_out_key("g", "u", i64::MAX, "z", "t");
    assert!(k1.starts_with(&p));
    assert!(k2.starts_with(&p));
    // A different src must not fall under this prefix.
    let other = ks.graph_out_key("g", "uu", 1, "v", "");
    assert!(!other.starts_with(&p));
    // The bytes after the prefix begin with weight_obe.
    assert_eq!(&k1[p.len()..p.len() + 8], &weight_obe(1));
}

#[test]
fn in_prefix_is_exact_prefix_of_in_keys() {
    let ks = EvidenceKeyspace::new("acme");
    let p = ks.graph_in_prefix("g", "v");
    let k = ks.graph_in_key("g", "v", 7, "u", "");
    assert!(k.starts_with(&p));
    assert_eq!(&k[p.len()..p.len() + 8], &weight_obe(7));
}
```

- [ ] **Step 3:** Run `cd /Users/satya/work/bc/bluedb-evidence-wt && cargo test -p bluedb-evidence keyspace 2>&1` — all PASS.
- [ ] **Step 4: Commit**
```bash
git add crates/bluedb-evidence/src/keyspace.rs
git commit -m "feat(evidence): graph adjacency scan prefixes (out/in by node)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task P4-2: traverse.rs — adjacency scan (`out_neighbors`/`in_neighbors`)

**Files:** Create `crates/bluedb-evidence/src/traverse.rs`; Modify `crates/bluedb-evidence/src/lib.rs`; Test `crates/bluedb-evidence/tests/traverse.rs`

- [ ] **Step 1: Create `traverse.rs`** with the scan+parse primitives (the rest of the file is added in P4-3/P4-4):

```rust
//! Read-only graph traversal over the `out`/`in` adjacency indexes (Plan 3).
//! A node's edges are a bounded prefix range scan with the weight folded
//! order-preserving into the key. No write lease — works on a read replica.
//! v1 caveat: there is no cross-scan snapshot, so a traversal sees read-committed
//! state across its scans; output is sorted (`reachable`) / maximin-unique
//! (`widest_path`), so it is deterministic for a fixed graph.

use bluedb_storage::Substrate;

use crate::error::EvidenceError;
use crate::keyspace::{weight_from_obe, EvidenceKeyspace};

/// Read `(neighbor, weight, type)` for every edge under `prefix`, keeping only
/// edges with `weight >= floor`. `prefix` is an out/in adjacency scan prefix;
/// the bytes after it are `weight_obe(8) ‖ neighbor_lp ‖ type_lp`.
async fn scan_adjacency(
    substrate: &Substrate,
    prefix: &[u8],
    floor: i64,
) -> Result<Vec<(String, i64, String)>, EvidenceError> {
    // Start at weight = floor (range lower bound); end at the prefix upper bound.
    let mut start = prefix.to_vec();
    start.extend_from_slice(&crate::keyspace::weight_obe(floor));
    let end = bluedb_sql::prefix_upper_bound(prefix);
    let mut iter = substrate.scan_range(&start, end.as_deref()).await?;
    let mut out = Vec::new();
    let plen = prefix.len();
    while let Some(kv) = iter.next().await.map_err(|e| EvidenceError::Storage(anyhow::anyhow!("{e}")))? {
        let key = kv.key.as_ref();
        let tail = &key[plen..];
        let w = {
            let arr: [u8; 8] = tail[0..8].try_into().map_err(|_| {
                EvidenceError::Storage(anyhow::anyhow!("adjacency key too short for weight"))
            })?;
            weight_from_obe(&arr)
        };
        let mut p = 8usize;
        let neighbor = read_lp(tail, &mut p)?;
        let etype = read_lp(tail, &mut p)?;
        out.push((neighbor, w, etype));
    }
    Ok(out)
}

/// Read a `<u32-be len> <utf8 bytes>` segment from `buf` at `*pos`, advancing it.
fn read_lp(buf: &[u8], pos: &mut usize) -> Result<String, EvidenceError> {
    let err = || EvidenceError::Storage(anyhow::anyhow!("malformed adjacency key segment"));
    let len = {
        let arr: [u8; 4] = buf.get(*pos..*pos + 4).ok_or_else(err)?.try_into().map_err(|_| err())?;
        u32::from_be_bytes(arr) as usize
    };
    *pos += 4;
    let bytes = buf.get(*pos..*pos + len).ok_or_else(err)?;
    *pos += len;
    String::from_utf8(bytes.to_vec()).map_err(|_| err())
}

/// Out-neighbors `(dst, weight, type)` of `src` with `weight >= floor`, ascending.
pub(crate) async fn out_neighbors(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    graph: &str,
    src: &str,
    floor: i64,
) -> Result<Vec<(String, i64, String)>, EvidenceError> {
    scan_adjacency(substrate, &ks.graph_out_prefix(graph, src), floor).await
}

/// In-neighbors `(src, weight, type)` of `dst` with `weight >= floor`, ascending.
pub(crate) async fn in_neighbors(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    graph: &str,
    dst: &str,
    floor: i64,
) -> Result<Vec<(String, i64, String)>, EvidenceError> {
    scan_adjacency(substrate, &ks.graph_in_prefix(graph, dst), floor).await
}
```

- [ ] **Step 2: Wire the module** in `lib.rs`: add `mod traverse;` near the other `mod` lines. (No re-export needed yet; functions are `pub(crate)`.)

- [ ] **Step 3: Add an in-module unit test for the scan/parse** (keeps P4-2 independently green; `out_neighbors`/`in_neighbors` are `pub(crate)` so they're visible to an in-crate `#[cfg(test)]` module, and `Graph`/`EdgeUpsert`/`Merge` are reachable via `crate::`). The separate `tests/traverse.rs` integration suite is created in P4-3. Add to the bottom of `traverse.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use bluedb_sql::Database;
    use slatedb::{object_store::memory::InMemory, Db};
    use crate::graph::{Graph, EdgeUpsert};
    use crate::model::Merge;

    async fn db() -> Database {
        let d = Db::open("trav-unit", Arc::new(InMemory::new())).await.unwrap();
        Database::new(Arc::new(d))
    }

    #[tokio::test]
    async fn out_neighbors_ascending_and_floor_filter() {
        let database = db().await;
        let g = Graph::new(&database, "_");
        // Parallel edges A->B distinguished by type, plus A->C.
        g.upsert("g", &[
            EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 2, etype: "x".into() },
            EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 9, etype: "y".into() },
            EdgeUpsert { src: "A".into(), dst: "C".into(), weight: 5, etype: String::new() },
        ], Merge::Set).await.unwrap();

        let substrate = database.substrate();
        let ks = EvidenceKeyspace::new("_");
        let all = out_neighbors(&substrate, &ks, "g", "A", i64::MIN).await.unwrap();
        // Ascending by weight: (B,2,x),(C,5,""),(B,9,y)
        assert_eq!(all, vec![
            ("B".to_string(), 2, "x".to_string()),
            ("C".to_string(), 5, "".to_string()),
            ("B".to_string(), 9, "y".to_string()),
        ]);
        // floor=5 drops the weight-2 edge.
        let hi = out_neighbors(&substrate, &ks, "g", "A", 5).await.unwrap();
        assert_eq!(hi, vec![
            ("C".to_string(), 5, "".to_string()),
            ("B".to_string(), 9, "y".to_string()),
        ]);
        // in_neighbors of B sees both A->B edges.
        let inb = in_neighbors(&substrate, &ks, "g", "B", i64::MIN).await.unwrap();
        assert_eq!(inb, vec![
            ("A".to_string(), 2, "x".to_string()),
            ("A".to_string(), 9, "y".to_string()),
        ]);
    }
}
```
(`EvidenceKeyspace::new` is `pub(crate)`; visible from the in-crate test module. Adjust the `use crate::graph::{Graph, EdgeUpsert}` / `crate::model::Merge` paths if the actual module paths differ.)

- [ ] **Step 4:** Run `cargo test -p bluedb-evidence traverse 2>&1` — the in-module unit test PASSES. (Do NOT create `tests/traverse.rs` in this task; it lands in P4-3.) Then `cargo test -p bluedb-evidence 2>&1` — full crate green.
- [ ] **Step 5: Commit**
```bash
git add crates/bluedb-evidence/src/traverse.rs crates/bluedb-evidence/src/lib.rs
git commit -m "feat(evidence): graph adjacency scan (out/in neighbors, weight floor)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task P4-3: `reachable` (BFS over a weight floor)

**Files:** Modify `crates/bluedb-evidence/src/traverse.rs`, `crates/bluedb-evidence/src/graph.rs`; Create/extend `crates/bluedb-evidence/tests/traverse.rs`

**Semantics:** `reachable(from_set, floor, directed)` returns the **sorted** set of nodes reachable from any seed by traversing only edges with `weight >= floor`. The seed nodes ARE included (a node reaches itself). `directed=true` follows `out` only; `directed=false` follows `out` and `in` (each stored edge usable both ways at its weight).

- [ ] **Step 1: Add `reachable` to `traverse.rs`** (above the `#[cfg(test)]` block):

```rust
use std::collections::{HashSet, VecDeque};

/// Nodes reachable from any of `from`, traversing only edges with weight ≥
/// `floor`. Seeds are included. `directed=false` also follows `in` edges.
/// Output is sorted (order-independent).
pub(crate) async fn reachable(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    graph: &str,
    from: &[String],
    floor: i64,
    directed: bool,
) -> Result<Vec<String>, EvidenceError> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    for n in from {
        if visited.insert(n.clone()) {
            queue.push_back(n.clone());
        }
    }
    while let Some(u) = queue.pop_front() {
        for (v, _w, _t) in out_neighbors(substrate, ks, graph, &u, floor).await? {
            if visited.insert(v.clone()) {
                queue.push_back(v);
            }
        }
        if !directed {
            for (v, _w, _t) in in_neighbors(substrate, ks, graph, &u, floor).await? {
                if visited.insert(v.clone()) {
                    queue.push_back(v);
                }
            }
        }
    }
    let mut out: Vec<String> = visited.into_iter().collect();
    out.sort();
    Ok(out)
}
```

- [ ] **Step 2: Add `Graph::reachable`** in `graph.rs` (a read method — no lease):

```rust
    /// Nodes reachable from `from` over edges with weight ≥ `floor`. Seeds are
    /// included; output sorted. `directed=false` also follows in-edges.
    pub async fn reachable(
        &self,
        graph: &str,
        from: &[String],
        floor: i64,
        directed: bool,
    ) -> Result<Vec<String>, EvidenceError> {
        crate::traverse::reachable(&self.substrate, &self.keyspace, graph, from, floor, directed).await
    }
```

- [ ] **Step 3: Create `crates/bluedb-evidence/tests/traverse.rs`** (the integration suite; use the canonical graph A→B(5), B→C(3), A→C(1), C→D(10)):

```rust
//! Graph traversal integration tests (reachable + widest_path).

use std::sync::Arc;

use bluedb_evidence::{EdgeUpsert, Graph, Merge};
use bluedb_sql::Database;
use slatedb::{object_store::memory::InMemory, Db};

async fn db() -> Database {
    let d = Db::open("trav-test", Arc::new(InMemory::new())).await.unwrap();
    Database::new(Arc::new(d))
}

async fn build(database: &Database) {
    let g = Graph::new(database, "_");
    let e = |s: &str, d: &str, w: i64| EdgeUpsert { src: s.into(), dst: d.into(), weight: w, etype: String::new() };
    g.upsert("g", &[e("A", "B", 5), e("B", "C", 3), e("A", "C", 1), e("C", "D", 10)], Merge::Set)
        .await
        .unwrap();
}

#[tokio::test]
async fn reachable_directed_includes_seed_and_all_downstream() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let r = g.reachable("g", &["A".into()], i64::MIN, true).await.unwrap();
    assert_eq!(r, vec!["A", "B", "C", "D"]); // already sorted
}

#[tokio::test]
async fn reachable_floor_prunes_low_weight_edges() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    // floor=4 keeps A->B(5) and C->D(10); A->C(1) and B->C(3) pruned.
    // From A: reach B. B has no out-edge >=4. So {A,B}.
    let r = g.reachable("g", &["A".into()], 4, true).await.unwrap();
    assert_eq!(r, vec!["A", "B"]);
}

#[tokio::test]
async fn reachable_undirected_follows_in_edges() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    // From D undirected: D<-C, C<-A,C<-B(in), so reach C, then A,B, and D itself.
    let r = g.reachable("g", &["D".into()], i64::MIN, false).await.unwrap();
    assert_eq!(r, vec!["A", "B", "C", "D"]);
}

#[tokio::test]
async fn reachable_directed_from_sink_is_just_itself() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let r = g.reachable("g", &["D".into()], i64::MIN, true).await.unwrap();
    assert_eq!(r, vec!["D"]);
}

#[tokio::test]
async fn reachable_multi_seed_dedups() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let r = g.reachable("g", &["A".into(), "C".into()], i64::MIN, true).await.unwrap();
    assert_eq!(r, vec!["A", "B", "C", "D"]);
}

#[tokio::test]
async fn reachable_unknown_seed_returns_itself() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let r = g.reachable("g", &["Z".into()], i64::MIN, true).await.unwrap();
    assert_eq!(r, vec!["Z"]);
}
```

- [ ] **Step 4:** Run `cargo test -p bluedb-evidence --test traverse 2>&1` and `cargo test -p bluedb-evidence 2>&1` — all PASS.
- [ ] **Step 5: Commit**
```bash
git add crates/bluedb-evidence/src/traverse.rs crates/bluedb-evidence/src/graph.rs crates/bluedb-evidence/tests/traverse.rs
git commit -m "feat(evidence): graph reachable (BFS over weight floor, directed/undirected)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task P4-4: `widest_path` (max-bottleneck / maximin Dijkstra)

**Files:** Modify `crates/bluedb-evidence/src/traverse.rs`, `crates/bluedb-evidence/src/graph.rs`, `crates/bluedb-evidence/src/lib.rs`; extend `crates/bluedb-evidence/tests/traverse.rs`

**Semantics:** `widest_path(from, to, directed)` → `{ connected, bottleneck }`. The bottleneck of a path is its minimum edge weight; the widest path maximizes that minimum (maximin). `connected=false` (no `bottleneck`) when `to` is unreachable — not an error. `from == to` → `connected=true`, `bottleneck=None` (empty path, no edge). `directed=false` makes each stored edge usable both directions at its weight.

- [ ] **Step 1: Add the result type + algorithm to `traverse.rs`:**

```rust
use std::collections::{BinaryHeap, HashMap};

/// Result of [`widest_path`]: whether `to` is reachable and, if so, the maximum
/// bottleneck (the widest path's minimum edge weight). `from == to` → connected
/// with `bottleneck: None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WidestPath {
    pub connected: bool,
    pub bottleneck: Option<i64>,
}

/// Max-bottleneck path from `from` to `to` (maximin Dijkstra with a max-heap).
pub(crate) async fn widest_path(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    graph: &str,
    from: &str,
    to: &str,
    directed: bool,
) -> Result<WidestPath, EvidenceError> {
    if from == to {
        return Ok(WidestPath { connected: true, bottleneck: None });
    }
    // best[node] = best-known bottleneck to reach `node`. Source has +inf.
    let mut best: HashMap<String, i64> = HashMap::new();
    best.insert(from.to_string(), i64::MAX);
    // Max-heap by bottleneck; ties broken by node id (deterministic).
    let mut heap: BinaryHeap<(i64, String)> = BinaryHeap::new();
    heap.push((i64::MAX, from.to_string()));

    while let Some((bw, u)) = heap.pop() {
        // Skip stale heap entries.
        if best.get(&u).copied() != Some(bw) {
            continue;
        }
        if u == to {
            return Ok(WidestPath { connected: true, bottleneck: Some(bw) });
        }
        // Relax out-edges (and in-edges when undirected).
        let mut edges = out_neighbors(substrate, ks, graph, &u, i64::MIN).await?;
        if !directed {
            edges.extend(in_neighbors(substrate, ks, graph, &u, i64::MIN).await?);
        }
        for (v, w, _t) in edges {
            let nb = bw.min(w);
            if nb > best.get(&v).copied().unwrap_or(i64::MIN) {
                best.insert(v.clone(), nb);
                heap.push((nb, v));
            }
        }
    }
    Ok(WidestPath { connected: false, bottleneck: None })
}
```
> A `BinaryHeap` is a max-heap: `pop()` returns the largest `(bottleneck, node)` tuple first, which is exactly the maximin frontier order — no `Reverse` wrapper needed.

- [ ] **Step 2: Add `Graph::widest_path`** in `graph.rs`:

```rust
    /// Widest (max-bottleneck) path from `from` to `to`. `connected=false` when
    /// unreachable (not an error); `from==to` → connected, `bottleneck=None`.
    /// `directed=false` follows in-edges too.
    pub async fn widest_path(
        &self,
        graph: &str,
        from: &str,
        to: &str,
        directed: bool,
    ) -> Result<crate::traverse::WidestPath, EvidenceError> {
        crate::traverse::widest_path(&self.substrate, &self.keyspace, graph, from, to, directed).await
    }
```

- [ ] **Step 3: Re-export `WidestPath`** in `lib.rs`: extend the graph re-export line to `pub use graph::{EdgeRef, EdgeUpsert, Graph};` AND add `pub use traverse::WidestPath;` (or re-export from `graph` if you prefer a single surface — pick one and be consistent; `traverse::WidestPath` is where it's defined).

- [ ] **Step 4: Extend `tests/traverse.rs`:**

```rust
use bluedb_evidence::WidestPath;

#[tokio::test]
async fn widest_path_picks_max_bottleneck() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    // A->B->C->D: min(5,3,10)=3 ; A->C->D: min(1,10)=1 ; widest = 3.
    let wp = g.widest_path("g", "A", "D", true).await.unwrap();
    assert_eq!(wp, WidestPath { connected: true, bottleneck: Some(3) });
}

#[tokio::test]
async fn widest_path_unreachable_directed() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let wp = g.widest_path("g", "D", "A", true).await.unwrap(); // D is a sink
    assert_eq!(wp, WidestPath { connected: false, bottleneck: None });
}

#[tokio::test]
async fn widest_path_undirected_uses_reverse_edges() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    // Undirected D..A: D-C(10)-B(3)-A(5) => 3 ; D-C(10)-A(1) => 1 ; widest = 3.
    let wp = g.widest_path("g", "D", "A", false).await.unwrap();
    assert_eq!(wp, WidestPath { connected: true, bottleneck: Some(3) });
}

#[tokio::test]
async fn widest_path_from_equals_to() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let wp = g.widest_path("g", "A", "A", true).await.unwrap();
    assert_eq!(wp, WidestPath { connected: true, bottleneck: None });
}

#[tokio::test]
async fn widest_path_to_unknown_node() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let wp = g.widest_path("g", "A", "Z", true).await.unwrap();
    assert_eq!(wp, WidestPath { connected: false, bottleneck: None });
}
```

- [ ] **Step 5:** Run `cargo test -p bluedb-evidence --test traverse 2>&1` and `cargo test -p bluedb-evidence 2>&1` — all PASS. `cargo clippy -p bluedb-evidence --all-targets 2>&1` — no new warnings.
- [ ] **Step 6: Commit**
```bash
git add crates/bluedb-evidence/src/traverse.rs crates/bluedb-evidence/src/graph.rs crates/bluedb-evidence/src/lib.rs crates/bluedb-evidence/tests/traverse.rs
git commit -m "feat(evidence): graph widest_path (maximin Dijkstra, directed/undirected)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task P4-5: HTTP traversal routes + workspace green

**Files:** Modify `crates/bluedb-server/src/graph_api.rs`, `crates/bluedb-server/src/lib.rs`; extend `crates/bluedb-server/tests/graph.rs`

**Routes (spec §10):**
- `POST /graph/{graph}/reachable` `{from:[…], floor?, directed?}` → `{nodes:[…]}` — `data:read` + tenant.
- `POST /graph/{graph}/widest-path` `{from, to, directed?}` → `{connected, bottleneck?}` — `data:read` + tenant.

Defaults: `floor` absent → `i64::MIN` (no floor); `directed` absent → `true`.

- [ ] **Step 1: Add the handlers to `graph_api.rs`** (reads → NO `require_active`; `Scope::DataRead`):

```rust
#[derive(Deserialize)]
pub(crate) struct ReachableBody {
    from: Vec<String>,
    #[serde(default)]
    floor: Option<i64>,
    #[serde(default)]
    directed: Option<bool>,
}

/// `POST /graph/{graph}/reachable` → `{ nodes: [...] }`.
pub async fn reachable(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(graph): Path<String>,
    Json(body): Json<ReachableBody>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let floor = body.floor.unwrap_or(i64::MIN);
    let directed = body.directed.unwrap_or(true);
    let nodes = state
        .graph(&tenant)
        .await?
        .reachable(&graph, &body.from, floor, directed)
        .await
        .map_err(map_evidence_err)?;
    Ok(Json(json!({ "graph": graph, "nodes": nodes })))
}

#[derive(Deserialize)]
pub(crate) struct WidestPathBody {
    from: String,
    to: String,
    #[serde(default)]
    directed: Option<bool>,
}

/// `POST /graph/{graph}/widest-path` → `{ connected, bottleneck? }`.
pub async fn widest_path(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(graph): Path<String>,
    Json(body): Json<WidestPathBody>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let directed = body.directed.unwrap_or(true);
    let wp = state
        .graph(&tenant)
        .await?
        .widest_path(&graph, &body.from, &body.to, directed)
        .await
        .map_err(map_evidence_err)?;
    let mut out = json!({ "connected": wp.connected });
    if let Some(b) = wp.bottleneck {
        out["bottleneck"] = json!(b);
    }
    Ok(Json(out))
}
```
(Confirm `Scope::DataRead` is the read scope used by other read handlers; `json!`, `Value`, `Deserialize` are already imported in `graph_api.rs`.)

- [ ] **Step 2: Register routes** in `lib.rs build_app`, after the `/graph/{graph}/edges` route:
```rust
        .route("/graph/{graph}/reachable", post(graph_api::reachable))
        .route("/graph/{graph}/widest-path", post(graph_api::widest_path))
```
(Confirm `post` is imported — it is, used by other routes.)

- [ ] **Step 3: Add e2e tests** to `crates/bluedb-server/tests/graph.rs` (reuse the file's harness — `promoted`, `call`, `json!`, `StatusCode`). Build the canonical graph via `PUT /graph/g/edges`, then:

```rust
#[tokio::test]
async fn http_reachable_and_widest_path() {
    let (_, app) = promoted(None).await;
    // Build A->B(5),B->C(3),A->C(1),C->D(10).
    let (s, _b) = call(&app, "PUT", "/graph/g/edges", Some("acme"), None, Some(json!({
        "edges": [
            {"src":"A","dst":"B","weight":5},
            {"src":"B","dst":"C","weight":3},
            {"src":"A","dst":"C","weight":1},
            {"src":"C","dst":"D","weight":10}
        ]
    }))).await;
    assert_eq!(s, StatusCode::OK);

    // reachable from A (directed) → [A,B,C,D]
    let (s, body) = call(&app, "POST", "/graph/g/reachable", Some("acme"), None, Some(json!({ "from": ["A"] }))).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["nodes"], json!(["A","B","C","D"]));

    // reachable with floor=4 → [A,B]
    let (s, body) = call(&app, "POST", "/graph/g/reachable", Some("acme"), None, Some(json!({ "from": ["A"], "floor": 4 }))).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["nodes"], json!(["A","B"]));

    // widest_path A->D → connected, bottleneck 3
    let (s, body) = call(&app, "POST", "/graph/g/widest-path", Some("acme"), None, Some(json!({ "from":"A", "to":"D" }))).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["connected"], true);
    assert_eq!(body["bottleneck"], 3);

    // widest_path D->A directed → not connected, no bottleneck key
    let (s, body) = call(&app, "POST", "/graph/g/widest-path", Some("acme"), None, Some(json!({ "from":"D", "to":"A" }))).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["connected"], false);
    assert!(body.get("bottleneck").is_none(), "no bottleneck when disconnected: {body}");
}

#[tokio::test]
async fn http_traversal_requires_read_scope() {
    // authz mode: a token without data:read is rejected; mirror graph_edge_auth_negatives.
    // (Reuse this file's authz harness: build with Authz, send a no-scope/insufficient token, expect 403.)
}
```
For `http_traversal_requires_read_scope`, copy the EXACT authz harness already used by `graph_edge_auth_negatives` in this file (the `promoted(Some(Authz::parse_env(...)))` pattern). Assert a token lacking `data:read` → 403 on `POST /graph/g/reachable`. If that harness makes a no-scope negative awkward, at minimum assert a tenant-mismatch token → 403 (as the edge auth test does).

- [ ] **Step 4: Workspace green + clippy**
- `cd /Users/satya/work/bc/bluedb-evidence-wt && cargo test -p bluedb-server --test graph 2>&1` — PASS.
- `cargo test --workspace 2>&1` — exit 0, all suites pass.
- `cargo clippy --workspace --all-targets 2>&1` — no NEW warnings (pre-existing acceptable: `type_complexity` bluedb-engine, `field_reassign_with_default` bluedb-server lib.rs:164-165 + bluedb-sql tests, `items_after_test_module` fts/rest, `bool_assert_comparison` bluedb-sql tests). Fix any new warning in your code.

- [ ] **Step 5: Commit**
```bash
git add crates/bluedb-server/src/graph_api.rs crates/bluedb-server/src/lib.rs crates/bluedb-server/tests/graph.rs
git commit -m "feat(evidence): HTTP graph traversal routes (reachable + widest-path)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Acceptance (whole plan)

- `cargo test --workspace 2>&1` exits 0; `cargo clippy --workspace --all-targets 2>&1` adds no new warnings.
- `reachable` includes seeds, respects the weight floor, follows in-edges only when undirected, dedups multi-seed, sorts output.
- `widest_path` returns the true maximin bottleneck (A→D = 3 on the canonical graph), `connected=false` (no bottleneck key) when unreachable, handles `from==to` (connected, no bottleneck) and undirected reverse-edge traversal.
- HTTP `POST /graph/{graph}/reachable` and `/widest-path` enforce `data:read` + tenant and return the documented JSON; the `bottleneck` key is omitted when disconnected.
- Adjacency parsing is exact (weight ascending; parallel edges distinguished by type) and tenant-isolated.

## Out of scope (Plan 5)

- As-of scratch (§9): name-prefix `create_scratch`/`drop_scratch` + range-delete of a scratch graph's `0x1C/0x1D/0x1E` keys.
- Parallel frontier expansion (§8.5) — a latency optimization; v1 BFS expands a level's neighbor scans sequentially. Acceptable; note it.
- Persisted internal Merkle nodes; vector/HNSW; explicit isolated nodes.
