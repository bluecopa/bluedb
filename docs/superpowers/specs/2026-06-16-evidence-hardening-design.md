# bluedb-evidence hardening — design

Two independent v1.1 improvements to the merged `bluedb-evidence` substrate,
each a self-contained plan that does **not** depend on the other (disjoint
files), so they may be implemented in parallel:

- **Component A — O(log N) Merkle proofs** (persist internal tree nodes).
- **Component B — Parallel frontier expansion** for `reachable` traversal.

No release/deployment has happened, so **no backward-compatibility path is
required**: every verified chain is created under the new code.

---

## Component A — O(log N) Merkle proofs

### Problem

Today `digest(chain)` is O(log N) (it folds the persisted RFC 6962 frontier),
but `inclusion(seq, size)` and `consistency(first, second)` read **all** leaf
hashes `1..=size` from storage and recompute subtree roots with the pure
`merkle::{inclusion_proof, consistency_proof}` (each takes a full `&[[u8;32]]`).
That is **O(N) per proof** — fine for small chains, linear for large ones.

### Approach

Persist the hash of every **complete subtree** (a perfect, power-of-two-sized,
aligned subtree) as it forms, then assemble proofs from those persisted nodes
plus O(log N) right-edge recomputation. These are exactly the parent nodes the
frontier's carry-merge already computes on append, so producing them is
amortized O(1) per append and adds no new traversal of the tree.

The emitted proofs are **byte-identical** to today's (same RFC 6962 algorithm,
only the subtree-root *source* changes: persisted node instead of from-scratch
recompute), so the existing independent Trillian-formulation verifiers and the
property tests over all sizes continue to gate correctness unchanged.

### Node identity & keyspace

A node at **(level `L`, index `J`)** is the Merkle root over leaves
`[J·2^L, (J+1)·2^L)` (0-based leaf offsets). Level 0 is the leaf itself.

New tag (graph used `0x1C–0x1E`, so the next free external tag is `0x1F`):

| Tag | Const | Key suffix (after tenant+tag) | Value |
|---|---|---|---|
| `0x1F` | `TAG_EVIDENCE_MERKLE_NODE` | `len(chain)::u32-be ‖ chain ‖ level::u8 ‖ index::u64-be` | 32-byte node hash |

`level` fits in a `u8` (≤ 64 levels covers `2^64` leaves). `index::u64-be` keeps
nodes within a level ordered, though the proof path addresses nodes by exact
`(level, index)` point-gets (no range scan needed). Keys route through
`EvidenceKeyspace::external_key` → tenant-namespaced for free.

**Only level ≥ 1 nodes are stored.** Level-0 siblings in a proof are read from
the entry's already-stored `leaf_hash` (`store::get_entry`) — no leaf
duplication.

### Producing nodes on append

`Frontier::push` already carry-merges equal-height peaks. The append path will
use a variant that **emits each merged parent** `(level, index, hash)` so the
caller can `batch.put` it in the **same `WriteBatch`** as the entries, counter,
and frontier (crash-consistent, atomic). When merging two height-`h` peaks into
a height-`h+1` node during the append that brings the tree to `size` leaves, the
new node is at `level = h+1` and `index = (start_leaf_offset) >> (h+1)`, where
`start_leaf_offset` is the first leaf the merged subtree covers. The merge loop
already tracks enough state to compute this; the exact index arithmetic is
specified in the plan with a property test (`node(level,index)` reconstructed
from storage == `merkle_root` of that leaf range, for all sizes 0..N).

Number of nodes written per K-entry append = total carry-merges across those K
pushes (amortized O(1) per entry, O(log N) worst case). Total node storage for a
chain of N leaves ≈ `N − popcount(N)` internal nodes (≈ N), i.e. roughly doubles
the chain's 32-byte-hash storage. This is **always-on for verified chains** — the
standard CT-log space/▢time tradeoff — and replaces the O(N) proof read path.

### `subtree_root(lo, hi)` — O(log N) range root from storage

Returns the Merkle root of leaves `[lo, hi)` using persisted nodes:

1. `hi - lo == 1` → read the single leaf at `lo` from its entry (`leaf_hash`).
2. `[lo, hi)` is complete (i.e. `lo` is `2^L`-aligned and `hi - lo == 2^L`) →
   one point-get of node `(L, lo >> L)`.
3. Otherwise → `k = largest_pow2_lt(hi - lo)`; the left part `[lo, lo+k)` is a
   complete subtree (case 2, one get), the right part `[lo+k, hi)` recurses. The
   recursion only descends the right spine → O(log N) gets, O(log N) hashes.

Because every complete subtree within `[0, head)` was persisted when it formed,
**no node is ever missing** for a chain built under this code. A missing node is
therefore a bug, not an expected state: `subtree_root` returns a loud
`EvidenceError::Storage("merkle node (L,J) missing")` rather than silently
recomputing. (No backward-compat / backfill path — none is needed.)

### Storage-backed proofs

A new private module `proof` (in `crates/bluedb-evidence/src/proof.rs`) provides
storage-backed analogues that mirror the pure recursion in `merkle.rs` but
substitute `subtree_root` for `merkle_root(slice)`:

- `inclusion(substrate, ks, chain, index, size) -> Vec<[u8;32]>`
- `consistency(substrate, ks, chain, first, second) -> Vec<[u8;32]>`

`chain.rs::inclusion`/`consistency` switch from `leaf_hashes(...) + merkle::*` to
these. The `leaf_hashes` helper (and its O(N) `read_range`) is removed from the
proof path. The pure `merkle.rs` functions remain as the in-crate test
reference. `digest` is unchanged (already frontier-folded).

### Testing (A)

- **Node correctness:** for sizes 0..N, every persisted `(level, index)` equals
  `merkle_root` of its leaf range (property test).
- **Proof equivalence:** for all `(index, size)` and `(first, second)` over a
  spread of sizes (mirroring the existing `1..130` / `1..70` property tests),
  the storage-backed proof equals the pure-`merkle` proof **byte-for-byte**, and
  verifies against the independent RFC 6962 verifiers.
- **End-to-end:** the existing `merkle_chain` integration test (digest ==
  recompute == inclusion-reconstruction) still passes; add a multi-append case
  asserting proofs are correct without ever reading all leaves (e.g. assert the
  proof length is the RFC 6962 expected O(log N)).
- **Redaction:** a redacted entry keeps its `leaf_hash`, so nodes are unaffected
  — assert digest/proofs still verify after a redact (already covered; re-assert).

### Files (A)

- `crates/bluedb-evidence/src/keyspace.rs` — `TAG_EVIDENCE_MERKLE_NODE` +
  `merkle_node_key(chain, level, index)`.
- `crates/bluedb-evidence/src/store.rs` — `get_merkle_node(...) -> Option<[u8;32]>`.
- `crates/bluedb-evidence/src/model.rs` or `merkle.rs` — a `push`-with-emit that
  yields merged `(level, index, hash)` parents.
- `crates/bluedb-evidence/src/chain.rs` — append persists nodes in the batch;
  `inclusion`/`consistency` call the new `proof` module; drop `leaf_hashes` from
  the proof path.
- `crates/bluedb-evidence/src/proof.rs` (new) — `subtree_root` + storage-backed
  `inclusion`/`consistency`.
- `crates/bluedb-evidence/src/lib.rs` — `mod proof;`.
- Tests: extend `crates/bluedb-evidence/tests/merkle_chain.rs` (+ unit tests in
  `proof.rs`/`merkle.rs`).

No HTTP change — the `/evidence/{chain}/proof` and `/consistency` responses are
identical; only their cost drops.

---

## Component B — Parallel frontier expansion (`reachable`)

### Problem

`reachable` BFS awaits each dequeued node's `out_neighbors` (and `in_neighbors`)
scan **sequentially**, so wall-clock latency ∝ the number of nodes reached. On
object storage each scan carries real latency, so a wide graph is slow even when
total work is small.

### Approach

Expand the BFS **level by level**, scanning all of a level's nodes concurrently:

1. Seed `visited` and `frontier` with the (deduped) `from` set.
2. While `frontier` is non-empty: issue every frontier node's `out_neighbors`
   (and `in_neighbors` when `directed == false`) scan **concurrently** under a
   bounded concurrency cap; await them all.
3. Collect the returned neighbors; for each unseen one, insert into `visited`
   and into the next `frontier`.
4. Repeat. Return `sorted(visited)`.

Output is still sorted, and `visited` dedups regardless of completion order, so
the result is **identical and deterministic** — parallelism changes only timing.
Cycle termination is unchanged (the `visited` set).

### Concurrency primitive

Use **`tokio::task::JoinSet`** (already a dependency — no new crate). Each task
gets cheap clones of `Substrate` and `EvidenceKeyspace` (both are `Arc`-backed /
small `Vec`) plus the node id, and returns that node's neighbor list. A fixed
**cap of 16** in-flight scans bounds fan-out: drain the frontier into the
JoinSet up to the cap, and as each task completes, admit the next. (Verify
`Substrate: Clone`; it wraps `Writer(Arc<Db>)` / `Reader(Arc<…>)`, so it is — if
not, hold an `Arc<Substrate>`/`Arc<EvidenceKeyspace>` and clone the `Arc`.)

A small module-private helper `expand_level(substrate, ks, graph, nodes, floor,
directed, cap) -> Vec<(String, …)>` keeps `reachable` readable and unit-testable.

### Scope

**`reachable` only.** `widest_path` is a max-bottleneck Dijkstra whose priority
frontier is inherently sequential; parallelizing it is low-value and error-prone,
so it stays as-is (documented). No HTTP or signature change — `Graph::reachable`
keeps its signature; only its internals parallelize.

### Testing (B)

- The existing `tests/traverse.rs` reachable cases (seed-inclusion, floor prune,
  undirected in-edges, sink, multi-seed dedup, unknown seed) must pass unchanged
  — they are the correctness gate (output is identity-stable under parallelism).
- Add a **wide-graph** case: a star / fan-out (one root → many children → one
  sink) whose result is order-independent, exercising a multi-node level.
- Add a **diamond / cycle** case to confirm dedup across concurrent discovery of
  the same node (two parents discover the same child in one level → appears once).

### Files (B)

- `crates/bluedb-evidence/src/traverse.rs` — rewrite `reachable`'s loop to
  level-parallel via a `JoinSet` helper; `widest_path` untouched.
- Tests: extend `crates/bluedb-evidence/tests/traverse.rs`.

No keyspace, store, chain, or HTTP changes.

---

## Non-goals (unchanged from v1)

Digest signing, snapshot-consistent traversal, redaction of `type`/`at`, SQL
projection of the raw chain, payload canonicalization, explicit isolated nodes,
vector/HNSW, and the `widest_path` descending-scan micro-optimization remain out
of scope.

## Docs

After both land, update `docs/evidence/chains.md` (remove the "O(N) on demand"
limitation; note O(log N) proofs via a persisted node store) and
`docs/evidence/graph.md` (note parallel level expansion; `widest_path` still
sequential).
