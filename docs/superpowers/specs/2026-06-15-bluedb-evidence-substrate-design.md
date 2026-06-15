# bluedb evidence substrate — design

**Status:** approved design (spec), pre-implementation. Not a task plan.
**Date:** 2026-06-15.
**Audience:** a coding agent implementing this in the bluedb repo.
**Source requirements:** `fx-runtime/docs/plans/2026-06-15-bluedb-evidence-substrate-requirements.md` (R1–R8, M1–M4). This spec realizes those requirements **and deliberately extends them** in three places (cryptographic verification, erasure, and a native graph store) — all flagged below as intentional decisions, not over-reach.

---

## 1. Goal

Add a new native subsystem, the crate **`bluedb-evidence`**, that turns bluedb into a substrate for an external, event-sourced **evidence-graph** service. bluedb stores **opaque bytes** and provides **domain-agnostic primitives**; the consumer owns all meaning (§5 of the requirements). The substrate provides:

- an **append-only, densely-sequenced, byte-exact event log** ("evidence chain"), per `(tenant, chain)`;
- **cryptographic verifiability** of that chain (RFC 6962 Merkle tree: digest + inclusion + consistency proofs) — *irrepudiability*;
- a per-chain **`verified` mode** with **redaction** (and **hard-delete** on plain chains), so the same substrate serves both *irrepudiability* and *repudiability* / **GDPR/CCPA right-to-erasure**;
- a native **graph store + traversal** — adjacency-indexed weighted edges (HelixDB-style out/in edge indexes) with `reachable` and `widest_path`; an append can **carry the edges it justifies**, so one atomic write updates both the chain and the graph (no separate fold pass) — replacing the SQL engine's missing `WITH RECURSIVE`;
- **as-of scratch** namespacing for horizon/as-of replay;
- a native **axum HTTP surface** for all of the above, tenant-scoped.

It mirrors the **`bluedb-ledger`** subsystem's shape: native records via postcard + an atomic `WriteBatch` inside the serialized writer + `AppError` HTTP mapping.

## 2. Relationship to the requirements doc — honored vs. deliberately extended

**Honored as written:** R1 dense server-assigned seq; R2 ordered/horizon/paged reads; R3 byte-exact opaque payloads; R4 native traversal semantics (`reachable`/`widest_path`, weighted, deterministic — but over a native graph store, see extension 3); R5 as-of scratch (SHOULD); R6 determinism; R7 native HTTP; R8 tenancy.

**Deliberately extended (explicit user decisions, 2026-06-15):**

1. **Cryptographic verification in bluedb** — the requirements §5 said "no hashing in bluedb" and R3 put hashing entirely in the consumer. We **override** that: bluedb maintains an RFC 6962 Merkle tree over entries and serves digest/inclusion/consistency proofs. Byte-exactness is preserved — bluedb hashes a *length-framed copy* of the exact bytes and never alters what is stored or returned. The consumer's own content hashing (if any) still works, independently.
2. **Erasure / deletion** — the requirements §5 said "no event mutation or deletion APIs." We **override** that with two forms keyed to the chain's mode: **redaction-in-place** (any chain — erase the payload, keep the seq slot and, on verified chains, the leaf hash, so sequencing, replay, and proofs survive) and **hard-delete** (plain chains only — remove the slot entirely, gaps allowed, for genuine deniability-of-existence). A verified chain cannot hard-delete without breaking its own consistency proof; a plain chain can, because it makes no completeness promise. seq still *orders and addresses* in both modes — it only carries cryptographic weight (gap-free + Merkle) on a verified chain.
3. **Native graph store** — R4 described traversal as reading the *consumer's* projection rows via a full row-scan. We **override** that: `bluedb-evidence` owns an **adjacency-indexed edge store** (out-edge + in-edge indexes keyed by source/target node, HelixDB-style), so neighbor lookup is a bounded prefix range scan, not an O(E) table scan. The consumer writes edges through a native edges API (or folds the log into it); traversal walks only the subgraph it touches. This trades R4's "traverse any table" for real traversal performance (§8).

These three are the whole reason the primitive is more than "an append log": it is an *irrepudiable-by-default, erasure-capable* evidence chain with a *traversable* graph beside it.

## 3. Scope

- **M1 — Evidence chain core + Merkle** (R1, R2, R3, R6, R8 + HTTP slice): append (optionally carrying edges, §6.4/§8.3), reads, per-chain mode, redaction + hard-delete, the Merkle tree and proof endpoints.
- **M2 — Native graph store + traversal** (R4): adjacency-indexed edges (edges API), `reachable`, `widest_path`.
- **M3 — As-of scratch** (R5).
- **M4 — out of scope** (absorbing domain semantics). Do not start.

**Phase-0 prerequisite — ✅ LANDED on `dev`** (lakehouse-MT PR #4, merge `674ed2d`; this branch is rebased on it). The **shared tenant seam** is in the tree and evidence **consumes** it (does not build it): `X-Bluedb-Tenant` resolution (`crates/bluedb-server/src/lib.rs:309`), `connection_for_tenant(tenant)` (`crates/bluedb-sql/src/connection.rs:189`), `Keyspace::new(tenant)` (`crates/bluedb-sql/src/keyspace.rs:160`), `tenant:<name>` authz + `Authz::allows_tenant` (`crates/bluedb-server/src/authz.rs:66`); `crates/bluedb-server/tests/multitenant.rs` is a reference e2e. Default tenant `"_"`. **Coding is unblocked.**

## 4. Verified assumptions (confirmed in code, 2026-06-15)

All six load-bearing assumptions from the requirements §0.1 hold:

1. `/sql` is single-statement (`crates/bluedb-engine/src/rest_sql.rs:114` rejects `parsed.len() != 1`); transactions only via `/admin/sql` (`Superuser` + `BLUEDB_ENABLE_ADMIN_SQL`, `crates/bluedb-server/src/lib.rs:651`). → atomicity is a server-side batch.
2. No auto-id; `POST /tables/{table}` needs a client PK (`crates/bluedb-server/src/lib.rs:787`). → seq must be a dedicated native path.
3. No SlateDB fork/clone/snapshot — substrate is only Writer or Reader; isolation = key-prefix + overlay. → as-of = replay into a prefix.
4. No multi-tenant routing today; isolation is the SlateDB key prefix. → the tenant seam (Phase 0) supplies routing.
5. The generic `/tables` array insert *is* atomic — wraps `BEGIN;…;COMMIT;` into one `WriteBatch` (`crates/bluedb-engine/src/rest_sql.rs:86`, `crates/bluedb-sql/src/storage.rs:1062`). We do not use it (client-PK), but it confirms the primitive.
6. Serialized writer `WriteLease = Arc<Mutex<()>>` + read-your-own-writes overlay `TxnState.overlay: BTreeMap`, reads merge overlay over snapshot (`crates/bluedb-sql/src/storage.rs:471`). → the basis for contiguous server-assigned seqs inside one batch.

## 5. Architecture

New crate **`bluedb-evidence`** with focused modules:

| Module | Responsibility |
|---|---|
| `chain` | append (optionally carrying edge-deltas), reads, per-chain seq counter, idempotency, per-chain mode, redaction + hard-delete (R1, R2, R3, R6) |
| `merkle` | RFC 6962 leaf/node hashing, incremental frontier, digest, inclusion + consistency proofs |
| `graph` | native adjacency store (out/in edge indexes), edges API, `reachable`, `widest_path` (R4) |
| `scratch` | as-of name-prefix namespacing for chains/graphs/tables (R5) |
| `keyspace` | evidence key encoding (mirrors `bluedb-ledger/src/keyspace.rs`) |
| `store` | postcard encode/decode + point/range reads (mirrors `bluedb-ledger/src/store.rs`) |

HTTP handlers live in **`crates/bluedb-server/src/evidence_api.rs`** (mirrors `ledger_api.rs`); routes registered in `crates/bluedb-server/src/lib.rs` alongside `/ledger/*`.

### 5.1 Tenancy (R8) — rides the Phase-0 seam

Every evidence endpoint resolves the tenant from the `X-Bluedb-Tenant` header (absent → `"_"`), routes through `connection_for_tenant(tenant)`, and builds **all** evidence keys under `Keyspace::new(tenant)`. Consequences, for free:

- The per-chain seq counter is per-tenant → **two tenants appending the same `chain` name each start at seq 1** (R8 acceptance).
- `idem_key` is naturally scoped to `(tenant, chain)`.
- Endpoints additionally require the `tenant:<name>` pseudo-scope (superuser = any; open mode trusts the header).

## 6. Component — evidence chain (R1, R2, R3, R6)

### 6.1 Keyspace & tags

Reserve external tags after ledger (`0x10–0x15`) and CDC (`0x16`). All keys are tenant-namespaced by `Keyspace`; `chain` is length-prefixed *before* `seq` so each chain's entries form one contiguous range ordered by seq.

| Tag | Const | Key suffix (after tenant+tag) | Value |
|---|---|---|---|
| `0x17` | `TAG_EVIDENCE_ENTRY` | `len(chain)::u32-be ‖ chain_utf8 ‖ seq::i64-be` | postcard `EntryRecord` |
| `0x18` | `TAG_EVIDENCE_SEQ` | `len(chain)::u32-be ‖ chain_utf8` | `i64-be` last-assigned seq (= Merkle tree size) |
| `0x19` | `TAG_EVIDENCE_IDEM` | `len(chain)::u32-be ‖ chain_utf8 ‖ idem_key_utf8` | postcard `IdemRecord` |
| `0x1A` | `TAG_EVIDENCE_MERKLE` | `len(chain)::u32-be ‖ chain_utf8` | postcard `Frontier` |
| `0x1B` | `TAG_EVIDENCE_CHAIN` | `len(chain)::u32-be ‖ chain_utf8` | postcard `ChainMeta` |

`seq` is always ≥ 1, so plain `i64::to_be_bytes()` gives numeric == byte order → range scans need **no in-memory sort** (R2). All keys reuse `Keyspace::external_key(tag, suffix)` (asserts `tag >= TAG_EXTERNAL_BASE`).

### 6.2 Records

```rust
struct EntryRecord {
    r#type: String,        // opaque tag, stored verbatim
    payload: Vec<u8>,      // opaque bytes, stored verbatim (empty when redacted)
    at: String,            // optional, stored verbatim, never defaulted/generated (R6)
    edges: Vec<EdgeDelta>, // edge-deltas this event justified (empty if none); replayed to rebuild the graph
    leaf_hash: Option<[u8; 32]>, // Some on verified chains, retained across redaction
    redacted: bool,
}
struct EdgeDelta { graph: String, src: String, dst: String, weight: i64, etype: String, op: EdgeOp }
enum EdgeOp { Upsert { merge: Merge }, Delete }   // Merge = Set | Max
struct IdemRecord { base_seq: i64, seqs: Vec<i64>, fingerprint: [u8; 32] }
struct Frontier  { size: i64, peaks: Vec<[u8; 32]> }   // RFC 6962 perfect-subtree roots
struct ChainMeta { verified: bool }                     // mode, set at creation
```

### 6.3 Per-chain mode

A chain's mode is fixed at creation:

- `PUT /evidence/{chain}` with `{ "verified": bool }` (default `true`) creates the chain (writes `ChainMeta`). Idempotent: re-`PUT` with the same mode is a no-op; with a different mode → `409 E_CHAIN_MODE_CONFLICT`.
- Appending to a chain that does not exist **auto-creates** it with `verified: true`.
- `verified: true` → Merkle tree maintained, immutable (only redaction may alter an entry).
- `verified: false` → plain append log, **no** Merkle work; proof endpoints return `400 E_NOT_VERIFIED`.

### 6.4 Append path (R1, R6) — mirrors `ledger.create_transfers`

Endpoint accepts a single event object **or an array** (batch); single = array-of-one. For a K-entry batch on chain `C`:

1. Acquire the per-tenant write lease.
2. Load `ChainMeta` (auto-create `verified:true` if absent).
3. Read the seq counter `base` (0 if absent).
4. **Idempotency** (if `idem_key` present): read `IdemRecord`. Matching key + matching `fingerprint` → return the original `{base_seq, seqs}` with **no write, no flush**. Matching key + different fingerprint → `409 E_IDEM_CONFLICT`. (`fingerprint = SHA256` over the framed bytes of the whole batch — *byte-identity only, not canonicalization*.)
5. Assign `seqs = [base+1 .. base+K]`.
6. For each entry, build `EntryRecord` (including any `edges[]` it carries); on a verified chain compute `leaf_hash` over the framed entry **including its edge-deltas** (§7.1).
7. On a verified chain, read the `Frontier`, fold in the K leaf hashes (§7.2) → new `Frontier`.
8. Build one `WriteBatch`: put K `EntryRecord`s + updated counter (`base+K`) + `IdemRecord` (if any) + updated `Frontier` (verified) + `ChainMeta` (if newly created) + **the graph-store mutations for every entry's `edges[]`** (canonical + out + in per §8.3; an upsert that changes an existing edge's weight reads the canonical and deletes the stale out/in first — read-your-own-writes covers same-batch reads). Chain and graph advance **atomically — exactly-once**.
9. `write_with_options(batch, await_durable:false)` → `drop(lease)` → `flush()` (durability-before-ack + group-commit, exactly as `ledger.rs:223–231`).
10. Return `{ base_seq: base, seqs }`.

**Invariants:** seq is writer-side, stored-counter (not scan-MAX), read-modify-written inside the same serialized batch → **gap-free, monotonic, contiguous within a batch, never reused**; a discarded batch burns no seq; durability-before-ack. `base_seq` = max_seq **before** the batch; first new entry is `base_seq + 1`. Because each entry stores its `edges[]`, **replaying the chain reconstructs the graph** (and any as-of graph) — the graph is a reproducible projection, not separate truth.

### 6.5 Read path (R2)

- `read_range(chain, lo, hi)` — inclusive byte-range scan `[…‖lo_be, …‖hi_be]`, ascending, byte-exact. `hi < lo` → `[]` (never an error); `read_all` = `read_range(1, max_seq)`.
- `read_from(chain, after, limit?)` — `after` exclusive; server caps at max page size `P`; paging reproduces `read_range(1, max_seq)` with no gaps/overlaps/dupes.
- `head(chain)` — read the counter, O(1), 0 if empty (this is `max_seq`).
- Reads use a point-in-time snapshot → repeatable, identical on writer and replicas (R6).
- A **redacted** entry returns `{ seq, type, at, redacted: true }` with **no `payload_b64`** (and `leaf_hash` if verified).

### 6.6 Erasure — redaction & hard-delete

Two erasure forms, keyed to the chain's mode. Both are destructive and irreversible → require `schema:admin` (or `superuser`), not `data:write`.

**Redaction (any chain)** — `POST /evidence/{chain}/entries/{seq}/redact`:

1. Acquire lease; read the `EntryRecord` at `seq` (404 if absent).
2. Blank `payload` (→ empty), set `redacted = true`. **Keep** `seq`, `type`, `at`, `edges`, and `leaf_hash`.
3. One `WriteBatch` put; durable flush. The counter and `Frontier` are **unchanged** — on a verified chain the retained `leaf_hash` means the **digest and all proofs still verify**; only the payload preimage is gone (crypto-shred / redactable Merkle).
4. Idempotent (re-redact → `200`). Dense seq (R1) and replay structure (R6) are preserved — content erased, **existence still provable**. v1 redacts `payload` only (extending to `type`/`at` is a future option; proofs are unaffected because `leaf_hash` is retained).

**Hard-delete (plain chains only)** — `DELETE /evidence/{chain}/entries/{seq}`:

1. Verified chain → `409 E_VERIFIED_NO_DELETE` (dropping a slot would break the consistency proof).
2. Plain chain → remove the `EntryRecord` slot (and, per a request flag defaulting to **yes**, retract the entry's `edges[]` from the graph). The seq counter does **not** decrement → a **gap** appears; reads skip it, paging tolerates it.
3. This is genuine **deniability-of-existence** — the only mode that can claim an entry never was. Replaying the chain after a hard-delete no longer reproduces the deleted event's effects (by design — erasure on a repudiable log). seq here is purely an ordering/addressing key, not a completeness guarantee.

## 7. Component — Merkle verification (`merkle`)

RFC 6962 (the Certificate Transparency / Trillian model; what QLDB does internally). Compact O(log N) proofs and, critically, **consistency proofs** that prove the chain only ever grew.

### 7.1 Hashing

- **Leaf:** `leaf_hash = SHA256(0x00 ‖ frame(type, payload, at, edges))`, where `frame` length-delimits `type`, `payload`, `at`, and a canonical encoding of the entry's `edges[]` (each field length-prefixed; edges sorted by `(graph, src, dst, etype, op)`) — a copy of the **exact** bytes (no payload normalization), so the proof attests the whole entry **including the edge-deltas it justified**.
- **Node:** `SHA256(0x01 ‖ left ‖ right)`.
- **Empty tree digest:** `SHA256(<empty>)`.

### 7.2 Incremental frontier (cheap writes)

Persist the `Frontier` — the ≤ log N **perfect-subtree roots ("peaks")** covering the current size — under `TAG_EVIDENCE_MERKLE`, updated **in the same `WriteBatch`** as the entries (so the tree advances atomically with the log; crash-consistent). Appending one leaf = add a height-0 peak, then carry-merge equal-height peaks (binary-counter style), O(log N) amortized. The **digest at size N** = right-associative fold of the peaks (largest subtree first), O(log N).

### 7.3 Proofs

- `digest(chain)` → `{ size, root_hash }` — folds the frontier. O(log N). For an empty chain, `size: 0` and `root_hash = SHA256(<empty>)` (§7.1).
- `inclusion(chain, seq, size)` → `{ seq, size, audit_path: [hash…] }`. **`seq` is the entry's public 1-based sequence number** (what the caller knows); the internal RFC 6962 leaf index is `seq - 1`, and `size` is the tree size (= number of leaves = `head`). The handler maps `seq → seq-1` so callers never deal with 0-based indices.
- `consistency(chain, first, second)` → `{ first, second, proof: [hash…] }` — `first`/`second` are tree **sizes** (leaf counts), `first ≤ second`.

**Verification is client-side** (standard RFC 6962): the consumer verifies a proof against a digest it trusts.

**v1 simplification (flagged):** proofs are computed **on demand from the stored `leaf_hash`es** (read the needed leaf ranges, recompute the relevant subtree hashes) — O(N) per proof, streamed/memory-bounded, **no persisted internal-node store**. Future optimization: persist internal nodes for O(log N) proofs on very large chains.

### 7.4 Trust model (honest caveat — goes in the docs)

A digest from the same server is not proof against a **malicious operator** by itself. Full irrepudiability requires the consumer to **externally anchor digests** (periodically record a `{size, root_hash}` elsewhere) and/or bluedb to **sign** digests. v1 ships **unsigned digests + anchoring guidance**; **digest signing is an opt-in documented extension** (server keypair, signed `{size, root_hash, ts?}`), not in v1 unless requested.

## 8. Component — native graph store + traversal (R4)

`bluedb-evidence` **owns the edges** (it does not scan a consumer SQL table). A **graph** is a first-class object per `(tenant, graph)`: a set of directed, weighted, optionally-typed edges with **adjacency indexes** so neighbor lookup is a bounded prefix range scan — the HelixDB model (separate `out_edges`/`in_edges` keyed by node), realized on SlateDB's ordered keyspace.

### 8.1 Edge model

An edge is `(src: TEXT, dst: TEXT, weight: i64, type: TEXT = "")`. Edge **identity** is `(graph, src, dst, type)` — at most one weight per identity (a re-upsert updates it). Parallel edges between the same ordered pair are modeled with distinct `type` values; for an untyped graph, `(src, dst)` is unique and a re-upsert combines by the chosen `merge` mode (honoring R4's "parallel edges combine by max"). Node ids are `TEXT`; weights are 64-bit `INTEGER`.

### 8.2 Keyspace & tags

Continuing the evidence tags (chains used `0x17–0x1B`):

| Tag | Const | Key suffix (after tenant+tag) | Value |
|---|---|---|---|
| `0x1C` | `TAG_GRAPH_EDGE` | `len(graph)‖graph ‖ len(src)‖src ‖ len(dst)‖dst ‖ len(type)‖type` | `weight::i64-obe` (canonical edge, for upsert/delete) |
| `0x1D` | `TAG_GRAPH_OUT` | `len(graph)‖graph ‖ len(src)‖src ‖ weight::i64-obe ‖ len(dst)‖dst ‖ len(type)‖type` | empty |
| `0x1E` | `TAG_GRAPH_IN` | `len(graph)‖graph ‖ len(dst)‖dst ‖ weight::i64-obe ‖ len(src)‖src ‖ len(type)‖type` | empty |

`i64-obe` = order-preserving big-endian: `((w ^ i64::MIN) as u64).to_be_bytes()`, so signed weights sort numerically. Node ids are length-prefixed so a `src` prefix range is unambiguous (`"ab"` never matches `"abc"`). The `out` index orders a node's edges by **weight ascending** → `reachable(floor)` is a range scan from `weight = floor`, and `widest_path` best-first is the same index iterated in reverse.

### 8.3 Edges API (the consumer writes edges)

- **upsert** — `PUT /graph/{graph}/edges` `{ edges: [{src, dst, weight, type?}], merge?: "set"|"max" }` (default `"set"`). For each edge, in one `WriteBatch`: read the canonical `(graph,src,dst,type)`; if it exists, **delete its old `out`/`in` entries** (old weight) before writing the new canonical + `out` + `in`. `merge:"max"` keeps `max(old, new)`. Atomic, durable-before-ack.
- **delete** — `DELETE /graph/{graph}/edges` `{ edges: [{src, dst, type?}] }` — read canonical, delete canonical + `out` + `in`, one `WriteBatch`. (The graph is a rebuildable projection, so this is ordinary maintenance, not log erasure → `data:write`, not `schema:admin`.)

These two are the **bulk / rebuild** path (and scratch / as-of replay). The **hot path is append-with-edges** (§6.4): an event carries the `edges[]` it justifies and bluedb applies this exact canonical+out+in maintenance in the *same* `WriteBatch` as the chain entry — so the graph is a reproducible projection of the chain, and a standalone edge write is only for edges with no originating event (bulk import, rebuild, scratch).

Nodes are implicit (the union of `src`/`dst`); an explicit isolated-node set is a documented future option, not v1.

### 8.4 Traversal (read-only, one snapshot)

Both ops run under a single read snapshot for determinism, walking only the reached subgraph. `directed:true` follows `out` only; `directed:false` follows `out` and `in` (each stored edge usable both ways at its weight).

- **`reachable(from_set, floor, directed)` → sorted `[node_id]`:** BFS with a monotonic worklist. For each dequeued node `u`, prefix-scan `TAG_GRAPH_OUT` on `(graph, u)` from `weight = floor` upward (and `TAG_GRAPH_IN` if undirected); enqueue unseen `dst`. O(reached nodes + reached edges); terminates on cycles (visited set). Sorted output → order-independent.
- **`widest_path(from, to, directed)` → `{ connected, bottleneck? }`:** max-bottleneck Dijkstra. A max-heap keyed by best-known bottleneck-to-node; for each settled `u`, scan its `out` index **descending by weight**, relaxing `bottleneck(v) = max(bottleneck(v), min(bottleneck(u), w))`; settle `to` and stop. `connected:false` with no `bottleneck` when `to` is unreachable — no path is **not** an error. The maximin value is unique → deterministic.

### 8.5 Efficiency techniques

- **Adjacency by prefix scan** (never an edge-set scan) — the whole point of the native store.
- **Weight folded into the key** → floor pruning and best-first are range bounds, and the scan is **covering** (weight + neighbor read straight from the key, no row fetch).
- **Subgraph-only expansion** → memory and work ∝ nodes/edges reached, not graph size.
- **Parallel frontier expansion** → issue a BFS level's neighbor scans concurrently so latency rounds ≈ graph *diameter*, not |V| (important on object storage).
- **One snapshot + deterministic tie-break**; an optional per-snapshot adjacency cache amortizes repeated traversals.

*Attribution:* the out/in adjacency layout follows **HelixDB** (`out_edges`/`in_edges` keyed by `node_id + label`, big-endian for prefix scans); we additionally fold the **weight** into the key so floor/best-first are range operations.

## 9. Component — as-of scratch (R5) — ⚠ name-prefix model

The requirements imagined a `scratch_id`-scoped key-prefix passed to ops. With no per-request multi-tenant routing for arbitrary SQL writes, v1 implements scratch as a **reserved name prefix** applied to graphs and tables (this is the §7-q3 confirmation, signed off):

- `create_scratch()` → allocates a unique prefix (e.g. `_scratch_<n>` via a per-tenant counter) and returns it as `scratch_id`. Copies **zero** bytes of existing data (R5 invariant holds trivially).
- The consumer folds `read_range(1, N)` and writes its as-of projection into objects named with that prefix — a **scratch graph** (`PUT /graph/{prefix}_g/edges`, §8) and/or scratch SQL tables via the existing `/sql`+`/tables` surface.
- `graph.*` targets the prefixed graph name; the prefix *is* the scoping (no extra param).
- `drop_scratch(scratch_id)` → drops every graph and table whose name starts with the prefix (range-deletes the graph's `0x1C/0x1D/0x1E` keys and each `DROP TABLE`'s keys, batched).

Satisfies R5's intent (isolated as-of replay + cheap teardown, live chain untouched) while touching only the new endpoints. `drop_scratch` is O(rows) (scan-prefix + per-key delete batched into one `WriteBatch`; there is no native range-delete in SlateDB).

## 10. HTTP surface (R7)

Native axum routes in `evidence_api.rs`, alongside `/ledger/*`. **Wire format: base64-in-JSON `payload_b64`** (v1 default), decoded straight to `BYTEA` in the native handler; byte-exactness asserted via SHA-256 in == out **through HTTP**. Logical → endpoint mapping (recorded at the top of the test module per §0.2 of the requirements):

| Logical (requirements) | Endpoint | Scope |
|---|---|---|
| create chain (mode) | `PUT /evidence/{chain}` `{verified?}` | `data:write` + `tenant:` |
| `append` (+edges) | `POST /evidence/{chain}/entries` `{events:[{type,payload_b64,at?,edges?:[{graph,src,dst,weight,type?,op?}]}], idem_key?}` → `{base_seq, seqs}` | `data:write` + `tenant:` |
| `max_seq` | `GET /evidence/{chain}/head` → `{seq}` | `data:read` + `tenant:` |
| `read_range` | `GET /evidence/{chain}/entries?from&to` | `data:read` + `tenant:` |
| `read_from` | `GET /evidence/{chain}/entries?after&limit` | `data:read` + `tenant:` |
| (redact) | `POST /evidence/{chain}/entries/{seq}/redact` (any chain) | `schema:admin` + `tenant:` |
| (hard-delete) | `DELETE /evidence/{chain}/entries/{seq}` (plain only; verified → `409`) | `schema:admin` + `tenant:` |
| (digest) | `GET /evidence/{chain}/digest` → `{size, root_hash}` | `data:read` + `tenant:` |
| (inclusion proof) | `GET /evidence/{chain}/proof?seq&size` | `data:read` + `tenant:` |
| (consistency proof) | `GET /evidence/{chain}/consistency?from&to` | `data:read` + `tenant:` |
| (upsert edges) | `PUT /graph/{graph}/edges` `{edges:[{src,dst,weight,type?}], merge?}` | `data:write` + `tenant:` |
| (delete edges) | `DELETE /graph/{graph}/edges` `{edges:[{src,dst,type?}]}` | `data:write` + `tenant:` |
| `graph.reachable` | `POST /graph/{graph}/reachable` `{from:[…], floor, directed}` → `{nodes:[…]}` | `data:read` + `tenant:` |
| `graph.widest_path` | `POST /graph/{graph}/widest-path` `{from, to, directed}` → `{connected, bottleneck?}` | `data:read` + `tenant:` |
| `create_scratch` | `POST /scratch` → `{scratch_id}` | `data:write` + `tenant:` |
| `drop_scratch` | `DELETE /scratch/{scratch_id}` | `data:write` + `tenant:` |

**Errors** via the existing `AppError` (`crates/bluedb-server/src/lib.rs:986`). Named errors: `E_IDEM_CONFLICT` → 409; `E_CHAIN_MODE_CONFLICT` → 409; `E_NOT_VERIFIED` (proof on a plain chain) → 400; `E_VERIFIED_NO_DELETE` (hard-delete on a verified chain) → 409; entry/chain not found → 404; over-limit payload → 413 (reuse the server's request-body limit; add an explicit `BYTEA` cap only if none exists).

## 11. Deliberately NOT built (YAGNI / out of scope)

- **No SQL projection of the raw evidence chain.** Unlike the ledger (whose records *are* the queryable surface), here the queryable surface is the *consumer's* projection tables, which it writes itself. The native read + proof endpoints are the contract. (If ad-hoc SQL over entries is later wanted, it can be added without changing storage.)
- **No payload canonicalization.** bluedb frames the exact bytes for hashing but never normalizes them (R3).
- **No digest signing in v1** (documented extension; see §7.4).
- **No persisted Merkle internal-node store in v1** (proofs are O(N) on demand; §7.3).
- **No vector / HNSW index.** HelixDB is graph+vector; here R4 is a pure weighted graph. Vector search is out of scope (a possible future module, not now).
- **No explicit node set / node properties** in the graph store v1 (nodes are the union of edge endpoints; §8.3).
- **No M4** (absorbing domain semantics).

## 12. Testing & acceptance

In the new crate (unit/integration) + HTTP e2e in `bluedb-server`. Each requirement's acceptance becomes a test; the test module begins with the logical→endpoint mapping table (§10).

- **Seq density & contiguity:** N single appends → seqs `1..N`, no gaps; a K-batch → K contiguous seqs; rollback leaves `head` unchanged (no burned seqs).
- **Concurrency (mechanical):** ≥8 concurrent clients × ≥100 appends to one `(tenant, chain)`; returned-seq multiset `== {1..total}` exactly; ≥5 iterations.
- **Idempotency:** retry with used `idem_key` + identical bytes → same `{base_seq, seqs}`, chain does not grow; differing bytes → `E_IDEM_CONFLICT`.
- **Ordering across a digit boundary:** append `1..1000`; read order is numeric (`9` before `10`, `999` before `1000`), not lexical.
- **Paging:** full-chain paging via `read_from` reproduces `read_range(1, head)` exactly.
- **Byte fidelity (through HTTP):** adversarial payloads — unsorted-key JSON, mixed whitespace, multi-byte unicode, a non-UTF-8 byte sequence — round-trip with identical bytes and identical SHA-256; over-limit payload rejected.
- **Merkle:** golden vectors for leaf/node/root hashes vs. an RFC 6962 reference; inclusion + consistency proofs verify with an independent verifier; digest matches a from-scratch recomputation; **redaction keeps proofs valid** (redact an entry → its inclusion proof and the chain's consistency proof still verify against the unchanged digest).
- **Mode:** plain chain does no Merkle work and returns `E_NOT_VERIFIED` on proof endpoints; re-`PUT` with a conflicting mode → `E_CHAIN_MODE_CONFLICT`.
- **Redaction:** redacted entry returns no payload, keeps seq/type/at/edges; `head` and dense seq unchanged; replay structure still gap-free; on a verified chain its inclusion + the chain's consistency proof still verify against the unchanged digest.
- **Hard-delete:** on a plain chain `DELETE …/entries/{seq}` removes the slot → reads show a **gap** there, `head` unchanged; default flag also retracts the entry's edges from the graph; on a verified chain it returns `E_VERIFIED_NO_DELETE` and the entry remains.
- **Append-with-edges (atomicity + reproducibility):** an append carrying `edges[]` updates chain + graph in **one** batch — kill-after-ack shows both or neither; replaying `read_range(1, head)` and applying each entry's `edges[]` reconstructs the live graph exactly (graph is a reproducible projection); on a verified chain the edge-deltas are covered by the inclusion proof.
- **Graph store:** edge upsert writes canonical + out + in; re-upsert updates the weight (and `merge:max` keeps the larger), deleting the stale-weight out/in entries first; delete removes all three; out/in prefix scans return a node's neighbors ordered by weight **without scanning the edge set** (assert via a scan-count or that an unrelated node's edges are never read).
- **Graph traversal:** golden vectors (the requirements' 4-node cyclic graph `A→B(5) B→D(3) A→C(2) D→A(4)`: `reachable(from={A}, floor=3) → [A,B,D]`; `widest_path(A,D) → {connected:true, bottleneck:3}`; `widest_path(A,C)`/unreachable → `{connected:false}`); then property tests vs. reference BFS-at-floor and reference maximin on random graphs (cycles, parallel edges via distinct types, ties), deterministic across runs and edge-insertion orders.
- **Scratch:** projections folded into a scratch from `read_range(1, N)` match the live projections truncated at `N`; create+drop leaves `head` and live state unchanged.
- **Determinism (R6):** two in-process read/fold passes over the same chain are byte-identical (and across a replica + writer-restart if fixtures exist).
- **Tenancy (R8):** two tenants, same `chain` name → independent sequences from 1; cross-tenant reads/traversal impossible.
- **Durability:** kill-after-ack → acked entry present at its seq, `head` unchanged, replay byte-identical. If no crash/restart fixture exists, flag and treat as an integration gate (requirements §7-q6); copy a ledger durability test if one exists.

## 13. Open risks / flags

- ~~**Tenant seam dependency**~~ — **RESOLVED:** the seam landed on `dev` (PR #4, merge `674ed2d`) and this branch is rebased on it (§3). Coding unblocked.
- **Proof cost in v1:** O(N) per proof (no node store). Fine for moderate chains; flag if a consumer needs proofs over very large chains → add the node store.
- **Operator trust:** unsigned digests need external anchoring for true irrepudiability (§7.4); documented, signing deferred.
- **Crash-durability fixture:** may not exist in the harness → durability acceptance becomes an integration gate (§12).
- **Body-size limit:** confirm the server's existing request-body limit and its over-limit error; add an explicit cap only if absent.
- **Graph adjacency consistency:** every edge upsert/delete must keep the canonical edge and its `out`/`in` entries in lock-step within one `WriteBatch` — a re-upsert must delete the old-weight `out`/`in` before writing the new, or stale adjacency leaks. Cover in tests (§12 graph store).
- **Edges-through-us coupling:** edges are written through bluedb — normally **atomically with the justifying append** (§6.4), or via the standalone edges API for bulk/rebuild — not read from an arbitrary SQL table (deliberate departure from R4, §2 extension 3). A bulk import from an existing edge table is a deferred convenience.
- **Hard-delete vs. replay:** hard-delete on a plain chain intentionally changes what replay reconstructs (it's erasure). The consumer must understand a plain chain is not a faithful-forever record — that's the repudiability trade. Verified chains never hard-delete, so their replay stays faithful (modulo redacted payloads).
