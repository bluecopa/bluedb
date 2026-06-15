# bluedb evidence substrate — design

**Status:** approved design (spec), pre-implementation. Not a task plan.
**Date:** 2026-06-15.
**Audience:** a coding agent implementing this in the bluedb repo.
**Source requirements:** `fx-runtime/docs/plans/2026-06-15-bluedb-evidence-substrate-requirements.md` (R1–R8, M1–M4). This spec realizes those requirements **and deliberately extends them** in two places (cryptographic verification + erasure) — both flagged below as intentional decisions, not over-reach.

---

## 1. Goal

Add a new native subsystem, the crate **`bluedb-evidence`**, that turns bluedb into a substrate for an external, event-sourced **evidence-graph** service. bluedb stores **opaque bytes** and provides **domain-agnostic primitives**; the consumer owns all meaning (§5 of the requirements). The substrate provides:

- an **append-only, densely-sequenced, byte-exact event log** ("evidence chain"), per `(tenant, chain)`;
- **cryptographic verifiability** of that chain (RFC 6962 Merkle tree: digest + inclusion + consistency proofs) — *irrepudiability*;
- a per-chain **`verified` mode** and **redaction-in-place**, so the same substrate also serves *repudiability* and **GDPR/CCPA right-to-erasure**;
- a native **graph traversal** primitive (`reachable`, `widest_path`) over the consumer's projection tables, replacing the SQL engine's missing `WITH RECURSIVE`;
- **as-of scratch** namespacing for horizon/as-of replay;
- a native **axum HTTP surface** for all of the above, tenant-scoped.

It mirrors the **`bluedb-ledger`** subsystem's shape: native records via postcard + an atomic `WriteBatch` inside the serialized writer + `AppError` HTTP mapping.

## 2. Relationship to the requirements doc — honored vs. deliberately extended

**Honored as written:** R1 dense server-assigned seq; R2 ordered/horizon/paged reads; R3 byte-exact opaque payloads; R4 native traversal; R5 as-of scratch (SHOULD); R6 determinism; R7 native HTTP; R8 tenancy.

**Deliberately extended (explicit user decisions, 2026-06-15):**

1. **Cryptographic verification in bluedb** — the requirements §5 said "no hashing in bluedb" and R3 put hashing entirely in the consumer. We **override** that: bluedb maintains an RFC 6962 Merkle tree over entries and serves digest/inclusion/consistency proofs. Byte-exactness is preserved — bluedb hashes a *length-framed copy* of the exact bytes and never alters what is stored or returned. The consumer's own content hashing (if any) still works, independently.
2. **Erasure / deletion** — the requirements §5 said "no event mutation or deletion APIs." We **override** that with **redaction-in-place** (below): a payload can be erased while the seq slot and (on verified chains) the entry's leaf hash are retained, so dense sequencing, replay determinism, and proofs all survive.

These two are the whole reason the primitive is more than "an append log": it is an *irrepudiable-by-default, erasure-capable* evidence chain.

## 3. Scope

- **M1 — Evidence chain core + Merkle** (R1, R2, R3, R6, R8 + HTTP slice): append, reads, per-chain mode, redaction, the Merkle tree and proof endpoints.
- **M2 — Graph traversal** (R4): `reachable`, `widest_path`.
- **M3 — As-of scratch** (R5).
- **M4 — out of scope** (absorbing domain semantics). Do not start.

**Phase-0 prerequisite (not built here):** the **shared tenant seam** — `X-Bluedb-Tenant` header → `connection_for_tenant(tenant)`, `Keyspace::new(tenant)` parameterization, and a `tenant:<name>` authz pseudo-scope (default tenant `"_"`). This is built by the separate **lakehouse multi-tenancy** workstream. **Evidence implementation is gated on that seam landing on `dev`** — the evidence branch consumes it and does not build it.

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
| `chain` | append, reads, per-chain seq counter, idempotency, per-chain mode, redaction (R1, R2, R3, R6) |
| `merkle` | RFC 6962 leaf/node hashing, incremental frontier, digest, inclusion + consistency proofs |
| `graph` | `reachable`, `widest_path` over projection tables (R4) |
| `scratch` | as-of table-name-prefix namespacing (R5) |
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
    leaf_hash: Option<[u8; 32]>, // Some on verified chains, retained across redaction
    redacted: bool,
}
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
6. For each entry, build `EntryRecord`; on a verified chain compute `leaf_hash` (§7.1).
7. On a verified chain, read the `Frontier`, fold in the K leaf hashes (§7.2) → new `Frontier`.
8. Build one `WriteBatch`: put K `EntryRecord`s + updated counter (`base+K`) + `IdemRecord` (if any) + updated `Frontier` (verified) + `ChainMeta` (if newly created).
9. `write_with_options(batch, await_durable:false)` → `drop(lease)` → `flush()` (durability-before-ack + group-commit, exactly as `ledger.rs:223–231`).
10. Return `{ base_seq: base, seqs }`.

**Invariants:** seq is writer-side, stored-counter (not scan-MAX), read-modify-written inside the same serialized batch → **gap-free, monotonic, contiguous within a batch, never reused**; a discarded batch burns no seq; durability-before-ack. `base_seq` = max_seq **before** the batch; first new entry is `base_seq + 1`.

### 6.5 Read path (R2)

- `read_range(chain, lo, hi)` — inclusive byte-range scan `[…‖lo_be, …‖hi_be]`, ascending, byte-exact. `hi < lo` → `[]` (never an error); `read_all` = `read_range(1, max_seq)`.
- `read_from(chain, after, limit?)` — `after` exclusive; server caps at max page size `P`; paging reproduces `read_range(1, max_seq)` with no gaps/overlaps/dupes.
- `head(chain)` — read the counter, O(1), 0 if empty (this is `max_seq`).
- Reads use a point-in-time snapshot → repeatable, identical on writer and replicas (R6).
- A **redacted** entry returns `{ seq, type, at, redacted: true }` with **no `payload_b64`** (and `leaf_hash` if verified).

### 6.6 Redaction-in-place (erasure)

`POST /evidence/{chain}/entries/{seq}/redact`:

1. Acquire lease; read the `EntryRecord` at `seq` (404 if absent).
2. Blank `payload` (→ empty), set `redacted = true`. **Keep** `seq`, `type`, `at`, and `leaf_hash`.
3. One `WriteBatch` put; durable flush. The counter and `Frontier` are **unchanged** — on a verified chain the retained `leaf_hash` means the **digest and all proofs still verify**; the payload preimage is simply gone (crypto-shred / redactable Merkle).
4. Idempotent: redacting an already-redacted entry returns `200`.

Dense seq (R1) and replay (R6) are preserved because the slot remains; only the content is erased. **Auth:** redaction is destructive and irreversible → requires `schema:admin` (or `superuser`), not `data:write`. v1 redacts the `payload` only; extending to `type`/`at` is a documented future option (proofs are unaffected either way because `leaf_hash` is retained).

## 7. Component — Merkle verification (`merkle`)

RFC 6962 (the Certificate Transparency / Trillian model; what QLDB does internally). Compact O(log N) proofs and, critically, **consistency proofs** that prove the chain only ever grew.

### 7.1 Hashing

- **Leaf:** `leaf_hash = SHA256(0x00 ‖ frame(type, payload, at))`, where `frame = varint(len(type)) ‖ type ‖ varint(len(payload)) ‖ payload ‖ varint(len(at)) ‖ at` — a length-delimited copy of the **exact** bytes (no normalization; covers all three fields so the proof attests the whole entry).
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

## 8. Component — graph traversal (R4)

A read-only compute primitive over the consumer's own projection tables. It **bypasses the SQL query-guardrail** (it needs a full edge scan, which the guardrail rejects — this is exactly why R4 is native, not SQL). All edge/node rows are read through the internal row-scan (`scan_data`/`collect_rows`, `crates/bluedb-sql/src/storage.rs:520`) under **one snapshot** for determinism, via the tenant's connection.

Inputs: `{ edges_table, src_col, dst_col, weight_col, nodes_table?, node_col? }`. Node ids are `TEXT`; weights are 64-bit `INTEGER`. If `nodes_table` is omitted, the node set is the union of `src`/`dst` values.

- **`reachable(from_set, floor, directed)` → sorted `[node_id]`:** label propagation with a monotonic worklist; only edges with `weight ≥ floor`; for parallel edges, any qualifying edge suffices; undirected = each stored edge usable both ways. Terminates on cyclic graphs (visited set).
- **`widest_path(from, to, directed)` → `{ connected, bottleneck? }`:** maximin via a max-bottleneck Dijkstra; parallel edges combine by **max** weight; `bottleneck` absent when `connected:false`; **no path is not an error**. The maximin value is unique → order-independent, deterministic.

v1 holds the working graph in memory (O(edges + nodes)); acceptable because traversal is tenant/scratch-scoped. Noted as a future bound.

## 9. Component — as-of scratch (R5) — ⚠ table-name-prefix model

The requirements imagined a `scratch_id`-scoped key-prefix passed to ops. With no per-request multi-tenant routing for arbitrary SQL writes, v1 implements scratch as a **reserved table-name prefix** (this is the §7-q3 confirmation, signed off):

- `create_scratch()` → allocates a unique prefix (e.g. `_scratch_<n>` via a per-tenant counter) and returns it as `scratch_id`. Copies **zero** bytes of existing data (R5 invariant holds trivially).
- The consumer folds `read_range(1, N)` and writes its as-of projection rows into tables named with that prefix, using the **existing `/sql` and `/tables` surface unchanged**.
- `graph.*` is called with those prefixed table names (the prefix *is* the scoping — no extra param).
- `drop_scratch(scratch_id)` → drops every table whose name starts with the prefix (each `DROP TABLE` range-deletes its keys).

Satisfies R5's intent (isolated as-of replay + cheap teardown, live chain untouched) while touching only the new endpoints. `drop_scratch` is O(rows) (scan-prefix + per-key delete batched into one `WriteBatch`; there is no native range-delete in SlateDB).

## 10. HTTP surface (R7)

Native axum routes in `evidence_api.rs`, alongside `/ledger/*`. **Wire format: base64-in-JSON `payload_b64`** (v1 default), decoded straight to `BYTEA` in the native handler; byte-exactness asserted via SHA-256 in == out **through HTTP**. Logical → endpoint mapping (recorded at the top of the test module per §0.2 of the requirements):

| Logical (requirements) | Endpoint | Scope |
|---|---|---|
| create chain (mode) | `PUT /evidence/{chain}` `{verified?}` | `data:write` + `tenant:` |
| `append` | `POST /evidence/{chain}/entries` `{events:[{type,payload_b64,at?}], idem_key?}` → `{base_seq, seqs}` | `data:write` + `tenant:` |
| `max_seq` | `GET /evidence/{chain}/head` → `{seq}` | `data:read` + `tenant:` |
| `read_range` | `GET /evidence/{chain}/entries?from&to` | `data:read` + `tenant:` |
| `read_from` | `GET /evidence/{chain}/entries?after&limit` | `data:read` + `tenant:` |
| (erasure) | `POST /evidence/{chain}/entries/{seq}/redact` | `schema:admin` + `tenant:` |
| (digest) | `GET /evidence/{chain}/digest` → `{size, root_hash}` | `data:read` + `tenant:` |
| (inclusion proof) | `GET /evidence/{chain}/proof?seq&size` | `data:read` + `tenant:` |
| (consistency proof) | `GET /evidence/{chain}/consistency?from&to` | `data:read` + `tenant:` |
| `graph.reachable` | `POST /graph/reachable` → `{nodes:[…]}` | `data:read` + `tenant:` |
| `graph.widest_path` | `POST /graph/widest-path` → `{connected, bottleneck?}` | `data:read` + `tenant:` |
| `create_scratch` | `POST /scratch` → `{scratch_id}` | `data:write` + `tenant:` |
| `drop_scratch` | `DELETE /scratch/{scratch_id}` | `data:write` + `tenant:` |

**Errors** via the existing `AppError` (`crates/bluedb-server/src/lib.rs:986`). Named errors: `E_IDEM_CONFLICT` → 409; `E_CHAIN_MODE_CONFLICT` → 409; `E_NOT_VERIFIED` (proof on a plain chain) → 400; entry/chain not found → 404; over-limit payload → 413 (reuse the server's request-body limit; add an explicit `BYTEA` cap only if none exists).

## 11. Deliberately NOT built (YAGNI / out of scope)

- **No SQL projection of the raw evidence chain.** Unlike the ledger (whose records *are* the queryable surface), here the queryable surface is the *consumer's* projection tables, which it writes itself. The native read + proof endpoints are the contract. (If ad-hoc SQL over entries is later wanted, it can be added without changing storage.)
- **No payload canonicalization.** bluedb frames the exact bytes for hashing but never normalizes them (R3).
- **No digest signing in v1** (documented extension; see §7.4).
- **No persisted Merkle internal-node store in v1** (proofs are O(N) on demand; §7.3).
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
- **Redaction:** redacted entry returns no payload, keeps seq/type/at; `head` and dense seq unchanged; replay over the chain is still gap-free.
- **Graph:** golden vectors (the requirements' 4-node cyclic graph: `reachable(from={A}, floor=3) → [A,B,D]`; `widest_path(A,D) → {connected:true, bottleneck:3}`; `widest_path(A,C)`/unreachable → `{connected:false}`); then property tests vs. reference BFS-at-floor and reference maximin on random graphs (cycles, multi-edges, ties), deterministic across runs and insertion orders.
- **Scratch:** projections folded into a scratch from `read_range(1, N)` match the live projections truncated at `N`; create+drop leaves `head` and live state unchanged.
- **Determinism (R6):** two in-process read/fold passes over the same chain are byte-identical (and across a replica + writer-restart if fixtures exist).
- **Tenancy (R8):** two tenants, same `chain` name → independent sequences from 1; cross-tenant reads/traversal impossible.
- **Durability:** kill-after-ack → acked entry present at its seq, `head` unchanged, replay byte-identical. If no crash/restart fixture exists, flag and treat as an integration gate (requirements §7-q6); copy a ledger durability test if one exists.

## 13. Open risks / flags

- **Tenant seam dependency:** evidence coding cannot start until the shared seam lands on `dev` (§3).
- **Proof cost in v1:** O(N) per proof (no node store). Fine for moderate chains; flag if a consumer needs proofs over very large chains → add the node store.
- **Operator trust:** unsigned digests need external anchoring for true irrepudiability (§7.4); documented, signing deferred.
- **Crash-durability fixture:** may not exist in the harness → durability acceptance becomes an integration gate (§12).
- **Body-size limit:** confirm the server's existing request-body limit and its over-limit error; add an explicit cap only if absent.
