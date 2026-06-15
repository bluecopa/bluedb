# bluedb-evidence Plan 1 — Chain core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `bluedb-evidence` crate with a working, multi-tenant, append-only **evidence chain** — server-assigned dense seq, atomic batches, idempotency, per-chain `verified` mode, ordered/paged reads — exposed over HTTP. (Merkle, graph, scratch are Plans 2–5.)

**Architecture:** Mirror `bluedb-ledger` exactly: native postcard records keyed under a tenant-namespaced `Keyspace`, written as one `slatedb::WriteBatch` inside the serialized writer (`write_with_options(await_durable:false)` → `drop(lease)` → `flush()` for durability-before-ack + group-commit). Tenancy rides the landed seam: handlers resolve `X-Bluedb-Tenant` via `AppState::tenant`, build `Evidence::new(&database, &tenant)`.

**Tech Stack:** Rust, `bluedb-sql` (`Keyspace`, `Database`, `Substrate`, `WriteLease`), `bluedb-storage`, `slatedb` (`WriteBatch`, `WriteOptions`), `postcard` 1.1, `serde`, `thiserror`, `axum`.

**Spec:** `docs/superpowers/specs/2026-06-15-bluedb-evidence-substrate-design.md` (§5, §6, §10). This plan covers M1 minus Merkle/erasure (Plan 2).

**Reference files to copy patterns from (read these first):**
- `crates/bluedb-ledger/Cargo.toml`, `src/keyspace.rs`, `src/store.rs`, `src/ledger.rs` (append path lines 290–459), `src/lib.rs`
- `crates/bluedb-sql/src/keyspace.rs` (`Keyspace::new`, `external_key`, `external_prefix`, `prefix_upper_bound`, `TAG_EXTERNAL_BASE`)
- `crates/bluedb-server/src/ledger_api.rs`, `src/lib.rs` (`AppState::tenant` ~line 312, `ledger()` ~548, `require_active` ~559, router ~575, `AppError` ~1036), `src/authz.rs`
- `crates/bluedb-server/tests/multitenant.rs` (e2e tenancy test harness to copy)

---

### Task 1: Scaffold the `bluedb-evidence` crate

**Files:**
- Create: `crates/bluedb-evidence/Cargo.toml`
- Create: `crates/bluedb-evidence/src/lib.rs`
- Create: `crates/bluedb-evidence/src/model.rs`, `error.rs`, `keyspace.rs`, `store.rs`, `chain.rs` (empty stubs)
- Modify: workspace root `Cargo.toml` (members already use `crates/*` glob — verify; no edit needed if glob)

- [ ] **Step 1: Create `crates/bluedb-evidence/Cargo.toml`** (copy `bluedb-ledger/Cargo.toml`)

```toml
[package]
name = "bluedb-evidence"
version.workspace = true
edition.workspace = true

[dependencies]
bluedb-storage = { workspace = true }
bluedb-sql     = { workspace = true }
slatedb        = { workspace = true }
serde          = { workspace = true }
postcard       = { workspace = true }
thiserror      = { workspace = true }
anyhow         = { workspace = true }
tokio          = { workspace = true }
sha2           = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

(If `sha2` is not yet a workspace dep, add `sha2 = "0.10"` to the root `[workspace.dependencies]`. It is needed for the idempotency fingerprint here and the Merkle tree in Plan 2.)

- [ ] **Step 2: Create `src/lib.rs`**

```rust
//! bluedb-evidence: append-only, verifiable evidence chains + a native graph
//! store, for an external event-sourced evidence-graph service. Mirrors the
//! bluedb-ledger subsystem shape (native postcard records + atomic WriteBatch
//! inside the serialized writer).

mod error;
mod keyspace;
mod model;
mod store;
pub mod chain;

pub use chain::{Appended, EntryInput, Evidence};
pub use error::EvidenceError;
pub use model::{ChainMeta, EdgeDelta, EdgeOp, EntryRecord, IdemRecord, Merge};
```

- [ ] **Step 3: Create empty module stubs** so the crate compiles.

`src/model.rs`, `src/error.rs`, `src/keyspace.rs`, `src/store.rs`, `src/chain.rs` — each just `// filled in later task`. (Add the real `pub use` targets in later tasks; for now make `lib.rs`'s `pub use` lines compile by stubbing minimal items, OR comment them out and uncomment per task. Recommended: comment the `pub use` lines, uncomment as each type lands.)

- [ ] **Step 4: Verify the crate builds**

Run: `cargo build -p bluedb-evidence 2>&1`
Expected: compiles (empty crate).

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-evidence Cargo.toml
git commit -m "feat(evidence): scaffold bluedb-evidence crate"
```

---

### Task 2: Model types

**Files:**
- Modify: `crates/bluedb-evidence/src/model.rs`
- Test: inline `#[cfg(test)]` in `model.rs`

- [ ] **Step 1: Write the failing test** (serde + postcard round-trip)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_record_roundtrips_through_postcard() {
        let rec = EntryRecord {
            etype: "attestation".into(),
            payload: vec![0u8, 159, 146, 150], // includes non-UTF-8 bytes
            at: "2026-06-15T10:00:00Z".into(),
            edges: vec![EdgeDelta {
                graph: "lineage".into(),
                src: "D1".into(),
                dst: "D2".into(),
                weight: 5,
                etype: String::new(),
                op: EdgeOp::Upsert { merge: Merge::Set },
            }],
            leaf_hash: None,
            redacted: false,
        };
        let bytes = postcard::to_allocvec(&rec).unwrap();
        let back: EntryRecord = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(rec, back);
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p bluedb-evidence model:: 2>&1`
Expected: FAIL (types not defined).

- [ ] **Step 3: Implement the types** in `src/model.rs`

```rust
use serde::{Deserialize, Serialize};

/// One event in an evidence chain. `etype`/`payload`/`at` are opaque to bluedb.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntryRecord {
    /// Opaque type tag, stored verbatim.
    pub etype: String,
    /// Opaque bytes, stored verbatim. Empty when redacted (Plan 2).
    pub payload: Vec<u8>,
    /// Optional informational timestamp; stored verbatim, never generated (R6).
    pub at: String,
    /// Edge-deltas this event justified (applied to the graph store in Plan 3).
    pub edges: Vec<EdgeDelta>,
    /// Merkle leaf hash; `Some` on verified chains (Plan 2). Retained across redaction.
    pub leaf_hash: Option<[u8; 32]>,
    /// True once the payload has been redacted (Plan 2).
    pub redacted: bool,
}

/// An edge mutation an event justifies. Applied to the adjacency store in Plan 3;
/// stored on the entry now so the chain is replayable into the graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeDelta {
    pub graph: String,
    pub src: String,
    pub dst: String,
    pub weight: i64,
    pub etype: String,
    pub op: EdgeOp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EdgeOp {
    Upsert { merge: Merge },
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Merge {
    Set,
    Max,
}

/// Idempotency dedup record, scoped to `(tenant, chain, idem_key)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdemRecord {
    pub base_seq: i64,
    pub seqs: Vec<i64>,
    pub fingerprint: [u8; 32],
}

/// Per-chain metadata, fixed at creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainMeta {
    pub verified: bool,
}
```

Uncomment the matching `pub use model::{...}` line in `lib.rs`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p bluedb-evidence model:: 2>&1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-evidence/src/model.rs crates/bluedb-evidence/src/lib.rs
git commit -m "feat(evidence): chain record model types"
```

---

### Task 3: Error type

**Files:**
- Modify: `crates/bluedb-evidence/src/error.rs`

- [ ] **Step 1: Implement `EvidenceError`** (no test needed; a plain enum)

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EvidenceError {
    #[error("idempotency key reused with a different payload")]
    IdemConflict,
    #[error("chain '{0}' already exists with a different mode")]
    ChainModeConflict(String),
    #[error("entry {seq} not found in chain '{chain}'")]
    EntryNotFound { chain: String, seq: i64 },
    #[error("node is a read-only replica (no writer)")]
    NotWriter,
    #[error(transparent)]
    Storage(#[from] anyhow::Error),
}
```

(`Substrate::require_writer` / `get` / `scan_range` return `anyhow::Result`, so `#[from] anyhow::Error` covers them via `?`. Map `require_writer`'s error to `NotWriter` explicitly in `chain.rs`.)

Uncomment `pub use error::EvidenceError;` in `lib.rs`.

- [ ] **Step 2: Verify build**

Run: `cargo build -p bluedb-evidence 2>&1`
Expected: compiles.

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-evidence/src/error.rs crates/bluedb-evidence/src/lib.rs
git commit -m "feat(evidence): error type"
```

---

### Task 4: Keyspace & key encoding

**Files:**
- Modify: `crates/bluedb-evidence/src/keyspace.rs`
- Test: inline `#[cfg(test)]` in `keyspace.rs`

- [ ] **Step 1: Write the failing test** — the critical property is **numeric == byte order across a digit boundary** (R2).

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_keys_sort_numerically_not_lexically() {
        let ks = EvidenceKeyspace::new("acme");
        // 9 must sort before 10, 999 before 1000.
        assert!(ks.entry_key("c", 9) < ks.entry_key("c", 10));
        assert!(ks.entry_key("c", 999) < ks.entry_key("c", 1000));
        assert!(ks.entry_key("c", 1) < ks.entry_key("c", i64::MAX));
    }

    #[test]
    fn chain_names_do_not_bleed_into_each_other() {
        let ks = EvidenceKeyspace::new("acme");
        // "ab" entries must not fall inside the "abc" range (length-prefixing).
        let ab_hi = ks.entry_key("ab", i64::MAX);
        let abc_lo = ks.entry_key("abc", 1);
        assert!(ab_hi < abc_lo);
    }

    #[test]
    fn tenant_isolates_keys() {
        let a = EvidenceKeyspace::new("acme").entry_key("c", 1);
        let b = EvidenceKeyspace::new("globex").entry_key("c", 1);
        assert_ne!(a, b);
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p bluedb-evidence keyspace:: 2>&1`
Expected: FAIL (not defined).

- [ ] **Step 3: Implement `src/keyspace.rs`**

```rust
use bluedb_sql::keyspace::{prefix_upper_bound, Keyspace, TAG_EXTERNAL_BASE};

// Evidence external tags. Ledger uses 0x10–0x15, CDC 0x16, so evidence starts at 0x17.
pub(crate) const TAG_EVIDENCE_ENTRY: u8 = TAG_EXTERNAL_BASE + 7; // 0x17
pub(crate) const TAG_EVIDENCE_SEQ: u8 = TAG_EXTERNAL_BASE + 8; // 0x18
pub(crate) const TAG_EVIDENCE_IDEM: u8 = TAG_EXTERNAL_BASE + 9; // 0x19
// 0x1A TAG_EVIDENCE_MERKLE (Plan 2), 0x1B TAG_EVIDENCE_CHAIN below.
pub(crate) const TAG_EVIDENCE_CHAIN: u8 = TAG_EXTERNAL_BASE + 11; // 0x1B
// 0x1C–0x1E reserved for the graph store (Plan 3).

pub(crate) struct EvidenceKeyspace {
    ks: Keyspace,
}

impl EvidenceKeyspace {
    pub(crate) fn new(tenant: &str) -> Self {
        Self { ks: Keyspace::new(tenant) }
    }

    /// `<len(chain)::u32-be> <chain_utf8>` — length-prefixed so chain names form
    /// disjoint ranges ("ab" never falls inside "abc").
    fn chain_suffix(chain: &str) -> Vec<u8> {
        let b = chain.as_bytes();
        let mut s = Vec::with_capacity(4 + b.len());
        s.extend_from_slice(&(b.len() as u32).to_be_bytes());
        s.extend_from_slice(b);
        s
    }

    /// Entry: `<chain_suffix> <seq::i64-be>`. seq ≥ 1 ⇒ big-endian order == numeric order.
    pub(crate) fn entry_key(&self, chain: &str, seq: i64) -> Vec<u8> {
        let mut s = Self::chain_suffix(chain);
        s.extend_from_slice(&seq.to_be_bytes());
        self.ks.external_key(TAG_EVIDENCE_ENTRY, &s)
    }

    /// Lower bound for a full-chain entry scan.
    pub(crate) fn entry_prefix(&self, chain: &str) -> Vec<u8> {
        self.ks.external_key(TAG_EVIDENCE_ENTRY, &Self::chain_suffix(chain))
    }

    /// Exclusive upper bound just past `entry_prefix(chain)`.
    pub(crate) fn entry_prefix_end(&self, chain: &str) -> Vec<u8> {
        prefix_upper_bound(&self.entry_prefix(chain)).expect("non-0xFF prefix")
    }

    pub(crate) fn seq_key(&self, chain: &str) -> Vec<u8> {
        self.ks.external_key(TAG_EVIDENCE_SEQ, &Self::chain_suffix(chain))
    }

    pub(crate) fn idem_key(&self, chain: &str, idem: &str) -> Vec<u8> {
        let mut s = Self::chain_suffix(chain);
        s.extend_from_slice(idem.as_bytes());
        self.ks.external_key(TAG_EVIDENCE_IDEM, &s)
    }

    pub(crate) fn chain_meta_key(&self, chain: &str) -> Vec<u8> {
        self.ks.external_key(TAG_EVIDENCE_CHAIN, &Self::chain_suffix(chain))
    }
}
```

(Confirm `prefix_upper_bound` and `TAG_EXTERNAL_BASE` are re-exported from `bluedb_sql::keyspace`. If `prefix_upper_bound` is private, copy it into this module — it is a 10-line pure fn, see `crates/bluedb-sql/src/keyspace.rs:356`.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p bluedb-evidence keyspace:: 2>&1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-evidence/src/keyspace.rs
git commit -m "feat(evidence): tenant-namespaced chain keyspace with order-preserving seq keys"
```

---

### Task 5: Store helpers (encode/decode + point reads)

**Files:**
- Modify: `crates/bluedb-evidence/src/store.rs`

- [ ] **Step 1: Implement `src/store.rs`** (mirror `bluedb-ledger/src/store.rs`)

```rust
use anyhow::{Context, Result};
use bluedb_storage::Substrate;
use serde::{de::DeserializeOwned, Serialize};

use crate::keyspace::EvidenceKeyspace;
use crate::model::{ChainMeta, EntryRecord, IdemRecord};

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(value).context("postcard encode evidence record")
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    postcard::from_bytes(bytes).context("postcard decode evidence record")
}

pub(crate) async fn get_chain_meta(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
) -> Result<Option<ChainMeta>> {
    match substrate.get(&ks.chain_meta_key(chain)).await? {
        Some(b) => Ok(Some(decode(&b)?)),
        None => Ok(None),
    }
}

/// Last-assigned seq for `chain`, or 0 if the chain is empty/absent.
pub(crate) async fn get_seq(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
) -> Result<i64> {
    match substrate.get(&ks.seq_key(chain)).await? {
        Some(b) => {
            let arr: [u8; 8] = b.as_ref().try_into().context("seq counter must be 8 bytes")?;
            Ok(i64::from_be_bytes(arr))
        }
        None => Ok(0),
    }
}

pub(crate) async fn get_idem(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
    idem: &str,
) -> Result<Option<IdemRecord>> {
    match substrate.get(&ks.idem_key(chain, idem)).await? {
        Some(b) => Ok(Some(decode(&b)?)),
        None => Ok(None),
    }
}

pub(crate) async fn get_entry(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    chain: &str,
    seq: i64,
) -> Result<Option<EntryRecord>> {
    match substrate.get(&ks.entry_key(chain, seq)).await? {
        Some(b) => Ok(Some(decode(&b)?)),
        None => Ok(None),
    }
}
```

(Confirm the `Substrate` import path — the ledger imports it from `bluedb_storage`. Check `bluedb-ledger/src/store.rs` line 1–10 for the exact `use`.)

- [ ] **Step 2: Verify build**

Run: `cargo build -p bluedb-evidence 2>&1`
Expected: compiles.

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-evidence/src/store.rs
git commit -m "feat(evidence): postcard store helpers + point reads"
```

---

### Task 6: `Evidence` struct + chain create/mode

**Files:**
- Modify: `crates/bluedb-evidence/src/chain.rs`
- Test: `crates/bluedb-evidence/tests/chain.rs`

- [ ] **Step 1: Write the failing test** (create a fresh in-memory `Database`, create a chain, re-create with conflicting mode → error). Copy the test harness for building an in-memory `Database` from `bluedb-ledger`'s tests (find the helper that builds `Db::builder(... in-memory ...)` → `Database::new`).

```rust
// crates/bluedb-evidence/tests/chain.rs
use bluedb_evidence::{ChainMeta, Evidence};

mod harness; // copy from bluedb-ledger tests: builds an in-memory Database

#[tokio::test]
async fn create_chain_is_idempotent_and_rejects_mode_change() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");

    ev.create_chain("audit", true).await.unwrap();
    // same mode → ok (no-op)
    ev.create_chain("audit", true).await.unwrap();
    // different mode → conflict
    let err = ev.create_chain("audit", false).await.unwrap_err();
    assert!(matches!(err, bluedb_evidence::EvidenceError::ChainModeConflict(_)));

    assert_eq!(ev.chain_meta("audit").await.unwrap(), Some(ChainMeta { verified: true }));
    assert_eq!(ev.chain_meta("missing").await.unwrap(), None);
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p bluedb-evidence --test chain 2>&1`
Expected: FAIL.

- [ ] **Step 3: Implement the `Evidence` struct + create/mode** in `src/chain.rs`

```rust
use bluedb_sql::Database;
use bluedb_sql::storage::WriteLease;        // confirm path: ledger imports WriteLease from here
use bluedb_storage::Substrate;
use slatedb::config::WriteOptions;
use slatedb::WriteBatch;

use crate::error::EvidenceError;
use crate::keyspace::EvidenceKeyspace;
use crate::model::{ChainMeta, EntryRecord, IdemRecord};
use crate::store;

/// Per-tenant handle to the evidence chains. Cheap to construct per request
/// (clones Arc handles), exactly like `Ledger::new`.
pub struct Evidence {
    substrate: Substrate,
    write_lease: WriteLease,
    keyspace: EvidenceKeyspace,
}

impl Evidence {
    pub fn new(database: &Database, tenant: &str) -> Self {
        Self {
            substrate: database.substrate(),
            write_lease: database.write_lease(),
            keyspace: EvidenceKeyspace::new(tenant),
        }
    }

    pub async fn chain_meta(&self, chain: &str) -> Result<Option<ChainMeta>, EvidenceError> {
        Ok(store::get_chain_meta(&self.substrate, &self.keyspace, chain).await?)
    }

    /// Create `chain` with the given mode. Idempotent for a matching mode;
    /// `ChainModeConflict` if it exists with a different mode.
    pub async fn create_chain(&self, chain: &str, verified: bool) -> Result<(), EvidenceError> {
        let _lease = self.write_lease.lock().await;
        let writer = self
            .substrate
            .require_writer()
            .map_err(|_| EvidenceError::NotWriter)?;
        if let Some(existing) = store::get_chain_meta(&self.substrate, &self.keyspace, chain).await? {
            if existing.verified != verified {
                return Err(EvidenceError::ChainModeConflict(chain.to_string()));
            }
            return Ok(()); // no-op
        }
        let mut batch = WriteBatch::new();
        batch.put(
            self.keyspace.chain_meta_key(chain),
            &store::encode(&ChainMeta { verified })?,
        );
        writer
            .write_with_options(batch, &WriteOptions { await_durable: false, ..Default::default() })
            .await?;
        drop(_lease);
        writer.flush().await?;
        Ok(())
    }
}
```

(Verify `WriteLease`'s import path and that `Database::substrate()`/`write_lease()` are public — they're used by `Ledger::new`, `bluedb-ledger/src/ledger.rs:100`.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p bluedb-evidence --test chain 2>&1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-evidence/src/chain.rs crates/bluedb-evidence/tests/
git commit -m "feat(evidence): Evidence handle + chain create with mode guard"
```

---

### Task 7: Append — dense seq + idempotency + atomic batch

**Files:**
- Modify: `crates/bluedb-evidence/src/chain.rs`
- Test: `crates/bluedb-evidence/tests/append.rs`

- [ ] **Step 1: Write the failing tests**

```rust
// crates/bluedb-evidence/tests/append.rs
use bluedb_evidence::{EntryInput, Evidence};

mod harness;

fn ev_entry(t: &str) -> EntryInput {
    EntryInput { etype: t.into(), payload: t.as_bytes().to_vec(), at: String::new(), edges: vec![] }
}

#[tokio::test]
async fn single_appends_yield_dense_seqs() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    for i in 1..=5i64 {
        let r = ev.append("c", vec![ev_entry("e")], None).await.unwrap();
        assert_eq!(r.base_seq, i - 1);
        assert_eq!(r.seqs, vec![i]);
    }
    assert_eq!(ev.head("c").await.unwrap(), 5);
}

#[tokio::test]
async fn batch_yields_contiguous_seqs() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    let r = ev.append("c", vec![ev_entry("a"), ev_entry("b"), ev_entry("c")], None).await.unwrap();
    assert_eq!(r.base_seq, 0);
    assert_eq!(r.seqs, vec![1, 2, 3]);
    assert_eq!(ev.head("c").await.unwrap(), 3);
}

#[tokio::test]
async fn idempotent_retry_returns_same_seqs_and_conflicts_on_change() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    let first = ev.append("c", vec![ev_entry("x")], Some("k1")).await.unwrap();
    // same key + same payload → original result, no growth
    let again = ev.append("c", vec![ev_entry("x")], Some("k1")).await.unwrap();
    assert_eq!(first.seqs, again.seqs);
    assert_eq!(ev.head("c").await.unwrap(), 1);
    // same key + different payload → conflict
    let err = ev.append("c", vec![ev_entry("y")], Some("k1")).await.unwrap_err();
    assert!(matches!(err, bluedb_evidence::EvidenceError::IdemConflict));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p bluedb-evidence --test append 2>&1`
Expected: FAIL (`EntryInput`/`append`/`head` not defined).

- [ ] **Step 3: Implement `EntryInput`, `Appended`, `append`, `head`** in `src/chain.rs`

```rust
use sha2::{Digest, Sha256};

/// Caller-supplied event to append. (Plan 3 will use `edges` to drive the graph;
/// Plan 1 just stores them on the entry.)
#[derive(Debug, Clone)]
pub struct EntryInput {
    pub etype: String,
    pub payload: Vec<u8>,
    pub at: String,
    pub edges: Vec<crate::model::EdgeDelta>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Appended {
    pub base_seq: i64,
    pub seqs: Vec<i64>,
}

impl Evidence {
    /// SHA-256 over the framed bytes of the whole batch — byte-identity only,
    /// not canonicalization. Length-delimit every field so distinct inputs differ.
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

    pub async fn head(&self, chain: &str) -> Result<i64, EvidenceError> {
        Ok(store::get_seq(&self.substrate, &self.keyspace, chain).await?)
    }

    pub async fn append(
        &self,
        chain: &str,
        entries: Vec<EntryInput>,
        idem_key: Option<&str>,
    ) -> Result<Appended, EvidenceError> {
        let _lease = self.write_lease.lock().await;
        let writer = self
            .substrate
            .require_writer()
            .map_err(|_| EvidenceError::NotWriter)?;

        let existing_meta = store::get_chain_meta(&self.substrate, &self.keyspace, chain).await?;
        let fp = Self::fingerprint(&entries);

        if let Some(key) = idem_key {
            if let Some(rec) = store::get_idem(&self.substrate, &self.keyspace, chain, key).await? {
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
        if existing_meta.is_none() {
            // auto-create verified chain
            batch.put(self.keyspace.chain_meta_key(chain), &store::encode(&ChainMeta { verified: true })?);
        }
        for (entry, &seq) in entries.iter().zip(&seqs) {
            let rec = EntryRecord {
                etype: entry.etype.clone(),
                payload: entry.payload.clone(),
                at: entry.at.clone(),
                edges: entry.edges.clone(),
                leaf_hash: None, // Plan 2
                redacted: false,
            };
            batch.put(self.keyspace.entry_key(chain, seq), &store::encode(&rec)?);
        }
        batch.put(self.keyspace.seq_key(chain), &(base + k).to_be_bytes());
        if let Some(key) = idem_key {
            let rec = IdemRecord { base_seq: base, seqs: seqs.clone(), fingerprint: fp };
            batch.put(self.keyspace.idem_key(chain, key), &store::encode(&rec)?);
        }

        writer
            .write_with_options(batch, &WriteOptions { await_durable: false, ..Default::default() })
            .await?;
        drop(_lease);
        writer.flush().await?;
        Ok(Appended { base_seq: base, seqs })
    }
}
```

Add `pub use chain::{Appended, EntryInput, Evidence};` to `lib.rs` (already listed in Task 1 — confirm).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p bluedb-evidence --test append 2>&1`
Expected: PASS (all three).

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-evidence/src/chain.rs crates/bluedb-evidence/src/lib.rs crates/bluedb-evidence/tests/append.rs
git commit -m "feat(evidence): atomic append with dense server-assigned seq + idempotency"
```

---

### Task 8: Reads — `read_range`, `read_from`

**Files:**
- Modify: `crates/bluedb-evidence/src/chain.rs`
- Test: `crates/bluedb-evidence/tests/reads.rs`

- [ ] **Step 1: Write the failing tests**

```rust
// crates/bluedb-evidence/tests/reads.rs
use bluedb_evidence::{EntryInput, Evidence};

mod harness;

fn e(t: &str) -> EntryInput {
    EntryInput { etype: t.into(), payload: t.as_bytes().to_vec(), at: String::new(), edges: vec![] }
}

#[tokio::test]
async fn read_range_roundtrips_in_numeric_order_across_digit_boundary() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    for i in 1..=1000 {
        ev.append("c", vec![e(&i.to_string())], None).await.unwrap();
    }
    let all = ev.read_range("c", 1, ev.head("c").await.unwrap()).await.unwrap();
    assert_eq!(all.len(), 1000);
    // numeric order: entry at index 8 is seq 9, index 9 is seq 10, etc.
    assert_eq!(all[8].1.payload, b"9");
    assert_eq!(all[9].1.payload, b"10");
    assert_eq!(all[998].1.payload, b"999");
    assert_eq!(all[999].1.payload, b"1000");
    // seqs are 1..=1000 contiguous
    assert_eq!(all.first().unwrap().0, 1);
    assert_eq!(all.last().unwrap().0, 1000);
}

#[tokio::test]
async fn read_range_hi_lt_lo_is_empty() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    assert!(ev.read_range("c", 1, 0).await.unwrap().is_empty()); // empty chain read_all
}

#[tokio::test]
async fn read_from_paging_reproduces_full_range() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    for _ in 0..250 { ev.append("c", vec![e("x")], None).await.unwrap(); }
    let head = ev.head("c").await.unwrap();
    let mut paged = Vec::new();
    let mut after = 0;
    loop {
        let page = ev.read_from("c", after, Some(100)).await.unwrap();
        if page.is_empty() { break; }
        after = page.last().unwrap().0;
        paged.extend(page);
    }
    let full = ev.read_range("c", 1, head).await.unwrap();
    assert_eq!(paged.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
               full.iter().map(|(s, _)| *s).collect::<Vec<_>>());
    assert_eq!(paged.len(), 250);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p bluedb-evidence --test reads 2>&1`
Expected: FAIL.

- [ ] **Step 3: Implement `read_range` and `read_from`** in `src/chain.rs`

```rust
/// Server cap on a single page (R2 max page size `P`).
const MAX_PAGE: usize = 1000;

impl Evidence {
    /// Inclusive `[lo, hi]`, ascending, byte-exact. `hi < lo` ⇒ empty.
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
        // exclusive upper bound: one past `hi`, or the chain-prefix end if hi == MAX.
        let end = match hi.checked_add(1) {
            Some(next) => self.keyspace.entry_key(chain, next),
            None => self.keyspace.entry_prefix_end(chain),
        };
        self.scan_entries(&start, &end, usize::MAX).await
    }

    /// `after` exclusive; returns ≤ `min(limit, MAX_PAGE)` entries ascending.
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

    async fn scan_entries(
        &self,
        start: &[u8],
        end: &[u8],
        cap: usize,
    ) -> Result<Vec<(i64, EntryRecord)>, EvidenceError> {
        let mut out = Vec::new();
        let mut iter = self.substrate.scan_range(start, Some(end)).await?;
        while let Some(kv) = iter.next().await? {
            if out.len() >= cap {
                break;
            }
            // seq is the trailing 8 bytes of the key.
            let key = kv.key.as_ref();
            let tail: [u8; 8] = key[key.len() - 8..].try_into().unwrap();
            let seq = i64::from_be_bytes(tail);
            let rec: EntryRecord = store::decode(&kv.value)?;
            out.push((seq, rec));
        }
        Ok(out)
    }
}
```

(Confirm the `DbIterator` API: `scan_range(start, Some(end))` → iterator with `.next().await? -> Option<kv>` where `kv.key`/`kv.value` are `Bytes`. This matches `bluedb-ledger/src/store.rs:scan_expired` lines 90–99 — copy that exact iteration shape.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p bluedb-evidence --test reads 2>&1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-evidence/src/chain.rs crates/bluedb-evidence/tests/reads.rs
git commit -m "feat(evidence): ordered read_range + paged read_from"
```

---

### Task 9: HTTP surface + tenancy

**Files:**
- Create: `crates/bluedb-server/src/evidence_api.rs`
- Modify: `crates/bluedb-server/src/lib.rs` (add `mod evidence_api;`, an `evidence(&tenant)` helper, and routes)
- Modify: `crates/bluedb-server/Cargo.toml` (add `bluedb-evidence = { workspace = true }`; add to root workspace deps)

- [ ] **Step 1: Add the dependency**

In `crates/bluedb-server/Cargo.toml` `[dependencies]`: `bluedb-evidence = { workspace = true }`. In root `Cargo.toml` `[workspace.dependencies]`: `bluedb-evidence = { path = "crates/bluedb-evidence" }`.

- [ ] **Step 2: Add the `evidence()` helper + error mapping** to `crates/bluedb-server/src/lib.rs`

Near the `ledger()` helper (~line 548), add:

```rust
async fn evidence(&self, tenant: &str) -> Result<bluedb_evidence::Evidence, AppError> {
    match self.inner.db.read().await.as_ref() {
        Some(db) => Ok(bluedb_evidence::Evidence::new(db, tenant)),
        None => Err(AppError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "node has no database yet (no writer has been promoted)".to_string(),
        }),
    }
}
```

Add an `EvidenceError → AppError` mapping (free function in `evidence_api.rs`):

```rust
fn map_err(e: bluedb_evidence::EvidenceError) -> AppError {
    use bluedb_evidence::EvidenceError as E;
    match e {
        E::IdemConflict => AppError::conflict("E_IDEM_CONFLICT: idem_key reused with a different payload"),
        E::ChainModeConflict(c) => AppError::conflict(format!("E_CHAIN_MODE_CONFLICT: chain '{c}' exists with a different mode")),
        E::EntryNotFound { chain, seq } => AppError::not_found(format!("entry {seq} not found in chain '{chain}'")),
        E::NotWriter => AppError::service_unavailable("node is passive (not the active writer)"),
        E::Storage(err) => AppError::internal(format!("evidence storage: {err}")),
    }
}
```

If `AppError` lacks `conflict`/`service_unavailable` constructors, add them next to `bad_request` (~line 1036) following the same shape with `StatusCode::CONFLICT` / `StatusCode::SERVICE_UNAVAILABLE`.

- [ ] **Step 3: Write the failing e2e test** — copy the harness from `crates/bluedb-server/tests/multitenant.rs`.

```rust
// crates/bluedb-server/tests/evidence.rs
mod common; // reuse the app-spawning harness used by multitenant.rs

use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_and_read_through_http_with_tenancy() {
    let app = common::spawn_app().await; // promotes a writer, returns base URL + client

    // append two events to tenant acme, chain "audit"
    let resp = app.post("/evidence/audit/entries")
        .header("x-bluedb-tenant", "acme")
        .json(&json!({ "events": [
            { "type": "attestation", "payload_b64": base64("hello") },
            { "type": "attestation", "payload_b64": base64(&[0u8,159,146,150]) } // non-UTF-8
        ]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["base_seq"], 0);
    assert_eq!(body["seqs"], json!([1, 2]));

    // head
    let head: serde_json::Value = app.get("/evidence/audit/head")
        .header("x-bluedb-tenant", "acme").send().await.unwrap().json().await.unwrap();
    assert_eq!(head["seq"], 2);

    // read back — byte-exact (SHA-256 in == out)
    let entries: serde_json::Value = app.get("/evidence/audit/entries?from=1&to=2")
        .header("x-bluedb-tenant", "acme").send().await.unwrap().json().await.unwrap();
    assert_eq!(entries.as_array().unwrap().len(), 2);
    assert_eq!(entries[1]["payload_b64"], base64(&[0u8,159,146,150]));

    // tenant isolation: globex's "audit" starts at 1
    let resp = app.post("/evidence/audit/entries")
        .header("x-bluedb-tenant", "globex")
        .json(&json!({ "events": [{ "type": "x", "payload_b64": base64("g") }]}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["seqs"], json!([1]));
}
```

(Add a small `base64(bytes) -> String` test helper and `common::spawn_app` mirroring `multitenant.rs`.)

- [ ] **Step 4: Run to verify it fails**

Run: `cargo test -p bluedb-server --test evidence 2>&1`
Expected: FAIL (routes 404 / handler missing).

- [ ] **Step 5: Implement `crates/bluedb-server/src/evidence_api.rs`**

```rust
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use base64::Engine;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{authz, AppError, AppState};
use bluedb_evidence::EntryInput;

fn b64() -> base64::engine::general_purpose::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

fn parse_event(v: &Value) -> Result<EntryInput, AppError> {
    let etype = v.get("type").and_then(Value::as_str).unwrap_or_default().to_string();
    let at = v.get("at").and_then(Value::as_str).unwrap_or_default().to_string();
    let payload = match v.get("payload_b64").and_then(Value::as_str) {
        Some(s) => b64().decode(s).map_err(|e| AppError::bad_request(format!("payload_b64: {e}")))?,
        None => Vec::new(),
    };
    Ok(EntryInput { etype, payload, at, edges: Vec::new() }) // edges wired in Plan 3
}

#[derive(Deserialize)]
pub struct CreateChainBody { #[serde(default = "default_true")] verified: bool }
fn default_true() -> bool { true }

pub async fn create_chain(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
    body: Option<Json<CreateChainBody>>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    state.authorize(&headers, authz::Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    let verified = body.map(|b| b.0.verified).unwrap_or(true);
    state.evidence(&tenant).await?.create_chain(&chain, verified).await.map_err(super::evidence_api::map_err)?;
    Ok(Json(json!({ "chain": chain, "verified": verified })))
}

pub async fn append(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.require_active()?;
    state.authorize(&headers, authz::Scope::DataWrite)?;
    let tenant = state.tenant(&headers)?;
    let events = body.get("events").and_then(Value::as_array)
        .ok_or_else(|| AppError::bad_request("missing 'events' array"))?;
    let entries = events.iter().map(parse_event).collect::<Result<Vec<_>, _>>()?;
    let idem = body.get("idem_key").and_then(Value::as_str);
    let r = state.evidence(&tenant).await?.append(&chain, entries, idem).await.map_err(map_err)?;
    Ok(Json(json!({ "base_seq": r.base_seq, "seqs": r.seqs })))
}

pub async fn head(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let seq = state.evidence(&tenant).await?.head(&chain).await.map_err(map_err)?;
    Ok(Json(json!({ "seq": seq })))
}

#[derive(Deserialize)]
pub struct ReadQuery { from: Option<i64>, to: Option<i64>, after: Option<i64>, limit: Option<usize> }

pub async fn read_entries(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(chain): Path<String>,
    Query(q): Query<ReadQuery>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, authz::Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let ev = state.evidence(&tenant).await?;
    let rows = if let Some(after) = q.after {
        ev.read_from(&chain, after, q.limit).await
    } else {
        let lo = q.from.unwrap_or(1);
        let hi = match q.to { Some(h) => h, None => ev.head(&chain).await.map_err(map_err)? };
        ev.read_range(&chain, lo, hi).await
    }.map_err(map_err)?;
    let out: Vec<Value> = rows.into_iter().map(|(seq, e)| {
        let mut o = json!({ "seq": seq, "type": e.etype, "at": e.at, "redacted": e.redacted });
        if e.redacted {
            // no payload for redacted entries (Plan 2)
        } else {
            o["payload_b64"] = json!(b64().encode(&e.payload));
        }
        o
    }).collect();
    Ok(Json(Value::Array(out)))
}

pub(crate) fn map_err(e: bluedb_evidence::EvidenceError) -> AppError { /* as in Step 2 */ unreachable!() }
```

(Replace the `map_err` stub with the body from Step 2. Add `base64 = { workspace = true }` if the server crate doesn't already depend on it — check; the lakehouse work likely added it.)

- [ ] **Step 6: Register routes** in `crates/bluedb-server/src/lib.rs` `build_app` (after the `/ledger/*` block) and add `mod evidence_api;` at the top:

```rust
.route("/evidence/{chain}", axum::routing::put(evidence_api::create_chain))
.route("/evidence/{chain}/entries", post(evidence_api::append).get(evidence_api::read_entries))
.route("/evidence/{chain}/head", get(evidence_api::head))
```

- [ ] **Step 7: Run the e2e test to verify it passes**

Run: `cargo test -p bluedb-server --test evidence 2>&1`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/bluedb-server/src/evidence_api.rs crates/bluedb-server/src/lib.rs crates/bluedb-server/Cargo.toml Cargo.toml crates/bluedb-server/tests/evidence.rs
git commit -m "feat(evidence): HTTP surface for chains (create/append/head/read) with tenancy"
```

---

### Task 10: Workspace green + concurrency stress

**Files:**
- Test: `crates/bluedb-evidence/tests/concurrency.rs`

- [ ] **Step 1: Write the failing concurrency test** (R1/§6 mechanical density)

```rust
// crates/bluedb-evidence/tests/concurrency.rs
use std::collections::BTreeSet;
use std::sync::Arc;
use bluedb_evidence::{EntryInput, Evidence};

mod harness;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_are_dense_and_gap_free() {
    for _iter in 0..5 {
        let db = Arc::new(harness::memory_db().await);
        let clients = 8usize;
        let per = 100i64;
        let mut handles = Vec::new();
        for _ in 0..clients {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                let ev = Evidence::new(&db, "_");
                let mut got = Vec::new();
                for _ in 0..per {
                    let r = ev.append("c", vec![EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![] }], None).await.unwrap();
                    got.push(r.seqs[0]);
                }
                got
            }));
        }
        let mut all = BTreeSet::new();
        for h in handles { for s in h.await.unwrap() { assert!(all.insert(s), "duplicate seq {s}"); } }
        let total = (clients as i64) * per;
        assert_eq!(all, (1..=total).collect::<BTreeSet<_>>(), "must be exactly 1..=total, no gaps");
    }
}
```

(`harness::memory_db` must return a `Database` usable from multiple tasks — the serialized writer's `WriteLease` mutex serializes them. If `Database` isn't `Send + Sync + Clone`, wrap in `Arc` and have `Evidence::new(&db, ...)` take `&Database` as shown.)

- [ ] **Step 2: Run to verify it fails or passes**

Run: `cargo test -p bluedb-evidence --test concurrency 2>&1`
Expected: PASS (the `WriteLease` already serializes; this test *proves* density under contention). If it FAILS with duplicates/gaps, the seq RMW is escaping the lease — fix by ensuring the counter read in `append` happens *after* `write_lease.lock()` (it does in Task 7).

- [ ] **Step 3: Run the whole workspace**

Run: `cargo test --workspace 2>&1`
Expected: PASS. Run `cargo clippy --workspace 2>&1` and fix warnings. **Do not run `cargo fmt`** (repo has no rustfmt.toml; it reformats ~90 files — match style by hand).

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-evidence/tests/concurrency.rs
git commit -m "test(evidence): concurrent-append density (8x100, gap-free, 5 iters)"
```

---

## Self-review notes (addressed)

- **Spec coverage (Plan 1 slice):** R1 seq/idempotency/atomic-batch (Tasks 7, 10); R2 reads/paging/ordering (Tasks 4, 8); R3 byte-exact through HTTP (Task 9 non-UTF-8 round-trip); R6 determinism via the single serialized writer (Task 10); R8 tenancy (Tasks 6–9, isolation in Task 9). Merkle (R-extension 1), redaction/hard-delete (extension 2), graph (extension 3 / R4), scratch (R5) are Plans 2–5.
- **Open verification points to confirm while implementing (flagged, not placeholders):** exact import paths for `Substrate` (`bluedb_storage`), `WriteLease` (`bluedb_sql::storage`), `Database::substrate()/write_lease()` visibility, the `DbIterator` `.next().await?` shape (copy from `ledger/src/store.rs:90`), and whether `base64`/`sha2` are already workspace deps. Each has a named source file to copy from.
- **Type consistency:** `EntryInput`/`Appended`/`Evidence`/`EvidenceError` names match across Tasks 6–10 and `lib.rs` exports; `map_err` defined once in `evidence_api.rs` (Step 2 body, Step 5 stub replaced).

## Next plans (queued)

- **Plan 2 — Merkle + erasure:** `merkle.rs` (leaf/node hash, `Frontier`, digest, inclusion + consistency proofs), wire `leaf_hash` + frontier into `append` for verified chains, `redact` + `DELETE` (hard-delete plain-only), proof endpoints.
- **Plan 3 — Graph store + append-with-edges:** `graph.rs` (tags `0x1C–0x1E`, canonical+out+in upsert/delete), apply `EntryInput.edges` inside the append `WriteBatch`, edges API.
- **Plan 4 — Traversal:** `reachable`, `widest_path` over the adjacency.
- **Plan 5 — Scratch:** as-of name-prefix create/drop.
