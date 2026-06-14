# bluedb-ledger Core Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `bluedb-ledger` crate's core engine — a TigerBeetle-style double-entry ledger (typed `Account`/`Transfer`, u128 amounts) that creates accounts and applies single transfers with balance constraints atomically inside bluedb's serialized writer.

**Architecture:** A new `bluedb-ledger` crate stores native `postcard`-encoded `Account`/`Transfer` records in their own tenant-namespaced keyspace tags (≥ `0x10`, disjoint from bluedb-sql's `0x01/0x02/0x03`). A `Ledger` is built from a `bluedb_sql::Database`, sharing its `Substrate` (writer/reader) and `write_lease`. `apply` takes the exclusive lease, reads touched accounts live (consistent because we are the sole mutator), validates + mutates an in-memory working set, and commits one atomic `slatedb::WriteBatch` — reusing the durable-before-ack + epoch-fencing already Jepsen-proven.

**Tech Stack:** Rust, SlateDB (`slatedb::Db`/`WriteBatch`), GlueSQL keyspace encoding (reused for the tenant prefix), `postcard` (native record encoding), `thiserror`, `tokio`.

**Scope of THIS plan (Plan 1 of 3):** model + keyspace + store + `Ledger` + `create_accounts` + `create_transfers` (single transfers) + balance constraints + conservation tests. **Out of scope (later plans):** linked chains, two-phase (pending/post/void) + timeouts, balancing transfers (Plan 2: engine completion); SQL projection, `/ledger/*` HTTP routes, Jepsen `ledger` workload (Plan 3: integration). The `Transfer` struct is defined in full here (so later plans don't reshape it); Plan 1 only honors the core/constraint fields.

**Reference spec:** `docs/superpowers/specs/2026-06-14-bluedb-ledger-design.md`

---

## File Structure

**Modified (bluedb-sql — expose reusable internals):**
- `crates/bluedb-sql/src/keyspace.rs` — add `pub const TAG_EXTERNAL_BASE` + `pub fn external_key`/`external_prefix`.
- `crates/bluedb-sql/src/lib.rs` — re-export `Keyspace` + `TAG_EXTERNAL_BASE`.
- `crates/bluedb-sql/src/connection.rs` — add `pub fn substrate()` + `pub fn write_lease()` on `Database`.

**Created (the new crate):**
- `crates/bluedb-ledger/Cargo.toml` — crate manifest.
- `crates/bluedb-ledger/src/lib.rs` — crate root, module decls, re-exports, docs.
- `crates/bluedb-ledger/src/model.rs` — `Account`, `NewAccount`, `Transfer`, `AccountFlags`, `TransferFlags`, `CreateResult`, `LedgerError`.
- `crates/bluedb-ledger/src/keyspace.rs` — `LedgerKeyspace` (native record key encoding).
- `crates/bluedb-ledger/src/store.rs` — `postcard` encode/decode + point reads + an in-memory test harness.
- `crates/bluedb-ledger/src/ledger.rs` — the public `Ledger` struct: `new`, `lookup_*`, `create_accounts`, `create_transfers`.

Each file has one responsibility: `model` = data, `keyspace` = key bytes, `store` = record I/O, `ledger` = the apply state machine + public API.

---

## Task 1: Expose external-namespace keys in bluedb-sql keyspace

**Files:**
- Modify: `crates/bluedb-sql/src/keyspace.rs`
- Modify: `crates/bluedb-sql/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `crates/bluedb-sql/src/keyspace.rs` (before the closing `}` of `mod tests`):

```rust
    #[test]
    fn external_namespace_is_disjoint_and_ordered() {
        let ks = ks();
        // External tags (>= 0x10) sort after sql's own namespaces and don't
        // collide with each other.
        let acct = ks.external_key(TAG_EXTERNAL_BASE, &7u128.to_be_bytes());
        let xfer = ks.external_key(TAG_EXTERNAL_BASE + 1, &7u128.to_be_bytes());
        let data = ks.data_prefix("t");
        assert!(data < acct, "sql data namespace sorts before external tags");
        assert!(acct < xfer, "external tag 0x10 sorts before 0x11");
        // Every account key starts with the account prefix; no transfer key does.
        let acct_prefix = ks.external_prefix(TAG_EXTERNAL_BASE);
        assert!(acct.starts_with(&acct_prefix));
        assert!(!xfer.starts_with(&acct_prefix));
    }

    #[test]
    fn external_keys_sort_by_u128_suffix() {
        let ks = ks();
        let a = ks.external_key(TAG_EXTERNAL_BASE, &1u128.to_be_bytes());
        let b = ks.external_key(TAG_EXTERNAL_BASE, &2u128.to_be_bytes());
        let big = ks.external_key(TAG_EXTERNAL_BASE, &u128::MAX.to_be_bytes());
        assert!(a < b && b < big, "big-endian u128 suffixes sort numerically");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bluedb-sql external_ -- --nocapture`
Expected: FAIL to compile — `no method named external_key` / `cannot find value TAG_EXTERNAL_BASE`.

- [ ] **Step 3: Add the public API**

In `crates/bluedb-sql/src/keyspace.rs`, after the `const TAG_INDEX: u8 = 0x03;` line, add:

```rust
/// Tag floor for namespaces owned by layers *above* bluedb-sql (e.g.
/// `bluedb-ledger`). bluedb-sql's own tags (`TAG_SCHEMA`/`TAG_DATA`/`TAG_INDEX`)
/// stay below this, so an external namespace can never collide with a SQL one
/// inside a shared tenant keyspace.
pub const TAG_EXTERNAL_BASE: u8 = 0x10;
```

Then, inside `impl Keyspace { ... }` (after `index_value_prefix`, before the closing `}`), add:

```rust
    /// A key in an **external** namespace: `<tenant> <tag> <suffix>`. The caller
    /// owns the `suffix` encoding (e.g. a `u128` big-endian id). `tag` must be
    /// `>= TAG_EXTERNAL_BASE` so it cannot collide with bluedb-sql's own
    /// namespaces; this is debug-asserted.
    pub fn external_key(&self, tag: u8, suffix: &[u8]) -> Vec<u8> {
        debug_assert!(tag >= TAG_EXTERNAL_BASE, "external tag must be >= TAG_EXTERNAL_BASE");
        let mut key = self.tagged(tag, suffix.len());
        key.extend_from_slice(suffix);
        key
    }

    /// The shared prefix of every key in external namespace `tag`
    /// (`<tenant> <tag>`), for range scans. `tag` must be `>= TAG_EXTERNAL_BASE`.
    pub fn external_prefix(&self, tag: u8) -> Vec<u8> {
        debug_assert!(tag >= TAG_EXTERNAL_BASE, "external tag must be >= TAG_EXTERNAL_BASE");
        self.tagged(tag, 0)
    }
```

In `crates/bluedb-sql/src/lib.rs`, change the keyspace re-export line:

```rust
pub use keyspace::{Keyspace, DEFAULT_TENANT, TAG_EXTERNAL_BASE};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p bluedb-sql external_ -- --nocapture`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-sql/src/keyspace.rs crates/bluedb-sql/src/lib.rs
git commit -m "bluedb-sql: expose external-namespace keyspace tags for layered crates"
```

---

## Task 2: Expose Database substrate + write_lease accessors

**Files:**
- Modify: `crates/bluedb-sql/src/connection.rs`

- [ ] **Step 1: Write the failing test**

Add a `tests` module at the end of `crates/bluedb-sql/src/connection.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;
    use slatedb::Db;

    #[tokio::test]
    async fn writer_database_exposes_substrate_and_lease() {
        let db = Arc::new(Db::open("conn-test", Arc::new(InMemory::new())).await.unwrap());
        let database = Database::new(db);
        assert!(database.substrate().is_writer());
        // Two clones of the lease are the same underlying mutex (Arc).
        let l1 = database.write_lease();
        let l2 = database.write_lease();
        assert!(Arc::ptr_eq(&l1, &l2));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bluedb-sql writer_database_exposes -- --nocapture`
Expected: FAIL to compile — `no method named substrate` / `no method named write_lease`.

- [ ] **Step 3: Add the accessors**

In `crates/bluedb-sql/src/connection.rs`, inside `impl Database { ... }` (after `is_writer`), add:

```rust
    /// A clone of the bound [`Substrate`] (writer `Db` or read replica). Layers
    /// above SQL (e.g. `bluedb-ledger`) read/write through the same handle this
    /// database uses, so they see the node's current role.
    pub fn substrate(&self) -> Substrate {
        self.substrate.clone()
    }

    /// A clone of the shared write lease. A layered writer (e.g. the ledger) that
    /// takes this lease for the duration of a read-modify-write serializes against
    /// this database's explicit SQL transactions on the same node.
    pub fn write_lease(&self) -> WriteLease {
        self.write_lease.clone()
    }
```

`Substrate` and `WriteLease` are already imported at the top of the file (`use bluedb_storage::Substrate;` and `use crate::storage::{... WriteLease};`). `WriteLease` is `pub(crate)`; returning it from a `pub fn` requires it to be `pub`. In `crates/bluedb-sql/src/storage.rs`, change its declaration:

```rust
pub type WriteLease = Arc<Mutex<()>>;
```

(was `pub(crate) type WriteLease`). Re-export it from `crates/bluedb-sql/src/lib.rs` by adding to the storage line:

```rust
pub use storage::{SlateDbStorage, WriteLease};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p bluedb-sql writer_database_exposes -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-sql/src/connection.rs crates/bluedb-sql/src/storage.rs crates/bluedb-sql/src/lib.rs
git commit -m "bluedb-sql: expose Database::substrate() and write_lease() for layered writers"
```

---

## Task 3: Scaffold the bluedb-ledger crate

**Files:**
- Create: `crates/bluedb-ledger/Cargo.toml`
- Create: `crates/bluedb-ledger/src/lib.rs`

- [ ] **Step 1: Create the manifest**

Create `crates/bluedb-ledger/Cargo.toml`:

```toml
[package]
name = "bluedb-ledger"
version = "0.1.0"
edition.workspace = true
license.workspace = true
rust-version.workspace = true

[dependencies]
bluedb-storage = { workspace = true }
bluedb-sql = { workspace = true }
slatedb = { workspace = true }
serde = { workspace = true }
postcard = { workspace = true }
thiserror = { workspace = true }
anyhow = { workspace = true }
tokio = { workspace = true }

[dev-dependencies]
# InMemory object store for tests is re-exported by slatedb (slatedb::object_store).
```

- [ ] **Step 2: Create the crate root**

Create `crates/bluedb-ledger/src/lib.rs`:

```rust
//! `bluedb-ledger` — a TigerBeetle-style double-entry ledger over bluedb.
//!
//! Typed [`Account`]/[`Transfer`] records (u128 amounts) are applied inside
//! bluedb's serialized writer and committed as one atomic SlateDB
//! [`WriteBatch`](slatedb::WriteBatch), reusing the lease + epoch fencing +
//! durable-before-ack that the rest of bluedb already provides. See
//! `docs/superpowers/specs/2026-06-14-bluedb-ledger-design.md`.

mod keyspace;
mod ledger;
mod model;
mod store;

pub use ledger::Ledger;
pub use model::{Account, AccountFlags, CreateResult, LedgerError, NewAccount, Transfer, TransferFlags};
```

- [ ] **Step 3: Register in the workspace dependency table**

In the repo-root `Cargo.toml`, under `[workspace.dependencies]` in the `# --- internal crates ---` group, add:

```toml
bluedb-ledger  = { path = "crates/bluedb-ledger" }
```

(The crate is already a workspace *member* via `members = ["crates/*"]`; this entry lets other crates depend on it later.)

- [ ] **Step 4: Verify it builds**

Run: `cargo build -p bluedb-ledger`
Expected: FAIL — `file not found for module ledger` / `model` / `keyspace` / `store` (modules declared, not yet created). This is expected; the next tasks create them. To confirm the manifest + workspace wiring are valid first, temporarily comment the four `mod` lines and the `pub use` lines, run `cargo build -p bluedb-ledger` (expect: clean build of an empty crate), then uncomment.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-ledger/Cargo.toml crates/bluedb-ledger/src/lib.rs Cargo.toml
git commit -m "bluedb-ledger: scaffold crate"
```

---

## Task 4: Data model — accounts, transfers, flags, results, errors

**Files:**
- Create: `crates/bluedb-ledger/src/model.rs`

- [ ] **Step 1: Write the model with its unit tests**

Create `crates/bluedb-ledger/src/model.rs`:

```rust
//! Ledger data model: TigerBeetle-faithful fixed records with u128 amounts.

use serde::{Deserialize, Serialize};

/// Account behavior flags (bitset). Mirrors TigerBeetle's account flags.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountFlags(pub u16);

impl AccountFlags {
    pub const NONE: Self = Self(0);
    pub const LINKED: Self = Self(1 << 0);
    pub const DEBITS_MUST_NOT_EXCEED_CREDITS: Self = Self(1 << 1);
    pub const CREDITS_MUST_NOT_EXCEED_DEBITS: Self = Self(1 << 2);
    pub const HISTORY: Self = Self(1 << 3);

    /// True if every bit in `other` is set in `self`.
    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

/// Transfer behavior flags (bitset). Mirrors TigerBeetle's transfer flags.
/// Plan 1 honors none of these directly (single posted transfers); later plans
/// implement linked / two-phase / balancing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferFlags(pub u16);

impl TransferFlags {
    pub const NONE: Self = Self(0);
    pub const LINKED: Self = Self(1 << 0);
    pub const PENDING: Self = Self(1 << 1);
    pub const POST_PENDING_TRANSFER: Self = Self(1 << 2);
    pub const VOID_PENDING_TRANSFER: Self = Self(1 << 3);
    pub const BALANCING_DEBIT: Self = Self(1 << 4);
    pub const BALANCING_CREDIT: Self = Self(1 << 5);

    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

/// A persisted account with its four balance buckets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    pub id: u128,
    pub ledger: u32,
    pub code: u16,
    pub flags: AccountFlags,
    pub debits_pending: u128,
    pub debits_posted: u128,
    pub credits_pending: u128,
    pub credits_posted: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub timestamp: u64,
}

/// The caller-supplied form for creating an account (no balances; the engine
/// initializes them to zero and assigns `timestamp`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewAccount {
    pub id: u128,
    pub ledger: u32,
    pub code: u16,
    pub flags: AccountFlags,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
}

impl NewAccount {
    /// A minimal account spec: id + ledger, everything else default.
    pub fn new(id: u128, ledger: u32) -> Self {
        Self { id, ledger, code: 0, flags: AccountFlags::NONE, user_data_128: 0, user_data_64: 0, user_data_32: 0 }
    }

    /// Set behavior flags (builder).
    pub fn with_flags(mut self, flags: AccountFlags) -> Self {
        self.flags = flags;
        self
    }

    /// Materialize a zero-balance [`Account`] at `timestamp`.
    pub(crate) fn into_account(self, timestamp: u64) -> Account {
        Account {
            id: self.id,
            ledger: self.ledger,
            code: self.code,
            flags: self.flags,
            debits_pending: 0,
            debits_posted: 0,
            credits_pending: 0,
            credits_posted: 0,
            user_data_128: self.user_data_128,
            user_data_64: self.user_data_64,
            user_data_32: self.user_data_32,
            timestamp,
        }
    }
}

/// A transfer request / record. `timestamp` is assigned by the engine at apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transfer {
    pub id: u128,
    pub debit_account_id: u128,
    pub credit_account_id: u128,
    pub amount: u128,
    pub pending_id: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub ledger: u32,
    pub code: u16,
    pub flags: TransferFlags,
    pub timeout: u32,
    pub timestamp: u64,
}

impl Transfer {
    /// A minimal posted transfer: id, debit, credit, amount, ledger; everything
    /// else default.
    pub fn new(id: u128, debit_account_id: u128, credit_account_id: u128, amount: u128, ledger: u32) -> Self {
        Self {
            id,
            debit_account_id,
            credit_account_id,
            amount,
            pending_id: 0,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            ledger,
            code: 0,
            flags: TransferFlags::NONE,
            timeout: 0,
            timestamp: 0,
        }
    }
}

/// Per-item outcome of a batch create. Mirrors TigerBeetle's per-event results.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateResult {
    /// Created/applied successfully.
    Ok,
    /// An item with this `id` already existed; no change made (idempotent).
    Exists,
    /// Rejected by a validation rule; no change made.
    Failed(LedgerError),
}

/// A per-transfer / per-account validation failure (does not abort the batch;
/// surfaced in [`CreateResult::Failed`]). I/O failures are surfaced separately
/// as `anyhow::Error` from the `Ledger` methods.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LedgerError {
    #[error("debit and credit accounts must differ")]
    AccountsMustDiffer,
    #[error("transfer ledger must match both accounts' ledger")]
    LedgerMismatch,
    #[error("account {0} not found")]
    AccountNotFound(u128),
    #[error("debit account would breach debits_must_not_exceed_credits")]
    ExceedsCredits,
    #[error("credit account would breach credits_must_not_exceed_debits")]
    ExceedsDebits,
    #[error("balance arithmetic overflowed u128")]
    Overflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_flags_contains() {
        let f = AccountFlags(AccountFlags::LINKED.0 | AccountFlags::HISTORY.0);
        assert!(f.contains(AccountFlags::LINKED));
        assert!(f.contains(AccountFlags::HISTORY));
        assert!(!f.contains(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS));
        assert!(AccountFlags::NONE.contains(AccountFlags::NONE));
    }

    #[test]
    fn new_account_materializes_zero_balances() {
        let a = NewAccount::new(7, 1)
            .with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS)
            .into_account(42);
        assert_eq!(a.id, 7);
        assert_eq!(a.ledger, 1);
        assert_eq!(a.timestamp, 42);
        assert_eq!((a.debits_posted, a.credits_posted, a.debits_pending, a.credits_pending), (0, 0, 0, 0));
        assert!(a.flags.contains(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS));
    }

    #[test]
    fn transfer_new_defaults() {
        let t = Transfer::new(1, 10, 20, 500, 1);
        assert_eq!((t.debit_account_id, t.credit_account_id, t.amount), (10, 20, 500));
        assert_eq!(t.flags, TransferFlags::NONE);
        assert_eq!(t.pending_id, 0);
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p bluedb-ledger model::`
Expected: PASS (3 tests). (`store`/`ledger`/`keyspace` modules are still empty files — create them as empty stubs so the crate compiles: `touch crates/bluedb-ledger/src/keyspace.rs crates/bluedb-ledger/src/store.rs crates/bluedb-ledger/src/ledger.rs` and add `pub struct Ledger;` placeholder is **not** needed yet because `lib.rs` re-exports them — instead, for this task only, temporarily comment the `mod keyspace; mod store; mod ledger;` lines and their `pub use` lines in `lib.rs`, leaving only `mod model;`.)

- [ ] **Step 3: Restore lib.rs module lines**

Re-add the commented `mod`/`pub use` lines in `lib.rs` (they'll be satisfied by Tasks 5–7).

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-ledger/src/model.rs crates/bluedb-ledger/src/lib.rs
git commit -m "bluedb-ledger: account/transfer model, flags, results, errors"
```

---

## Task 5: Ledger keyspace (native record key encoding)

**Files:**
- Create: `crates/bluedb-ledger/src/keyspace.rs`

- [ ] **Step 1: Write the keyspace with its tests**

Create `crates/bluedb-ledger/src/keyspace.rs`:

```rust
//! Native-record key encoding for the ledger, layered on bluedb-sql's
//! tenant-namespaced [`Keyspace`] via its external-namespace tags.

use bluedb_sql::{Keyspace, TAG_EXTERNAL_BASE};

/// Tag for native account records.
const TAG_ACCOUNT: u8 = TAG_EXTERNAL_BASE; // 0x10
/// Tag for native transfer records.
const TAG_TRANSFER: u8 = TAG_EXTERNAL_BASE + 1; // 0x11

/// Builds the storage keys for ledger records within one tenant. Account and
/// transfer ids are encoded big-endian so a range scan yields them in id order.
pub(crate) struct LedgerKeyspace {
    ks: Keyspace,
}

impl LedgerKeyspace {
    pub(crate) fn new(tenant: &str) -> Self {
        Self { ks: Keyspace::new(tenant) }
    }

    pub(crate) fn account_key(&self, id: u128) -> Vec<u8> {
        self.ks.external_key(TAG_ACCOUNT, &id.to_be_bytes())
    }

    pub(crate) fn transfer_key(&self, id: u128) -> Vec<u8> {
        self.ks.external_key(TAG_TRANSFER, &id.to_be_bytes())
    }

    #[allow(dead_code)] // used by range scans in later plans (lookup-all / sweeps)
    pub(crate) fn account_prefix(&self) -> Vec<u8> {
        self.ks.external_prefix(TAG_ACCOUNT)
    }

    #[allow(dead_code)]
    pub(crate) fn transfer_prefix(&self) -> Vec<u8> {
        self.ks.external_prefix(TAG_TRANSFER)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_and_transfer_keys_are_distinct_and_ordered() {
        let ks = LedgerKeyspace::new("_");
        let a = ks.account_key(5);
        let x = ks.transfer_key(5);
        assert_ne!(a, x, "same id, different namespace → different keys");
        assert!(a < x, "account tag (0x10) sorts before transfer tag (0x11)");
        assert!(a.starts_with(&ks.account_prefix()));
        assert!(x.starts_with(&ks.transfer_prefix()));
        assert!(!x.starts_with(&ks.account_prefix()));
    }

    #[test]
    fn account_keys_sort_by_id() {
        let ks = LedgerKeyspace::new("_");
        assert!(ks.account_key(1) < ks.account_key(2));
        assert!(ks.account_key(2) < ks.account_key(u128::MAX));
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p bluedb-ledger keyspace::`
Expected: PASS (2 tests).

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-ledger/src/keyspace.rs
git commit -m "bluedb-ledger: native record keyspace encoding"
```

---

## Task 6: Store — encode/decode + point reads + test harness

**Files:**
- Create: `crates/bluedb-ledger/src/store.rs`

- [ ] **Step 1: Write the store with a round-trip + persistence test**

Create `crates/bluedb-ledger/src/store.rs`:

```rust
//! Native record I/O: `postcard` (de)serialization and point reads of accounts
//! and transfers through a [`Substrate`].

use anyhow::{Context, Result};
use bluedb_storage::Substrate;
use serde::{de::DeserializeOwned, Serialize};

use crate::keyspace::LedgerKeyspace;
use crate::model::{Account, Transfer};

/// Encode a native record with `postcard` (compact, fast fixed-struct encoding).
pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(value).context("postcard encode ledger record")
}

/// Decode a native record from `postcard` bytes.
pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    postcard::from_bytes(bytes).context("postcard decode ledger record")
}

/// Point read of an account by id (committed state), or `None` if absent.
pub(crate) async fn get_account(
    substrate: &Substrate,
    ks: &LedgerKeyspace,
    id: u128,
) -> Result<Option<Account>> {
    match substrate.get(&ks.account_key(id)).await? {
        Some(bytes) => Ok(Some(decode(&bytes)?)),
        None => Ok(None),
    }
}

/// Point read of a transfer by id (committed state), or `None` if absent.
pub(crate) async fn get_transfer(
    substrate: &Substrate,
    ks: &LedgerKeyspace,
    id: u128,
) -> Result<Option<Transfer>> {
    match substrate.get(&ks.transfer_key(id)).await? {
        Some(bytes) => Ok(Some(decode(&bytes)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
pub(crate) mod test_harness {
    //! Shared test helper: an in-memory writer `Database` over a fresh
    //! `InMemory` object store.
    use std::sync::Arc;

    use bluedb_sql::Database;
    use slatedb::object_store::memory::InMemory;
    use slatedb::Db;

    /// Open a brand-new in-memory writer database for a test.
    pub(crate) async fn writer_database() -> Database {
        let db = Db::open("ledger-test", Arc::new(InMemory::new()))
            .await
            .expect("open in-memory db");
        Database::new(Arc::new(db))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Account, AccountFlags};

    #[test]
    fn account_round_trips_through_postcard() {
        let a = Account {
            id: 99,
            ledger: 1,
            code: 7,
            flags: AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS,
            debits_pending: 1,
            debits_posted: 2,
            credits_pending: 3,
            credits_posted: 4,
            user_data_128: u128::MAX,
            user_data_64: 64,
            user_data_32: 32,
            timestamp: 123,
        };
        let bytes = encode(&a).unwrap();
        let back: Account = decode(&bytes).unwrap();
        assert_eq!(a, back);
    }

    #[tokio::test]
    async fn get_account_reads_what_was_written() {
        let database = test_harness::writer_database().await;
        let substrate = database.substrate();
        let ks = LedgerKeyspace::new(bluedb_sql::DEFAULT_TENANT);

        assert!(get_account(&substrate, &ks, 1).await.unwrap().is_none());

        let a = Account {
            id: 1, ledger: 1, code: 0, flags: AccountFlags::NONE,
            debits_pending: 0, debits_posted: 10, credits_pending: 0, credits_posted: 0,
            user_data_128: 0, user_data_64: 0, user_data_32: 0, timestamp: 5,
        };
        let writer = substrate.require_writer().unwrap();
        writer.put(&ks.account_key(1), &encode(&a).unwrap()).await.unwrap();

        let back = get_account(&substrate, &ks, 1).await.unwrap().unwrap();
        assert_eq!(back, a);
    }
}
```

Note: `store.rs` imports `LedgerKeyspace` from `crate::keyspace`, so add `use crate::keyspace::LedgerKeyspace;` at the top (already shown). The `test_harness` submodule is `pub(crate)` so `ledger.rs` tests can reuse `writer_database()`.

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p bluedb-ledger store::`
Expected: PASS (2 tests).

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-ledger/src/store.rs
git commit -m "bluedb-ledger: native record encode/decode + point reads + test harness"
```

---

## Task 7: Ledger struct — construction + lookups

**Files:**
- Create: `crates/bluedb-ledger/src/ledger.rs`

- [ ] **Step 1: Write the Ledger skeleton with lookup tests**

Create `crates/bluedb-ledger/src/ledger.rs`:

```rust
//! The public [`Ledger`]: built from a [`bluedb_sql::Database`], it runs the
//! double-entry apply state machine inside that database's active writer.

use anyhow::Result;
use bluedb_sql::{Database, WriteLease, DEFAULT_TENANT};
use bluedb_storage::Substrate;

use crate::keyspace::LedgerKeyspace;
use crate::model::{Account, Transfer};
use crate::store::{get_account, get_transfer};

/// A double-entry ledger over one bluedb database.
///
/// Construct one per use from the live [`Database`] (e.g. per request on the
/// server), so it always reflects the node's current writer/replica role. All
/// writes require the active writer; reads work on a replica (eventually
/// consistent).
pub struct Ledger {
    substrate: Substrate,
    write_lease: WriteLease,
    keyspace: LedgerKeyspace,
}

impl Ledger {
    /// Build a ledger over `database` under the default tenant, sharing its
    /// substrate and write lease (so ledger applies serialize against the
    /// database's explicit SQL transactions).
    pub fn new(database: &Database) -> Self {
        Self {
            substrate: database.substrate(),
            write_lease: database.write_lease(),
            keyspace: LedgerKeyspace::new(DEFAULT_TENANT),
        }
    }

    /// Look up one account by id (committed state).
    pub async fn lookup_account(&self, id: u128) -> Result<Option<Account>> {
        get_account(&self.substrate, &self.keyspace, id).await
    }

    /// Look up one transfer by id (committed state).
    pub async fn lookup_transfer(&self, id: u128) -> Result<Option<Transfer>> {
        get_transfer(&self.substrate, &self.keyspace, id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_harness::writer_database;

    #[tokio::test]
    async fn lookups_on_empty_ledger_return_none() {
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        assert!(ledger.lookup_account(1).await.unwrap().is_none());
        assert!(ledger.lookup_transfer(1).await.unwrap().is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p bluedb-ledger ledger::`
Expected: PASS (1 test). The whole crate now compiles: run `cargo test -p bluedb-ledger` → all prior tests still PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-ledger/src/ledger.rs
git commit -m "bluedb-ledger: Ledger struct, construction, and lookups"
```

---

## Task 8: create_accounts (idempotent batch)

**Files:**
- Modify: `crates/bluedb-ledger/src/ledger.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/bluedb-ledger/src/ledger.rs`:

```rust
    #[tokio::test]
    async fn create_accounts_persists_and_is_idempotent() {
        use crate::model::{AccountFlags, CreateResult, NewAccount};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);

        let specs = [
            NewAccount::new(1, 7),
            NewAccount::new(2, 7).with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS),
        ];
        let results = ledger.create_accounts(&specs, 100).await.unwrap();
        assert_eq!(results, vec![CreateResult::Ok, CreateResult::Ok]);

        let a1 = ledger.lookup_account(1).await.unwrap().unwrap();
        assert_eq!(a1.ledger, 7);
        assert_eq!(a1.timestamp, 100);
        assert_eq!(a1.debits_posted, 0);
        let a2 = ledger.lookup_account(2).await.unwrap().unwrap();
        assert!(a2.flags.contains(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS));

        // Re-creating id 1 is a no-op Exists; the original is untouched.
        let again = ledger.create_accounts(&[NewAccount::new(1, 999)], 200).await.unwrap();
        assert_eq!(again, vec![CreateResult::Exists]);
        let a1b = ledger.lookup_account(1).await.unwrap().unwrap();
        assert_eq!(a1b.ledger, 7, "existing account not overwritten");
        assert_eq!(a1b.timestamp, 100);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p bluedb-ledger create_accounts_persists -- --nocapture`
Expected: FAIL to compile — `no method named create_accounts`.

- [ ] **Step 3: Implement create_accounts**

Add these imports at the top of `ledger.rs` (merge with existing `use` lines):

```rust
use std::collections::HashMap;

use slatedb::WriteBatch;

use crate::model::{CreateResult, NewAccount};
use crate::store::{encode, get_account};
```

Add inside `impl Ledger { ... }`:

```rust
    /// Create accounts (batch, idempotent). Each spec whose id already exists
    /// yields [`CreateResult::Exists`] and is left untouched; the rest are
    /// created with zero balances at `timestamp` and committed in one atomic
    /// batch. Requires the active writer.
    pub async fn create_accounts(&self, specs: &[NewAccount], timestamp: u64) -> Result<Vec<CreateResult>> {
        let _lease = self.write_lease.lock().await; // exclusive: we are the sole mutator
        let writer = self.substrate.require_writer()?;

        let mut batch = WriteBatch::new();
        let mut results = Vec::with_capacity(specs.len());
        // Track ids staged in THIS batch so a duplicate id within one call is
        // also treated as Exists (committed-state check can't see them yet).
        let mut staged: HashMap<u128, ()> = HashMap::new();

        for spec in specs {
            if staged.contains_key(&spec.id)
                || get_account(&self.substrate, &self.keyspace, spec.id).await?.is_some()
            {
                results.push(CreateResult::Exists);
                continue;
            }
            let account = spec.into_account(timestamp);
            batch.put(&self.keyspace.account_key(account.id), &encode(&account)?);
            staged.insert(spec.id, ());
            results.push(CreateResult::Ok);
        }

        writer.write(batch).await?;
        Ok(results)
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p bluedb-ledger create_accounts_persists -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-ledger/src/ledger.rs
git commit -m "bluedb-ledger: create_accounts (idempotent batch)"
```

---

## Task 9: create_transfers (single posted transfers)

**Files:**
- Modify: `crates/bluedb-ledger/src/ledger.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `ledger.rs`:

```rust
    async fn setup_two_accounts(ledger: &Ledger) {
        use crate::model::NewAccount;
        ledger
            .create_accounts(&[NewAccount::new(1, 7), NewAccount::new(2, 7)], 1)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn single_transfer_moves_balances_and_conserves() {
        use crate::model::{CreateResult, Transfer};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;

        let res = ledger
            .create_transfers(&[Transfer::new(1000, 1, 2, 500, 7)], 50)
            .await
            .unwrap();
        assert_eq!(res, vec![CreateResult::Ok]);

        let debit = ledger.lookup_account(1).await.unwrap().unwrap();
        let credit = ledger.lookup_account(2).await.unwrap().unwrap();
        assert_eq!(debit.debits_posted, 500);
        assert_eq!(credit.credits_posted, 500);
        // Conservation: total debits_posted == total credits_posted.
        assert_eq!(debit.debits_posted, credit.credits_posted);

        let t = ledger.lookup_transfer(1000).await.unwrap().unwrap();
        assert_eq!(t.timestamp, 50);
        assert_eq!(t.amount, 500);
    }

    #[tokio::test]
    async fn transfer_validation_rejects() {
        use crate::model::{CreateResult, LedgerError, Transfer};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;

        // Same debit and credit account.
        let r = ledger.create_transfers(&[Transfer::new(1, 1, 1, 10, 7)], 2).await.unwrap();
        assert_eq!(r, vec![CreateResult::Failed(LedgerError::AccountsMustDiffer)]);

        // Missing account.
        let r = ledger.create_transfers(&[Transfer::new(2, 1, 99, 10, 7)], 2).await.unwrap();
        assert_eq!(r, vec![CreateResult::Failed(LedgerError::AccountNotFound(99))]);

        // Ledger mismatch (accounts are ledger 7, transfer says 8).
        let r = ledger.create_transfers(&[Transfer::new(3, 1, 2, 10, 8)], 2).await.unwrap();
        assert_eq!(r, vec![CreateResult::Failed(LedgerError::LedgerMismatch)]);

        // None of the rejected transfers changed balances or were persisted.
        assert_eq!(ledger.lookup_account(1).await.unwrap().unwrap().debits_posted, 0);
        assert!(ledger.lookup_transfer(1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn duplicate_transfer_id_is_exists() {
        use crate::model::{CreateResult, Transfer};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;

        let t = Transfer::new(7, 1, 2, 100, 7);
        assert_eq!(ledger.create_transfers(&[t], 1).await.unwrap(), vec![CreateResult::Ok]);
        // Same id again: idempotent, no double-apply.
        assert_eq!(ledger.create_transfers(&[t], 2).await.unwrap(), vec![CreateResult::Exists]);
        assert_eq!(ledger.lookup_account(1).await.unwrap().unwrap().debits_posted, 100);
    }

    #[tokio::test]
    async fn batch_applies_good_skips_bad() {
        use crate::model::{CreateResult, LedgerError, Transfer};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        setup_two_accounts(&ledger).await;

        let batch = [
            Transfer::new(1, 1, 2, 100, 7),  // ok
            Transfer::new(2, 1, 99, 50, 7),  // missing account
            Transfer::new(3, 2, 1, 30, 7),   // ok (reverse direction)
        ];
        let res = ledger.create_transfers(&batch, 9).await.unwrap();
        assert_eq!(
            res,
            vec![CreateResult::Ok, CreateResult::Failed(LedgerError::AccountNotFound(99)), CreateResult::Ok]
        );

        let a1 = ledger.lookup_account(1).await.unwrap().unwrap();
        let a2 = ledger.lookup_account(2).await.unwrap().unwrap();
        assert_eq!(a1.debits_posted, 100);
        assert_eq!(a1.credits_posted, 30);
        assert_eq!(a2.credits_posted, 100);
        assert_eq!(a2.debits_posted, 30);
        // Conservation across the batch: Σ debits_posted == Σ credits_posted.
        assert_eq!(a1.debits_posted + a2.debits_posted, a1.credits_posted + a2.credits_posted);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p bluedb-ledger create_transfers -- --nocapture` (and the named tests above)
Expected: FAIL to compile — `no method named create_transfers`.

- [ ] **Step 3: Implement create_transfers + the per-item state machine**

Add to the imports in `ledger.rs`:

```rust
use crate::model::{LedgerError, Transfer};
use crate::store::get_transfer;
```

Add inside `impl Ledger { ... }`:

```rust
    /// Apply transfers (batch). Each item is validated and applied independently
    /// (Plan 1: single posted transfers — no linked/pending/balancing flags).
    /// A failing item yields [`CreateResult::Failed`] and applies no change; a
    /// duplicate id yields [`CreateResult::Exists`]. All accepted items commit in
    /// one atomic batch. Requires the active writer.
    pub async fn create_transfers(&self, transfers: &[Transfer], timestamp: u64) -> Result<Vec<CreateResult>> {
        let _lease = self.write_lease.lock().await; // exclusive
        let _ = self.substrate.require_writer()?; // fail fast on a replica

        // Working set of touched accounts (loaded read-through, mutated in place
        // only when a transfer is accepted).
        let mut working: HashMap<u128, Account> = HashMap::new();
        let mut accepted: Vec<Transfer> = Vec::new();
        let mut results = Vec::with_capacity(transfers.len());

        for t in transfers {
            match self.stage_transfer(t, timestamp, &mut working).await? {
                Some(applied) => {
                    accepted.push(applied);
                    results.push(CreateResult::Ok);
                }
                None => {
                    // stage_transfer returns the verdict via results-out-param? No —
                    // it returns Ok(None) only for Exists; Failed is returned as Err
                    // below. See match arms.
                    results.push(CreateResult::Exists);
                }
            }
        }

        // This block is replaced below; see the corrected control flow.
        let _ = (&mut working, &accepted);
        unreachable!()
    }
```

The two-outcome `Option` above can't carry the `Failed(LedgerError)` case cleanly. Replace the whole `create_transfers` body with this final version (delete the sketch above):

```rust
    pub async fn create_transfers(&self, transfers: &[Transfer], timestamp: u64) -> Result<Vec<CreateResult>> {
        let _lease = self.write_lease.lock().await; // exclusive
        let _ = self.substrate.require_writer()?; // fail fast on a replica

        let mut working: HashMap<u128, Account> = HashMap::new();
        let mut accepted: Vec<Transfer> = Vec::new();
        let mut results = Vec::with_capacity(transfers.len());

        for t in transfers {
            match self.stage_transfer(t, timestamp, &mut working).await? {
                StageOutcome::Applied(applied) => {
                    accepted.push(applied);
                    results.push(CreateResult::Ok);
                }
                StageOutcome::Exists => results.push(CreateResult::Exists),
                StageOutcome::Rejected(err) => results.push(CreateResult::Failed(err)),
            }
        }

        // Build one atomic batch: every mutated account + every accepted transfer.
        let writer = self.substrate.require_writer()?;
        let mut batch = WriteBatch::new();
        for account in working.values() {
            batch.put(&self.keyspace.account_key(account.id), &encode(account)?);
        }
        for t in &accepted {
            batch.put(&self.keyspace.transfer_key(t.id), &encode(t)?);
        }
        writer.write(batch).await?;

        Ok(results)
    }

    /// Validate one transfer and, if accepted, fold its balance deltas into the
    /// `working` set and return the transfer record (stamped with `timestamp`).
    /// `Exists` if the id is already committed; `Rejected` (no change) on a
    /// validation failure. Errors bubble for I/O failures only.
    async fn stage_transfer(
        &self,
        t: &Transfer,
        timestamp: u64,
        working: &mut HashMap<u128, Account>,
    ) -> Result<StageOutcome> {
        if t.debit_account_id == t.credit_account_id {
            return Ok(StageOutcome::Rejected(LedgerError::AccountsMustDiffer));
        }
        // Idempotency: committed transfer with this id already exists.
        if get_transfer(&self.substrate, &self.keyspace, t.id).await?.is_some() {
            return Ok(StageOutcome::Exists);
        }

        // Load both accounts into the working set (read-through, copies).
        let mut debit = match self.load_account(t.debit_account_id, working).await? {
            Some(a) => a,
            None => return Ok(StageOutcome::Rejected(LedgerError::AccountNotFound(t.debit_account_id))),
        };
        let mut credit = match self.load_account(t.credit_account_id, working).await? {
            Some(a) => a,
            None => return Ok(StageOutcome::Rejected(LedgerError::AccountNotFound(t.credit_account_id))),
        };

        if t.ledger != debit.ledger || t.ledger != credit.ledger {
            return Ok(StageOutcome::Rejected(LedgerError::LedgerMismatch));
        }

        // Apply posted deltas with overflow checks.
        debit.debits_posted = match debit.debits_posted.checked_add(t.amount) {
            Some(v) => v,
            None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
        };
        credit.credits_posted = match credit.credits_posted.checked_add(t.amount) {
            Some(v) => v,
            None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
        };

        // (Balance-constraint checks added in Task 10.)

        // Accept: write the mutated copies back into the working set.
        working.insert(debit.id, debit);
        working.insert(credit.id, credit);
        Ok(StageOutcome::Applied(Transfer { timestamp, ..*t }))
    }

    /// Get an account from the working set, loading it read-through on first
    /// touch. Returns a copy; mutations are written back by the caller on accept.
    async fn load_account(
        &self,
        id: u128,
        working: &mut HashMap<u128, Account>,
    ) -> Result<Option<Account>> {
        if let Some(a) = working.get(&id) {
            return Ok(Some(*a));
        }
        match get_account(&self.substrate, &self.keyspace, id).await? {
            Some(a) => {
                working.insert(id, a);
                Ok(Some(a))
            }
            None => Ok(None),
        }
    }
```

Add this private enum at the bottom of `ledger.rs` (outside `impl`, above the `#[cfg(test)]` module):

```rust
/// The outcome of staging one transfer in [`Ledger::stage_transfer`].
enum StageOutcome {
    /// Accepted; deltas folded into the working set. Carries the stamped record.
    Applied(Transfer),
    /// The id already exists (idempotent no-op).
    Exists,
    /// A validation rule rejected it; no change made.
    Rejected(LedgerError),
}
```

> **Note for the implementer:** Step 3 deliberately shows the throwaway `Option`-based sketch first and then the corrected `StageOutcome` version, because the first naturally fails to express the three-way outcome. Write **only** the corrected version — the sketch is there to explain why `StageOutcome` exists. The final `create_transfers` has no `unreachable!()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p bluedb-ledger`
Expected: PASS — all model/keyspace/store/ledger tests including the four new transfer tests.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-ledger/src/ledger.rs
git commit -m "bluedb-ledger: create_transfers (single posted transfers) + working-set state machine"
```

---

## Task 10: Balance constraints

**Files:**
- Modify: `crates/bluedb-ledger/src/ledger.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `ledger.rs`:

```rust
    #[tokio::test]
    async fn debits_must_not_exceed_credits_is_enforced() {
        use crate::model::{AccountFlags, CreateResult, LedgerError, NewAccount, Transfer};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        // Account 1 may not let debits_posted exceed its credits_posted.
        ledger
            .create_accounts(
                &[
                    NewAccount::new(1, 7).with_flags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS),
                    NewAccount::new(2, 7),
                ],
                1,
            )
            .await
            .unwrap();

        // With zero credits, any debit on account 1 breaches the constraint.
        let r = ledger.create_transfers(&[Transfer::new(10, 1, 2, 100, 7)], 2).await.unwrap();
        assert_eq!(r, vec![CreateResult::Failed(LedgerError::ExceedsCredits)]);
        assert_eq!(ledger.lookup_account(1).await.unwrap().unwrap().debits_posted, 0);

        // Give account 1 some credits first (2 → 1), then an equal debit is fine.
        assert_eq!(
            ledger.create_transfers(&[Transfer::new(11, 2, 1, 100, 7)], 3).await.unwrap(),
            vec![CreateResult::Ok]
        );
        assert_eq!(
            ledger.create_transfers(&[Transfer::new(12, 1, 2, 100, 7)], 4).await.unwrap(),
            vec![CreateResult::Ok]
        );
        // But one more unit over its credits is rejected.
        let r = ledger.create_transfers(&[Transfer::new(13, 1, 2, 1, 7)], 5).await.unwrap();
        assert_eq!(r, vec![CreateResult::Failed(LedgerError::ExceedsCredits)]);
    }

    #[tokio::test]
    async fn credits_must_not_exceed_debits_is_enforced() {
        use crate::model::{AccountFlags, CreateResult, LedgerError, NewAccount, Transfer};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        ledger
            .create_accounts(
                &[
                    NewAccount::new(1, 7),
                    NewAccount::new(2, 7).with_flags(AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS),
                ],
                1,
            )
            .await
            .unwrap();
        // Crediting account 2 (1 → 2) with no debits on 2 breaches its constraint.
        let r = ledger.create_transfers(&[Transfer::new(20, 1, 2, 100, 7)], 2).await.unwrap();
        assert_eq!(r, vec![CreateResult::Failed(LedgerError::ExceedsDebits)]);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p bluedb-ledger _must_not_exceed_ -- --nocapture`
Expected: FAIL — the transfers currently succeed (no constraint check yet), so the `assert_eq!` on `ExceedsCredits`/`ExceedsDebits` fails.

- [ ] **Step 3: Add the constraint checks**

In `stage_transfer` (in `ledger.rs`), replace the comment line `// (Balance-constraint checks added in Task 10.)` with:

```rust
        // Balance constraints (checked on the post-mutation copies). The
        // "available" on the constrained side must not go negative.
        if debit.flags.contains(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS) {
            let used = match debit.debits_posted.checked_add(debit.debits_pending) {
                Some(v) => v,
                None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
            };
            if used > debit.credits_posted {
                return Ok(StageOutcome::Rejected(LedgerError::ExceedsCredits));
            }
        }
        if credit.flags.contains(AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS) {
            let used = match credit.credits_posted.checked_add(credit.credits_pending) {
                Some(v) => v,
                None => return Ok(StageOutcome::Rejected(LedgerError::Overflow)),
            };
            if used > credit.debits_posted {
                return Ok(StageOutcome::Rejected(LedgerError::ExceedsDebits));
            }
        }
```

Add `AccountFlags` to the model import in `ledger.rs` (merge into the existing `use crate::model::{...};`): ensure it includes `AccountFlags`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p bluedb-ledger`
Expected: PASS — all tests including the two new constraint tests.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-ledger/src/ledger.rs
git commit -m "bluedb-ledger: enforce balance constraints on transfers"
```

---

## Task 11: Conservation property test + lints + crate docs

**Files:**
- Modify: `crates/bluedb-ledger/src/ledger.rs`

- [ ] **Step 1: Write a multi-transfer conservation test**

Add to the `tests` module in `ledger.rs`:

```rust
    #[tokio::test]
    async fn conservation_holds_over_a_sequence() {
        use crate::model::{NewAccount, Transfer};
        let database = writer_database().await;
        let ledger = Ledger::new(&database);
        // Four unconstrained accounts in one ledger.
        ledger
            .create_accounts(
                &[NewAccount::new(1, 7), NewAccount::new(2, 7), NewAccount::new(3, 7), NewAccount::new(4, 7)],
                1,
            )
            .await
            .unwrap();

        // A deterministic spread of transfers (ids unique, varied directions).
        let mut id = 1000u128;
        let pairs = [(1, 2, 100), (2, 3, 40), (3, 4, 25), (4, 1, 10), (1, 3, 7), (2, 4, 3)];
        for (d, c, amt) in pairs {
            let r = ledger.create_transfers(&[Transfer::new(id, d, c, amt, 7)], id as u64).await.unwrap();
            assert_eq!(r, vec![crate::model::CreateResult::Ok]);
            id += 1;
        }

        // Conservation: Σ debits_posted == Σ credits_posted across all accounts.
        let mut total_debits = 0u128;
        let mut total_credits = 0u128;
        for acct_id in 1..=4u128 {
            let a = ledger.lookup_account(acct_id).await.unwrap().unwrap();
            total_debits += a.debits_posted;
            total_credits += a.credits_posted;
        }
        let moved: u128 = pairs.iter().map(|&(_, _, amt)| amt).sum();
        assert_eq!(total_debits, moved);
        assert_eq!(total_credits, moved);
        assert_eq!(total_debits, total_credits);
    }
```

- [ ] **Step 2: Run the full crate test suite**

Run: `cargo test -p bluedb-ledger`
Expected: PASS — every test green.

- [ ] **Step 3: Run clippy across the workspace**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings. Fix any that appear (common: remove the `#[allow(dead_code)]` on `transfer_prefix`/`account_prefix` only if clippy still complains — they are intentionally unused until later plans, so keep the `allow`).

- [ ] **Step 4: Run the whole workspace test suite (no regressions)**

Run: `cargo test --workspace`
Expected: PASS — the bluedb-sql changes from Tasks 1–2 and the new crate all green.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-ledger/src/ledger.rs
git commit -m "bluedb-ledger: conservation property test for the core engine"
```

---

## Self-Review (completed during planning)

**Spec coverage (Plan 1 portion):**
- §4 architecture (crate, shared substrate + lease, apply inside writer) → Tasks 2, 7, 9.
- §5 data model (Account/Transfer/flags, u128, single-ledger rule, idempotency, overflow) → Tasks 4, 9.
- §5 balance constraints → Task 10.
- §7 native canonical records (postcard, new tags, u128-be keys) → Tasks 1, 5, 6.
- §9 atomicity (one WriteBatch) + writer-only (require_writer) → Tasks 8, 9.
- §10 unit + conservation tests → Tasks 9, 10, 11.
- **Deferred to Plans 2–3 (explicitly out of scope here):** linked chains, two-phase + timeouts, balancing transfers (§5), SQL projection (§7), HTTP routes (§8), Jepsen workload (§10). Listed in the header.

**Placeholder scan:** No "TBD"/"handle edge cases"/"similar to" — every code step is complete. The one intentional sketch-then-correct in Task 9 is called out with an implementer note and the final code is shown in full.

**Type consistency:** `StageOutcome { Applied(Transfer), Exists, Rejected(LedgerError) }` is produced by `stage_transfer` and consumed by `create_transfers` (Task 9). `LedgerError` variants used in tests (`AccountsMustDiffer`, `AccountNotFound`, `LedgerMismatch`, `ExceedsCredits`, `ExceedsDebits`, `Overflow`) all exist in `model.rs` (Task 4). `AccountFlags`/`TransferFlags` constants match between model and usage. `Database::substrate()`/`write_lease()` (Task 2) match `Ledger::new` (Task 7). `external_key`/`external_prefix`/`TAG_EXTERNAL_BASE` (Task 1) match `LedgerKeyspace` (Task 5).

---

## Next plans (generated after Plan 1 lands green)

- **Plan 2 — engine completion:** linked all-or-nothing chains (chain-buffer staging), two-phase pending/post/void (+ lazy timeout expiry), balancing transfers. Extends `create_transfers`/`stage_transfer`; adds the pending-resolution + chain logic and their tests.
- **Plan 3 — integration:** SQL-readable projection (`ledger_accounts`/`ledger_transfers` via a `pub` row-encoder in `bluedb-sql`, written in the same batch), `/ledger/*` HTTP routes on `bluedb-server` with a monotonic timestamp source, and the Jepsen `ledger` workload + invariant checker (conservation, no-lost, no-double-apply across faults).
