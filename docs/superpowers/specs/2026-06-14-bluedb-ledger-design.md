# bluedb-ledger — a TigerBeetle-style double-entry ledger on bluedb

- **Date:** 2026-06-14
- **Status:** Approved (design) — pending implementation plan
- **Author:** Satya + Claude
- **Related:** Books/rulebooks architecture (PostingRuleBook + AccountingRuleBook → JE-dataset sink)

## 1. Motivation

We want TigerBeetle's *ledger primitives* — accounts, transfers, balance
constraints, two-phase transfers, linked atomic chains — without TigerBeetle's
architecture. TigerBeetle's defining property (millions of transfers/sec on a
replicated log over local NVMe, with deterministic storage-fault recovery) is
the *opposite* tradeoff to bluedb's (object-storage-native, single serial
writer, cloud-elastic, max operability). We don't need Visa-scale throughput;
we need **correct, strict-serializable double-entry with auditability** for an
R2R / posting workload.

The key realisation: the only hard prerequisite for these primitives is a
**deterministic serialized writer with safe read-modify-write under failover** —
and bluedb already has that, Jepsen-proven (the `counter` workload proves
no-lost-update RMW; the `set` workload proves no-lost-acked-writes through
kill/partition/pause; `unique` proves the same-key race is closed). TigerBeetle's
data model is two fixed structs and an `apply()` function; the consensus and
durability beneath it are exactly what bluedb provides. So we build the structs
+ state machine natively on top of bluedb and inherit the rest.

This also maps almost 1:1 onto the Books/rulebooks direction: `Account.code` =
chart-of-accounts code; `Account.ledger` = book partition (one `ledger` value per
GAAP for additive-delta multi-GAAP); a balanced multi-line journal entry = a
`linked` transfer chain; the JE-dataset sink = the persisted transfer log.

## 2. Goals / Non-goals

### Goals
- A native, typed double-entry ledger: `Account` / `Transfer` mirroring
  TigerBeetle's model, with **u128** amounts.
- **Full TigerBeetle-parity primitives**: balance constraints, two-phase
  (pending → post / void) transfers with timeouts, linked all-or-nothing
  chains, and balancing transfers (transfer up-to-available).
- Atomic, strict-serializable application, durable-before-ack, correct across
  writer failover — reusing bluedb's existing single-writer + lease + epoch
  fencing + `WriteBatch` machinery (no new consensus or durability code).
- **Hybrid persistence**: a fast native canonical record + a SQL-readable
  projection (`ledger_accounts` / `ledger_transfers`) so R2R reporting can
  `SELECT`/query through the existing GlueSQL + REST surface.
- A Jepsen `ledger` workload that adversarially proves the ledger invariants
  hold under faults.

### Non-goals (v1)
- Matching TigerBeetle's throughput or its local-disk fault model (we delegate
  durability to object storage / SlateDB, as the rest of bluedb does).
- A query/reporting engine beyond what GlueSQL already offers over the
  projection.
- Multi-currency FX conversion logic (the `ledger` field *partitions* by
  currency/book; cross-ledger transfers are rejected — conversion is an
  application concern, expressible as two transfers in two ledgers later).
- Distributed/sharded ledgers (single logical writer, same as the rest of
  bluedb).

## 3. Locked decisions

| Decision | Choice | Rationale |
|---|---|---|
| Layering | **Hybrid** — native KV write path + derived SQL projection | native-speed atomic writes *and* SQL reporting for R2R |
| Scope | **Full TB parity** — core + linked + two-phase + balancing + timeouts | maximum fidelity; matches JE/accrual needs |
| Amounts | **u128** (TigerBeetle parity) | exact; never negative because debit/credit buckets are separate; confirmed `Key::U128`/`Value::U128` exist in GlueSQL 0.19.0 |
| Timestamps | **caller/server-supplied monotonic timestamp at apply** | mirrors TB's "primary assigns the timestamp"; keeps the ledger wall-clock-free (bluedb deliberately avoids wall-clock deps on the data path) |
| Canonical vs projection | **native is canonical; projection is derived** in the same batch | writes stay fast; projection cannot drift from the source |
| Packaging | **`bluedb-ledger` crate** + a small `pub` row-encoder exposed from `bluedb-sql` | clean boundary + own test/Jepsen surface; single source of truth for projection-row encoding |

## 4. Architecture

New workspace crate **`bluedb-ledger`**:
- depends on `bluedb-storage` (`Substrate`: writer/reader, `get`, `scan_range`,
  `require_writer`, `Db::write`),
- depends on `bluedb-sql` for `Keyspace` and a newly-exposed projection-row
  encoder (so the projection row format has one definition),
- is constructed from a `bluedb_sql::Database` so it **shares that database's
  `write_lease`** — ledger applies serialize against SQL writes on the same node.

The ledger invents no durability or consensus. It runs its state machine inside
the active writer and reuses:

| Ledger requirement | Reused bluedb mechanism |
|---|---|
| atomic multi-account update (incl. linked chains) | a single `Db::write(WriteBatch)`, durable-before-ack |
| strict-serializable order / no lost updates | the shared `write_lease` (`Arc<Mutex<()>>`) held for the whole apply |
| no split-brain after failover | SlateDB `writer_epoch` fencing (inherited) |
| reads on standbys | `Substrate::Reader` (manifest-following), eventually-consistent |
| writer-only mutation | `Substrate::require_writer` → 503 on a replica |

### Rust API (sketch)
```rust
pub struct Ledger { /* shares Substrate + write_lease + a ledger keyspace */ }

impl Ledger {
    pub fn new(db: &bluedb_sql::Database) -> Self;          // shares lease + substrate

    pub async fn create_accounts(&self, specs: &[NewAccount], ts: u64)
        -> Result<Vec<CreateResult>>;                       // batch, per-item result
    pub async fn create_transfers(&self, transfers: &[Transfer], ts: u64)
        -> Result<Vec<CreateResult>>;                       // batch, linked/pending/etc.

    pub async fn lookup_account(&self, id: u128) -> Result<Option<Account>>;
    pub async fn lookup_transfer(&self, id: u128) -> Result<Option<Transfer>>;
}

pub enum CreateResult { Ok, Exists, Failed(LedgerError) }   // per-item, TB-style
```

## 5. Data model (u128, TigerBeetle-faithful)

```rust
pub struct Account {
    pub id: u128,
    pub ledger: u32,        // currency / book partition; transfers stay within one
    pub code: u16,          // app-defined chart-of-accounts code
    pub flags: AccountFlags,
    pub debits_pending: u128,
    pub debits_posted: u128,
    pub credits_pending: u128,
    pub credits_posted: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub timestamp: u64,     // assigned at creation
}

bitflags AccountFlags {
    LINKED                       = 1 << 0,  // account-creation chain
    DEBITS_MUST_NOT_EXCEED_CREDITS  = 1 << 1,
    CREDITS_MUST_NOT_EXCEED_DEBITS  = 1 << 2,
    HISTORY                      = 1 << 3,  // (reserved; see §11)
}

pub struct Transfer {
    pub id: u128,
    pub debit_account_id: u128,
    pub credit_account_id: u128,
    pub amount: u128,
    pub pending_id: u128,   // 0 unless post_pending/void_pending
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub ledger: u32,
    pub code: u16,
    pub flags: TransferFlags,
    pub timeout: u32,       // seconds; only meaningful with PENDING
    pub timestamp: u64,     // assigned at apply
}

bitflags TransferFlags {
    LINKED                = 1 << 0,  // linked to the NEXT transfer (chain)
    PENDING               = 1 << 1,  // two-phase reserve
    POST_PENDING_TRANSFER = 1 << 2,  // settle a pending (amount ≤ original)
    VOID_PENDING_TRANSFER = 1 << 3,  // release a pending
    BALANCING_DEBIT       = 1 << 4,  // transfer min(amount, available) on debit side
    BALANCING_CREDIT      = 1 << 5,
}
```

### Invariants enforced at apply
- **Single ledger:** `transfer.ledger == debit.ledger == credit.ledger`; else
  reject. Debit and credit accounts must differ.
- **Balance constraints:** after applying, if the debit account has
  `DEBITS_MUST_NOT_EXCEED_CREDITS` then
  `debits_posted + debits_pending ≤ credits_posted`; symmetric for
  `CREDITS_MUST_NOT_EXCEED_DEBITS` on the credit account. Violation → reject the
  transfer (and its whole linked chain).
- **Idempotency:** creating an account/transfer whose `id` already exists →
  `Exists` (not an error that aborts the batch, unless it is inside a linked
  chain). Because apply holds the exclusive lease, existence is checked against
  live committed state with no TOCTOU.
- **u128 arithmetic:** all balance updates are checked adds/subs; overflow →
  reject.

### Two-phase semantics
- `PENDING`: `debit.debits_pending += amount`, `credit.credits_pending +=
  amount`. Records `expires_at = timestamp + timeout` (0 = no expiry).
- `POST_PENDING_TRANSFER` (references `pending_id`, may post a `amount ≤`
  original): on the original's accounts, `debits_pending -= original`,
  `debits_posted += posted`, `credits_pending -= original`, `credits_posted +=
  posted`. The pending is marked resolved.
- `VOID_PENDING_TRANSFER`: `debits_pending -= original`, `credits_pending -=
  original`; no posted change. Pending marked resolved.
- **Timeout:** a pending referenced after `expires_at` is treated as already
  voided (post/void → fails with `pending_expired`). Expiry is **lazy** (checked
  on reference) plus an optional explicit sweep; no background wall clock. A
  resolved/expired pending cannot be posted or voided twice.

### Balancing transfers
`BALANCING_DEBIT` / `BALANCING_CREDIT`: the *actual* amount is
`min(amount, available)` where available is computed from the constrained
account's current balances and its constraint flag. The persisted transfer
records the actual amount. A balancing transfer that resolves to 0 still
succeeds (records a 0 transfer), matching TB.

### Linked chains
A maximal run of transfers where each has `LINKED` set except the last is a
**chain**: all succeed or all fail. If any member fails validation, the entire
chain is rolled back (no deltas applied) and every member's result is
`Failed(linked_event_failed)` except the actual offender, which carries its real
error. Implemented by validating+staging the whole chain against the in-memory
working set before committing any of its deltas.

## 6. The `apply` algorithm

`create_transfers(transfers, ts)`:
1. **Acquire the shared `write_lease`** (exclusive). We are now the sole mutator
   of this Db, so live reads are consistent — no separate snapshot needed.
2. **Working set:** a `HashMap<u128, Account>` populated read-through from the
   writer `Db` (native records) as accounts are touched.
3. **Process item-by-item, chain-aware:**
   - resolve the chain boundary (run of `LINKED`),
   - for each member: load referenced accounts, validate (single-ledger,
     distinct accounts, two-phase rules, balancing resolution, constraint check,
     overflow check, idempotent id), and stage the balance deltas + the transfer
     record into a *pending chain buffer*,
   - if every member of the chain validates, fold the chain buffer into the
     working set + the output batch; otherwise discard the chain buffer and mark
     results failed.
   - assign `transfer.timestamp = ts` (monotonic; the server increments per
     apply — see §8).
4. **Assemble one `WriteBatch`:** for every mutated account and every accepted
   transfer, put the native canonical record **and** the derived SQL projection
   row (§7).
5. `writer.write(batch).await` — durable, atomic. On a replica this errors at
   step 1/4 via `require_writer` → surfaced as 503.
6. Release the lease; return per-item `CreateResult`s.

`create_accounts` is the same shape (idempotent create, optional `LINKED`
account-creation chains, no balances to mutate beyond initialisation).

## 7. Hybrid persistence

### Canonical — native records
Encoded with **`postcard`** (compact, fast; already a workspace dep) under new
keyspace tags, within the existing tenant-prefixed scheme (see
`bluedb-sql/src/keyspace.rs`):

```
account:   <tenant> [TAG_LEDGER_ACCT=0x10] <id::u128-be>      -> postcard(Account)
transfer:  <tenant> [TAG_LEDGER_XFER=0x11] <id::u128-be>      -> postcard(Transfer)
```
`u128`-big-endian key suffixes keep id-ordered range scans. Post/void look the
original up directly by `pending_id` in the transfer keyspace — no separate
index required. An **optional** expiry index
`<tenant> [TAG_LEDGER_PENDING=0x12] <expires_at::u64-be> <id::u128-be>` supports
range-scanning expired pendings for the sweep (deferred if not needed by v1
tests).

New tag bytes are chosen above the SQL tags (`0x01/0x02/0x03`) so the namespaces
never interleave; the ledger and SQL data coexist in one Db keyspace per tenant.

### Projection — SQL-readable rows
In the **same `WriteBatch`**, each account/transfer is also written as a GlueSQL
`StoredRow` into the `TAG_DATA` namespace for tables **`ledger_accounts`** and
**`ledger_transfers`**, with `Key::U128(id)` as the primary key. A GlueSQL
`Schema` record (`TAG_SCHEMA`) for each table is written on first use, declaring
typed columns (`UINT128`/`UINT32`/`UINT16` etc.), so:
- `GET /tables/ledger_accounts?...` and `POST /sql "SELECT ... FROM
  ledger_transfers"` work through the existing engine, and
- ordered scans / secondary indexes on the projection are available for free.

The projection is **derived from** the native record during apply; `bluedb-sql`
exposes a small `pub fn ledger_projection_row(tenant, table, key, columns) ->
(Vec<u8>, Vec<u8>)` (and schema helper) so the row/`StoredRow` encoding has a
single definition shared with the SQL store. The projection is read-only from
SQL's perspective (writers go through the ledger API); direct SQL writes to these
tables are out of scope (and can be guarded later).

## 8. HTTP surface (bluedb-server)

New routes on the existing axum router, all writes gated by `require_active`:

| Method + path | Action |
|---|---|
| `POST /ledger/accounts` | create accounts (batch JSON array) → per-item results |
| `POST /ledger/transfers` | create/apply transfers (batch; linked/pending/post/void/balancing) → per-item results |
| `GET /ledger/accounts/{id}` | look up one account (balances) |
| `GET /ledger/transfers/{id}` | look up one transfer |

Reads are additionally available via the projection: `GET
/tables/ledger_accounts`, `/tables/ledger_transfers`, and `/sql`.

**Timestamp source:** the server assigns a **monotonic** `ts` per apply
(non-decreasing; e.g. `max(prev+1, wall_millis)` kept on the writer), passed into
`create_*`. This mirrors TigerBeetle's primary-assigned timestamp and keeps the
ledger crate itself clock-free. The monotonic counter is re-seeded from the max
persisted `timestamp` after a failover (like the `SeqAllocator`).

## 9. Consistency & failover semantics

- **Atomic:** all writes for one apply (including a whole linked chain) are one
  `WriteBatch` — a crash commits all or none.
- **Durable-before-ack:** SlateDB `WriteOptions { await_durable: true }` (the
  bluedb default) means an acked transfer survives writer death — the property
  the `set` workload already verifies.
- **Strict-serializable:** the exclusive `write_lease` for the whole apply gives
  a single serial order; combined with durable commit and replica reads bounded
  by the manifest, the ledger presents the same consistency class TigerBeetle
  does (single logical writer).
- **Failover:** a promoted standby opens the writer (epoch bump fences the old
  one), re-seeds its in-memory counters (`SeqAllocator`-style timestamp + any
  per-table seeds) from persisted state, and continues. A fenced old writer's
  `write` fails — no split-brain, no double-apply.
- **Replica reads:** `lookup_*` and projection `SELECT`s served by a standby are
  eventually-consistent (manifest lag), identical to today's read semantics.

## 10. Testing

### Unit (in-memory SlateDB over `InMemory` object store)
- balance-constraint accept/reject (both flags, both directions),
- two-phase: pending → post (full & partial), pending → void, double-resolve
  rejected, expired pending rejected,
- balancing transfers (resolves to min, including 0),
- linked chains: all-or-nothing, offender carries the real error,
- idempotent create (`Exists`), single-ledger + distinct-account rejects,
- u128 overflow rejects.

### Invariant / property
- **Conservation:** after any sequence, per ledger,
  `Σ debits_posted == Σ credits_posted` and `Σ debits_pending == Σ
  credits_pending`.
- **Model-based balances:** a reference model replays the accepted transfers and
  asserts every account's four buckets match the engine — catches both lost and
  double-applied transfers.

### Jepsen — new `ledger` workload
Concurrent `create_transfers` over a small account set (a few accounts in one
ledger) under `kill | partition | pause | chaos`. The client follows the active
writer (existing leader discovery). The checker proves, over the recorded
history + a final read of all accounts/transfers:
1. **Conservation** holds in the final state,
2. **No acked transfer lost** — every transfer create that returned `Ok` is
   present and reflected in balances,
3. **No double-apply** — reconstructed expected balances (from the set of acked
   transfers) equal actual balances,
4. **No constraint breach** — no account violates its flags in any observed
   state.

This sits alongside `set` / `list-append` / `counter` / `unique` as the ledger's
adversarial proof across failover.

## 11. Phasing (milestones within Full-parity)

1. **Skeleton + model:** `bluedb-ledger` crate, `Account`/`Transfer` + flags,
   native keyspace encoding, `bluedb-sql` projection-encoder `pub` helper.
2. **Core apply:** account creation (idempotent, flags) + single transfers +
   balance constraints + atomic `WriteBatch` + shared lease. Unit + conservation
   tests.
3. **Linked chains:** all-or-nothing staging. Tests.
4. **Two-phase + balancing:** pending/post/void + balancing resolution. Tests.
5. **Timeouts:** lazy expiry (+ optional sweep / expiry index). Tests.
6. **Projection + HTTP:** SQL projection tables wired into the batch; `/ledger/*`
   routes; monotonic timestamp source on the writer.
7. **Jepsen `ledger` workload** + invariant checker.
8. **Soak:** `--test-count` runs under faults; confirm all-green.

Each milestone keeps the workspace green (all tests + clippy) before the next.

## 12. Open questions / risks

- **GlueSQL projection writes vs. ledger writes:** the projection tables must be
  written *only* by the ledger to stay consistent. v1 simply documents this;
  a later guard could reject direct `/tables/ledger_*` mutations. (Low risk.)
- **`HISTORY` flag:** TigerBeetle records per-transfer balance history when set.
  v1 reserves the flag bit but does not implement history materialisation —
  the transfer log already provides the audit trail. (Deferred, not blocking.)
- **Timestamp monotonicity across failover:** re-seeding from the max persisted
  `timestamp` must happen before the new writer serves applies; covered by the
  same pattern as `SeqAllocator` seeding. Verify in milestone 6 + Jepsen.
- **Projection storage cost:** every account/transfer is stored twice (native +
  projection). Acceptable for v1; if it bites, the projection can become
  opt-in per deployment.

## 13. Out of scope / future
- Per-transfer balance history (`HISTORY`).
- Cross-ledger / FX transfers.
- Account/transfer query filters beyond GlueSQL over the projection.
- Sharding / multiple concurrent ledgers per node beyond tenant namespacing.
- Wiring the ledger as the execution substrate for `PostingRuleBook` (the Books
  work) — this spec deliberately stops at the ledger primitive; Books consumes
  it later.
