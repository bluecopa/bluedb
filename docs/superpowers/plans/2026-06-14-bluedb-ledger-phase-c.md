# bluedb-ledger Phase C — timeouts (expiry index + apply-time sweep)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Un-gate pending transfers with a nonzero `timeout` and implement TigerBeetle's timeout semantics: `overflows_timeout` (66) at reserve, an expiry index, an apply-time sweep that auto-voids expired pendings (releasing reservations), and lazy `pending_transfer_expired` (53) on a resolution past expiry — all under a deterministic, injectable clock.

**Architecture:** A pending with `timeout != 0` reserves as before and additionally records an **expiry-index** entry `<expires_at::u64-be><pending_id::u128-be>` (tag `0x13`), where `expires_at = timestamp + timeout * 1e9` (ns). On every `create_transfers`, before processing the batch, a **sweep** scans the expiry index for `expires_at <= now`, releases each expired pending's reservation, marks its pending-state `Expired`, and deletes the index entry — folded into the same atomic batch. A resolution of an expired pending is rejected lazily with `pending_transfer_expired`. Resolving a timed pending before expiry deletes its index entry. The clock is injectable (`Clock::Wall` in production, a manual clock in tests) and drives both timestamp assignment and the sweep.

**Reference:** spec §5 (timeout expiry), §11 (expiry index tag 0x13); TB create_transfers codes 53 (`pending_transfer_expired`) and 66 (`overflows_timeout`).

---

## Design decisions
- `expires_at = timestamp + timeout * 1_000_000_000`. `overflows_timeout` (66) fires when `timestamp + timeout*1e9 > 2^63 - 1` (TB's max) or the multiply/add overflows. Checked at reserve against the **prospective** assigned timestamp (`max(watermark_or_last + 1, now)`), which equals the actual timestamp this item will get on accept.
- Sweep runs at the START of `create_transfers` (and only there — `create_accounts` has no pendings). "Now" is captured once per apply from the clock; the same value drives timestamp assignment and expiry.
- An expiry does **not** create a transfer record and does **not** consume a timestamp; it writes a pending-state `Expired` record + releases the reservation + deletes the index entry.
- Expired iff `expires_at <= now`.
- Clock is injected via a `Clock` enum the `Ledger` holds; `Ledger::new` uses `Clock::Wall`. A `pub(crate) fn with_clock` is added for tests.

## TB order touchpoints
- Reserve (stage_pending): … 60/61 → 64/65 → **66 overflows_timeout** → 67/68. (66 only when `timeout != 0`.)
- Resolution (stage_resolution): … 49/50 amount → 51/52 already posted/voided → **53 pending_transfer_expired** (resolved-state `Expired`, or lazy `expires_at <= now`).

---

## Task 1: injectable clock + expiry keyspace + PendingStatus::Expired

**Files:** `ledger.rs` (Clock), `model.rs` (PendingStatus), `keyspace.rs` (expiry key), `store.rs` (scan helper)

- [ ] **Step 1: `Clock` (ledger.rs)**
```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Time source for timestamp assignment and timeout expiry. `Wall` reads the
/// system clock; `Manual` is an advanceable clock for deterministic tests.
#[derive(Clone)]
pub enum Clock { Wall, Manual(Arc<AtomicU64>) }
impl Clock {
    fn now_ns(&self) -> u64 {
        match self { Clock::Wall => crate::store::now_ns(), Clock::Manual(a) => a.load(Ordering::SeqCst) }
    }
}
```
Add `clock: Clock` to `Ledger`; `Ledger::new` sets `Clock::Wall`; add `pub(crate) fn with_clock(database: &Database, clock: Clock) -> Self`.

- [ ] **Step 2: `TimestampSource` takes `now`**: `fn peek(&self, now: u64) -> u64 { now.max(self.last.saturating_add(1)) }` and `fn next(&mut self, now: u64) -> u64 { self.last = self.peek(now); self.last }`. Drop the internal `now_ns()` call. Update `create_accounts`/`create_transfers` to capture `let now = self.clock.now_ns();` and pass it.

- [ ] **Step 3: `PendingStatus::Expired` (model.rs)** — add the variant; update its doc.

- [ ] **Step 4: expiry keyspace (keyspace.rs)**
```rust
const TAG_EXPIRY: u8 = TAG_EXTERNAL_BASE + 3; // 0x13
pub(crate) fn expiry_key(&self, expires_at: u64, pending_id: u128) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(24);
    suffix.extend_from_slice(&expires_at.to_be_bytes());
    suffix.extend_from_slice(&pending_id.to_be_bytes());
    self.ks.external_key(TAG_EXPIRY, &suffix)
}
pub(crate) fn expiry_prefix(&self) -> Vec<u8> { self.ks.external_prefix(TAG_EXPIRY) }
/// Exclusive scan upper bound covering all entries with `expires_at <= now`.
pub(crate) fn expiry_scan_end(&self, now: u64) -> Vec<u8> {
    self.ks.external_key(TAG_EXPIRY, &now.saturating_add(1).to_be_bytes())
}
```
Add a keyspace test: ordering by `expires_at` then `pending_id`; `expiry_scan_end(now)` separates `<=now` from `>now`.

- [ ] **Step 5: store scan helper (store.rs)**
```rust
/// Pending ids whose expiry-index entry has `expires_at <= now`, with the full
/// index key (for deletion). Returned in ascending `expires_at` order.
pub(crate) async fn scan_expired(
    substrate: &Substrate, ks: &LedgerKeyspace, now: u64,
) -> Result<Vec<(Vec<u8>, u128)>> {
    let start = ks.expiry_prefix();
    let end = ks.expiry_scan_end(now);
    let mut out = Vec::new();
    let mut iter = substrate.scan_range(&start, Some(&end)).await?;
    while let Some(kv) = iter.next().await? {
        let key = kv.key.to_vec();
        // suffix layout: <expires_at::8><pending_id::16>; pending_id = last 16 bytes.
        let n = key.len();
        let pid = u128::from_be_bytes(key[n - 16..].try_into().context("expiry key pending_id")?);
        out.push((key, pid));
    }
    Ok(out)
}
```

## Task 2: reserve overflow + expiry index write + un-gate timeout

**Files:** `model.rs` (classify), `ledger.rs` (stage_pending, batch)

- [ ] **Step 1: un-gate PENDING+timeout in `classify`** — remove the `if t.timeout != 0 { Gated }` line; a `PENDING` with any timeout is now `PendingReserve`.

- [ ] **Step 2: `overflows_timeout` in `stage_pending`** — after the 64/65 total-overflow guards and before 67/68, when `t.timeout != 0`:
```rust
let ts_ns = prospective_ts; // = ts.peek(now) for this item
let timeout_ns = (t.timeout as u64).checked_mul(1_000_000_000);
let expires = timeout_ns.and_then(|d| ts_ns.checked_add(d));
match expires {
    Some(e) if e <= (i64::MAX as u64) => {} // ok (TB cap is 2^63 - 1)
    _ => return Ok(Err(R::OverflowsTimeout)),
}
```
Pass `prospective_ts: u64` (= `ts.peek(now)`) into `stage_pending`; compute it in the loop right before the `match op`.

- [ ] **Step 3: write the expiry-index entry on accept** — in the batch-assembly loop over `accepted`, for each transfer with `flags.contains(PENDING) && timeout != 0`, compute `expires_at = t.timestamp + t.timeout as u64 * 1e9` and `batch.put(self.keyspace.expiry_key(expires_at, t.id), [1u8])`.

## Task 3: sweep + lazy expiry + index removal on resolve

**Files:** `ledger.rs`

- [ ] **Step 1: `ApplyState.expiry_removals: Vec<Vec<u8>>`** — index keys to delete (from resolutions + sweep).

- [ ] **Step 2: `sweep_expired` (runs first in `create_transfers`)**
```rust
async fn sweep_expired(&self, now: u64, state: &mut ApplyState) -> Result<()> {
    for (key, pending_id) in scan_expired(&self.substrate, &self.keyspace, now).await? {
        // Skip if already resolved (committed) — its index entry is stale; drop it.
        if get_pending_state(&self.substrate, &self.keyspace, pending_id).await?.is_some() {
            state.expiry_removals.push(key);
            continue;
        }
        let pending = match get_transfer(&self.substrate, &self.keyspace, pending_id).await? {
            Some(p) => p, None => { state.expiry_removals.push(key); continue; }
        };
        // Release the reservation.
        let mut debit = self.load_account(pending.debit_account_id, state).await?
            .ok_or_else(|| anyhow::anyhow!("ledger invariant: expired pending debit missing"))?;
        let mut credit = self.load_account(pending.credit_account_id, state).await?
            .ok_or_else(|| anyhow::anyhow!("ledger invariant: expired pending credit missing"))?;
        debit.debits_pending = debit.debits_pending.checked_sub(pending.amount)
            .ok_or_else(|| anyhow::anyhow!("ledger invariant: debits_pending underflow on expiry"))?;
        credit.credits_pending = credit.credits_pending.checked_sub(pending.amount)
            .ok_or_else(|| anyhow::anyhow!("ledger invariant: credits_pending underflow on expiry"))?;
        state.working.insert(debit.id, debit); state.dirty.insert(debit.id);
        state.working.insert(credit.id, credit); state.dirty.insert(credit.id);
        state.resolved.insert(pending_id, PendingStatus::Expired);
        state.expiry_removals.push(key);
    }
    Ok(())
}
```
Call `self.sweep_expired(now, &mut state).await?;` immediately after building `state`, before the per-transfer loop.

- [ ] **Step 3: lazy expiry + `Expired` in `stage_resolution`** — pass `now: u64` in. In the already-resolved match add `Some(PendingStatus::Expired) => return Ok(Err(R::PendingTransferExpired))`. After that match, before applying, add the lazy check:
```rust
if pending.timeout != 0 {
    let expires = pending.timestamp + pending.timeout as u64 * 1_000_000_000;
    if expires <= now { return Ok(Err(R::PendingTransferExpired)); }
}
```

- [ ] **Step 4: delete the index entry when resolving a timed pending** — in `stage_resolution`, on success, if `pending.timeout != 0` push `self.keyspace.expiry_key(pending.timestamp + pending.timeout as u64 * 1e9, pending.id)` to `state.expiry_removals`.

- [ ] **Step 5: batch assembly** — apply `for key in &state.expiry_removals { batch.delete(key); }`; write pending-state for every `state.resolved` (now incl. `Expired`); change the write guard to `if !batch.is_empty() { writer.write(batch).await?; }` (keep the watermark put guarded by `!accepted.is_empty()` so a sweep-only batch doesn't bump it). Sweep-only batches (no accepted transfers) still commit account releases + Expired records + index deletes.

## Task 4: tests (ledger.rs)

Use the manual clock: `let clock = Clock::Manual(Arc::new(AtomicU64::new(T0))); let l = Ledger::with_clock(&db, clock.clone());` and advance via the `Arc<AtomicU64>`.

- [ ] **overflows_timeout**: reserve with a `timeout` so large that `timestamp + timeout*1e9 > 2^63` → `OverflowsTimeout`; reservation not applied. (Use `timeout = u32::MAX` with a large manual `now`.)
- [ ] **expiry index written**: reserve with timeout, assert an expiry entry exists (via a sweep that finds it after advancing the clock).
- [ ] **sweep auto-voids**: reserve (timeout=10s) at T0; advance clock past expiry; call `create_transfers(&[])`-equivalent (a no-op transfer batch, or a dummy regular transfer) to trigger the sweep; assert `debits_pending`/`credits_pending` released and pending-state is Expired.
- [ ] **lazy expiry on resolve**: reserve (timeout) at T0; advance past expiry WITHOUT a prior sweep; post it → `PendingTransferExpired`; reservation released by the same call's sweep (assert pending cleared).
- [ ] **resolve before expiry deletes index**: reserve (timeout); post before expiry; advance past old expiry; sweep → nothing to release (already posted), no double-release; balances stable.
- [ ] **no timeout = never expires**: reserve with timeout 0; advance clock far; sweep → still outstanding (pending intact).
- [ ] **sweep-only batch commits**: a `create_transfers` whose only effect is the sweep still persists the release (re-lookup after).
- [ ] Gated test: drop PENDING+timeout from the gated set (now handled); keep linked/balancing/closing/imported.

Run `cargo test -p bluedb-ledger` + `cargo clippy --workspace --all-targets -- -D warnings`. Commit. Spec-compliance + code-quality review; fix; mark Phase C complete.

> Note for triggering a sweep with no transfers: `create_transfers(&[])` returns early today only via the loop; ensure the sweep runs even for an empty slice (call sweep before the `is_empty` short-circuit; there is none — the loop just doesn't iterate). Confirm an empty batch still runs the sweep and commits if it released anything.
