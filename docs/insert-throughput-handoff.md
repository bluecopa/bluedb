# Handoff: single-row INSERT throughput (~100ms/insert)

**Status: RESOLVED (2026-06-14) via group commit (Option 2).** See the
"Resolution" section at the bottom. The diagnosis below is preserved for context.

This is a standalone write-up; you don't need prior context.

## Symptom

Single-row autocommit `INSERT`s into `bluedb-sql` (GlueSQL-on-SlateDB) take
**~100 ms each**. This makes single-row write workloads ~10 inserts/sec and
makes bulk loads (and the SQL conformance test suite) impractically slow.

**Measured evidence:** loading 1,200 rows as 1,200 single-row autocommit
`INSERT` statements into in-memory SlateDB took **~124 s (≈103 ms/insert)**. A
selective join over the loaded tables added ~0 s, so the cost is entirely the
inserts, not query execution. (A ~8,000-insert corpus took ~10 min.)

## Root cause

In `crates/bluedb-sql/src/storage.rs`:

- GlueSQL's executor wraps every non-transaction statement in `begin(true)`
  (autocommit). bluedb's `Transaction::begin(autocommit = true)` returns `false`
  → **write-through mode** (no overlay; the write goes straight to SlateDB).
- In write-through, each `INSERT` calls SlateDB's `Db::put(...).await` with
  **default write options**.
- SlateDB's default `WriteOptions` has **`await_durable: true`**, so every
  `put` blocks until the write is **durable in object storage** (WAL/SST/
  manifest round trip) before returning. On object storage that's ~100 ms.
- This defeats SlateDB's design: it is built to **batch** writes (memtable →
  periodic SST flush). Forcing per-write durability serializes every single-row
  insert behind its own object-store round trip.

The explicit-transaction path (`Transaction::commit` → one `WriteBatch` →
`Db::write`) is fine — it's **one** durable write per `COMMIT`, which amortizes
correctly. The problem is specifically the **autocommit single-row** path doing
one synchronous durable write **per row**.

Relevant code (reference by function, line numbers may have shifted):
- `storage.rs` — `Transaction::begin` (autocommit → write-through), the
  autocommit `writer().put(&key, &value).await`, and `Transaction::commit`
  (`WriteBatch` + `writer().write(batch).await`).
- `connection.rs` — `Database::flush()` already exists (calls `Db::flush()`);
  it's the natural explicit-durability lever.

## Why it matters

An OLTP database that does ~10 single-row inserts/sec is not viable for real
workloads, and it's the dominant cost in any test/bench that loads data. This is
likely the single highest-impact perf issue in `bluedb-sql` today.

## Fix options

### Option 1 — Relaxed durability on autocommit + background/periodic flush (recommended)
Issue autocommit writes with `WriteOptions { await_durable: false }` (via
`Db::put_with_options` / `Db::write_with_options` — confirm exact API against the
pinned slatedb 0.13). The write lands in the WAL/memtable and returns
immediately; SlateDB flushes to object storage in the background / on an
interval. Durability is then achieved at flush points.

- **Pro:** the big throughput win; this is the intended SlateDB usage pattern.
- **Con / decision needed:** a write acked with `await_durable: false` is **not
  guaranteed durable** until the next flush. On crash/failover before a flush,
  recently-acked writes can be lost. This **interacts with the HA model**
  (single-writer lease + `Database::flush()` on graceful step-down). You must
  define and document the durability contract, e.g. *"autocommit writes are
  durable-on-flush; flush runs every N ms / N writes, and on graceful
  step-down."* Verify failover cannot ack-then-lose.

### Option 2 — Group commit
Coalesce concurrent autocommit writes into a single durable object-store write,
amortizing the round trip across many writers. Stronger durability than Option 1
but more machinery, and it doesn't help a single serial writer.

### Option 3 — Encourage transactional bulk loads
Batch inserts in one `BEGIN … COMMIT` (already → one durable `WriteBatch`).
Doesn't help clients doing single-row autocommit inserts, and note the **related
bug** below.

**Recommendation:** Option 1 (relaxed durability + periodic/explicit flush),
with the durability contract worked out against the HA/failover design. Consider
Option 2 for the strongly-durable path. The existing `Database::flush()` +
single-writer lease are the levers to build the contract on.

## Verification plan

1. **Reproduce:** time N (e.g. 1,000) single-row autocommit inserts into
   in-memory SlateDB; confirm ~100 ms each.
2. **Apply** relaxed durability to the autocommit write path; re-measure; expect
   a 1–2 order-of-magnitude speedup.
3. **Durability check:** write without flushing, drop/reopen the DB, confirm
   exactly what is and isn't lost; then confirm the chosen flush points (step-
   down, interval) preserve the HA durability contract. A small crash/recovery
   test (and the Jepsen suite) should cover this.
4. **Regression:** run the existing `bluedb-sql` transaction/isolation tests
   (`crates/bluedb-sql/tests/{transactions,isolation}.rs`) — these assert
   snapshot isolation and that concurrent write transactions don't lose updates;
   relaxed durability must not break them.

## Related (separate) bug — multi-row VALUES

GlueSQL/sqlparser **rejects large multi-row `INSERT ... VALUES (..),(..),…`** —
a 50-tuple multi-row insert failed to parse (`parser: ParserError`), while
single-row inserts work. This blocks the obvious "batch the load into one
statement" workaround and is worth a separate look (likely a parser recursion/
list limit). Single-row-per-statement is currently the only working insert form,
which is exactly the slow path above.

## Resolution (2026-06-14)

Fixed via **group commit (Option 2)**, not relaxed durability — so the strong
durability contract is kept (`await_durable: true`; an acked write is durable in
object storage before the ack) and the Jepsen kill/partition tests stay green.

What changed (`crates/bluedb-sql/src/storage.rs`, `connection.rs`):

- **Autocommit writes no longer take the write lease.** The lease previously
  serialized every writer, so only one `Db::write` was ever in flight and
  SlateDB's WAL never had concurrent writes to coalesce. Now autocommit commits
  run concurrently and SlateDB's WAL **group-commits** them into one object-store
  flush. The lease is now held only for explicit `BEGIN..COMMIT` blocks (to keep
  their read-modify-writes serializable).
- **Auto-increment row keys come from a shared in-memory counter**
  (`SeqAllocator`), not a `max(...)+1` scan. This is what makes dropping the lease
  safe: concurrent autocommit `INSERT`s still get distinct keys (the counter is
  atomic under a brief map lock), so they can't clobber each other. The counter
  seeds lazily from the live committed max per table and re-derives after
  failover. (This also removes the previous O(n) per-insert max-scan.)

Measured (Jepsen grow-only-set, concurrency 10, 45s, real 3-node cluster):
**450 → 2052 acked inserts (~4.5×)**, `:valid? true`, `lost-count 0`; the kill
run sustained 2172 acked inserts across crash-failover with zero loss. The
durability + isolation regression tests (`tests/{transactions,isolation}.rs`)
still pass.

**Note:** this is the *concurrent* throughput win (group commit needs multiple
in-flight writers). A single serial writer still pays ~one durable round-trip per
insert — for bulk loads from one client, wrap inserts in a `BEGIN..COMMIT` (one
`WriteBatch`). The multi-row `VALUES` parser bug above is still open. The
remaining auto-increment-counter improvement (persist it instead of re-deriving
from a live scan on first touch) is optional; the lazy re-derive is correct.
