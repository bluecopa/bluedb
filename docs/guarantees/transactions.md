# Transactions & isolation

bluedb supports multi-statement transactions (`BEGIN` / `COMMIT` / `ROLLBACK`)
with **snapshot isolation**, and its explicit-transaction path is validated up to
**strict-serializable** by the [Jepsen](jepsen.md) suite.

## Isolation level

| Property | bluedb |
|----------|--------|
| Base isolation | **Snapshot isolation** |
| Explicit `BEGIN…COMMIT` (Jepsen Elle) | **Serializable** (default), **strict-serializable** (with real-time order) |
| Dirty reads | No |
| Non-repeatable reads | No (snapshot taken at `BEGIN`) |
| Lost updates | No |

## How it works

- **`BEGIN`** captures a consistent point-in-time snapshot of the object-storage
  database and opens an in-memory write overlay.
- **Reads** inside the transaction merge the overlay over the snapshot — you see
  a stable view plus your own uncommitted writes (read-your-own-writes).
- **`COMMIT`** applies the whole overlay as a single atomic batch. **`ROLLBACK`**
  discards it. DDL, `CREATE INDEX`, and index maintenance inside the transaction
  commit or roll back with it.

Because bluedb is [single-writer](../concepts/single-writer.md), there is no
concurrent-writer conflict to detect: an explicit transaction holds the write
lease for its duration, so its read-modify-write is serialized against other
explicit transactions.

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;   -- both land atomically, or neither (ROLLBACK)
```

## Autocommit

A statement outside an explicit `BEGIN` is its own durable transaction.
Concurrent autocommit writes from different connections are coalesced into one
object-store flush (group commit) for throughput. Autocommit single-statement
read-modify-writes (e.g. `UPDATE c SET n = n + 1`) are serialized so they cannot
lose updates.

!!! tip "Bulk loads"
    Each autocommit `INSERT` is one durable object-storage write. Wrap large
    loads in a single `BEGIN … COMMIT` to batch them into one flush.

## What Jepsen checks

- **`list-append`** (Elle) reconstructs the transaction dependency graph from
  observed reads and flags any serializability anomaly (G0/G1/G2, write skew,
  lost update) — checked against `serializable` or `strict-serializable`.
- **`counter`** verifies concurrent autocommit increments lose nothing.
- **`unique`** verifies a racing `INSERT` of the same primary key is acked at
  most once.

See the full [Jepsen report](jepsen.md).
