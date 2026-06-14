# Transactions

[← SQL index](README.md)

bluedb supports multi-statement transactions with **snapshot isolation**.

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;     -- both updates land atomically
```

```sql
BEGIN;
DELETE FROM staging;
-- changed your mind:
ROLLBACK;   -- nothing was deleted
```

## Semantics

- **Atomic commit.** Everything between `BEGIN` and `COMMIT` is applied as a
  single atomic batch. `ROLLBACK` discards all of it. DDL, `CREATE INDEX`, and
  index maintenance inside the transaction roll back too.
- **Snapshot isolation.** Reads inside a transaction see a consistent
  point-in-time snapshot taken at `BEGIN`, plus the transaction's own buffered
  writes (read-your-own-writes). A long-running transaction never sees writes
  others commit after it began.
- **Read-your-own-writes.** A `SELECT` inside the transaction reflects rows the
  same transaction has already inserted/updated/deleted, in correct order.

## Concurrency

- bluedb is **single-writer** per database: only one transaction mutates a given
  database at a time, so there is no write-write conflict to detect or retry.
- **Autocommit** statements (any statement run outside an explicit `BEGIN`) are
  each their own durable transaction. Concurrent autocommit writes from
  different connections are coalesced into one object-storage flush (group
  commit) for throughput.
- An explicit `BEGIN … COMMIT` block serializes its read-modify-write against
  other explicit transactions.

> **Tip — bulk loads.** Each autocommit `INSERT` is one durable object-storage
> write. To load many rows fast, wrap them in a single transaction:
>
> ```sql
> BEGIN;
> INSERT INTO t VALUES (…);
> INSERT INTO t VALUES (…);
> -- … many rows …
> COMMIT;
> ```
