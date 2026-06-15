# Ledger

bluedb embeds a **TigerBeetle-style double-entry ledger** (`bluedb-ledger`) and
exposes it over HTTP at `/ledger/*`. Accounts and transfers are typed records
with 128-bit ids and amounts; every batch is applied inside bluedb's serialized
writer and committed as **one atomic write** (reusing the lease + epoch fencing +
durable-before-ack guarantees of the rest of the engine).

Canonical state lives as native records, but a **crash-consistent SQL
projection** dual-writes accounts and transfers into the `ledger_accounts` and
`ledger_transfers` tables — so balances are queryable through ordinary
[`/sql`](rest.md#post-sql-run-one-parameterized-statement) and
[`/tables`](rest.md) reads alongside everything else.

!!! note "Large integers cross the wire as strings"
    128-bit and 64-bit fields (`id`, `amount`, `user_data_*`, balances,
    `timestamp`, …) are serialized as **decimal strings** so JSON clients that
    parse numbers as `f64`/`i64` (browsers, some language runtimes) don't truncate
    large values. Inputs are lenient — a JSON number is accepted too — but outputs
    are always strings for those fields.

## `POST /ledger/accounts` — create accounts

The body is a single account object **or** an array of objects. The response is
one TigerBeetle-style result code per input item, in order.

```bash
curl -s -X POST localhost:8081/ledger/accounts \
  -H 'content-type: application/json' \
  -d '[
        {"id": "1", "ledger": 700, "code": 10},
        {"id": "2", "ledger": 700, "code": 10}
      ]'
```

```json
{
  "results": [
    {"index": 0, "id": "1", "result": "ok"},
    {"index": 1, "id": "2", "result": "ok"}
  ]
}
```

Fields: `id` (required, 128-bit), `ledger` (required, `u32`), and optional
`code` (`u16`), `flags` (`u16`), `user_data_128` / `user_data_64` /
`user_data_32`.

### Account flags

| Bit | Flag | Effect |
|-----|------|--------|
| `1 << 0` | `linked` | Chain this account to the next item in the batch (all-or-nothing) |
| `1 << 1` | `debits_must_not_exceed_credits` | Reject a transfer that would push debits above credits |
| `1 << 2` | `credits_must_not_exceed_debits` | Reject a transfer that would push credits above debits |
| `1 << 3` | `history` | Retain balance history |
| `1 << 4` | `imported` | Import an account with a caller-supplied timestamp |

## `POST /ledger/transfers` — create transfers

Same batch shape — a single object or an array — with one result code per item.

```bash
curl -s -X POST localhost:8081/ledger/transfers \
  -H 'content-type: application/json' \
  -d '{
        "id": "10",
        "debit_account_id": "1",
        "credit_account_id": "2",
        "amount": "100",
        "ledger": 700,
        "code": 1
      }'
```

```json
{ "results": [ {"index": 0, "id": "10", "result": "ok"} ] }
```

Fields: `id`, `debit_account_id`, `credit_account_id`, `amount`, and `ledger`
(required); optional `code`, `flags`, `pending_id`, `timeout`, and the
`user_data_*` set.

### Two-phase transfers

A transfer can move money in two phases — reserve, then resolve — driven by
flags:

| Bit | Flag | Effect |
|-----|------|--------|
| `1 << 0` | `linked` | Chain to the next transfer in the batch (all-or-nothing) |
| `1 << 1` | `pending` | A **reserve**: holds funds (pending debit/credit) without posting |
| `1 << 2` | `post_pending_transfer` | Post a prior pending transfer (named by `pending_id`) |
| `1 << 3` | `void_pending_transfer` | Void a prior pending transfer (release the reservation) |
| `1 << 4` | `balancing_debit` | Transfer up to the debit account's available balance |
| `1 << 5` | `balancing_credit` | Transfer up to the credit account's available balance |
| `1 << 6` | `closing_debit` | Close the debit account (must be pending) |
| `1 << 7` | `closing_credit` | Close the credit account (must be pending) |
| `1 << 8` | `imported` | Import a transfer with a caller-supplied timestamp |

A pending transfer reserves funds; a later `post_pending_transfer` (optionally
for a partial amount) posts them, and a `void_pending_transfer` releases them.
A pending transfer may carry a `timeout` (seconds) after which it expires
automatically. `linked` flags form an all-or-nothing chain: if any item in the
chain fails, the whole chain is rolled back (the others report
`linked_event_failed`).

### Result codes

`result` is the snake_case TigerBeetle result code. `ok` means the item applied;
anything else explains the rejection. A few common ones:

| Result | Meaning |
|--------|---------|
| `ok` | Applied |
| `linked_event_failed` | Rolled back because another item in its linked chain failed |
| `exists` | An identical record with this id already exists (idempotent replay) |
| `exceeds_credits` / `exceeds_debits` | Would violate an account's must-not-exceed flag |
| `pending_transfer_not_found` | `post`/`void` named a `pending_id` that doesn't exist |
| `pending_transfer_already_posted` / `_already_voided` | The pending transfer was already resolved |
| `pending_transfer_expired` | The pending transfer's `timeout` elapsed |

The full set is the TigerBeetle named result-code set (the engine has full
validation-order parity); the codes above are illustrative.

## `GET /ledger/accounts/{id}` — look up an account

Returns the canonical account state, or `404` if it doesn't exist:

```bash
curl -s localhost:8081/ledger/accounts/1
```

```json
{
  "id": "1",
  "ledger": 700,
  "code": 10,
  "flags": 0,
  "debits_pending": "0",
  "debits_posted": "100",
  "credits_pending": "0",
  "credits_posted": "0",
  "user_data_128": "0",
  "user_data_64": "0",
  "user_data_32": 0,
  "timestamp": "1718409600000000000"
}
```

## `GET /ledger/transfers/{id}` — look up a transfer

Returns the canonical transfer state, or `404`:

```bash
curl -s localhost:8081/ledger/transfers/10
```

## Querying balances via SQL

Because the canonical records are projected into `ledger_accounts` and
`ledger_transfers`, you can aggregate and join them like any other table through
[`/sql`](rest.md#post-sql-run-one-parameterized-statement):

```sql
-- A single account's posted balance
SELECT debits_posted, credits_posted FROM ledger_accounts WHERE id = $1;

-- Every transfer on a ledger
SELECT id, debit_account_id, credit_account_id, amount
FROM ledger_transfers WHERE ledger = $1;
```

The projection is dual-written into the same atomic batch as the canonical
records, so it is crash-consistent with them — a balance you read over SQL never
disagrees with the canonical state.
