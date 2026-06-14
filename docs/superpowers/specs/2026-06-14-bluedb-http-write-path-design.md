# Spec A — bluedb HTTP surface split & write-path throughput/safety

**Date:** 2026-06-14
**Status:** Design (awaiting review)
**Depends on:** nothing (foundation)
**Depended on by:** [Spec B — SQL-integrated FTS](2026-06-14-bluedb-sql-integrated-fts-design.md) (its index DDL lands on the DDL surface here)

## 1. Context & problem

bluedb exposes **HTTP only** — there is no client driver. The integration surface
is `bluedb-server` (axum) over `bluedb-engine`. Two problems motivate this spec.

### 1.1 The surface conflates trust levels and has no authz

Current endpoints (`crates/bluedb-server/src/lib.rs`):

- `POST /sql` — runs **arbitrary SQL** (DDL + DML + transactions). Gated only by
  `require_active()` (i.e. "am I the active writer") — **no authentication at all.**
  This is an unauthenticated arbitrary-SQL-execution endpoint.
- `GET/POST/PATCH/DELETE /tables/{t}` — PostgREST-style DSL. Values are
  string-interpolated into SQL by `bluedb_rest::render_value` (single-quote
  doubling). The escaping is correct for gluesql's dialect today, but it is a
  *fragile* defense: any gap becomes injection, and the blast radius grows if we
  emit multi-statement SQL.

### 1.2 Write throughput is poorly understood and the HTTP shape constrains it

Measured (in-memory object store, `crates/bluedb-sql/tests/throughput_bench.rs`,
serialized, release):

| Lever | Result |
|---|---|
| Serial durable write (`await_durable=true`) | **9.9/s @ 101 ms** |
| Serial write `await_durable=false` | **76,921/s @ 13 µs** |
| Concurrency sweep (default 100 ms flush) | linear: **~9.8 × concurrent writers** (128 → 1259/s) |
| `flush_interval` sweep (concurrency 32) | linear in 1/interval: 100 ms→314/s, 25 ms→**1197/s**, 10 ms→2767/s |

**Key facts established:**

- The "~100 ms per write" is **SlateDB's WAL `flush_interval` (default 100 ms)** —
  the wait for the next durable flush tick — **not** object-store PUT latency
  (proven: in-memory store, zero PUT latency, still 101 ms). bluedb currently calls
  `Db::open` with defaults and never tunes it.
- Throughput is **latency-bound, not CPU-bound**: a write parks on the durable
  watcher (`db.rs:364`, `if options.await_durable { durable_watcher.await_value() }`).
  Group commit coalesces all in-flight writes into one flush, so **throughput =
  (writes in flight) × (flush rate)**.
- Over HTTP, **concurrency = concurrent requests**. The insert handler uses the
  **non-serialized group-commit connection** (`state.connection()`), so N concurrent
  `POST /tables/{t}` = N in-flight writes → group commit. `POST /sql`, `PATCH`,
  `DELETE` use `connection_serialized()` → they do **not** scale with concurrency.
- A **single sequential HTTP client** is the serial-writer case (~10/s strong-durable),
  because HTTP/1.1 is request-response per connection — the flush wait becomes
  per-request latency that a driver's pipelining would otherwise hide.
- The bulk path is broken: `POST /tables/{t}` with a JSON **array** renders one
  multi-row `INSERT … VALUES (..),(..)` (`bluedb_rest::render.rs:103`), which trips
  the **known sqlparser multi-row-VALUES rejection** (~50 tuples; see
  `docs/insert-throughput-handoff.md`). So large bulk inserts fail to parse today.
- gluesql 0.19 exposes **parameter binding** (`Glue::execute_with_params`,
  `glue.rs:76`) with `$N` positional placeholders (1-based; `ParamLiteral(Value)`),
  and **typed-AST execution** (`Glue::execute_stmt`, `glue.rs:66`). Params are a
  single slice shared across all statements in a multi-statement string.

## 2. Goals / non-goals

**Goals**

1. Split the HTTP surface into a **four-tier capability ladder** (DML / SQL / DDL /
   Admin), where **three tiers are injection-proof by construction** and the fourth
   is a locked, audited operator capability.
2. Make all untrusted-facing SQL **parameterized** (`$N` + typed `Value`s), never
   string-concatenated — at the engine boundary, not per-endpoint.
3. Give clients a **batching primitive without a driver**: `POST /tables/{t}` with an
   array becomes one `BEGIN; <param-bound INSERTs>; COMMIT;` → one durable flush.
4. Expose **`flush_interval`** as a per-DB PRAGMA to trade per-request latency vs
   object-store PUT cost (the *only* throughput knob — durability stays strong).
5. Enable **HTTP/2** so one connection can multiplex many in-flight writes.
6. Add an **authz scope** seam (`data:read` → `data:query` → `schema:admin` →
   `superuser`) composing with the existing `require_active()` HA gate.

**Non-goals**

- A true prepared-statement **plan cache** (parse/plan once, reuse). gluesql 0.19
  re-plans on every `execute_with_params`; "prepared" here means wire-shape/safety
  discipline, not plan reuse. Deferred (perf, not security).
- **Stateful client-driven transactions** over HTTP (a session/txn token held across
  round-trips while holding the write lease). Multi-op txns go through the Admin
  surface as one `BEGIN…COMMIT` request. (YAGNI; holding the lease across network
  round-trips is an availability hazard.)
- Fixing the upstream sqlparser multi-row-VALUES bug. We **sidestep** it via
  `BEGIN…COMMIT` of single-row inserts.
- **Relaxed durability** (`await_durable=false`). Explicitly rejected: an acked write
  is always durable in object storage before the ack (Jepsen `lost-count 0`). Throughput
  comes from concurrency + group commit + the `flush_interval` knob, never from weakening
  the durability contract. The `flush_interval` PRAGMA is the only latency/throughput
  lever.
- An identity provider. We design the authz **seam**; the principal source
  (bearer/JWT/API-key/mTLS) is pluggable and out of scope.
- Cross-region concerns (Spec for HA covers those).

## 3. The four-surface model

| Surface | Endpoints | Input | Allows | Authz scope | Injection posture |
|---|---|---|---|---|---|
| **DML** | `GET/POST/PATCH/DELETE /tables/{t}` | structured DSL (filters + JSON values) | simple CRUD | `data:read` / `data:write` (tenant) | client authors **no SQL**; values `$N`-bound, idents allow-listed → **injection-proof** |
| **SQL** | `POST /sql` | `{sql, params}` | **one** non-DDL statement (SELECT/INSERT/UPDATE/DELETE) with full SQL expressivity | `data:query` (tenant) | **parameterized** (`$N`) + single-statement/no-DDL → **injection-proof** |
| **DDL** | `POST/DELETE /schema/tables[/{t}]`, `…/indexes`, `…/fulltext-indexes`, `GET /schema/…` | structured JSON schema | schema changes | `schema:admin` | client authors **no SQL**; idents + type keywords allow-listed → **injection-proof** |
| **Admin** | `POST /admin/sql` | `{sql, params?}` | **arbitrary** raw SQL — multi-statement, DDL, txns | `superuser`, **audited, off by default in prod** | raw authorship; control = **authz + audit** |

**Capability ladder:** each tier is strictly more privileged and less restricted than
the one above. SQL injection is *possible* on exactly one surface — Admin — which is
superuser-only, audited, and disabled by default in production.

## 4. Detailed design

### 4.1 DML surface — parameterize the compiler

`bluedb-rest` currently renders values inline via `render_value`. Change the render
layer to emit **`$N` placeholders** and return a parallel **params vector** of typed
values, instead of interpolated literals.

- `RestQuery`/`InsertRequest`/`UpdateRequest`/`DeleteRequest` `.to_sql()` →
  `.to_sql_with_params() -> (String, Vec<ParamLiteral>)`. Filters, `SET`
  assignments, and `IN` lists all bind as `$N`.
- Identifiers (table from the URL path; columns from JSON keys / `select`) stay
  interpolated but **`validate_ident`-allow-listed** (`[A-Za-z_][A-Za-z0-9_]*`).
  Parameters cannot bind identifiers — allow-listing is the correct defense.
- `bluedb-engine::rest_sql::execute_*` calls `glue.execute_with_params(sql, params)`.
- JSON scalar → `ParamLiteral`: `null`→Null, bool→Bool, integer→I64, float→F64,
  string→Str. (No more "does it parse as a number" heuristic — the JSON type decides.)

**Bulk / batch (the driverless throughput primitive).** `POST /tables/{t}` accepts an
object *or array of objects* (`build_insert`, `lib.rs:319`); the general form is a batch
of `{sql, params}` elements (the array-of-objects is sugar over it). **The client sends
only the array — no transaction syntax.** The server wraps the batch in a transaction
internally:

Client sends:
```json
[ {"sql": "INSERT INTO docs (id, body) VALUES ($1,$2)", "params": [1, "…"]},
  {"sql": "INSERT INTO docs (id, body) VALUES ($1,$2)", "params": [2, "…"]} ]
```

Server executes (one parameterized multi-statement string, `$N` indices renumbered
**globally** across the batch since gluesql shares one params slice, all values flattened
in order):
```
BEGIN; INSERT INTO docs (id, body) VALUES ($1,$2); INSERT INTO docs (id, body) VALUES ($3,$4); COMMIT;
```
via `execute_with_params` on the **serialized** connection (the txn holds the write lease).

Properties (deliberate):
- **Atomic** — all-or-nothing. The server-emitted `BEGIN…COMMIT` produces **one
  `WriteBatch` → one durable flush** for the whole batch, *regardless of batch size*
  (bounded by memory, not flush ticks). This is the best bulk-load throughput: a
  10,000-row batch is one ~100 ms (or 25 ms) flush, not 10,000.
- **Dodges the multi-row-`VALUES` parser bug** — each element is a single-row
  parameterized statement; we never emit `VALUES (..),(..)`.
- **Injection-proof** — values are `$N`-bound; the client authors no SQL keywords (the
  `BEGIN/COMMIT/INSERT` skeleton is server-generated, identifiers `validate_ident`-checked).
- **Cost:** the batch holds the write lease for its duration (serialized) — acceptable for
  a bulk load, which is one big commit anyway.

### 4.2 SQL surface — `POST /sql` `{sql, params}`, injection-proof

- Body: `{ "sql": "SELECT … WHERE x = $1", "params": [ … ] }`.
- Server **parses** the SQL (`gluesql_core::parse`), and **rejects** unless it is a
  **single** statement of type SELECT/INSERT/UPDATE/DELETE (no DDL, no `BEGIN`/`COMMIT`,
  no multi-statement). This bounds blast radius.
- Executes via `execute_with_params` — values flow **only** through `params` (`$N`),
  bound as typed literals, never concatenated. This is the libpq/JDBC guarantee:
  the app developer authors the static template; runtime/user data goes in params, so
  user data cannot change query structure.
- Tenant-scoped (the connection is `connection_for_tenant`). Writes still go through
  group commit unless the statement requires serialization (UPDATE/DELETE are RMW →
  serialized connection).
- Identifiers in the template are developer-authored (trusted), so they are not an
  injection vector here.

### 4.3 DDL surface — structured schema API

A typed JSON API that compiles to **validated** DDL, so there is no SQL authorship:

- `POST /schema/tables` — body `{name, columns:[{name, type, nullable, primaryKey, unique, default}]}`.
  Compiles to `CREATE TABLE` via the typed path (`execute_stmt`) or an
  allow-listed-identifier + allow-listed-type-keyword SQL string.
- `DELETE /schema/tables/{t}` — drop table.
- `POST /schema/tables/{t}/indexes` `{name, columns}` / `DELETE …/indexes/{name}`.
- `POST /schema/tables/{t}/fulltext-indexes` `{column, analyzer}` — **Spec B's index DDL.**
- `GET /schema/tables[/{t}]` — introspection via the `Metadata`/`GLUE_OBJECTS` surface
  already implemented (the `TAG_META` creation-timestamp registry).

Type keywords are allow-listed (`TEXT`, `INTEGER`, `DECIMAL`, `BOOLEAN`, `TIMESTAMP`, …);
identifiers via `validate_ident`. Exotic DDL not covered by the vocabulary falls back to
the Admin surface.

### 4.4 Admin surface — `POST /admin/sql`, arbitrary

- Body: `{sql, params?}`; multi-statement, DDL, transactions — anything.
- `superuser` scope only; every call **audited** (principal, statement, timestamp,
  outcome). **Disabled by default** via config (`BLUEDB_ENABLE_ADMIN_SQL`); enabled
  only for break-glass.
- The only surface where SQL injection is even possible — by design, locked down.

### 4.5 Write-path knob — `flush_interval` PRAGMA

`PRAGMA flush_interval = '25ms'` (per-DB). Plumbed to the SlateDB open path: replace
`Db::open` with `Db::builder(path, store).with_settings(s)` where
`s.flush_interval = Some(d)` (`slatedb::Settings`, default `Some(100ms)`). Trade: lower
interval → lower per-request latency and higher throughput, more object-store PUTs
(SlateDB docs: ~$130/mo per the 100 ms tier on S3 standard; 25 ms ≈ 4×). Default unchanged
(100 ms).

**Durability stays strong** (`await_durable=true`) — an acked write is durable in object
storage before the ack, always. `flush_interval` is the *only* latency/throughput lever;
we never relax durability. Higher aggregate throughput comes from concurrency + group
commit (and a shorter interval), per §5.

### 4.6 HTTP/2

`axum::serve` already uses hyper-util's auto connection builder (serves HTTP/1 and
HTTP/2 on the same socket). Document that an **h2 client** (over TLS, or h2c with
prior knowledge) can multiplex many concurrent in-flight writes on **one** connection —
removing the need for a large connection pool. Over plaintext, most clients default to
HTTP/1.1, so without TLS/h2c, concurrency = a client connection pool. No server code
change beyond confirming h2 is enabled in the builder; add TLS config as a deployment
concern.

### 4.7 Authz seam

A tower middleware layer that extracts a **principal + scopes** from the request
(bearer token / API key — pluggable `PrincipalExtractor` trait) and checks the required
scope per route. Scopes: `data:read`, `data:write`, `data:query`, `schema:admin`,
`superuser`. Composes with `require_active()` (HA write-gate) — both must pass for
writes. Tenant derived from the principal (replaces/validates any client-supplied
tenant). The identity provider itself is out of scope; we ship the seam + an in-memory
static-token impl for dev/tests.

## 5. Throughput & durability contract (to document for users)

- **Single sequential client:** latency ≈ `flush_interval` (≈10/s @ 100 ms, ≈40/s @ 25 ms).
- **Concurrency:** linear — **1000/s ≈ ~100 concurrent** `POST /tables/{t}` (or fewer
  via the bulk endpoint). Achieve concurrency with a client connection pool or HTTP/2.
- **Bulk:** one request → one flush → N rows.
- **Real object storage** adds the PUT round-trip per flush; expect **~3–4× lower
  constants** than the in-memory bench, with a hard per-flush latency floor that a
  shorter interval cannot go under. The *shape* (linear in concurrency and in
  1/interval) holds.
- **`/sql`, `PATCH`, `DELETE`** are serialized and do **not** scale with concurrency
  (~flush_interval each) — use `POST /tables/{t}` / bulk for throughput.

## 6. Components & isolation

- `bluedb-rest`: render layer returns `(sql, params)`; new `to_sql_with_params`. No
  dependency change.
- `bluedb-engine`: `rest_sql` switches to `execute_with_params`; new structured-DDL
  compiler module; new bulk-insert builder (BEGIN..COMMIT param batch).
- `bluedb-sql`: `flush_interval` plumbed through the `Database`/open path; PRAGMA
  intercept. (Durability stays strong; no write-option change.)
- `bluedb-server`: route split (DML/SQL/DDL/Admin), authz middleware, `{sql, params}`
  decoding, audit log for Admin, HTTP/2/TLS config, admin-enable flag.

Each is independently testable: the param-render is a pure function; the DDL compiler
maps JSON→AST; the authz layer is a middleware unit; the throughput is the existing bench.

## 7. Testing

- **Injection:** param-bound values containing `'`, `;`, `-- `, `'); DROP TABLE` are
  stored as data, never executed (DML, SQL surfaces). Identifier allow-list rejects
  crafted table/column names.
- **Bulk one-flush:** a 1000-row array insert issues exactly one durable flush (count
  via a flush-counting store / observe SlateDB stats) and inserts all rows.
- **SQL surface restriction:** DDL / multi-statement / `BEGIN` rejected on `/sql`.
- **Authz:** each route enforces its scope; Admin disabled by default returns 404/403.
- **DDL API:** create/drop table+index round-trips through the schema registry;
  introspection reflects it.
- **flush_interval PRAGMA:** setting it changes the measured per-request latency.
- **Durability unchanged:** an acked write survives crash/reopen (the transaction/
  isolation + durability regression tests stay green).
- **Throughput:** `throughput_bench` (moved into this branch) as a non-CI `#[ignore]`
  bench; numbers tracked in the docs.

## 8. Rollout / breaking changes

- `POST /sql` **changes meaning**: it becomes the parameterized, single-statement,
  non-DDL surface. Arbitrary SQL moves to `POST /admin/sql`. Document the migration.
- Authz becomes required; ship a dev default (static token) so local/test flows work.
- Default `flush_interval` and durability are unchanged — no silent behavior change for
  existing throughput/durability.

## 9. Open questions

- Principal/identity source (bearer vs API key vs mTLS) — pick at implementation; seam
  is provider-agnostic.
- Whether the SQL surface should additionally **reject value-literals** in the parsed
  AST (forcing all values to `$N`). Leaning **no** (kills legitimate constants like
  `LIMIT 10`, `WHERE active = TRUE`); the single-statement/no-DDL restriction + param
  binding is sufficient.
