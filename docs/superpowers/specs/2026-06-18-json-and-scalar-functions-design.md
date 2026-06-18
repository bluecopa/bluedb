# JSON support + Postgres scalar functions — design

**Status:** design (approved in brainstorming 2026-06-18; not yet implemented)
**Driver:** input-table-v2 is replacing Postgres with bluedb as the per-(workspace,
solution) backend. Postgres parity requires JSON columns that are **queryable**,
clean value serialization on the wire, and a few Postgres scalar functions. Scope
was confirmed against `~/Downloads/2026-06-16-bluedb-upstream-asks-for-inputtable-crud.md`
and the CockroachDB Postgres-dialect cross-check (see
`2026-06-17-datafusion-front-door-design.md` §"Postgres-dialect cross-check").

## Goal

1. JSON columns that can be **stored, retrieved as real JSON, and filtered/sorted**
   — including through the `/tables` grid path.
2. Faithful scalar-value fidelity on the `/tables` wire (the doc's asks #2/#3).
3. Postgres formatting functions `to_number`, `format`, `to_char(numeric)`.

## Findings that scoped this (from exploration)

- **Ask #2 (value serialization) is already done** except `Uuid`. `sql_value_to_json`
  (`crates/bluedb-server/src/lib.rs`) already renders Decimal → `"10.5"`,
  Date/Timestamp/Time → ISO, Bool/ints/floats canonical, matching the analytical
  path (commit `107c403`). Only `Uuid` (and exotic types) still hit the debug
  fallback `format!("{other:?}")`.
- **Ask #3 (int→decimal write coercion) is open.** The `/tables` data plane is
  param-only; a JSON `int` binds as `Param::Int` → GlueSQL `Value::I64`, which
  GlueSQL rejects for a Decimal column. Inline `VALUES (20)` works only because the
  *literal* is coerced — bound params skip that path.
- **"Query into JSON on `/tables`" is not "JSON on GlueSQL."** `/tables` is the
  PostgREST→GlueSQL fast path; GlueSQL has no JSON ops. The right architecture is to
  **extend the DataFusion front door to the `/tables` surface**: a JSON predicate is
  one kind of arbitrary filter GlueSQL can't serve — the same class doc ask #4
  (grids filter arbitrary columns) is about. Both are solved by routing
  arbitrary-predicate `/tables` reads to DataFusion.
- **Seams identified:** type normalization in `crates/bluedb-sql/src/rewrite.rs`
  (`normalize_data_type`); the `/tables` value serializer `sql_value_to_json` and
  the JSON→param binder `json_to_param` in `crates/bluedb-server/src/lib.rs`; the
  DataFusion entry `query_via_catalog` (`crates/bluedb-query/src/lib.rs`) + its
  `SessionContext` (UDF registration point); the PostgREST→SQL renderer in
  `crates/bluedb-rest` (`model.rs`/`render.rs`); the scan/index classifier in
  `crates/bluedb-sql/src/guardrail.rs` (reused to decide GlueSQL vs DataFusion);
  the mirror's `build_arrow_column` in `crates/bluedb-lakehouse/src/writer.rs`.

## Decisions locked

- JSON is **text-backed**, not a native binary JSONB type: a JSON column is `Utf8`
  everywhere (storage, mirror, DataFusion). "JSON-ness" = validate-on-write,
  parse-on-read for `/tables`, and JSON functions on the query path.
- JSON accessors come from the **`datafusion-functions-json`** crate (Apache-2.0;
  provides the functions *and* the `->`/`->>` operator rewrite). Fallback if it is
  not compatible with our DataFusion 52: hand-roll ~4 ScalarUDFs over `serde_json`
  + an operator-rewrite rule. **Compatibility must be confirmed first** (cargo add +
  build + `cargo deny check`).
- `/tables` read routing is **general arbitrary-filter**, not JSON-only: any read
  whose predicates/sort touch non-PK/non-indexed columns (JSON included) routes to
  DataFusion; point/indexed reads and all writes stay on the GlueSQL fast path.
  This closes doc ask #4 in the same architecture.
- #3 numeric coercion lives in the **bluedb-sql execute path** (it owns the schema
  map + statement + params), so it fixes `/tables` and parameterized `/sql`
  uniformly.

## Phases

Built in order; each is independently shippable.

### P1 — `/tables` value fidelity (asks #2 remnant + #3)

- **#2:** add `Uuid` → canonical hyphenated string (and `Bytea` → base64) to
  `sql_value_to_json`, kept in lockstep with `record_batches_to_json`.
- **#3:** in the bluedb-sql execute path, coerce parameterized-DML values to their
  target column types before binding — numeric widening only (`I64`/`F64` → Decimal
  for a decimal column; `I64` → `F64` for a float column). Reuses the schema map the
  `coerce` module already has. Out of scope: lossy/narrowing coercions.
- **Data flow:** write — JSON body → `json_to_param` → params → bluedb-sql execute
  (coerce params vs column types) → GlueSQL. Read — GlueSQL `Value` →
  `sql_value_to_json` → JSON.
- **Errors:** coercion that can't apply (e.g. text into an int column) → existing
  typed 400; never a debug repr.
- **Tests:** reproduction first — `POST /tables {"amount":1}` into a Decimal column
  is 400 today → 200 after; parameterized `/sql` int→decimal INSERT; `Uuid` round-trip
  asserting identical JSON on `/tables` and `/sql`.

### P2a — JSON column type (store + retrieve)

- **Type:** `JSON`/`JSONB` → `TEXT` in `normalize_data_type`; the column is `Utf8`
  end-to-end. Mirror: a string column (free; `build_arrow_column` already handles
  `Utf8`, and the current map-cell null-out no longer applies since values are text).
- **Write (`/tables`):** `json_to_param` becomes **schema-aware** — for a JSON
  column it accepts a JSON object/array (today rejected as "not a scalar"), validates
  by parsing, and binds the **canonical JSON text** as `Param::Str`. The write path
  fetches the table schema to know which columns are JSON.
- **Read (`/tables`):** the row serializer becomes **schema-aware** — a JSON
  column's text is parsed and emitted as real JSON (object/array), not an escaped
  string. Non-JSON text columns are unchanged.
- **Errors:** invalid JSON on write → typed 400; a JSON column holding non-JSON text
  (e.g. written via raw SQL) → read emits it as a JSON string rather than failing.
- **Tests:** round-trip a JSON object through `POST`/`GET /tables` (stored as text,
  returned as an object); mirror a JSON column to Iceberg as a string; `/sql`
  `SELECT` of a JSON column returns the text.

### P2b — JSON accessors on the query path

- Depend on `datafusion-functions-json`; register its functions + operator rewrite
  on the `SessionContext` `query_via_catalog` builds, so `->`, `->>`,
  `json_get`/`json_get_str` work over JSON-as-`Utf8` columns. `jsonb_path_query`
  (JSONPath) is **deferred** unless a grid needs it (YAGNI).
- **Tests:** `/sql` `SELECT data->>'k' FROM t WHERE data->>'status' = 'active'` over
  a JSON column returns the expected rows; a gated `df/json.slt` case.

### P2c — `/tables` arbitrary-filter routing (closes #4)

- **Classifier:** reuse `guardrail`'s index-awareness. A `/tables` **read** whose
  filters/sort are all PK/indexed → GlueSQL fast path (unchanged). Otherwise (any
  arbitrary column, including a JSON subfield) → DataFusion via `query_via_catalog`.
  This is the front door's "reject → route" flip, applied to `/tables` (it already
  governs `/sql`).
- **Translation:** bluedb-rest renders the PostgREST request to SQL for the chosen
  engine; PostgREST JSON path predicates/orders (`col->>key=eq.val`,
  `order=col->>key`) translate to DataFusion `->>'key'` SQL. Standard
  arbitrary-column filters need no JSON-specific translation — the same SQL runs on
  DataFusion.
- **Scope:** **reads only.** Writes (POST/PATCH/DELETE) stay GlueSQL; arbitrary-filter
  *writes* (e.g. DELETE on a non-indexed column) keep today's behavior and are out of
  scope here. The freshness gate (already in `exec_sql_read`) applies to routed reads.
- **Tests:** `GET /tables/t?label=eq.a` (non-indexed) returns rows instead of 400;
  `GET /tables/t?data->>status=eq.active`; `order=` on an arbitrary/JSON column; a
  point/indexed read still takes the GlueSQL path (assert via the provider/route
  counter).

### P3 — formatting functions

- `to_number(text, fmt)`, `format(fmt, …)`, `to_char(numeric, fmt)` as DataFusion
  ScalarUDFs on the same `SessionContext`. `to_char` must coexist with the built-in
  temporal `to_char` (shadow/extend to also accept numeric). Independent of JSON.
- **Tests:** gated `df/format.slt` covering each, value-checked against Postgres
  semantics.

## Non-goals

- Native binary JSONB (on-disk format, GIN-indexed `@>`/`?`).
- JSON querying on the **GlueSQL** engine (no fork).
- Arbitrary-filter **writes** routed to DataFusion (read-only engine).
- `jsonb_path_query` / full SQL-JSON-path (deferred until needed).

## Risks

- `datafusion-functions-json` ↔ DataFusion 52 compatibility (confirm first;
  hand-roll fallback documented).
- P2c changes `/tables` from pure-GlueSQL to a routed surface — the classifier must
  exactly match the guardrail's index logic so point reads never regress to the
  slower path. Strong route-counter tests required.
- Cross-surface consistency: a JSON column must read identically via `/tables`
  (parsed object) and `/sql` (text + accessors); covered by consistency tests.
