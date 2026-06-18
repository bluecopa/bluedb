# JSON support + Postgres scalar functions — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give bluedb queryable JSON columns (store/retrieve/filter/sort, including through `/tables`), faithful scalar serialization on the `/tables` wire, and the Postgres `to_number`/`format`/`to_char(numeric)` functions — the JSON + scalar-function parity input-table-v2 needs to replace Postgres.

**Architecture:** JSON is **text-backed** (`Utf8`/`TEXT` everywhere). A per-table **`JsonCatalog`** (mirrors the existing `PkCatalog`) remembers which columns were declared `JSON`/`JSONB` so the `/tables` read serializer can re-inflate their text to real JSON — the type word itself is lost when `normalize_data_type` maps `JSON → TEXT` for GlueSQL. JSON accessors and the format UDFs are registered on the DataFusion `SessionContext` that `query_via_catalog` builds; arbitrary-filter `/tables` reads (JSON subfields included) are caught at the guardrail-reject boundary and re-routed to that same front door. Numeric write-coercion is a CAST-insertion pass at the bluedb-sql execution chokepoint.

**Tech Stack:** Rust, GlueSQL (OLTP/`/tables`), DataFusion 52 + `datafusion-functions-json` (analytical/`/sql`), arrow 57, axum, sqlparser.

---

## Phase order & shippability

P1 → P2a → P2b → P2c → P3. Each phase compiles, tests green, and is committed independently. P2c depends on P2b (JSON `->>'` SQL needs the JSON functions registered). P3 is independent of all JSON phases. Commit after each phase. **Never push.**

## File map

- `crates/bluedb-server/src/lib.rs` — `sql_value_to_json` (Uuid/Bytea), `record_batches_to_json` (keep in lockstep), read serializer made JSON-catalog-aware, `select` handler gains guardrail-reject → DataFusion routing, `Param`→`serde_json::Value` bridge.
- `crates/bluedb-sql/src/rewrite.rs` — `normalize_data_type` maps `JSON`/`JSONB` → `TEXT`.
- `crates/bluedb-sql/src/jsoncat.rs` (**new**) — `JsonCatalog` (per-table JSON column set), persisted via storage tag, written at CREATE TABLE.
- `crates/bluedb-sql/src/coerce.rs` — numeric write-coercion (CAST insertion for INSERT/UPDATE into Decimal/Float columns).
- `crates/bluedb-sql/src/storage.rs` — wire the JsonCatalog tag read/write helpers; invoke write-coercion in `plan()` (or chokepoint fallback).
- `crates/bluedb-sql/src/compositepk.rs` — capture JSON columns at CREATE TABLE (it already walks every CREATE TABLE for `normalize_data_type`).
- `crates/bluedb-query/src/lib.rs` — register `datafusion-functions-json` + the format UDFs on the `SessionContext` in `query_via_catalog`.
- `crates/bluedb-query/src/format_udfs.rs` (**new**) — `to_number` / `format` / `to_char(numeric)` ScalarUDFs.
- `crates/bluedb-rest/src/{parse.rs,model.rs,render.rs}` — accept `col->>key` / `col->key` in filter columns and `order=`; render to `col ->> 'key'`.
- `crates/bluedb-server/Cargo.toml`, `crates/bluedb-query/Cargo.toml` — deps as needed.
- Tests: Rust unit tests beside each module; server integration tests in `crates/bluedb-server/src/lib.rs` test modules / `crates/bluedb-server/tests/`; gated `crates/bluedb-sqltest/df/json.slt` + `df/format.slt`.

---

## Phase P1 — `/tables` value fidelity (ask #2 remnant + #3)

### Task 1: `Uuid`/`Bytea` clean serialization on `/tables`

**Files:** Modify `crates/bluedb-server/src/lib.rs` (`sql_value_to_json` ~1520, `record_batches_to_json` ~946 doc-comment lockstep).

- [ ] **Step 1: Failing test** — add to the server test module:

```rust
#[test]
fn sql_value_to_json_renders_uuid_and_bytea_cleanly() {
    use gluesql_core::data::Value as SqlValue;
    // Uuid is stored as u128 in gluesql; render canonical hyphenated form.
    let u = SqlValue::Uuid(0x550e8400_e29b_41d4_a716_446655440000u128);
    assert_eq!(super::sql_value_to_json(&u),
        serde_json::Value::String("550e8400-e29b-41d4-a716-446655440000".into()));
    let b = SqlValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]);
    assert_eq!(super::sql_value_to_json(&b),
        serde_json::Value::String("3q2+7w==".into())); // base64 std
}
```

- [ ] **Step 2: Run, expect FAIL** (`Uuid`/`Bytea` currently hit the `format!("{other:?}")` arm).

Run: `cargo test -p bluedb-server sql_value_to_json_renders_uuid 2>&1`

- [ ] **Step 3: Implement** — add arms before the `other =>` fallback in `sql_value_to_json`. Use `uuid::Uuid::from_u128(*n).hyphenated().to_string()`; base64 via the crate already in the tree (check `cargo tree`; likely `base64`). Confirm gluesql's `Value::Uuid` payload type (u128) and `Value::Bytea` (Vec<u8>) by reading `gluesql_core::data::Value`.

```rust
SqlValue::Uuid(n) => Value::String(uuid::Uuid::from_u128(*n).hyphenated().to_string()),
SqlValue::Bytea(b) => Value::String(base64_encode(b)),
```

- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5:** Update the `record_batches_to_json` doc-comment "must match" list to mention Uuid/Bytea; if Arrow ever yields these, mirror the rendering (FixedSizeBinary/Binary → same base64). Commit.

### Task 2: numeric write-coercion (int/float → Decimal, int → Float)

**Files:** Modify `crates/bluedb-sql/src/coerce.rs` (+ wire-in point); Test: `crates/bluedb-server/src/lib.rs` integration test + `coerce.rs` unit test.

**Approach:** A pass that, for `INSERT` value rows and `UPDATE` assignments, wraps a value expression in `CAST(expr AS <coltype>)` when the target column is `Decimal` or `Float` — reusing GlueSQL's working CAST executor exactly as `coerce_comparisons` does. Unconditional-by-column-type wrap is safe: `CAST($n AS DECIMAL)` widens a bound `I64`/`F64`, is identity on a `Decimal`, and `NULL`→`NULL`. First probe whether `Planner::plan` is invoked for `Statement::Insert`; if yes, implement as an AST pass there (schema map already built). If not, implement at the `prepare_and_run` chokepoint as a string rewrite (guaranteed to run, mirrors `prepare_composite_pk`).

- [ ] **Step 1: Reproduction integration test** (server) — `POST /tables` of a JSON int into a Decimal column must succeed:

```rust
// pseudo: spin the in-mem app (see existing server tests for the harness),
// CREATE TABLE t (id INTEGER PRIMARY KEY, amount DECIMAL);
// POST /tables/t {"id":1,"amount":1}  → expect 200 {"inserted":1}, NOT 400.
// GET /tables/t?id=eq.1 → amount renders "1".
```

- [ ] **Step 2: Run, expect FAIL** — 400 `incompatible data type, data type: Decimal, value: I64(1)`.

Run: `cargo test -p bluedb-server <test_name> 2>&1`

- [ ] **Step 3: Probe plan() for INSERT** — unit test in `coerce.rs`: build the schema map for `t(amount DECIMAL)`, call the new `coerce_writes(&schema_map, INSERT … VALUES ($1))`, assert the value expr is wrapped in `CAST(... AS DECIMAL)`. Implement `coerce_writes` matching `Statement::Insert { source: Values }` and `Statement::Update { assignments }`, wrapping value exprs whose target column type is `DataType::Decimal`/`Float` (skip exprs already `Expr::Cast`). Add it to `coerce_comparisons`' dispatch or as a sibling called from `plan()` right after `coerce_comparisons`.
- [ ] **Step 4: Run unit + integration.** If integration still 400 (plan() skips INSERT), move the call into `bluedb_engine::rest_sql::prepare_and_run` as a string rewrite: parse → `coerce_writes_stmt` per statement → re-serialize only if changed (mirror `prepare_composite_pk`). Re-run.
- [ ] **Step 5: Also cover parameterized `/sql`** — `{"sql":"INSERT INTO t (id,amount) VALUES (?,?)","params":[2,2]}` into Decimal succeeds. Commit phase P1.

---

## Phase P2a — JSON column type (store + retrieve)

### Task 3: `JSON`/`JSONB` → `TEXT` normalization

**Files:** Modify `crates/bluedb-sql/src/rewrite.rs` (`normalize_data_type` ~318); Test: `rewrite.rs` unit test.

- [ ] **Step 1: Failing test** — `JSON` and `JSONB` normalize to `DataType::Text`:

```rust
#[test]
fn json_types_normalize_to_text() {
    let mut dt = sqlparser_datatype("JSON");   // helper: parse a column type
    assert!(super::normalize_data_type(&mut dt));
    assert_eq!(dt, DataType::Text);
    let mut dt2 = sqlparser_datatype("JSONB");
    assert!(super::normalize_data_type(&mut dt2));
    assert_eq!(dt2, DataType::Text);
}
```

- [ ] **Step 2: Run, expect FAIL** (JSON/JSONB fall through to `None`). `JSON` may parse as `DataType::JSON`; `JSONB` may be `DataType::Custom("JSONB")` or `JSON` — read sqlparser's `DataType` to match both. Add to the match.
- [ ] **Step 3: Implement** — add a match arm: base word `"JSON" | "JSONB"` → `Some(DataType::Text)`. Because `normalize_data_type` compares by rendered base word, the `JSONB`/`Custom` spelling is covered by the same `base` string switch already used for VARCHAR/etc.
- [ ] **Step 4: Run, expect PASS.** Commit.

### Task 4: `JsonCatalog` — remember which columns are JSON

**Files:** Create `crates/bluedb-sql/src/jsoncat.rs`; Modify `crates/bluedb-sql/src/storage.rs` (tag + read/write helpers, mirror `write_pk_catalog`/`read_pk_catalog` / `TAG_PKCAT`), `crates/bluedb-sql/src/lib.rs` (module + re-export), `crates/bluedb-sql/src/compositepk.rs` (capture at CREATE TABLE). Test: `jsoncat.rs` unit + storage round-trip.

**Why:** `normalize_data_type` erases JSON→TEXT before GlueSQL sees it, so the original JSON-ness must be persisted out-of-band to re-inflate on read. Mirror the proven `PkCatalog` pattern exactly.

- [ ] **Step 1:** Read `storage.rs` `write_pk_catalog`/`read_pk_catalog` + the `TAG_PKCAT` keyspace tag; read `pkcodec`/keyspace conventions. Write a failing storage round-trip test: write a `JsonCatalog{ columns: ["data","meta"] }` for table `t`, read it back equal; absent table → `None`.
- [ ] **Step 2: Run, expect FAIL** (no such API).
- [ ] **Step 3: Implement** — `JsonCatalog { pub columns: Vec<String> }` (`Serialize`/`Deserialize`, like `PkCatalog`). Add `TAG_JSONCAT` and `write_json_catalog`/`read_json_catalog` on `SlateDbStorage` keyed by table name. Re-export `JsonCatalog` from `lib.rs`.
- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Capture at CREATE TABLE** — in `compositepk::apply`, in the `Statement::CreateTable` branch (before `normalize_data_type` mutates types away, OR by base-word check), collect columns whose declared type base word is `JSON`/`JSONB`; if non-empty, `storage.write_json_catalog(&table, &JsonCatalog{columns})`. Add a test: CREATE TABLE with a JSON column persists a JsonCatalog. Commit.

### Task 5: write path accepts JSON objects/arrays as canonical text

**Files:** Modify `crates/bluedb-server/src/lib.rs` (`json_scalar_to_dsl` ~1461, and the `/sql` `json_to_param` ~861). Test: server integration.

**Approach:** Schema-agnostic on write — serialize any JSON object/array value to canonical compact text and bind as a string. The column type governs acceptance (a JSON→TEXT column stores it; a numeric column errors in GlueSQL, which is correct). Validation is automatic: the body already parsed as `serde_json::Value`.

- [ ] **Step 1: Failing test** — `POST /tables/t {"id":1,"data":{"k":"v","n":3}}` into `t(id INT PK, data JSON)` → 200; today 400 (`expected a scalar value`).
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** — in `json_scalar_to_dsl`, replace the `other => Err(...)` arm with: `Value::Object(_) | Value::Array(_) => Ok(value.to_string())` (compact canonical JSON). Mirror in `json_to_param`: an object/array → `Param::Str(v.to_string())` instead of the reject. (Keep the unrepresentable-number error.)
- [ ] **Step 4: Run, expect PASS** (row inserts; raw stored text is canonical JSON). Commit.

### Task 6: read path re-inflates JSON columns to real JSON

**Files:** Modify `crates/bluedb-server/src/lib.rs` (`select` handler ~1251 + `payload_to_json`/`sql_value_to_json` call sites). Test: server integration round-trip.

**Approach:** `select` fetches the table's `JsonCatalog`; when serializing a `Payload::Select`, for any column in the catalog parse its `Str` cell back to `serde_json::Value` (object/array). Non-JSON columns unchanged. A JSON column holding non-JSON text (e.g. written via raw SQL) → emit as a JSON string (parse-failure fallback), never error.

- [ ] **Step 1: Failing test** — `GET /tables/t?id=eq.1` after Task 5's insert returns `{"id":1,"data":{"k":"v","n":3}}` with `data` a real JSON **object**, not the string `"{\"k\":\"v\",\"n\":3}"`.
- [ ] **Step 2: Run, expect FAIL** (returns escaped string).
- [ ] **Step 3: Implement** — give `payload_to_json` (or a new `payloads_to_json_with_json_cols`) the set of JSON column names; for those labels, `serde_json::from_str(text).unwrap_or(Value::String(text))`. The `select` handler reads the catalog: `state.read_json_catalog(&tenant, &table)` (add a thin AppState accessor that opens a storage handle for the tenant, or read it via the same connection used for the query). Thread the JSON column set into the serializer.
- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Mirror check** — add/extend a lakehouse test that a JSON(→TEXT) column mirrors to Iceberg as a string column (`build_arrow_column` already handles `Utf8`; confirm no map-null-out path triggers). Commit phase P2a.

---

## Phase P2b — JSON accessors on the query path

### Task 7: add `datafusion-functions-json` (compat gate) + register

**Files:** Modify `crates/bluedb-query/Cargo.toml`, `crates/bluedb-query/src/lib.rs` (`query_via_catalog` ~53). Test: `crates/bluedb-query` unit test over a `MemTable`.

- [ ] **Step 1: Compatibility gate FIRST** — `cargo add datafusion-functions-json -p bluedb-query` then `cargo build -p bluedb-query 2>&1`. If it pins a DataFusion version incompatible with our 52 (duplicate-arrow/datafusion link errors), STOP and use the hand-roll fallback (Task 7b). Run `cargo deny check licenses 2>&1` — must be Apache-2.0 clean.
- [ ] **Step 2: Failing test** — register the schema provider over a `MemTable` with a `Utf8` JSON column, run `SELECT data->>'k' FROM t` and `... WHERE data->>'status' = 'active'`; assert rows. (Build the ctx exactly like `query_via_catalog`.)
- [ ] **Step 3: Run, expect FAIL** (`->>` unknown / function not found).
- [ ] **Step 4: Implement** — in `query_via_catalog`, after `SessionContext::new()`, call `datafusion_functions_json::register_all(&mut ctx)?` (confirm the crate's exact registration entry point from its docs). This installs `->`, `->>`, `json_get`, `json_get_str`, etc. + operator rewrite.
- [ ] **Step 5: Run, expect PASS.** Add gated `crates/bluedb-sqltest/df/json.slt` and run it through `DataFusionTester`. Commit (or proceed to 7b first if gate failed).

### Task 7b (fallback, only if Step 1 gate fails): hand-rolled JSON UDFs

**Files:** Create `crates/bluedb-query/src/json_udfs.rs`; register in `query_via_catalog`.

- [ ] Implement `json_get_str(text, key) -> Utf8` and `json_get(text, key) -> Utf8` as `ScalarUDF`s over `serde_json` (already in tree), plus a `->`/`->>` operator-rewrite `AnalyzerRule` mapping `BinaryExpr(Operator::Arrow/LongArrow)` to the UDF call. TDD each: failing test (function unknown) → implement → pass. Commit.

---

## Phase P2c — `/tables` arbitrary-filter routing (closes doc ask #4)

### Task 8: `Param` → `serde_json::Value` bridge

**Files:** Modify `crates/bluedb-server/src/lib.rs`. Test: unit.

- [ ] **Step 1: Failing test** — `param_to_json(&Param::Int(3)) == json!(3)`, `Float`→number, `Bool`→bool, `Null`→null, `Str`→string.
- [ ] **Step 2–4:** Implement `fn param_to_json(p: &bluedb_rest::Param) -> serde_json::Value` (inverse of `json_to_param`). Run, pass. Commit.

### Task 9: route guardrail-rejected `/tables` reads to DataFusion

**Files:** Modify `crates/bluedb-server/src/lib.rs` (`select` handler). Test: server integration with a route-counter assertion.

**Approach:** In `select`, run the GlueSQL fast path; if it errors with the guardrail-reject sentinel (`is_guardrail_reject`), re-render the same PostgREST request to SQL+params (`parse_query(table, qs)?.to_sql_with_params()?`), bridge params to JSON, and run `bluedb_query::query_via_catalog(engine, &sql, &json_params)`; serialize via `record_batches_to_json`. Point/indexed reads never error, so they keep the GlueSQL path untouched — verify with a counter.

- [ ] **Step 1: Failing test** — `GET /tables/t?label=eq.a` where `label` is **non-indexed** returns rows (200) instead of 400; and `GET /tables/t?id=eq.1` (PK) still goes through GlueSQL (assert via a provider/route counter or a tracing hook).
- [ ] **Step 2: Run, expect FAIL** (400 guardrail reject).
- [ ] **Step 3: Implement** — wrap the `execute_query_str` call; on `Err` matching `is_guardrail_reject`, fetch the tenant lakehouse engine (as `exec_sql_read` does), translate + route, return `record_batches_to_json`. Keep watermark headers consistent with `exec_sql_read` (writer → write watermark, else sealed). Apply the freshness gate the same way as `exec_sql_read` (routed reads read Iceberg).
- [ ] **Step 4: Run, expect PASS.** Commit.

### Task 10: PostgREST JSON-path predicates/orders → `->>'key'`

**Files:** Modify `crates/bluedb-rest/src/parse.rs` (accept `col->>key` / `col->key` in filter LHS and `order=`), `crates/bluedb-rest/src/model.rs`/`render.rs` (render a JSON-path column to `col ->> 'key'`, base column via `validate_ident`, key as a safe single-quoted literal). Test: bluedb-rest unit (SQL string assertion) + server integration.

- [ ] **Step 1: Failing test** (bluedb-rest unit) — `parse_query("t","data->>status=eq.active").to_sql_with_params()` renders `... WHERE data ->> 'status' = $1` with `params == [Param::Str("active")]`; `order=data->>n` renders `ORDER BY data ->> 'n' ASC`. Key must be validated (`^[A-Za-z_][A-Za-z0-9_]*$`) to keep injection-proof.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** — extend the column-token parse to split an optional `->>`/`->` + key; carry it on `Filter`/`OrderKey` (e.g. an optional `json_key: Option<(JsonOp, String)>`); render `base ->> 'key'` / `base -> 'key'`. Default (no key) unchanged.
- [ ] **Step 4: Run unit, expect PASS.**
- [ ] **Step 5: Server integration** — `GET /tables/t?data->>status=eq.active` returns matching rows (routes to DataFusion via Task 9; needs P2b's `->>`). Commit phase P2c.

---

## Phase P3 — formatting functions

### Task 11: `to_number(text, fmt)` UDF

**Files:** Create `crates/bluedb-query/src/format_udfs.rs`; register in `query_via_catalog`. Test: unit + gated `df/format.slt`.

- [ ] **Step 1: Failing test** — `SELECT to_number('1,234.50', '9G999D99')` → `1234.50` (Decimal/Float). Start with the common masks input-table uses; document supported mask tokens.
- [ ] **Step 2–4:** Implement a `ScalarUDF` parsing the Postgres numeric mask (`9`,`0`,`D`,`G`,`.`,`,`,`S`) → strip group separators, parse to f64/Decimal. Register. Run, pass.

### Task 12: `format(fmt, …)` UDF

- [ ] **Step 1: Failing test** — `SELECT format('%s has %s', 'a', 'b')` → `'a has b'` (Postgres `%s`/`%I`/`%L` minimal subset; start with `%s`).
- [ ] **Step 2–4:** Implement variadic `ScalarUDF`; register; run, pass.

### Task 13: `to_char(numeric, fmt)` UDF coexisting with temporal `to_char`

- [ ] **Step 1: Failing test** — `SELECT to_char(1234.5, 'FM9,999.00')` → `'1,234.50'`; confirm temporal `to_char(timestamp, fmt)` still works (don't shadow it — register a numeric overload or dispatch on arg type).
- [ ] **Step 2–4:** Implement; ensure signature dispatch (numeric vs temporal). Register. Run, pass. Add `df/format.slt`, run via `DataFusionTester`. Commit phase P3.

---

## Cross-cutting: build & test discipline

- Run tests directly with `2>&1`, never piped through `tail`/`grep` (buffers, looks hung).
- No `cargo fmt` (rustfmt skew reformats ~90 files); match style by hand. `cargo clippy` is fine.
- Keep `record_batches_to_json` and `sql_value_to_json` renderings identical for every type (cross-surface consistency tests).
- Commit message trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. Use `git commit -F -` heredoc.
- "bluecopa" always lowercase in any prose/commit.

## Self-review (spec coverage)

- Spec P1 #2 → Task 1; #3 → Task 2. ✓
- Spec P2a type → Task 3; store → Tasks 4–5; retrieve → Task 6; mirror → Task 6 step 5. ✓ (JsonCatalog added — spec's read-side "schema-aware serializer" requires persisted JSON-ness since normalize erases the type word.)
- Spec P2b → Tasks 7 (+7b fallback). ✓
- Spec P2c classifier/route → Task 9; translation → Tasks 8+10; reads-only → Task 9 (writes untouched). ✓
- Spec P3 → Tasks 11–13. ✓
- Non-goals (native JSONB, JSON-on-GlueSQL, arbitrary-filter writes, `jsonb_path_query`) — not in any task. ✓
