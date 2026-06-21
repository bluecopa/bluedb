# bluedb UAT Suite

This directory contains a black-box user-acceptance suite for the behavior
published in the bluedb docs. The suite starts `bluedb-server`, drives only HTTP
endpoints, and writes a Markdown report with scenario evidence.

The current full profile contains 1,655 normal UAT scenarios plus 4
lifecycle/security workflows, for 1,659 total black-box scenarios. The original
99 broad workflows are still present in the `core` profile; the larger `full`
profile adds generated, doc-traceable REST table and SQL expression/function
acceptance cases.

GitHub Actions runs `core` on pull requests and `full` on pushes to `dev`. The
generated report is uploaded as a workflow artifact. Manually blessed release
reports are checked in under `uat/reports/blessed/`.

## Layout

`uat/uat_suite.py` is a thin compatibility entrypoint. The implementation lives
under `uat/bluedb_uat/`:

- `cli.py`: argument parsing, profile filtering, report orchestration;
- `registry.py`: deterministic case registration and duplicate-ID checks;
- `legacy.py`: preserved original 99-case harness and shared HTTP/server helpers;
- `features/`: feature modules for broad workflows and generated cases;
- `matrix.py`: helpers for stable generated cases from documented matrices.

## Run

Build the server first:

```bash
cargo build -p bluedb-server
```

Run the full UAT profile and generate a report:

```bash
python3 uat/uat_suite.py --server-bin target/debug/bluedb-server
```

Run the preserved broad-workflow baseline:

```bash
python3 uat/uat_suite.py --server-bin target/debug/bluedb-server --profile core
```

List all registered cases:

```bash
python3 uat/uat_suite.py --list-cases
```

Run a focused feature or single case:

```bash
python3 uat/uat_suite.py --server-bin target/debug/bluedb-server --feature sql.functions
python3 uat/uat_suite.py --server-bin target/debug/bluedb-server --case-id UAT-SQL-FUNC-001
```

By default the runner:

- starts a single local server on `127.0.0.1:18180`;
- starts a restart-durability server on `127.0.0.1:18182`;
- starts an admin-SQL-enabled server on `127.0.0.1:18183`;
- starts an expanded admin-DDL server on `127.0.0.1:18184`;
- starts a separate auth-enabled server on `127.0.0.1:18181`;
- uses temporary local filesystem storage;
- writes a report under `uat/reports/`;
- exits `0` even when product scenarios fail, so the report is still generated.

Use `--fail-on-uat-failure` in CI if UAT failures should fail the job.

After a passing full-profile release run, bless the report with:

```bash
python3 uat/bless_report.py --source uat/reports/latest-uat-report.md
```

This updates:

- `uat/reports/blessed/latest-uat-report.md`;
- a versioned `uat/reports/blessed/bluedb-<git-sha>-full-uat-report.md`;
- `uat/reports/blessed/uat-badge.json` for the README badge.

## Profiles

| Profile | Current size | Purpose |
|---|---:|---|
| `smoke` | P0 subset | Fast lifecycle and critical-surface check |
| `core` | 99 | The original broad black-box UAT workflows |
| `full` | 1,659 | Release/nightly acceptance sweep |
| `negative` | error/guardrail subset | Error-contract and rejection checks |

## CI

`.github/workflows/uat.yml` selects profiles by event:

- pull requests: `core`;
- pushes to `dev`: `full`;
- manual dispatch: selected profile, with optional blessed-report commit for a
  passing `full` run on `dev`.

Transient reports under `uat/reports/` are ignored by Git. Blessed reports under
`uat/reports/blessed/` are intentionally checked in.

## Scope

Covered:

- health and writer status;
- default admin SQL lockout plus explicit opt-in admin SQL scripts,
  transactions, DDL lifecycle, and authorization;
- quickstart schema + SQL behavior;
- local restart durability for acknowledged writes;
- REST schema/table CRUD, pagination count headers, no-match representations,
  JSON-path projection and filtering, freshness headers, unfiltered mutation
  rejection plus explicit bulk-filter mutation checks, and error contracts;
- generated REST table matrices for equality and range filters, pagination, and
  exact count headers;
- structured DDL validation, uniqueness, table drop, nullability/primary-key
  description, index lifecycle, and full-text/trigram index endpoint validation;
- SQL parameterization, `/sql` index-only guardrails, `/query` analytical reads,
  `RETURNING`, lookup-vs-scan routing, freshness watermarks, read-wait PRAGMA
  validation, default row-order checks, secondary-index range/order reads, and
  script guardrails;
- SQL reads: `/sql` point/index lookups, `/query` joins, `USING`, `LEFT JOIN`,
  aggregates, CTEs, windows, subqueries, set operations, ordering, pagination,
  expressions, functions, casts, metadata tables, limitation errors, and JSON
  operators;
- generated SQL expression/function matrices for arithmetic, comparison,
  `BETWEEN`, `IN`, `CASE`, `CAST`, `TRY_CAST`, null handling, math functions,
  string functions, padding/trimming, concatenation, replacement, splitting,
  character code conversion, and `GREATEST`;
- SQL type and value-encoding coverage for booleans, decimals, temporal values,
  and UUIDs;
- JSON paths, JSON containment/path-query behavior on `/query`, `/sql`
  `NO_INDEX` rejection for analytical JSON operators, and tenant isolation;
- SQL full-text/trigram search, query variants, ranked filtered pagination,
  missing-index/join guardrails, `/query` LIKE fallback, and indexed-content
  updates;
- collections insert/find/update/count/index, aggregation, lookup, delete,
  projection, sorting, pagination, upsert, multi-update/delete, uniqueness,
  compound indexes, TTL expiry, multikey membership, comparison/logical filters,
  generated ids, request validation, regex, top-level `$not`, missing-field
  semantics, duplicate `_id` handling, tenant isolation, replacement updates,
  update initializers, scalar multikey behavior, index backfill, idempotent index
  creation, and supported/unsupported update operators, filter operators, index
  paths, collection operations, and aggregation stages;
- collection search mapping, backfill, hits, highlighting, DSL filters,
  exact term/range/exists/match_all queries, pagination, numeric sort,
  phrase queries, bool `should`, projection/source controls, mapping
  replacement, tenant isolation, live update/delete maintenance, and
  unmapped-field/range/sort/unsupported-query errors;
- ledger accounts/transfers, retry semantics, missing lookups, two-phase
  pending/post/void flows, transfer lookup, `/query` projection, pending
  lifecycle error codes, already-voided handling, linked chains, user-data
  round-trips, account constraints, and SQL projection;
- evidence chains, plain-vs-verified behavior, idempotency, proofs, redaction,
  delete guard, append-with-edges, idempotency conflicts, tenant sequence
  isolation, unsigned-signing behavior, named errors, invalid ranges, batch
  append ranges, unknown heads, repeated redaction, plain hard-delete graph
  retraction and no-retraction modes, native graph traversal, typed parallel
  edges, undirected traversal, graph floor/multi-seed behavior, graph
  mutate/delete/no-op semantics, invalid graph request handling, sandbox
  analysis, and graph tenant isolation;
- lakehouse mirror catalog visibility, tenant namespace isolation, and Iceberg
  REST catalog protocol endpoints, opt-out table behavior, and compaction target
  PRAGMA validation plus global-off/per-table opt-in behavior;
- bearer-scope authorization and tenant-bound tokens.

Not covered by this runner:

- multi-node HA/failover;
- Jepsen nemeses;
- real cloud object stores;
- long soak/load testing;
- warehouse engines reading returned Iceberg files.

Those should run as separate acceptance gates once the single-node docs contract
is stable.
