# bluedb

An **object-storage-native data substrate**, in Rust. Durable state lives in
object storage (S3 / GCS / Azure Blob) via [**SlateDB**](https://slatedb.io);
compute is **stateless and horizontally scalable**, with single-writer safety and
automatic failover. One HTTP service gives you a PostgREST-style data plane, raw
parameterized SQL, SQL-integrated full-text search, a TigerBeetle-style
double-entry ledger, and a continuous **Apache Iceberg mirror** for warehouse
joins — all over the one substrate.

## 📚 Documentation

**Everything is in the docs → [`docs/`](docs/index.md).** They build into a
site with MkDocs Material:

```bash
pip install -r docs/requirements.txt
mkdocs serve   # http://127.0.0.1:8000
```

Start with the [Quickstart](docs/quickstart.md) and the
[Architecture overview](docs/concepts/architecture.md).

## What it does

| Capability | Docs |
|---|---|
| **SQL** — typed tables with a required `PRIMARY KEY`, online `ALTER` (no row rewrite), transactions, secondary indexes, and a plan-time scan/sort guardrail that keeps every read index-served | [SQL reference](docs/sql/README.md) · [Query guardrail](docs/sql/query-guardrail.md) |
| **Full-text search** — SQL-integrated BM25 (Postgres `@@`/`ts_rank`), read-your-writes, no separate search cluster | [Full-text search](docs/sql/full-text-search.md) |
| **Ledger** — TigerBeetle-style double-entry (typed accounts/transfers, two-phase, balances queryable over SQL) | [Ledger](docs/api/ledger.md) |
| **Lakehouse mirror** — continuous **Apache Iceberg** mirror in the same bucket (full CRUD, seconds-fresh, exactly-once) + a read-only Iceberg REST catalog, so BigQuery/Databricks/Snowflake/DuckDB join bluedb data with no ETL | [Iceberg mirror](docs/lakehouse/iceberg-mirror.md) |
| **Evidence chains** — append-only, **verifiable** log: server-assigned dense sequencing, RFC 6962 Merkle inclusion/consistency proofs (O(log N)), optional KMS-signed digests (ES256), GDPR-grade redaction | [Evidence chains](docs/evidence/chains.md) |
| **Graph store** — native weighted-edge adjacency with traversal (`reachable`, `widest_path`); edges can be appended atomically with evidence events | [Graph store](docs/evidence/graph.md) |
| **High availability** — single-writer lease election + SlateDB epoch fencing, automatic failover (RPO 0 intra-region) | [Active-passive HA](docs/ha/active-passive.md) |
| **Guarantees** — snapshot-isolated transactions, Jepsen-verified consistency | [Consistency](docs/guarantees/consistency.md) · [Jepsen](docs/guarantees/jepsen.md) |
| **HTTP API** — `/tables` CRUD, `/sql`, `/schema/*` DDL, `/ledger/*`, `/evidence/*`, `/graph/*`, `/catalog/v1/*` | [REST API](docs/api/rest.md) · [Configuration](docs/deployment/configuration.md) |

## Repository layout

| Crate | Role |
|---|---|
| `bluedb-storage` | object-store seam (SlateDB + chunked blobs) |
| `bluedb-sql` | SQL engine (GlueSQL over SlateDB): PK'd tables, online ALTER, transactions, indexes, scan guardrail, CDC log |
| `bluedb-fts` | BM25 full-text search (vendored Quickwit read path over object storage) |
| `bluedb-ledger` | TigerBeetle-style double-entry ledger |
| `bluedb-lakehouse` | Apache Iceberg CDC mirror + Iceberg REST catalog |
| `bluedb-evidence` | verifiable evidence chains (RFC 6962 Merkle) + native graph store |
| `bluedb-engine` | facade composing the pillars (incl. SQL-integrated FTS) |
| `bluedb-rest` | PostgREST-style query DSL → SQL |
| `bluedb-server` | the HTTP/REST service (axum) — the integration surface |
| `bluedb-ha` | single-writer lease election + self-fencing |

## Status

M1–M4 core complete (FTS, SQL, engine + HTTP service, single-writer HA), plus the
HTTP/write-path hardening, SQL-integrated FTS, the double-entry ledger, the
schema regime (required PK + online ALTER + scan guardrail), and the Iceberg
lakehouse mirror (all four v1 spike items shipped: multi-tenancy, composite PKs,
schema-evolution reconciliation, incremental compaction). Consistency is
Jepsen-verified on the live cluster against the post-group-commit write path, and
the tri-cloud object store is exercised end-to-end (S3/Azure via emulators, GCS
against real GCS). See [ROADMAP.md](ROADMAP.md). Remaining is the deployment layer
(concrete shared lease store + cross-region orchestration).

## Licensing

bluedb's own code is **Apache-2.0**. `crates/bluedb-fts/vendor/` holds a minimal
copy of Quickwit's `quickwit-directories` read path, copied from
[`quickwit-oss/quickwit@main`](https://github.com/quickwit-oss/quickwit)
(**Apache-2.0**); per-file headers are preserved (see `crates/bluedb-fts/vendor/NOTICE`).
Do **not** depend on the crates.io `quickwit-*` packages — they are stale, AGPL `v0.3.0`.
This is enforced by [`cargo-deny`](deny.toml) in CI: a license **allowlist** (any
non-permissive license, including AGPL, fails the build) plus an explicit ban on
the crates.io `quickwit-*` packages. Run it locally with `cargo deny check`.
