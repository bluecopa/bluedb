# Mirror-enabled testkit mode — design

**Status:** approved (2026-06-19)
**Crates:** `bluedb-py` (testkit), `bluedb-server`, `bluedb-lakehouse`

## Goal

Let `bluedb-testkit` run the embedded server with the **lakehouse mirror on**, backed
by a **local temp object store** (no cloud creds), so a Python test can do
`write → seal → read` against the Iceberg mirror deterministically — and so an
external warehouse stand-in (DuckDB) can read the mirror through `/catalog/v1`.

This unblocks local read-path / warehouse-integration tests, which are impossible
today: the embedded server runs with the mirror off and an in-memory store, so
nothing seals to Iceberg and `/catalog/v1` is empty.

## Current state (what already exists vs. what's missing)

Verified in code during design:

| Capability | State | Location |
|------------|-------|----------|
| Synchronous seal (`seal_all`) | EXISTS, not exposed to Python | `bluedb-server` `AppState::seal_now()` (lib.rs:677) |
| Local filesystem / temp object store for the mirror | EXISTS | `objstore_io::object_store_file_io`; used in `tests/catalog_compat.rs` with `LocalFileSystem` |
| `file://` `loadTable` URIs an external client can open | EXISTS (needs `with_lakehouse_base("file://…")`) | `engine.rs:553` builds metadata location; `catalog_compat.rs` proves DuckDB reads it |
| `X-Bluedb-Watermark` / `X-Bluedb-Min-Watermark` | EXISTS in the shared app | `bluedb-server` lib.rs:936–974 |
| `namespace == tenant`, no sanitization; superuser sees all | EXISTS | `engine::namespace_for_tenant` (engine.rs:30); `catalog.rs` authz |
| DECIMAL / DATE / TIMESTAMP → Iceberg | EXISTS (schema + writer) | `schema.rs:127`, `writer.rs:build_arrow_column` |
| **UUID → Iceberg** | **BROKEN** — schema maps it, writer has no case | `schema.rs:152` maps `Uuid`; `writer.rs build_arrow_column` errors on UUID |
| **Testkit wires a `LakehouseManager`** | **MISSING** — in-memory store, mirror off | `bluedb-py` `embedded.rs:71,84` |
| **`db.seal()` exposed to Python** | **MISSING** | — |

So the build is: wire an fs-backed mirror into the embedded server, expose seal to
Python, fix the UUID writer; everything else is verification tests + a documented
recipe.

## Design

### Architecture

`serve(mirror=True)` wires a **`LocalFileSystem`-backed `LakehouseManager`** into the
embedded server through the **existing `promote()` path** the real server already
uses (`embedded.rs:92` already calls `state.promote()`). OLTP stays on the in-memory
SlateDB store (fast, ephemeral); only the **Iceberg mirror** lands on a temp dir with
a `file://` base, so `loadTable` URIs resolve. Mirroring defaults on for every tenant.
`db.seal()` drives the existing `seal_now()` synchronously from the test thread. No
new production surface.

### Components

**`bluedb-lakehouse`**
- `build_arrow_column`: add a `DataType::Uuid` arm producing a `FixedSizeBinary(16)`
  array from `Value::Uuid(u128)` (big-endian 16 bytes; null-safe). The Iceberg schema
  already maps `Uuid → PrimitiveType::Uuid`, which expects a 16-byte fixed binary.
- `LakehouseConfig` gains `default_mirror_on: bool` (default `false`). When `true`, a
  tenant whose registry has no explicit setting mirrors **on** by default. This is the
  mechanism for "auto-on for every tenant" (covers arbitrary tenants like
  `ws1_sol2_copa_collection_v2` with no per-tenant PRAGMA). The per-tenant engine
  consults this when no registry entry exists. The `PRAGMA lakehouse_mirror` still
  works for per-table opt-out.

**`bluedb-server`**
- `AppState::with_lakehouse_object_store(store)` builder, so the mirror's object store
  can differ from the SlateDB store. (`with_lakehouse_base` already exists; `promote()`
  already builds the manager from these fields.)
- Thread `default_mirror_on` into the `LakehouseConfig` the manager is opened with
  (e.g. `AppState::with_lakehouse_default_on(true)`).
- `seal_now()` already exists — no change.

**`bluedb-py` (testkit)**
- `EmbeddedConfig { mirror: bool }`. PyO3 `serve(mirror=False)` param + a
  `bluedb_mirrored` pytest fixture (function-scoped, like `bluedb`).
- In `EmbeddedServer::start`, when `mirror`:
  - create a `tempfile::TempDir`,
  - build `Arc<LocalFileSystem>` rooted at it,
  - `AppState::new(in_mem, …).with_lakehouse_object_store(fs).with_lakehouse_base("file://{abs}").with_lakehouse_default_on(true)` before `promote()`.
  - Hold the `TempDir` in `EmbeddedServer` so `Drop` cleans it; expose its path as
    `db.warehouse_path`.
- `db.seal()`: at boot the runtime thread sends back its `tokio::runtime::Handle` plus
  an `AppState` clone (the inner is `Arc`, cheap to clone) alongside the bound address.
  `EmbeddedServer::seal()` runs `handle.block_on(state.seal_now())` — synchronous,
  in-process, test-only. (Fallback if cross-thread `block_on` is awkward: a
  one-shot command channel to the runtime thread. No HTTP/PRAGMA surface either way.)

### Data flow (mirror mode)

```
HTTP write ──▶ GlueSQL/SlateDB (in-mem) + CDC log
   db.seal() ─▶ handle.block_on(seal_now) ─▶ manager.seal_all()
                 └─▶ Iceberg snapshots written to the temp fs, sealed watermark advances
   read ─┬─▶ POST /sql  (DataFusion over the fs mirror)
         └─▶ DuckDB iceberg_scan(metadata_location)  via GET /catalog/v1/.../loadTable (file://)
```

`X-Bluedb-Watermark` is stamped on read/write responses; `X-Bluedb-Min-Watermark`
gates the analytical read on the same in-process app.

### API surface (Python)

The existing handle exposes `url(path)`, `base_url`, and `token` (tests drive the
server over HTTP with httpx). Mirror mode **adds two members**: `seal()` and
`warehouse_path`. No `db.sql()` convenience method is introduced — out of scope.

```python
import httpx
from bluedb_testkit import serve

with serve(mirror=True) as db:
    h = {"authorization": f"Bearer {db.token}"}
    httpx.post(db.url("/admin/sql"), headers=h, json={"sql":
        "CREATE TABLE t (id INTEGER PRIMARY KEY, amt DECIMAL(10,2), at TIMESTAMP, uid UUID)"})
    httpx.post(db.url("/tables/t"), headers=h, json={"id": 1, "amt": 9.99})
    db.seal()                                   # synchronous: rows now in the Iceberg mirror
    meta = httpx.get(db.url("/catalog/v1/namespaces/default/tables/t"), headers=h).json()
    assert meta["metadata-location"].startswith("file://")
    # hand meta["metadata-location"] (or db.warehouse_path) to DuckDB iceberg_scan
```

Plus a `bluedb_mirrored` pytest fixture yielding the same handle (mirror on).

## Testing

**Rust (`bluedb-py` `embedded.rs`, `bluedb-lakehouse`):**
- `mirror=True` boots a `LakehouseManager`; after a write + `seal()`, the catalog
  `loadTable` returns a table with a `file://` metadata location.
- **Type-fidelity test:** a table with DECIMAL / DATE / TIMESTAMP / UUID columns seals
  and the `loadTable` schema reports the correct Iceberg types (UUID included).
- Underscore tenant (`ws1_sol2_copa_collection_v2`) maps to that namespace; a superuser
  token lists/loads it.
- Watermark headers present on write/read; `X-Bluedb-Min-Watermark` beyond the sealed
  point is gated.
- UUID `build_arrow_column` unit test (round-trips a `u128` through `FixedSizeBinary(16)`).

**Python (`pytest`):**
- `bluedb_mirrored` fixture: `write → seal → read` via `/sql` returns the rows.
- `loadTable` over `/catalog/v1` returns a `file://` location.
- A DuckDB `iceberg_scan` test, **gated** behind a marker / import guard so the suite
  passes without DuckDB installed.

**Docs:** the DuckDB-through-catalog recipe (the `catalog_compat.rs` `ATTACH`/
`iceberg_scan` + token + endpoint) documented as testkit usage in the testkit README /
mkdocs.

## Scope / non-goals

**In:** the three crate changes, the UUID writer fix, the verification tests, the
documented DuckDB recipe.

**Out:** a cloud-backed testkit mirror (local fs only); a production force-seal
PRAGMA or REST endpoint; lakehouse compaction tuning.

## Mapping to the original asks

1. Mirror-enabled testkit mode + local store → `serve(mirror=True)` / `bluedb_mirrored`, fs-backed, auto-on.
2. Deterministic seal-now → `db.seal()` (sync `seal_now`).
3. Locally-resolvable `file://` `loadTable` URIs → set via `with_lakehouse_base`; verified by test + the DuckDB recipe.
4. DuckDB-through-catalog recipe → documented testkit usage.
5. Watermark surfacing in the embedded server → verified by test (already wired).
6. `namespace == tenant` for arbitrary tenants + superuser → verified by test (already wired, no sanitization).
7. Iceberg type fidelity (DECIMAL/DATE/TIMESTAMP/UUID) → UUID writer fix + fidelity test.
