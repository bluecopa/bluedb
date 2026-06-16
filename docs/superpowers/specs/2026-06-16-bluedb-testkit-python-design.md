# bluedb-testkit — in-process bluedb for Python tests

**Date:** 2026-06-16
**Status:** Design (pending implementation)
**Topic:** Python-embeddable, test-only bluedb instance

## Problem

bluecopa Python services (evidence-graph, lakehouse, Airtable-thing and other
consumers) talk to bluedb over its HTTP surface. Their integration tests need a
**real** bluedb to point at, without standing up S3/Azure/GCS, Docker, a
Postgres lease arbiter, or a separately-managed server binary. Today the only
way to get a live bluedb is to run `bluedb-server` as an external process.

The whole stack already runs fully in-memory — `object_store::memory::InMemory`
plus the in-process `LocalLeaseProvider` — and `build_app(state)` returns a plain
`axum::Router`. So a test instance is mostly composition that already exists; the
missing piece is a way to drive it **from Python, in-process**.

## Goal

A pip-installable Python package, **`bluedb-testkit`**, that embeds the real
`bluedb-server` axum app inside the Python test process and hands tests a live
`base_url` to point their existing HTTP client (httpx/requests) at. No subprocess,
no port juggling, no cloud, fully isolated per instance, **authenticated by
default**.

Non-goals: HA/failover, multi-node, Postgres lease, real object stores,
lakehouse/Iceberg mirroring (off unless explicitly configured), and a bundled
HTTP client (consumers reuse their own).

## Shape

A new workspace crate **`crates/bluedb-py`** built with **PyO3 + maturin**,
producing the wheel **`bluedb-testkit`**. It depends on `bluedb-server` and
exposes a small Python API. App repos add `bluedb-testkit` as a dev dependency.

```
crates/bluedb-py/
  Cargo.toml          # cdylib, pyo3, depends on bluedb-server
  pyproject.toml      # maturin build backend, project name = bluedb-testkit
  src/lib.rs          # PyO3 module: TestServer (start/stop/base_url/token)
  python/bluedb_testkit/
    __init__.py       # serve() context manager, re-export TestServer
    pytest_plugin.py  # `bluedb` / `bluedb_session` fixtures
```

## How it boots (mirrors `bluedb-server::main`, minus cloud/HA)

`TestServer::start()` performs, on a dedicated background thread that owns a
multi-thread tokio runtime:

1. Object store → `Arc::new(object_store::memory::InMemory::new())` — ephemeral,
   one per instance.
2. Lease → `Arc::new(LocalLeaseProvider::new())`; `WriterController::new(node_id,
   lease, Arc::new(SystemClock), ttl, margin)`.
3. `AppState::new(object_store, db_path, writer)` with test-tuned builders:
   - `.with_admin_sql_enabled(true)` (tests routinely need DDL via `/admin/sql`).
   - `.with_authz(...)` — see **Auth** below (default on).
   - `.with_local_signer_for_tests()` available for evidence tests (opt-in flag).
4. `state.promote().await` → become the single in-process writer.
5. `build_app(state)` → `Router`.
6. `TcpListener::bind("127.0.0.1:0")` → `local_addr()` yields a free random port.
7. `axum::serve(listener, app).with_graceful_shutdown(rx)` driven on the runtime.

`start()` blocks the calling Python thread until the listener is bound (handshake
over a channel), then returns with `base_url = "http://127.0.0.1:<port>"`
populated. The HA background tick loop is **omitted** — a single in-process writer
never contends. Because the `WriterController` self-fences (goes Passive) once
within its safety margin of lease expiry, the writer is opened with a **long TTL**
(e.g. 3600 s) so it stays Active for the whole test session without a renewal task.

`flush_interval_ms` is currently read from `BLUEDB_FLUSH_INTERVAL_MS` at
writer-open time (`bluedb-server` `writer_settings`). The testkit sets that env
var for its own process before `promote()` when the knob is provided (default:
leave at the server default of 25 ms). A follow-up may thread it through a builder
to avoid the env dependency, but env is sufficient for v1.

## Lifecycle & isolation

- Each `serve()` / `TestServer` = its own `InMemory` store + its own random port →
  **fully isolated**. Multiple instances coexist, so suites run under
  `pytest-xdist` in parallel with no shared state.
- Teardown: `stop()` fires the graceful-shutdown channel, then joins the runtime
  thread. `__exit__` and `Drop` both call `stop()` (idempotent), so a leaked
  instance still tears down at GC.

## Auth (default = authenticated)

The server's authz is bearer-token → scopes (`data:read`, `data:write`,
`data:query`, `schema:admin`, `superuser`) with optional `tenant:<name>`
bindings; unset = open mode. Per-route required scopes are defined in
`bluedb-server`. The testkit wires this through `AppState::with_authz(...)`
exactly as production does.

**Default behaviour — `serve()` with no args:**

- Auth is **on**. A built-in **superuser** token is configured automatically.
- The token value defaults to a fixed constant (overridable via
  `serve(token="...")`) and is exposed as `db.token`.
- `db.headers()` with no args auto-injects `Authorization: Bearer <db.token>`, so
  ordinary calls "just work" while the real authz middleware runs on every
  request. `db.headers(tenant="acme")` adds `X-Bluedb-Tenant` (a superuser token
  reaches any tenant).

**Opting down / sideways:**

- `serve(authz=False)` → open mode (no enforcement). `db.headers()` returns no
  `Authorization` header.
- `serve(authz={...})` → custom token map (structured dict, recommended), e.g.
  ```python
  serve(authz={
      "admin":  ["superuser"],
      "writer": ["data:read", "data:write", "data:query"],
      "reader": ["data:read"],
      "acme":   ["data:read", "tenant:acme"],   # tenant-bound
  })
  ```
  Each dict key is the literal bearer token; the value list maps to
  `Scope::parse` + `tenant:<name>` → `Authz::insert` / `insert_tenants`. With a
  custom map there is no implicit default token — callers pass explicit
  `db.headers(token="writer", tenant="acme")`.
- `serve(authz="admin=superuser;acme=data:read,tenant:acme")` → raw prod string
  form, parsed by `Authz::parse_env`, for config parity with deployments.

This lets app repos test the real failure paths by default: 401 (missing/bad
token), 403 (insufficient scope), and cross-tenant denial.

## Python surface (thin — bring your own client)

```python
from bluedb_testkit import serve

# default: authenticated with a superuser key
with serve() as db:
    db.base_url                       # "http://127.0.0.1:54321"
    db.token                          # the default superuser bearer token
    db.url("/sql")                    # base_url + path helper
    db.headers()                      # {"Authorization": "Bearer <token>"}
    db.headers(tenant="acme")         # + {"X-Bluedb-Tenant": "acme"}
    # db.headers(token="reader")      # override token — only meaningful when a
    #                                 # custom authz map defines that token

    import httpx
    httpx.post(db.url("/sql"), headers=db.headers(),
               json={"sql": "SELECT 1", "params": []})
```

`serve(...)` keyword knobs (all optional):

| knob               | default            | effect                                            |
|--------------------|--------------------|---------------------------------------------------|
| `authz`            | `True` (superuser) | `True`/dict/str/`False` — see Auth                |
| `token`            | fixed constant     | override the default superuser token value        |
| `admin_sql`        | `True`             | enable `POST /admin/sql`                          |
| `flush_interval_ms`| `25`               | WAL flush interval                                |
| `db_path`          | `"bluedb"`         | SlateDB path/prefix inside the store              |
| `evidence_signing` | `False`            | apply `with_local_signer_for_tests()`             |

`pytest` fixtures shipped in the package:

- `bluedb` — **function-scoped**; fresh `InMemory` per test (full isolation).
- `bluedb_session` — **session-scoped**; one shared instance for suites that don't
  need per-test isolation (faster).

Both yield the same `TestServer` handle (`base_url`, `token`, `url()`,
`headers()`). Fixtures accept the same knobs via an indirect-param or a thin
`serve(**kwargs)` wrapper.

## Components & boundaries

- **`TestServer` (Rust, PyO3 class)** — owns the runtime thread, shutdown channel,
  bound addr, and configured token(s). Methods: `start` (in `__new__`/`serve`),
  `stop`, properties `base_url`, `token`. Single clear purpose: lifecycle of one
  embedded server. Depends only on `bluedb-server`'s public API
  (`build_app`, `AppState`, `authz::Authz`) + `bluedb-ha` (`LocalLeaseProvider`,
  `WriterController`, `SystemClock`).
- **`bluedb_testkit` (Python)** — `serve()` context manager + `db.url`/`db.headers`
  ergonomics + pytest fixtures. No network logic of its own beyond URL/header
  construction.

The boundary is the HTTP surface: the testkit produces a `base_url` + auth
headers and otherwise gets out of the way. Consumers' own client code is
exercised unchanged.

## Testing the testkit

- Rust unit test in `bluedb-py`: start a `TestServer`, `GET /health` over the
  bound port, assert 200; assert two instances get distinct ports and isolated
  data.
- Python tests (run in CI via maturin-built wheel): default-auth happy path
  (`db.headers()` → 200), missing token → 401, wrong scope → 403, cross-tenant →
  403, `authz=False` open mode → 200 without a token, teardown releases the port.

## Packaging & CI

- `pyproject.toml` uses the `maturin` build backend; `Cargo.toml` declares
  `crate-type = ["cdylib"]`. Wheels are platform-specific (Python ABI3 to limit
  the build matrix) for **manylinux** + **macOS** (arm64/x86_64).
- CI builds wheels with `maturin build`; app repos consume them as a dev
  dependency. During local bluedb dev, `pip install -e crates/bluedb-py` (or
  `maturin develop`) works against the workspace.

## Open questions / decisions taken

- **Thin surface, no bundled client** — decided. Consumers reuse httpx/requests.
- **Crate `crates/bluedb-py`, wheel `bluedb-testkit`** — decided (revisit naming
  if it collides with an internal package index name).
- **Default token value** — a fixed, documented constant (e.g.
  `"bluedb-test-superuser"`) so app test configs can hardcode it; overridable.
- **flush_interval via env vs builder** — env for v1; builder is a possible
  follow-up to remove the process-global env write.
