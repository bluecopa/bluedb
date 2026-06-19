# bluedb-testkit

In-process, test-only bluedb for Python integration tests. Embeds the real
`bluedb-server` axum app over an in-memory store on an ephemeral loopback port —
no S3, no Docker, no subprocess. **Authenticated by default.**

## Install

`bluedb-testkit` ships as **pre-built abi3 wheels** — no Rust toolchain needed to
install — attached to GitHub Releases (it is **not** on public PyPI). Wheels are
provided for **Linux x86_64**, **Linux aarch64**, and **macOS Apple Silicon**, and
work on CPython **3.9+** (one abi3 wheel per platform).

Install the wheel for your platform from a `testkit-v*` release:

```bash
pip install \
  https://github.com/bluecopa/bluedb/releases/download/testkit-v0.1.0/bluedb_testkit-0.1.0-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
```

Or point pip at a release as a find-links source (so it auto-selects the right
wheel for the host platform):

```bash
pip install bluedb-testkit \
  --find-links https://github.com/bluecopa/bluedb/releases/expanded_assets/testkit-v0.1.0
```

**Cutting a release:** bump `version` in `crates/bluedb-py/{Cargo.toml,pyproject.toml}`,
then push a tag — the `testkit-wheels` CI builds all three platforms and attaches the
wheels:

```bash
git tag testkit-v0.1.0 && git push origin testkit-v0.1.0
```

## Use

```python
from bluedb_testkit import serve
import httpx

with serve() as db:                       # authenticated with a superuser key
    httpx.post(db.url("/sql"), headers=db.headers(),
               json={"sql": "SELECT 1", "params": []})
```

`serve(...)` knobs: `authz` (`True` default superuser / `False` open / `dict` /
raw string), `token`, `admin_sql`, `flush_interval_ms`, `db_path`,
`evidence_signing`, `mirror`. Helpers: `db.base_url`, `db.token`, `db.url(path)`,
`db.headers(token=..., tenant=...)`, and (mirror mode) `db.seal()` /
`db.warehouse_path`.

pytest fixtures (auto-registered): `bluedb` (fresh per test), `bluedb_session`
(shared), and `bluedb_mirrored` (fresh, Iceberg mirror on). Instances are
isolated → safe under `pytest-xdist`.

## Mirror mode (Iceberg + DuckDB)

`serve(mirror=True)` (or the `bluedb_mirrored` fixture) runs the **lakehouse
mirror on**, backed by a **local temp dir** — no cloud creds. Mirroring defaults
on for every tenant, so you just write, `db.seal()` (synchronous), then read the
Iceberg mirror through `/sql` or the read-only Iceberg REST catalog at
`/catalog/v1`. `loadTable`'s `metadata-location` is a `file://` path an external
warehouse can open directly (the warehouse dir is `db.warehouse_path`, cleaned
when the server stops).

```python
import duckdb, httpx
from bluedb_testkit import serve

with serve(mirror=True) as db:
    h = db.headers()
    httpx.post(db.url("/admin/sql"), headers=h,
               json={"sql": "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)"})
    httpx.post(db.url("/tables/docs"), headers=h, json={"id": 1, "body": "hello"})
    db.seal()                                   # rows are now in the Iceberg mirror

    loc = httpx.get(db.url("/catalog/v1/namespaces/default/tables/docs"),
                    headers=h).json()["metadata-location"]   # file:///…
    con = duckdb.connect(); con.execute("INSTALL iceberg"); con.execute("LOAD iceberg")
    print(con.execute("SELECT * FROM iceberg_scan(?)", [loc]).fetchall())
```

Notes: the Iceberg **namespace == tenant** (the default tenant maps to `default`;
set `X-Bluedb-Tenant` via `db.headers(tenant=...)` for others), and a superuser
token lists/loads any namespace. Column types round-trip into Iceberg —
`DECIMAL` / `DATE` / `TIMESTAMP` / `UUID` included. Reads can carry
`X-Bluedb-Min-Watermark` to demand freshness; write/read responses surface
`X-Bluedb-Watermark`.

## Build from source

```bash
pip install maturin
cd crates/bluedb-py
maturin develop            # dev install into the active venv
maturin build --release    # produce a wheel under target/wheels/
```

Requires Python dev headers. Core Rust tests: `cargo test -p bluedb-py`.
Workspace builds are unaffected (PyO3 is behind the optional `python` feature):
`cargo build --workspace` / `cargo test --workspace` need no libpython.

After changing Rust code, re-run `maturin develop` before `pytest` — the installed
Python extension is a compiled artifact and won't pick up Rust changes otherwise.
