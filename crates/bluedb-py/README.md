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
`evidence_signing`. Helpers: `db.base_url`, `db.token`, `db.url(path)`,
`db.headers(token=..., tenant=...)`.

pytest fixtures (auto-registered): `bluedb` (fresh per test) and
`bluedb_session` (shared). Instances are isolated → safe under `pytest-xdist`.

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
