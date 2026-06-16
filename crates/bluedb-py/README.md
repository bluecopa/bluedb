# bluedb-testkit

In-process, test-only bluedb for Python integration tests. Embeds the real
`bluedb-server` axum app over an in-memory store on an ephemeral loopback port —
no S3, no Docker, no subprocess. **Authenticated by default.**

## Install (built wheel)

```bash
pip install bluedb-testkit
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
