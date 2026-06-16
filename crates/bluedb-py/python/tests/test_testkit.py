import httpx
import pytest

from bluedb_testkit import DEFAULT_TOKEN, serve


def _create_and_insert(db, headers):
    # Structured DDL (writer-gated) + a parameterized insert via /sql.
    r = httpx.post(
        db.url("/schema/tables"),
        headers=headers,
        json={"name": "t", "columns": [
            {"name": "id", "type": "INTEGER", "primary_key": True},
            {"name": "v", "type": "TEXT"},
        ]},
    )
    assert r.status_code in (200, 201), r.text
    r = httpx.post(
        db.url("/sql"),
        headers=headers,
        json={"sql": "INSERT INTO t (id, v) VALUES ($1, $2)", "params": [1, "hello"]},
    )
    assert r.status_code == 200, r.text


def test_default_auth_happy_path(bluedb):
    assert bluedb.token == DEFAULT_TOKEN
    _create_and_insert(bluedb, bluedb.headers())
    r = httpx.post(
        bluedb.url("/sql"),
        headers=bluedb.headers(),
        json={"sql": "SELECT v FROM t WHERE id = $1", "params": [1]},
    )
    assert r.status_code == 200, r.text
    assert "hello" in r.text


def test_missing_token_is_401(bluedb):
    r = httpx.get(bluedb.url("/tables/t"))  # no Authorization header
    assert r.status_code == 401


def test_wrong_scope_is_403():
    with serve(authz={"ro": ["data:read"]}) as db:
        # data:read token cannot create a table (needs schema:admin) -> 403
        r = httpx.post(
            db.url("/schema/tables"),
            headers=db.headers(token="ro"),
            json={"name": "x", "columns": [{"name": "id", "type": "INTEGER", "primary_key": True}]},
        )
        assert r.status_code == 403


def test_cross_tenant_is_403():
    # A non-superuser, tenant-bound token reaches only its own tenant.
    with serve(authz={"acme": ["data:read", "tenant:acme"]}) as db:
        ok = httpx.get(db.url("/tables/whatever"), headers=db.headers(token="acme", tenant="acme"))
        assert ok.status_code != 403  # allowed for its own tenant (404/200 fine)
        denied = httpx.get(db.url("/tables/whatever"), headers=db.headers(token="acme", tenant="globex"))
        assert denied.status_code == 403


def test_open_mode_needs_no_token():
    with serve(authz=False) as db:
        assert db.token is None
        r = httpx.get(db.url("/tables/whatever"), headers=db.headers())
        assert r.status_code != 401  # open mode allows unauthenticated


def test_instances_are_isolated():
    with serve() as a, serve() as b:
        assert a.base_url != b.base_url
