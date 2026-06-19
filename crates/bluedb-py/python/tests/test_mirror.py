"""Mirror-mode testkit: write -> seal -> read through the Iceberg mirror, and
(optionally) read it from DuckDB the way a warehouse would."""
import importlib.util

import httpx


def test_write_seal_read_through_mirror(bluedb_mirrored):
    db = bluedb_mirrored
    h = db.headers()
    httpx.post(
        db.url("/admin/sql"),
        headers=h,
        json={"sql": "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)"},
    ).raise_for_status()
    httpx.post(db.url("/tables/t"), headers=h, json={"id": 1, "body": "hi"}).raise_for_status()

    db.seal()  # synchronous: the row is now in the Iceberg mirror

    meta = httpx.get(db.url("/catalog/v1/namespaces/default/tables/t"), headers=h).json()
    assert meta["metadata-location"].startswith("file://"), meta
    assert db.warehouse_path


def test_duckdb_reads_the_mirror(bluedb_mirrored):
    if importlib.util.find_spec("duckdb") is None:
        import pytest

        pytest.skip("duckdb not installed")
    import duckdb

    db = bluedb_mirrored
    h = db.headers()
    httpx.post(
        db.url("/admin/sql"),
        headers=h,
        json={"sql": "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)"},
    ).raise_for_status()
    httpx.post(db.url("/tables/t"), headers=h, json={"id": 1, "body": "hi"}).raise_for_status()
    db.seal()

    loc = httpx.get(
        db.url("/catalog/v1/namespaces/default/tables/t"), headers=h
    ).json()["metadata-location"]

    con = duckdb.connect()
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    rows = con.execute("SELECT id, body FROM iceberg_scan(?) ORDER BY id", [loc]).fetchall()
    assert rows == [(1, "hi")]
