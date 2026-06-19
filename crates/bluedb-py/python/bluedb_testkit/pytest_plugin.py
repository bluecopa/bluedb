"""pytest fixtures shipped with bluedb-testkit (registered via the pytest11
entry point in pyproject.toml).

    def test_query(bluedb):           # fresh, authenticated instance per test
        import httpx
        r = httpx.post(bluedb.url("/sql"), headers=bluedb.headers(),
                       json={"sql": "SELECT 1", "params": []})
        assert r.status_code == 200
"""
import pytest

from . import serve


@pytest.fixture
def bluedb():
    """Function-scoped: a fresh, isolated in-memory bluedb per test."""
    with serve() as db:
        yield db


@pytest.fixture(scope="session")
def bluedb_session():
    """Session-scoped: one shared instance for suites that don't need per-test
    isolation (faster)."""
    with serve() as db:
        yield db


@pytest.fixture
def bluedb_mirrored():
    """Function-scoped: a fresh instance with the Iceberg mirror on, backed by a
    local temp dir. Write, then call ``db.seal()``, then read from the mirror /
    ``/catalog/v1`` (its ``loadTable`` URIs are ``file://`` paths a warehouse can
    open)."""
    with serve(mirror=True) as db:
        yield db
