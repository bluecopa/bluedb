"""In-process, test-only bluedb for Python integration tests.

Example:
    from bluedb_testkit import serve

    with serve() as db:                       # authenticated, superuser key
        import httpx
        httpx.post(db.url("/sql"), headers=db.headers(),
                   json={"sql": "SELECT 1", "params": []})
"""
from __future__ import annotations

from contextlib import contextmanager

from ._bluedb_testkit import DEFAULT_TOKEN, TestServer

__all__ = ["serve", "Handle", "TestServer", "DEFAULT_TOKEN"]

# Sentinel so headers(token=None) can mean "send no Authorization header",
# distinct from headers() meaning "use the default token".
_DEFAULT = object()


class Handle:
    """Ergonomic wrapper over a running :class:`TestServer`."""

    def __init__(self, server: TestServer):
        self._server = server

    @property
    def base_url(self) -> str:
        return self._server.base_url

    @property
    def token(self) -> str | None:
        return self._server.token

    def url(self, path: str) -> str:
        return self._server.base_url + path

    def headers(self, token=_DEFAULT, tenant: str | None = None) -> dict:
        """Build request headers. Omit ``token`` to use the default key (if any);
        pass ``token=None`` to send no Authorization header; pass a string to
        override. ``tenant`` adds ``X-Bluedb-Tenant``."""
        h: dict[str, str] = {}
        tok = self._server.token if token is _DEFAULT else token
        if tok is not None:
            h["Authorization"] = f"Bearer {tok}"
        if tenant is not None:
            h["X-Bluedb-Tenant"] = tenant
        return h

    def stop(self) -> None:
        self._server.stop()


@contextmanager
def serve(**kwargs):
    """Start an in-process bluedb and yield a :class:`Handle`. Accepts the same
    keyword args as :class:`TestServer` (``authz``, ``token``, ``admin_sql``,
    ``flush_interval_ms``, ``db_path``, ``evidence_signing``)."""
    server = TestServer(**kwargs)
    handle = Handle(server)
    try:
        yield handle
    finally:
        handle.stop()
