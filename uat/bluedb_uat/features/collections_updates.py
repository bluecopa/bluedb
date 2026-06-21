"""Collections update UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id("UAT-COLL-005", "UAT-COLL-011")
