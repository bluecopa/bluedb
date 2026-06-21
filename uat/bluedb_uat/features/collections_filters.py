"""Collections filter UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id("UAT-COLL-004", "UAT-COLL-010", "UAT-COLL-015")
