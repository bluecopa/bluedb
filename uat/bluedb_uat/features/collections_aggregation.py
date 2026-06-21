"""Collections aggregation UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id("UAT-COLL-002", "UAT-COLL-007", "UAT-COLL-012", "UAT-COLL-017")
