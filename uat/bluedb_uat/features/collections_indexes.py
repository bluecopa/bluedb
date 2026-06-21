"""Collections index UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id("UAT-COLL-003", "UAT-COLL-008", "UAT-COLL-013")
