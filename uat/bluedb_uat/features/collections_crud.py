"""Collections CRUD UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id(
        "UAT-COLL-001",
        "UAT-COLL-006",
        "UAT-COLL-009",
        "UAT-COLL-014",
        "UAT-COLL-016",
        "UAT-COLL-018",
    )
