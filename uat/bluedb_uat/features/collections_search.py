"""Collections search UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id(
        "UAT-COLL-SEARCH-001",
        "UAT-COLL-SEARCH-002",
        "UAT-COLL-SEARCH-003",
        "UAT-COLL-SEARCH-004",
        "UAT-COLL-SEARCH-005",
        "UAT-COLL-SEARCH-006",
        "UAT-COLL-SEARCH-007",
        "UAT-COLL-SEARCH-008",
        "UAT-COLL-SEARCH-009",
    )
