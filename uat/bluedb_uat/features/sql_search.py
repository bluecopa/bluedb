"""SQL search UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id(
        "UAT-SEARCH-001",
        "UAT-SEARCH-002",
        "UAT-SEARCH-003",
        "UAT-SEARCH-004",
        "UAT-SEARCH-005",
    )
