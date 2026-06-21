"""REST schema UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id(
        "UAT-SCHEMA-001",
        "UAT-SCHEMA-002",
        "UAT-SCHEMA-003",
        "UAT-SCHEMA-004",
        "UAT-SCHEMA-005",
    )
