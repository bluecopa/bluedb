"""SQL query syntax UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id(
        "UAT-SQL-002",
        "UAT-SQL-005",
        "UAT-SQL-006",
        "UAT-SQL-012",
        "UAT-SQL-013",
    )
