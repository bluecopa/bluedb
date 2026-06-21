"""SQL guardrails UAT feature module."""

from __future__ import annotations

from ._legacy import cases_by_id


def cases() -> list:
    return cases_by_id("UAT-SQL-001", "UAT-SQL-011", "UAT-SQL-014", "UAT-SQL-015")
